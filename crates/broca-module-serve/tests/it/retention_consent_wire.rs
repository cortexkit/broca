//! Retention withdrawals use the saved question owner's scope after transcript deletion.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broca_core::tool_plane::ApprovalWithdrawal;
use broca_module::retention_withdrawals::{
    RetentionWithdrawalAdapter, ScopedRetentionWithdrawalAdapter,
};
use broca_protocol::BindIdentity;
use broca_store::retention_withdrawals::{
    RetentionQuestionId, RetentionScopeIdentity, RetentionWithdrawal,
};
use broca_subc::{RouteOpenRetry, ScopedConsentPlane, SubcConnection};
use serde_json::{json, Value};
use subc_client_rs::{BindDecision, HandlerOutcome, ModuleHandler};
use subc_control::ConsumerIdentity;
use subc_protocol::manifest::{
    Concurrency, ManagementOperation, ManagementOperationKind, ModuleManifest, ProviderRole,
};

const OWNER: &str = "retention-owner";
const MARKER: &str = "RETENTION-MODULE-CONTENT-SENTINEL-59137";

fn fixture(name: &str) -> Value {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-subc/tests/fixtures/consent/upstream/consent-requester-v1");
    serde_json::from_slice(&std::fs::read(root.join(name)).unwrap()).unwrap()
}

#[derive(Default)]
struct Captured {
    scopes: Mutex<Vec<Value>>,
    bodies: Mutex<Vec<Vec<u8>>>,
}

struct Cingulate(Arc<Captured>);

#[async_trait::async_trait]
impl ModuleHandler for Cingulate {
    async fn on_bind(&self, req: &subc_client_rs::RouteBindRequest) -> BindDecision {
        assert_eq!(
            req.principal,
            Some(subc_protocol::Principal::Reserved {
                module_id: "broca".into()
            })
        );
        self.0
            .scopes
            .lock()
            .unwrap()
            .push(serde_json::to_value(&req.scope).unwrap());
        BindDecision::accept()
    }

    async fn handle(&self, _: subc_client_rs::RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        self.0.bodies.lock().unwrap().push(body);
        HandlerOutcome::Response(serde_json::to_vec(&fixture("withdraw-reply.json")).unwrap())
    }
}

struct Rig {
    daemon: tokio::process::Child,
    serving: tokio::task::JoinHandle<()>,
    dir: PathBuf,
    conn: Arc<SubcConnection>,
    captured: Arc<Captured>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        self.serving.abort();
        let _ = self.daemon.start_kill();
    }
}

impl Rig {
    async fn start() -> Option<Self> {
        let subc = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
        let stub = broca_test_support::subc_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
        if !subc.exists() || !stub.exists() {
            assert!(
                std::env::var_os("BROCA_REQUIRE_SUBC").is_none(),
                "retention consent requires ck-subc and fake-aft-stub: {subc:?}, {stub:?}"
            );
            println!("SKIP retention_consent_wire: missing daemon fixtures; set BROCA_REQUIRE_SUBC=1 to require them");
            return None;
        }
        let dir = broca_test_support::scratch_root()
            .join(format!("retention-consent-wire-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir = dir.canonicalize().unwrap();
        let config = dir.join("config/cortexkit/subc.jsonc");
        let runtime = dir.join("runtime");
        std::fs::create_dir_all(config.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&runtime).unwrap();
        let scopes = dir.join("scopes.json");
        std::fs::write(
            &scopes,
            json!([{"ref":"retention-head", "scope_epoch":7, "kind":"head", "attributes":{},
                "carriers":[{"principal":{"kind":"reserved","module_id":"broca"}}]}])
            .to_string(),
        )
        .unwrap();
        std::fs::write(
            config,
            json!({"version":1, "scope_authority_owners":[OWNER], "modules":{
                OWNER:{"program":stub, "args":[], "enabled":true, "reserved":true,
                    "env":{"FAKE_AFT_MODULE_ID":OWNER, "FAKE_AFT_SCOPE_SYNC_PATH":scopes,
                        "FAKE_AFT_EVENTS_PATH":dir.join("owner-events.jsonl")}},
                "broca":{"program":stub, "args":[], "enabled":true, "reserved":true,
                    "env":{"FAKE_AFT_MODULE_ID":"broca", "FAKE_AFT_LAUNCH_NONCE_PATH":dir.join("broca.nonce")}}
            }})
            .to_string(),
        )
        .unwrap();
        let cli = subc.with_file_name(format!("ck{}", std::env::consts::EXE_SUFFIX));
        let (subc, _) = broca_test_support::ckdev_binary_with_companion(subc, cli, &dir);
        let daemon = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(&subc, &dir, "daemon")?
                .env("HOME", &dir)
                .env("XDG_CONFIG_HOME", dir.join("config"))
                .env("XDG_RUNTIME_DIR", &runtime)
                .env("XDG_DATA_HOME", dir.join("data"))
                .env("SUBC_PORT", "0")
                .env_remove("SUBC_MODULE_ID")
                .env_remove("SUBC_LAUNCH_NONCE")
                .env_remove("SUBC_LAUNCH_NONCE_FD")
                .kill_on_drop(true)
                .spawn()
        })
        .unwrap();
        let connection = runtime.join("subc-connection.json");
        loop {
            let synced = std::fs::read_to_string(dir.join("owner-events.jsonl"))
                .unwrap_or_default()
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .any(|event| {
                    event["kind"] == "scope_sync_response"
                        && event["body_json"]["results"]
                            .as_array()
                            .is_some_and(|results| {
                                !results.is_empty()
                                    && results.iter().all(|entry| entry["outcome"] != "refused")
                            })
                });
            if synced && connection.exists() && dir.join("broca.nonce").exists() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let nonce = std::fs::read_to_string(dir.join("broca.nonce")).unwrap();
        let conn = Arc::new(
            SubcConnection::connect_with_consumer_identity(
                &connection,
                Some(ConsumerIdentity {
                    module_id: "broca".into(),
                    launch_nonce: nonce.trim().into(),
                }),
            )
            .await
            .unwrap(),
        );
        let manifest = ModuleManifest::builder("cingulate", env!("CARGO_PKG_VERSION"))
            .provides(vec![ProviderRole::ManagementSurface {
                operations: vec![ManagementOperation {
                    name: "consent.withdraw".into(),
                    kind: ManagementOperationKind::Mutate,
                    description: None,
                }],
                config_schema: json!({}),
                observability: vec![],
                identity_scope: vec![],
                concurrency: Concurrency::ModuleManaged,
            }])
            .build();
        let captured = Arc::new(Captured::default());
        let (_, serving) =
            subc_client_rs::serve_with_handle(&connection, manifest, Cingulate(captured.clone()))
                .await
                .unwrap();
        Some(Self {
            daemon,
            serving: tokio::spawn(async move { serving.await.unwrap() }),
            dir,
            conn,
            captured,
        })
    }
}

#[tokio::test]
async fn retained_consent_withdrawal_uses_frozen_scope_and_content_free_body() {
    tokio::time::timeout(Duration::from_secs(45), async {
        let Some(rig) = Rig::start().await else {
            return;
        };
        let scope = serde_json::from_value(json!({
            "owner":{"kind":"reserved", "module_id":OWNER},
            "ref":"retention-head", "scope_epoch":7
        }))
        .unwrap();
        // Expiry deletes prompts and arguments but keeps this identity-only debt.
        // Send that debt through serve's production adapter to check the actual wire body.
        let row = RetentionWithdrawal {
            lineage_id: "00000000-0000-0000-0000-000000000001".into(),
            session: BindIdentity::new(&rig.dir, "retention", "session"),
            call_key: "00000000-0000-0000-0000-000000000001:42".into(),
            scope: RetentionScopeIdentity::from_scope(&scope),
            question: RetentionQuestionId::Known("el_2".into()),
            provider_id: "cingulate".into(),
            provider_incarnation: None,
            expires_at_ms: u64::MAX,
        };
        let plane = Arc::new(ScopedConsentPlane::new(
            rig.conn.clone(),
            RouteOpenRetry::within(Duration::from_secs(5), Duration::from_millis(20)),
        ));
        let adapter = ScopedRetentionWithdrawalAdapter(plane);
        assert!(matches!(
            adapter.withdraw(&row, "el_2").await,
            ApprovalWithdrawal::Acknowledged
        ));
        let bodies = rig.captured.bodies.lock().unwrap();
        assert_eq!(bodies.len(), 1, "only one consent operation is sent");
        assert!(
            !bodies[0]
                .windows(MARKER.len())
                .any(|window| window == MARKER.as_bytes()),
            "private retention marker in consent.withdraw body"
        );
        assert_eq!(
            bodies[0],
            serde_json::to_vec(&fixture("withdraw-request.json")).unwrap(),
            "retention must send only the pinned consent withdrawal fields"
        );
        let scopes = rig.captured.scopes.lock().unwrap();
        assert_eq!(scopes.len(), 1);
        assert_eq!(
            scopes[0]["owner"],
            json!({"kind":"reserved", "module_id":OWNER})
        );
        assert_eq!(scopes[0]["ref"], "retention-head");
        assert_eq!(scopes[0]["scope_epoch"], 7);
    })
    .await
    .expect("retention consent wire test exceeded its liveness deadline");
}
