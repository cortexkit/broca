// Session retirement must release both role-plane caches, not just tool routes.

struct RetirementRoleProvider {
    binds: Arc<std::sync::Mutex<std::collections::BTreeMap<String, usize>>>,
}

#[async_trait::async_trait]
impl subc_client_rs::ModuleHandler for RetirementRoleProvider {
    async fn on_bind(
        &self,
        request: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        if let Some(roles) = serde_json::to_value(&request.role_versions).unwrap().as_object() {
            for role in roles.keys() {
                *self.binds.lock().unwrap().entry(role.clone()).or_default() += 1;
            }
        }
        subc_client_rs::BindDecision::accept()
    }

    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let answer = match request["method"].as_str() {
            Some("role.describe") => serde_json::json!({
                "majors":[
                    {"version":"step-transform-provider/v1", "ops":["role.describe","transform.declare","transform.hook"], "stability":"alpha"},
                    {"version":"compaction-provider/v1", "ops":["role.describe","compaction.setup","view.get","compaction.ride"], "stability":"alpha"}
                ],
                "implementation_version":"0.0.0-test", "capabilities":[]
            }),
            Some("transform.hook") => serde_json::json!({"answer":"pass"}),
            Some("compaction.setup") => serde_json::json!({
                "answer":"ready", "request_id":request["params"]["request_id"],
                "initial":{"compaction_id":"retirement", "version":1,
                    "range":{"from":0,"to":0}, "replacement":[]}
            }),
            _ => panic!("unexpected role retirement request: {request}"),
        };
        subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&answer).unwrap())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn session_close_releases_hook_and_compaction_routes() {
    use subc_protocol::manifest::{Concurrency, ModuleManifest, ProviderRole};
    let source = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
    if !source.exists() {
        assert!(
            std::env::var_os("BROCA_REQUIRE_SUBC").is_none(),
            "real daemon required: {} is missing",
            source.display()
        );
        println!("SKIP session_close_releases_hook_and_compaction_routes: {} missing", source.display());
        return;
    }
    let root = broca_test_support::scratch_root().join("role-plane-retirement");
    let runtime = root.join("runtime");
    let config = root.join("config/cortexkit/subc.jsonc");
    std::fs::create_dir_all(&runtime).unwrap();
    std::fs::create_dir_all(config.parent().unwrap()).unwrap();
    std::fs::write(&config, r#"{"version":1,"modules":{}}"#).unwrap();
    // The rig helper sends the daemon's output to log files under `root`, and RigChild kills and
    // reaps it on every exit path and prints those logs if the test fails.
    let daemon_bin = broca_test_support::ckdev_binary(&source, &root);
    let daemon = broca_test_support::retry_executable_file_busy(|| {
        broca_test_support::tokio_rig_command(&daemon_bin, &root, "daemon")?
        .env("XDG_CONFIG_HOME", root.join("config"))
        .env("XDG_RUNTIME_DIR", &runtime)
        .env("XDG_DATA_HOME", root.join("data"))
        .env("SUBC_PORT", "0")
        .env_remove("SUBC_MODULE_ID")
        .env_remove("SUBC_LAUNCH_NONCE")
        .env_remove("SUBC_LAUNCH_NONCE_FD")
        .kill_on_drop(true)
        .spawn()
    })
    .unwrap();
    let daemon = broca_test_support::RigChild::new(daemon, &root, "daemon");
    let connection = runtime.join("subc-connection.json");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(20);
    while !connection.exists() {
        assert!(tokio::time::Instant::now() < deadline, "daemon did not publish connection file");
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
    let binds = Arc::new(std::sync::Mutex::new(std::collections::BTreeMap::new()));
    let (_, serving) = subc_client_rs::serve_with_handle(
        &connection,
        ModuleManifest::builder("retirement-provider", env!("CARGO_PKG_VERSION"))
            .provides(vec![ProviderRole::ToolProvider {
                tools:vec![], identity_scope:vec![], concurrency:Concurrency::ModuleManaged,
                emits_push:false, sub_supervises:false,
            }]).build(),
        RetirementRoleProvider { binds:binds.clone() },
    ).await.unwrap();
    let serving = tokio::spawn(serving);
    let conn = Arc::new(broca_subc::SubcConnection::connect(&connection).await.unwrap());
    let hooks = Arc::new(broca_subc::SubcHookPlane::new(conn.clone()));
    let compaction = Arc::new(broca_subc::SubcCompactionPlane::new(conn.clone()));
    let executor = LiveLoopExecutor::new(
        Arc::new(FileWalStore::new(root.join("wal"))),
        Arc::new(broca_store::SqliteStore::open(root.join("store.db")).unwrap()),
        Arc::new(SubcToolPlane::new(broca_subc::SubcConnection::connect(&connection).await.unwrap(), "unused-tools".to_owned())),
        None,
    ).with_hook_plane(hooks.clone()).with_compaction_plane(compaction.clone());
    assert!(executor.hook_plane().is_some());
    assert!(executor.compaction_plane().is_some());
    let bind = BindIdentity::new(&root, "caller", "retirement-session");
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../broca-subc/tests/fixtures/step-transform/hook-requests.json"
    )).unwrap();
    let mut call = fixture["requests"][0]["request"].clone();
    call["session"] = serde_json::json!(bind.session);
    call["harness"] = serde_json::json!(bind.harness);
    for pass in 0..2 {
        hooks.hook_with_deadline(
            "retirement-provider", &bind, serde_json::from_value(call.clone()).unwrap(),
            tokio::time::Instant::now() + std::time::Duration::from_secs(2),
        ).await.unwrap();
        use broca_core::CompactionPlane;
        compaction.setup(
            "retirement-provider", &bind,
            broca_core::compaction_provider::setup::SetupRequest::new(
                bind.session.clone(), bind.harness.clone(), format!("setup-{pass}"),
                serde_json::Map::new(), "model", 0,
            ),
        ).await.unwrap();
        assert_eq!(binds.lock().unwrap().get("step-transform-provider").copied(), Some(pass+1), "hook channel must reopen after session retirement");
        assert_eq!(binds.lock().unwrap().get("compaction-provider").copied(), Some(pass+1), "compaction channel must reopen after session retirement");
        executor.on_session_close(&bind).await;
    }
    serving.abort();
    drop(daemon);
}
