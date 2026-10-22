//! The `classifier/v1` conformance suite (`cortexkit-role-classifier-conformance`) against the
//! real `ck-broca`, through a real daemon route, with the role's capability golden captured
//! from the same route.
//!
//! The suite drives broca's management route opened the way an outside caller opens it
//! (`SubcConnection::route_open_classifier_within`, declaring `role_versions
//! {"classifier": "v1"}`). The provider is a stand-in on a loopback port that broca reaches
//! through its ordinary `providers.typesafe.base_url` override; it counts and scripts calls by
//! the JCS of the item `state` it receives, as the suite's `ClassifierSubject` requires, and can
//! hold a call until the suite releases it. The catalog is broca's last-known-good fusiform
//! answer seeded into its state root: Jev as a priced decision model, a chat row, and a row that
//! states no kind.
//!
//! Needs the daemon binary (`SUBC_CORE_BIN`, or `.test-rigs/ck-subc`). Without it the test
//! skips, loudly; `BROCA_REQUIRE_SUBC=1` turns that into a failure.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_subc::{RouteHandle, RouteOpenRetry, SubcConnection};
use cortexkit_role_classifier_conformance::{
    run_suite, state_key, ClassifierRoute, ClassifierSubject, Reply, RouteFailure, Scripted,
    ServedModel, SuiteVerdict, CHECKS,
};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;

const JEV: &str = "typesafe/jev-1.13.0";
const CHAT_ROW: &str = "openai/conformance-chat";
const KINDLESS_ROW: &str = "openai/conformance-kindless";

fn require_subc() -> bool {
    std::env::var_os("BROCA_REQUIRE_SUBC").is_some()
}

// ---- the stand-in provider ----------------------------------------------------------------

#[derive(Default)]
struct StandIn {
    scripts: Mutex<HashMap<String, Scripted>>,
    calls: Mutex<HashMap<String, usize>>,
    /// Keys whose next call waits; the notify releases it.
    hold_next: Mutex<HashMap<String, Arc<Notify>>>,
    held: Mutex<HashMap<String, Arc<Notify>>>,
}

async fn start_stand_in(state: Arc<StandIn>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let state = state.clone();
            tokio::spawn(async move {
                let _ = serve_one(stream, state).await;
            });
        }
    });
    format!("http://{address}/v1")
}

/// Read one HTTP request, answer it, and close the connection.
async fn serve_one(mut stream: tokio::net::TcpStream, state: Arc<StandIn>) -> std::io::Result<()> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 16384];
    let body_start = loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&chunk[..n]);
        if let Some(at) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            break at + 4;
        }
    };
    let head = String::from_utf8_lossy(&buffer[..body_start]).to_ascii_lowercase();
    let length: usize = head
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0);
    while buffer.len() < body_start + length {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..n]);
    }
    let request: Value = serde_json::from_slice(&buffer[body_start..]).unwrap_or(Value::Null);
    let key = state_key(&request["state"]);
    *state.calls.lock().unwrap().entry(key.clone()).or_default() += 1;
    let hold = state.hold_next.lock().unwrap().remove(&key);
    if let Some(release) = hold {
        let released = release.notified();
        state
            .held
            .lock()
            .unwrap()
            .insert(key.clone(), release.clone());
        released.await;
        state.held.lock().unwrap().remove(&key);
    }
    let scripted = state.scripts.lock().unwrap().get(&key).cloned();
    let response = match scripted {
        Some(Scripted::Answer(body)) => format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        ),
        Some(Scripted::Status {
            status,
            retry_after_ms,
        }) => {
            let retry = retry_after_ms.map_or(String::new(), |ms| {
                format!("retry-after: {}\r\n", ms.div_ceil(1000))
            });
            format!("HTTP/1.1 {status} Scripted\r\ncontent-length: 0\r\n{retry}connection: close\r\n\r\n")
        }
        None => "HTTP/1.1 500 Unscripted\r\ncontent-length: 0\r\nconnection: close\r\n\r\n"
            .to_owned(),
    };
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

// ---- the rig --------------------------------------------------------------------------------

fn seed_catalog(state_root: &Path) {
    let dir = state_root.join("catalog");
    std::fs::create_dir_all(&dir).unwrap();
    let priced = |units: u64| {
        json!({"currency": "USD", "exponent": 9, "state": "priced", "units": units,
               "unit_provenance": {"kind": "stated"}})
    };
    let lkg = json!({
        "catalog_version": 1,
        "resolved_at_ms": 1,
        "source": "models.dev",
        "models": {
            JEV: {
                "existence": "present",
                "model.kind": "decision",
                "rate.input": priced(42_000_000),
                "rate.output": {"state": "stated_zero"},
            },
            CHAT_ROW: {
                "existence": "present",
                "model.kind": "chat",
                "limit.context": 128000,
                "capability.tool_call": true,
            },
            KINDLESS_ROW: {
                "existence": "present",
                "limit.context": 128000,
                "capability.tool_call": true,
            },
        },
    });
    std::fs::write(
        dir.join("fusiform-lkg.json"),
        serde_json::to_vec_pretty(&lkg).unwrap(),
    )
    .unwrap();
}

struct Rig {
    _daemon: broca_test_support::RigChild,
    connection_file: PathBuf,
    project_root: PathBuf,
    egress_host: String,
    state: Arc<StandIn>,
    state_root: PathBuf,
}

async fn start_rig(name: &str) -> Option<Rig> {
    let subc = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
    if !subc.exists() {
        assert!(
            !require_subc(),
            "BROCA_REQUIRE_SUBC=1 but the daemon binary is missing: {}",
            subc.display()
        );
        println!("SKIP {name}: {} missing", subc.display());
        return None;
    }
    let root = broca_test_support::scratch_root().join(format!(
        "classifier-conformance-{name}-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    let (config_home, runtime_dir, data_home, home) = (
        root.join("config"),
        root.join("runtime"),
        root.join("data-home"),
        root.join("home"),
    );
    for dir in [&config_home, &runtime_dir, &data_home, &home] {
        std::fs::create_dir_all(dir).unwrap();
    }
    std::fs::create_dir_all(config_home.join("cortexkit")).unwrap();
    let project_root = root.join("project");
    std::fs::create_dir_all(&project_root).unwrap();
    let state_root = root.join("broca-state");
    seed_catalog(&state_root);
    let state = Arc::new(StandIn::default());
    let stand_in = start_stand_in(state.clone()).await;
    let egress_host = url::Url::parse(&stand_in)
        .unwrap()
        .host_str()
        .unwrap()
        .to_owned();
    let broca_config = config_home.join("cortexkit/broca.jsonc");
    std::fs::write(
        &broca_config,
        serde_json::to_vec(&json!({"providers": {"typesafe": {"base_url": stand_in}}})).unwrap(),
    )
    .unwrap();
    let connection_file = runtime_dir.join("subc-connection.json");
    let config = json!({
        "version": 1,
        "modules": {
            "broca": {
                "program": broca_test_support::ckdev_binary(
                    Path::new(env!("CARGO_BIN_EXE_ck-broca")),
                    broca_test_support::scratch_root(),
                ),
                "args": [],
                "env": {
                    "BROCA_STATE_ROOT": state_root,
                    "XDG_DATA_HOME": data_home,
                    "BROCA_CONFIG_PATH": broca_config,
                    // The mock provider path: no vault is opened, so the stand-in is called
                    // without a credential, as every run is on this path.
                    "BROCA_PROVIDER_BASE_URL": stand_in,
                    "CK_LOG": "info",
                },
                "enabled": true
            }
        }
    });
    std::fs::write(
        config_home.join("cortexkit/subc.jsonc"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    let daemon = broca_test_support::retry_executable_file_busy(|| {
        broca_test_support::tokio_rig_command(
            broca_test_support::ckdev_binary(&subc, broca_test_support::scratch_root()),
            &root,
            "daemon",
        )?
        .env("XDG_CONFIG_HOME", &config_home)
        .env("XDG_RUNTIME_DIR", &runtime_dir)
        .env("XDG_DATA_HOME", &data_home)
        .env("HOME", &home)
        .env_remove("BROCA_STATE_ROOT")
        .env("SUBC_PORT", "0")
        .env_remove("SUBC_LAUNCH_NONCE")
        .env_remove("SUBC_LAUNCH_NONCE_FD")
        .env_remove("SUBC_MODULE_ID")
        .kill_on_drop(true)
        .spawn()
    })
    .unwrap();
    let daemon = broca_test_support::RigChild::new(daemon, &root, "daemon");
    broca_test_support::wait_until("the daemon's connection file", || {
        let published = connection_file.exists();
        async move {
            published
                .then_some(())
                .ok_or_else(|| "not published yet".to_owned())
        }
    })
    .await;
    Some(Rig {
        _daemon: daemon,
        connection_file,
        project_root: project_root.canonicalize().unwrap(),
        egress_host,
        state,
        state_root,
    })
}

/// A classifier route to the real `ck-broca`, opened as an outside caller opens it.
struct BrocaClassifierRoute {
    connection: Arc<SubcConnection>,
    route: RouteHandle,
}

impl Rig {
    async fn open(&self) -> Result<BrocaClassifierRoute, String> {
        let connection = Arc::new(
            SubcConnection::connect(&self.connection_file)
                .await
                .map_err(|e| format!("connect: {e}"))?,
        );
        let identity =
            broca_protocol::BindIdentity::new(self.project_root.clone(), "core", "classifier");
        let route = connection
            .route_open_classifier_within(
                "broca",
                &identity,
                RouteOpenRetry::within(Duration::from_secs(60), Duration::from_millis(200)),
            )
            .await
            .map_err(|e| format!("route.open: {e}"))?;
        Ok(BrocaClassifierRoute { connection, route })
    }
}

#[async_trait]
impl ClassifierRoute for BrocaClassifierRoute {
    async fn request(&self, method: &str, params: &str) -> Result<Reply, RouteFailure> {
        let body = format!(
            "{{\"method\":{},\"params\":{params}}}",
            serde_json::to_string(method).unwrap()
        );
        let terminal = self
            .connection
            .route_request_terminal_with_deadline(
                &self.route,
                body.into_bytes(),
                Duration::from_secs(120),
            )
            .await
            .map_err(|e| RouteFailure::new(e.to_string()))?;
        match terminal {
            Ok(response) => {
                #[derive(serde::Deserialize)]
                struct Envelope<'a> {
                    #[serde(borrow)]
                    result: &'a serde_json::value::RawValue,
                }
                let envelope: Envelope<'_> = serde_json::from_slice(&response)
                    .map_err(|e| RouteFailure::new(format!("undecodable response: {e}")))?;
                Ok(Reply::Response(envelope.result.get().to_owned()))
            }
            Err(error) => Ok(Reply::Error(serde_json::from_slice(&error).map_err(
                |e| RouteFailure::new(format!("undecodable error body: {e}")),
            )?)),
        }
    }
}

struct Subject<'a> {
    rig: &'a Rig,
}

#[async_trait]
impl ClassifierSubject for Subject<'_> {
    type Route = BrocaClassifierRoute;

    async fn route(&self) -> Result<Self::Route, String> {
        self.rig.open().await
    }

    fn served_models(&self) -> Vec<ServedModel> {
        vec![ServedModel {
            model: JEV.into(),
            priced: true,
            images_max: None,
            context_tokens: None,
            egress_host: self.rig.egress_host.clone(),
        }]
    }

    fn non_classifier_model(&self) -> Option<String> {
        Some(CHAT_ROW.into())
    }

    fn kindless_model(&self) -> Option<String> {
        Some(KINDLESS_ROW.into())
    }

    async fn script(&self, key: &str, scripted: Scripted) {
        self.rig
            .state
            .scripts
            .lock()
            .unwrap()
            .insert(key.to_owned(), scripted);
    }

    async fn calls(&self, key: &str) -> usize {
        self.rig
            .state
            .calls
            .lock()
            .unwrap()
            .get(key)
            .copied()
            .unwrap_or(0)
    }

    fn can_hold_calls(&self) -> bool {
        true
    }

    async fn hold_next(&self, key: &str) {
        self.rig
            .state
            .hold_next
            .lock()
            .unwrap()
            .insert(key.to_owned(), Arc::new(Notify::new()));
    }

    async fn is_held(&self, key: &str) -> bool {
        self.rig.state.held.lock().unwrap().contains_key(key)
    }

    async fn release(&self, key: &str) {
        let held = self.rig.state.held.lock().unwrap().get(key).cloned();
        if let Some(release) = held {
            release.notify_one();
        }
        self.rig.state.hold_next.lock().unwrap().remove(key);
    }

    async fn pause(&self) {
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn classifier_conformance_suite_passes_against_the_real_serve_path() {
    let Some(rig) = start_rig("suite").await else {
        return;
    };
    let report = run_suite(&Subject { rig: &rig }).await;
    for check in &report.checks {
        println!("{}: {:?}", check.check, check.outcome);
    }
    assert_eq!(report.checks.len(), CHECKS.len());
    assert_eq!(
        report.verdict,
        SuiteVerdict::Passed,
        "every check must run and pass: {:#?}",
        report.checks
    );
    // The batch store is broca's own file, beside (never inside) the run index.
    assert!(rig.state_root.join("system-one.db").exists());
}

/// The `classifier/v1` `role.describe` bytes, as broca serves them on a classifier route, are
/// the role's capability golden. Regenerate with `BROCA_UPDATE_GOLDEN=1`. The stand-in's
/// loopback host is the egress host here, since that is where this rig sends item text.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classifier_role_describe_matches_its_golden_through_the_real_serve_path() {
    let Some(rig) = start_rig("golden").await else {
        return;
    };
    let route = rig.open().await.expect("the classifier route opens");
    let Reply::Response(served) = route.request("role.describe", "{}").await.unwrap() else {
        panic!("role.describe was refused");
    };
    let golden = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/classifier_role_describe.json");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&golden, &served).unwrap();
    }
    let expected = std::fs::read_to_string(&golden).expect("the golden exists");
    assert_eq!(
        served,
        expected.trim_end(),
        "classifier role.describe moved"
    );
    let describe: cortexkit_role_classifier::describe::RoleDescribe =
        serde_json::from_str(&served).unwrap();
    cortexkit_role_classifier::describe::check_describe(&describe).unwrap();
    let listed: Vec<&str> = describe.models.iter().map(|m| m.model.as_str()).collect();
    assert_eq!(listed, [JEV], "exactly the models broca can serve now");
}

/// One route, one role, decided at the bind through the real daemon: a classifier route serves
/// no llm-runner op, an llm-runner route serves no `classify.run`, and a bind declaring both
/// roles, or a classifier version broca does not serve, is refused so the open fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_roles_are_chosen_at_the_bind_through_the_real_daemon() {
    let Some(rig) = start_rig("roles").await else {
        return;
    };
    let classifier = rig.open().await.expect("the classifier route opens");
    match classifier.request("session.read", "{}").await.unwrap() {
        Reply::Error(body) => assert_eq!(body.code, "unknown_method"),
        Reply::Response(text) => panic!("a classifier route served session.read: {text}"),
    }
    let connection = classifier.connection.clone();
    let identity =
        broca_protocol::BindIdentity::new(rig.project_root.clone(), "core", "classifier");
    let runner = BrocaClassifierRoute {
        connection: connection.clone(),
        route: connection
            .route_open_management("broca", &identity)
            .await
            .expect("an llm-runner route opens"),
    };
    match runner
        .request(
            "classify.run",
            &json!({"batch_id": "b", "model": JEV, "questions": {}, "items": []}).to_string(),
        )
        .await
        .unwrap()
    {
        Reply::Error(body) => assert_eq!(body.code, "unknown_method"),
        Reply::Response(text) => panic!("an llm-runner route served classify.run: {text}"),
    }
    let quick = RouteOpenRetry::within(Duration::from_secs(2), Duration::from_millis(200));
    for roles in [
        vec![("classifier", "v2")],
        vec![("classifier", "v1"), ("llm-runner", "v1")],
    ] {
        let roles: std::collections::BTreeMap<String, String> = roles
            .into_iter()
            .map(|(role, version)| (role.to_owned(), version.to_owned()))
            .collect();
        let refused = connection
            .route_open_management_roles_within("broca", &identity, roles.clone(), quick)
            .await;
        let error = refused.expect_err("the bind must be refused").to_string();
        assert!(
            error.contains("role_versions_unsupported"),
            "{roles:?}: the open failed without broca's refusal: {error}"
        );
    }
}
