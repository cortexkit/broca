//! The `classifier/v1` conformance suite (`cortexkit-role-classifier-conformance`) against the
//! real `ck-broca`, through a real daemon route, with the role's capability golden captured
//! from the same route.
//!
//! The suite drives broca's management route opened the way an outside caller opens it
//! (`SubcConnection::route_open_classifier_within`, declaring `role_versions
//! {"classifier": "v1"}`). The provider is a stand-in on a loopback port that broca reaches
//! through its ordinary `providers.<id>.base_url` overrides: TypeSafe's `/v1/systemone`, and
//! Cloudflare Workers AI's `/client/v4/accounts/{account}/ai/run/@cf/cloudflare/{model}`,
//! answered in Cloudflare's `{result, success, errors, messages}` envelope. It counts and
//! scripts calls by the JCS of the item `state` it receives, as the suite's `ClassifierSubject`
//! requires, and can hold a call until the suite releases it. The catalog is broca's
//! last-known-good fusiform answer seeded into its state root: Jev, Clef and Clef-flash as
//! priced decision models (which of them depends on the test), a chat row, and a row that
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
const CLEF: &str = "cloudflare-workers-ai/@cf/cloudflare/clef";
const CLEF_FLASH: &str = "cloudflare-workers-ai/@cf/cloudflare/clef-flash";
/// Where broca sends a Workers AI item on the mock provider path, where no vault is opened and
/// so no credential names an account.
const WORKERS_AI_RUN_PREFIX: &str = "/client/v4/accounts/no-credential/ai/run/@cf/cloudflare/";
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
    /// Every request's path and body, in arrival order.
    received: Mutex<Vec<(String, Value)>>,
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
    format!("http://{address}")
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
    let path = head
        .lines()
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .unwrap_or_default()
        .to_owned();
    // Workers AI wraps an answer in its API envelope and names an error by code; TypeSafe
    // answers bare and documents no error body.
    let workers_ai = path.contains("/ai/run/");
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
    state
        .received
        .lock()
        .unwrap()
        .push((path.clone(), request.clone()));
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
        Some(Scripted::Answer(body)) => {
            let body = if workers_ai {
                format!(r#"{{"result":{body},"success":true,"errors":[],"messages":[]}}"#)
            } else {
                body
            };
            format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            )
        }
        Some(Scripted::Status {
            status,
            retry_after_ms,
        }) => {
            let retry = retry_after_ms.map_or(String::new(), |ms| {
                format!("retry-after: {}\r\n", ms.div_ceil(1000))
            });
            // Cloudflare's API answers a bad token with its general authentication error, a
            // code the role's Workers AI table does not list, so the status decides; other
            // statuses carry no code here.
            let body = match (workers_ai, status) {
                (false, _) => String::new(),
                (true, 401) => r#"{"result":null,"success":false,"errors":[{"code":10000,"message":"Authentication error"}],"messages":[]}"#.to_owned(),
                (true, _) => r#"{"result":null,"success":false,"errors":[],"messages":[]}"#.to_owned(),
            };
            format!(
                "HTTP/1.1 {status} Scripted\r\ncontent-type: application/json\r\ncontent-length: {}\r\n{retry}connection: close\r\n\r\n{body}",
                body.len()
            )
        }
        None => {
            "HTTP/1.1 500 Unscripted\r\ncontent-length: 0\r\nconnection: close\r\n\r\n".to_owned()
        }
    };
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

// ---- the rig --------------------------------------------------------------------------------

/// Which System One rows the seeded catalog lists, beside the chat and kindless rows.
#[derive(Clone, Copy)]
enum Served {
    /// TypeSafe's Jev only.
    Jev,
    /// Cloudflare Workers AI's Clef and Clef-flash only.
    WorkersAi,
    /// All three: what a catalog listing every routed model gives `role.describe`.
    All,
}

fn seed_catalog(state_root: &Path, served: Served) {
    let dir = state_root.join("catalog");
    std::fs::create_dir_all(&dir).unwrap();
    let priced = |units: u64| {
        json!({"currency": "USD", "exponent": 9, "state": "priced", "units": units,
               "unit_provenance": {"kind": "stated"}})
    };
    let mut models = json!({
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
    });
    if matches!(served, Served::Jev | Served::All) {
        models[JEV] = json!({
            "existence": "present",
            "model.kind": "decision",
            "rate.input": priced(42_000_000),
            "rate.output": {"state": "stated_zero"},
        });
    }
    if matches!(served, Served::WorkersAi | Served::All) {
        // As fusiform curates them from Cloudflare's model pages: input priced at 0.24 and
        // 0.038 USD per million tokens, output stated free, images taken.
        for (model, context, input) in [
            (CLEF, 65_536, 240_000_000),
            (CLEF_FLASH, 24_576, 38_000_000),
        ] {
            models[model] = json!({
                "existence": "present",
                "model.kind": "decision",
                "limit.context": context,
                "capability.attachment": true,
                "rate.input": priced(input),
                "rate.output": {"state": "stated_zero"},
            });
        }
    }
    let lkg = json!({
        "catalog_version": 1,
        "resolved_at_ms": 1,
        "source": "models.dev",
        "models": models,
    });
    std::fs::write(
        dir.join("fusiform-lkg.json"),
        serde_json::to_vec_pretty(&lkg).unwrap(),
    )
    .unwrap();
}

struct Rig {
    // Held so the daemon lives as long as the rig; only the Unix kill test reads its pid.
    #[cfg_attr(not(unix), allow(dead_code))]
    daemon: broca_test_support::RigChild,
    served: Served,
    connection_file: PathBuf,
    project_root: PathBuf,
    egress_host: String,
    state: Arc<StandIn>,
    state_root: PathBuf,
}

async fn start_rig(name: &str, served: Served) -> Option<Rig> {
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
    broca_test_support::remove_rig_dir(&root);
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
    seed_catalog(&state_root, served);
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
        serde_json::to_vec(&json!({"providers": {
            "typesafe": {"base_url": format!("{stand_in}/v1")},
            "cloudflare-workers-ai": {"base_url": format!("{stand_in}/client/v4")},
        }}))
        .unwrap(),
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
                    "BROCA_PROVIDER_BASE_URL": format!("{stand_in}/v1"),
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
        daemon,
        served,
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
        let model = |model: &str, images_max, context_tokens| ServedModel {
            model: model.into(),
            priced: true,
            images_max,
            context_tokens,
            egress_host: self.rig.egress_host.clone(),
        };
        let jev = || model(JEV, None, None);
        let workers_ai = || {
            [
                model(CLEF, Some(4), Some(65_536)),
                model(CLEF_FLASH, Some(4), Some(24_576)),
            ]
        };
        match self.rig.served {
            Served::Jev => vec![jev()],
            Served::WorkersAi => workers_ai().into(),
            Served::All => std::iter::once(jev()).chain(workers_ai()).collect(),
        }
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
    let Some(rig) = start_rig("suite", Served::Jev).await else {
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

/// The suite again, with every served model a Cloudflare Workers AI one (Clef and Clef-flash),
/// so each check runs through broca's Workers AI client against a stand-in answering in
/// Cloudflare's shapes. Every provider call must have gone to the Workers AI run endpoint of
/// the model the batch named, with that model's own value in the body.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn classifier_conformance_suite_passes_through_workers_ai() {
    let Some(rig) = start_rig("workers-ai", Served::WorkersAi).await else {
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
    let received = rig.state.received.lock().unwrap().clone();
    assert!(!received.is_empty(), "the suite reached the stand-in");
    for (path, body) in &received {
        let model = path
            .strip_prefix(WORKERS_AI_RUN_PREFIX)
            .unwrap_or_else(|| panic!("a call outside the Workers AI run endpoint: {path}"));
        assert!(
            matches!(model, "clef" | "clef-flash"),
            "an unrouted model path: {path}"
        );
        assert_eq!(body["model"], model, "the body names the endpoint's model");
    }
}

/// The `classifier/v1` `role.describe` bytes, as broca serves them on a classifier route, are
/// the role's capability golden. Regenerate with `BROCA_UPDATE_GOLDEN=1`. The stand-in's
/// loopback host is the egress host here, since that is where this rig sends item text.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn classifier_role_describe_matches_its_golden_through_the_real_serve_path() {
    let Some(rig) = start_rig("golden", Served::All).await else {
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
    assert_eq!(
        listed,
        [JEV, CLEF, CLEF_FLASH],
        "exactly the models broca can serve now"
    );
}

/// One route, one role, decided at the bind through the real daemon: a classifier route serves
/// no llm-runner op, an llm-runner route serves no `classify.run`, and a bind declaring both
/// roles, or a classifier version broca does not serve, is refused so the open fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn route_roles_are_chosen_at_the_bind_through_the_real_daemon() {
    let Some(rig) = start_rig("roles", Served::Jev).await else {
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

/// Every process the daemon `pid` runs directly: broca, in these rigs.
#[cfg(unix)]
fn children(pid: u32) -> Vec<u32> {
    std::process::Command::new("pgrep")
        .args(["-P", &pid.to_string()])
        .output()
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .filter_map(|line| line.trim().parse().ok())
                .collect()
        })
        .unwrap_or_default()
}

/// The batch's `(state, owner)` and its recorded outcomes `(index, kind, body)`, read straight
/// from broca's batch store file, read-only.
#[cfg(unix)]
fn store_view(
    state_root: &Path,
    batch_id: &str,
) -> (String, Option<String>, Vec<(i64, String, String)>) {
    let conn = rusqlite::Connection::open_with_flags(
        state_root.join("system-one.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let (state, owner) = conn
        .query_row(
            "SELECT state, owner FROM batch WHERE role = 'classifier' AND batch_id = ?1",
            [batch_id],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .unwrap();
    let mut statement = conn
        .prepare(
            "SELECT item_index, kind, body FROM item_outcome
              WHERE role = 'classifier' AND batch_id = ?1 ORDER BY item_index",
        )
        .unwrap();
    let outcomes = statement
        .query_map([batch_id], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap()
        .map(Result::unwrap)
        .collect();
    (state, owner, outcomes)
}

/// broca killed (SIGKILL, a real process kill) while a Clef batch is in flight, with its
/// second item's provider call held open at the stand-in. The daemon starts broca again; the
/// new process releases the batch the dead one left running, before it serves anything; and a
/// re-send of the batch calls the provider only for the two items that had no answer, serving
/// the answered one byte for byte as the provider spelled it. Unix only: the kill is of the
/// daemon's broca child found with `pgrep -P`.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_in_flight_when_broca_is_killed_is_released_and_resumed() {
    let Some(rig) = start_rig("kill", Served::WorkersAi).await else {
        return;
    };
    let key = |state: &str| state_key(&Value::String(state.to_owned()));
    // Spellings a decoder would change, so a served answer that was re-encoded shows.
    let answered = r#"{"q":{"type":"noul","noul":0.950}}"#;
    let answer = |answers: &str| {
        format!(
            r#"{{"model":"clef","answers":{answers},"usage":{{"input_tokens":12,"output_tokens":0}}}}"#
        )
    };
    for (state, answers) in [
        ("killed-a", answered),
        ("killed-b", r#"{"q":{"type":"noul","noul":0.5}}"#),
        ("killed-c", r#"{"q":{"type":"noul","noul":0.25}}"#),
    ] {
        rig.state
            .scripts
            .lock()
            .unwrap()
            .insert(key(state), Scripted::Answer(answer(answers)));
    }
    rig.state
        .hold_next
        .lock()
        .unwrap()
        .insert(key("killed-b"), Arc::new(Notify::new()));
    let params = json!({
        "batch_id": "killed",
        "model": CLEF,
        "questions": {"q": {"type": "noul", "instructions": "Is it flagged?"}},
        "items": [{"state": "killed-a"}, {"state": "killed-b"}, {"state": "killed-c"}],
    })
    .to_string();

    let route = rig.open().await.expect("the classifier route opens");
    let in_flight = {
        let params = params.clone();
        tokio::spawn(async move { route.request("classify.run", &params).await })
    };
    broca_test_support::wait_until("the second item's call is held at the stand-in", || {
        let held = rig
            .state
            .held
            .lock()
            .unwrap()
            .contains_key(&key("killed-b"));
        async move { held.then_some(()).ok_or_else(|| "not held yet".to_owned()) }
    })
    .await;
    let (state, owner, outcomes) = store_view(&rig.state_root, "killed");
    assert_eq!(state, "running");
    assert!(owner.is_some());
    assert_eq!(
        outcomes.len(),
        1,
        "only the first item is answered: {outcomes:?}"
    );
    assert_eq!((outcomes[0].0, outcomes[0].1.as_str()), (0, "answered"));

    let broca = children(rig.daemon.id().unwrap());
    assert!(!broca.is_empty(), "broca is supervised by the daemon");
    for pid in &broca {
        let killed = std::process::Command::new("kill")
            .args(["-9", &pid.to_string()])
            .status()
            .unwrap();
        assert!(killed.success(), "SIGKILL delivered to {pid}");
    }
    // The call that was in flight never gets its reply.
    let lost = in_flight.await.unwrap();
    assert!(
        !matches!(lost, Ok(Reply::Response(_))),
        "the killed call answered: {lost:?}"
    );

    // A route opens only once the restarted broca serves, and it releases left-running
    // batches before it serves.
    let route = rig
        .open()
        .await
        .expect("the restarted broca serves a route");
    let after: Vec<u32> = children(rig.daemon.id().unwrap());
    assert!(
        after.iter().all(|pid| !broca.contains(pid)),
        "a new broca process serves: before {broca:?}, after {after:?}"
    );
    let (state, owner, outcomes) = store_view(&rig.state_root, "killed");
    assert_eq!(
        (state.as_str(), owner),
        ("idle", None),
        "the batch was released"
    );
    assert_eq!(outcomes.len(), 1);

    let Reply::Response(reply) = route.request("classify.run", &params).await.unwrap() else {
        panic!("the re-send was refused");
    };
    let calls = |state: &str| {
        rig.state
            .calls
            .lock()
            .unwrap()
            .get(&key(state))
            .copied()
            .unwrap_or(0)
    };
    assert_eq!(
        (calls("killed-a"), calls("killed-b"), calls("killed-c")),
        (1, 2, 1),
        "the re-send called only the unanswered items"
    );
    #[derive(serde::Deserialize)]
    struct Raw<'a> {
        #[serde(borrow)]
        items: Vec<RawItem<'a>>,
    }
    #[derive(serde::Deserialize)]
    struct RawItem<'a> {
        #[serde(borrow, default)]
        answers: Option<&'a serde_json::value::RawValue>,
    }
    let raw: Raw<'_> = serde_json::from_str(&reply).unwrap();
    let texts: Vec<Option<&str>> = raw
        .items
        .iter()
        .map(|item| item.answers.map(|answers| answers.get()))
        .collect();
    assert_eq!(
        texts,
        [
            Some(answered),
            Some(r#"{"q":{"type":"noul","noul":0.5}}"#),
            Some(r#"{"q":{"type":"noul","noul":0.25}}"#),
        ],
        "the stored answer is served as the provider spelled it"
    );
    let (state, _, outcomes) = store_view(&rig.state_root, "killed");
    assert_eq!(state, "idle");
    assert_eq!(outcomes.len(), 3);
    // Let the held connection from the killed process go.
    let held = rig
        .state
        .held
        .lock()
        .unwrap()
        .get(&key("killed-b"))
        .cloned();
    if let Some(release) = held {
        release.notify_one();
    }
}
