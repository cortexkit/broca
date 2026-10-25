//! Private-content admission through the real serve path: a real daemon, a real ck-broca and a
//! loopback provider.
//!
//! The route asks cerebellum and Basal, so the rig has to answer as both. It does that with
//! `fake-surface`, a stand-in module (its own package under `tests/support/fake-surface`) that
//! declares the two management operations and serves them from a fixture file. `Stub::Answering`
//! gives it a fixture; a fixture whose value is an error body is returned as the module's own
//! refusal, which is how Basal's `flow_not_approved` and an unreadable cerebellum answer are
//! delivered. Every other module here is `fake-aft-stub`, which declares no management surface,
//! so a request aimed at one of those is refused by the daemon.

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broca_subc::{RouteHandle, SubcConnection};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MODEL: &str = "typesafe/jev-1.13.0";

#[derive(Default)]
struct Provider {
    calls: Mutex<usize>,
}

async fn provider(state: Arc<Provider>) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let state = state.clone();
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut chunk = [0u8; 16384];
                let body_start = loop {
                    let n = stream.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    buffer.extend_from_slice(&chunk[..n]);
                    if let Some(at) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                        break at + 4;
                    }
                };
                let header = String::from_utf8_lossy(&buffer[..body_start]).to_ascii_lowercase();
                let length: usize = header
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length:"))
                    .unwrap()
                    .trim()
                    .parse()
                    .unwrap();
                while buffer.len() < body_start + length {
                    let n = stream.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        return;
                    }
                    buffer.extend_from_slice(&chunk[..n]);
                }
                *state.calls.lock().unwrap() += 1;
                let reply = br#"{"model":"jev-1.13.0","answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":100}}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                    reply.len(),
                    String::from_utf8_lossy(reply)
                );
                let _ = stream.write_all(response.as_bytes()).await;
                let _ = stream.flush().await;
            });
        }
    });
    format!("http://{address}")
}

/// How one stub module answers a management request that names no fixture.
#[derive(Clone, Copy, PartialEq)]
enum Stub {
    /// Refuse the operation: the stand-in answers every request with `unknown_method`, which is
    /// what a module that does not serve the operation answers.
    Unreachable,
    /// Use this JSON as the matching fixture's `stdout`: an error body refuses the request, any
    /// other value is wrapped in `{result}`.
    Answering(&'static str),
}

struct Rig {
    _daemon: broca_test_support::RigChild,
    root: PathBuf,
    state: PathBuf,
    provider: Arc<Provider>,
}

impl Rig {
    async fn start(name: &str, cerebellum: Stub, basal: Stub) -> Option<Self> {
        let subc = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
        if !subc.exists() {
            assert!(
                std::env::var_os("BROCA_REQUIRE_SUBC").is_none(),
                "required real daemon missing: {}",
                subc.display()
            );
            eprintln!("SKIP {name}: real daemon missing");
            return None;
        }
        let root = broca_test_support::scratch_root()
            .join(format!("llm-tools-private-{name}-{}", std::process::id()));
        broca_test_support::remove_rig_dir(&root);
        for dir in [
            "config/cortexkit",
            "runtime",
            "data",
            "home",
            "project",
            "state/catalog",
        ] {
            std::fs::create_dir_all(root.join(dir)).unwrap();
        }
        let state = root.join("state");
        let priced = |units| json!({"currency":"USD","exponent":9,"state":"priced","units":units,"unit_provenance":{"kind":"stated"}});
        let row = json!({"existence":"present","model.kind":"decision","rate.output":{"state":"stated_zero"},"rate.input":priced(42_000_000)});
        std::fs::write(state.join("catalog/fusiform-lkg.json"), json!({"catalog_version":1,"resolved_at_ms":1,"source":"models.dev","models":{MODEL:row}}).to_string()).unwrap();
        let provider_state = Arc::new(Provider::default());
        let url = provider(provider_state.clone()).await;
        let config = root.join("config/cortexkit/broca.jsonc");
        // The operator lists no provider: every provider a call names needs either cerebellum's
        // "unmarked" answer or the person's approval.
        std::fs::write(&config, json!({"providers":{"typesafe":{"base_url":format!("{url}/v1")}},"private_content_providers":[],"llm_tools":{"advertise_tags":true}}).to_string()).unwrap();
        let stub = broca_test_support::subc_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
        assert!(stub.exists(), "scope authority fixture missing");
        let scopes = root.join("scopes.json");
        std::fs::write(&scopes, json!([
            {"ref":"run","scope_epoch":1,"kind":"head","attributes":{"agent_id":"agent","run_id":"run"},"carriers":[{"principal":{"kind":"reserved","module_id":"basal"}}]},
            {"ref":"flow","scope_epoch":1,"kind":"head","attributes":{"flow_id":"tool-flow"},"carriers":[{"principal":{"kind":"reserved","module_id":"basal"}}]}
        ]).to_string()).unwrap();
        // Cerebellum and Basal are served by the `fake-surface` stand-in: a module that
        // declares the two operations this route asks and serves them from a fixture file. The
        // `owner` module above is the scope authority owner and publishes the scopes; the
        // stand-ins hold none of their own.
        //
        // The daemon names the module on the command line, so the handoff passes on only what
        // the daemon does not know: the fixture path and the request log.
        let mut modules = json!({});
        for id in ["owner", "cerebellum", "cingulate"] {
            let mut env = json!({"FAKE_AFT_MODULE_ID":id,"FAKE_AFT_LAUNCH_NONCE_PATH":root.join(format!("{id}.nonce")),"FAKE_AFT_EVENTS_PATH":json!(root.join(format!("{id}.events")))});
            if id == "owner" {
                env["FAKE_AFT_SCOPE_SYNC_PATH"] = json!(scopes);
                env["FAKE_AFT_EVENTS_PATH"] = json!(root.join("owner.events"));
            }
            modules[id] = json!({"program":broca_test_support::ckdev_binary(&stub, broca_test_support::scratch_root()),"args":[],"reserved":true,"enabled":true,"env":env});
        }
        for (module_id, answer) in [("cerebellum", cerebellum), ("basal", basal)] {
            let fixture = root.join(format!("{module_id}.fixtures.jsonl"));
            let method = if module_id == "cerebellum" {
                "private_content.query"
            } else {
                "flow.private_content_providers"
            };
            let lines = match answer {
                Stub::Answering(result) => vec![json!({"match":{"method":method},"stdout":result})],
                Stub::Unreachable => Vec::new(),
            };
            std::fs::write(&fixture, serde_json::to_string(&lines).unwrap()).unwrap();
            let handoff = root.join(format!("{module_id}.handoff.sh"));
            std::fs::write(
                &handoff,
                format!(
                    "#!/bin/sh\n# The daemon names our connection file on the command line.\nwhile [ \"$#\" -gt 0 ]; do\n  case \"$1\" in\n    --subc) SUBC_CONNECTION=\"$2\"; shift 2 ;;\n    *) shift ;;\n  esac\ndone\nexport SUBC_CONNECTION\nexec {surface} --module-id {module_id} --SURFACE_FIXTURES={fixtures} --SURFACE_REQUESTS={requests} 2>> {stderr}\n",
                    surface = fake_surface_bin().display(),
                    module_id = module_id,
                    fixtures = hex(&fixture),
                    requests = hex(&root.join(format!("{module_id}.requests"))),
                    stderr = root
                        .join(format!("{module_id}.stand-in.stderr.log"))
                        .display(),
                ),
            )
            .unwrap();
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&handoff, std::fs::Permissions::from_mode(0o755)).unwrap();
            }
            modules[module_id] = json!({
                "program": handoff,
                "args": [],
                "reserved": true,
                "enabled": true,
            });
        }
        let env = json!({"BROCA_STATE_ROOT":state,"BROCA_CONFIG_PATH":config,"BROCA_PROVIDER_BASE_URL":format!("{url}/v1"),"CK_LOG":"info"});
        modules["broca"] = json!({"program":broca_test_support::ckdev_binary(Path::new(env!("CARGO_BIN_EXE_ck-broca")), broca_test_support::scratch_root()),"args":[],"enabled":true,"env":env});
        std::fs::write(
            root.join("config/cortexkit/subc.jsonc"),
            json!({"version":1,"scope_authority_owners":["owner"],"modules":modules}).to_string(),
        )
        .unwrap();
        let child = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(
                broca_test_support::ckdev_binary(&subc, broca_test_support::scratch_root()),
                &root,
                "daemon",
            )?
            .env("XDG_CONFIG_HOME", root.join("config"))
            .env("XDG_RUNTIME_DIR", root.join("runtime"))
            .env("XDG_DATA_HOME", root.join("data"))
            .env_remove("BROCA_STATE_ROOT")
            .env("HOME", root.join("home"))
            .env("SUBC_PORT", "0")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env_remove("SUBC_MODULE_ID")
            .kill_on_drop(true)
            .spawn()
        })
        .unwrap();
        let rig = Self {
            _daemon: broca_test_support::RigChild::new(child, &root, "daemon"),
            root,
            state,
            provider: provider_state,
        };
        broca_test_support::wait_until("tool scope sync", || async {
            let events = std::fs::read_to_string(rig.root.join("owner.events")).unwrap_or_default();
            if events
                .lines()
                .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                .any(|e| e["kind"] == "scope_sync_response")
            {
                Ok(())
            } else {
                Err("scope not synced".to_string())
            }
        })
        .await;
        Some(rig)
    }

    /// The tool-provider route a consumer opens to broca.
    async fn open(&self, scope_ref: &str, flow: bool) -> Route {
        self.open_to("broca", scope_ref, flow).await
    }

    /// Open a tool-provider route to one broca process in this rig.
    async fn open_to(&self, target: &str, scope_ref: &str, flow: bool) -> Route {
        // A run scope is opened by its owner; the flow carrier is Basal, whose calls carry the
        // flow's scope.
        let caller = if flow { "basal" } else { "owner" };
        let nonce = self.root.join(format!("{caller}.nonce"));
        broca_test_support::wait_until("caller nonce", || async {
            std::fs::read_to_string(&nonce).map_err(|e| e.to_string())
        })
        .await;
        let connection = Arc::new(
            SubcConnection::connect_with_consumer_identity(
                &self.root.join("runtime/subc-connection.json"),
                Some(subc_control::ConsumerIdentity {
                    module_id: caller.into(),
                    launch_nonce: std::fs::read_to_string(nonce).unwrap().trim().into(),
                }),
            )
            .await
            .unwrap(),
        );
        let identity = broca_protocol::BindIdentity::new(
            self.root.join("project").canonicalize().unwrap(),
            "test",
            if flow { "flows" } else { "runs" },
        );
        let scope = subc_protocol::scope::ScopeSelector::new(
            subc_protocol::Principal::Reserved {
                module_id: "owner".into(),
            },
            scope_ref,
            Some(1),
        );
        let route = broca_test_support::wait_until("tool-provider route", || {
            let connection = connection.clone();
            let identity = identity.clone();
            let scope = scope.clone();
            let target = target.to_owned();
            async move {
                connection
                    .route_open_tool_provider_v1(&target, &identity, Some(scope))
                    .await
                    .map_err(|e| e.to_string())
            }
        })
        .await;
        Route { connection, route }
    }

    fn calls(&self) -> usize {
        *self.provider.calls.lock().unwrap()
    }

    /// Add a provider to the operator's private-content list, the way `broca.jsonc` does.
    ///
    /// The list is read when broca starts, so a change takes effect once broca has been
    /// restarted; this only writes the file.
    fn list_provider(&self, provider: &str) {
        let path = self.root.join("config/cortexkit/broca.jsonc");
        let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        config["private_content_providers"] = json!([provider]);
        std::fs::write(path, config.to_string()).unwrap();
    }

    /// Start broca again over the same state, with a module id of its own, and open a fresh
    /// route to it.
    async fn restart(&self) -> Route {
        let config = self.root.join("config/cortexkit/broca.jsonc");
        let settings: Value = serde_json::from_slice(&std::fs::read(&config).unwrap()).unwrap();
        let child = broca_test_support::tokio_rig_command(
            broca_test_support::ckdev_binary(
                Path::new(env!("CARGO_BIN_EXE_ck-broca")),
                broca_test_support::scratch_root(),
            ),
            &self.root,
            "restarted",
        )
        .unwrap()
        .arg("--subc")
        .arg(self.root.join("runtime/subc-connection.json"))
        .env("SUBC_MODULE_ID", "broca-restarted")
        .env_remove("SUBC_LAUNCH_NONCE")
        .env_remove("SUBC_LAUNCH_NONCE_FD")
        .env("BROCA_STATE_ROOT", &self.state)
        .env("BROCA_CONFIG_PATH", config)
        .env(
            "BROCA_PROVIDER_BASE_URL",
            settings["providers"]["typesafe"]["base_url"]
                .as_str()
                .unwrap(),
        )
        .env("XDG_CONFIG_HOME", self.root.join("config"))
        .env("XDG_DATA_HOME", self.root.join("data"))
        .env("HOME", self.root.join("home"))
        .kill_on_drop(false)
        .spawn()
        .unwrap();
        // Forgetting the handle leaves the restarted process running for the rig to use; the
        // handle's drop would otherwise kill it.
        std::mem::forget(broca_test_support::RigChild::new(
            child,
            &self.root,
            "restarted",
        ));
        // A freshly started broca registers with the daemon a moment after spawn. Wait until it
        // answers the provider catalog, so the first tool call reaches a served route.
        let connection = Arc::new(
            SubcConnection::connect_with_consumer_identity(
                &self.root.join("runtime/subc-connection.json"),
                Some(subc_control::ConsumerIdentity {
                    module_id: "owner".into(),
                    launch_nonce: std::fs::read_to_string(self.root.join("owner.nonce"))
                        .unwrap()
                        .trim()
                        .into(),
                }),
            )
            .await
            .unwrap(),
        );
        let identity = broca_protocol::BindIdentity::new(
            self.root.join("project").canonicalize().unwrap(),
            "test",
            "restart-probe",
        );
        broca_test_support::wait_until("the restarted broca", || {
            let connection = connection.clone();
            let identity = identity.clone();
            async move {
                let route = connection
                    .route_open_tool_provider_v1("broca-restarted", &identity, None)
                    .await
                    .map_err(|error| error.to_string())?;
                let body = serde_json::to_vec(&json!({"name":"tool.catalog","arguments":{}}))
                    .map_err(|error| error.to_string())?;
                let answer = connection
                    .route_request_terminal_with_deadline(&route, body, Duration::from_secs(10))
                    .await
                    .map_err(|error| error.to_string())?;
                let _ = connection.route_close(&route).await;
                answer
                    .map(|_| ())
                    .map_err(|body| String::from_utf8_lossy(&body).into_owned())
            }
        })
        .await;
        self.open_to("broca-restarted", "run", false).await
    }

    /// How many `private_content.query` requests reached cerebellum.
    fn cerebellum_queries(&self) -> usize {
        requests(self, "cerebellum", "private_content.query")
    }
}

/// The stand-in management surface this test points broca at for cerebellum and Basal.
///
/// That stand-in is another module, so it speaks the subc wire and the manifest shapes, and it
/// is built as a separate package (`tests/support/fake-surface`) whose manifest pins exactly
/// the releases broca links. Cargo builds no binary for a test, so the test builds this one
/// once per run: `CARGO` knows this crate's exact dependency revisions, and the result lands
/// under its own build directory and then publishes a copy outside the build tree, which is
/// where the rig launches it from: a supervising process must never execute a build-tree path,
/// and macOS refuses to exec some removable build volumes.
fn fake_surface_bin() -> PathBuf {
    let deps = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
    let stand_in = build_dir("fake-surface");
    let out = deps.join("fake-surface");
    let ready = deps.join("fake-surface.ready");
    if ready.exists() && out.exists() && built_after_source(&out) {
        return out;
    }
    let package = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/fake-surface");
    let status = std::process::Command::new(env!("CARGO"))
        .arg("build")
        .arg("--offline")
        .arg("--quiet")
        // A build, not a broca process: it writes only under the `--target-dir` below. The
        // data home is pinned to the same scratch area and an inherited state root is dropped,
        // so nothing this build runs can reach the operator's state.
        .env("XDG_DATA_HOME", build_dir("fake-surface/data"))
        .env_remove("BROCA_STATE_ROOT")
        .arg("--manifest-path")
        .arg(package.join("Cargo.toml"))
        .arg("--target-dir")
        .arg(&stand_in)
        .status()
        .unwrap();
    assert!(status.success(), "the stand-in surface must compile");
    let built = stand_in.join("debug/fake-surface");
    let published = broca_test_support::ckdev_binary(&built, broca_test_support::scratch_root());
    std::fs::copy(&published, &out).unwrap();
    std::fs::write(&ready, "").unwrap();
    out
}

/// A path under the shared build directory, for the stand-in's own build only. Nothing is ever
/// launched from here: the rig runs the published copy `fake_surface_bin` returns.
fn build_dir(relative: &str) -> PathBuf {
    std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("fake-surface")
        .join(relative)
}

/// The workspaces the stand-in's build could resolve against. They must pin the same crate
/// versions, or the stand-in would build against a wire this build never speaks: the offline
/// build fails on a moved pin, and a fully resolved lock would quietly link another release.
#[test]
fn the_stand_in_pins_the_daemon_wire_versions() {
    let workspace = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let helper = workspace.join("crates/broca-module-serve/tests/support/fake-surface");
    for crate_name in ["subc-protocol", "subc-transport"] {
        let built_against = pinned_version(&workspace.join("Cargo.lock"), crate_name);
        let declared = pinned_version(&helper.join("Cargo.lock"), crate_name);
        assert_eq!(
            declared, built_against,
            "the stand-in speaks {crate_name} {declared}, but the broker links {built_against}; \
             update tests/support/fake-surface/Cargo.toml (and its lock) with the workspace pin"
        );
        assert_eq!(
            required_version(&helper.join("Cargo.toml"), crate_name),
            built_against,
            "tests/support/fake-surface/Cargo.toml must require exactly {crate_name} \
             {built_against}, the release the workspace links"
        );
    }
}

/// The version a lock file resolved for `crate_name`, which is the one the build links.
fn pinned_version(lock: &Path, crate_name: &str) -> String {
    let text = std::fs::read_to_string(lock).unwrap_or_else(|error| {
        panic!("cannot read {}: {error}", lock.display());
    });
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        if line.trim() != "[[package]]" {
            continue;
        }
        let name = lines
            .next()
            .and_then(|line| line.strip_prefix("name = "))
            .map(|name| name.trim_matches('"'));
        if name != Some(crate_name) {
            continue;
        }
        return lines
            .next()
            .and_then(|line| line.strip_prefix("version = "))
            .map(|version| version.trim_matches('"').to_owned())
            .unwrap_or_else(|| panic!("{} entry has no version", lock.display()));
    }
    panic!("{} does not resolve {crate_name}", lock.display());
}

/// The exact version a manifest requires for `crate_name` (`crate = "=x.y.z"`).
fn required_version(manifest: &Path, crate_name: &str) -> String {
    let text = std::fs::read_to_string(manifest).unwrap_or_else(|error| {
        panic!("cannot read {}: {error}", manifest.display());
    });
    let prefix = format!("{crate_name} = \"");
    for line in text.lines() {
        if let Some(rest) = line.trim().strip_prefix(&prefix) {
            let requirement = rest.trim_end_matches('"');
            return requirement
                .strip_prefix('=')
                .unwrap_or_else(|| panic!("{crate_name} must be pinned with =, not {requirement}"))
                .to_owned();
        }
    }
    panic!("{} does not require {crate_name}", manifest.display());
}

/// Whether `binary` is newer than the stand-in's sources.
fn built_after_source(binary: &Path) -> bool {
    let source =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/fake-surface/src/main.rs");
    let built = std::fs::metadata(binary).and_then(|m| m.modified()).ok();
    let written = std::fs::metadata(source).and_then(|m| m.modified()).ok();
    matches!((built, written), (Some(built), Some(written)) if built >= written)
}

/// A path as the stand-in's command line carries it.
fn hex(path: &Path) -> String {
    path.to_string_lossy()
        .bytes()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// How many requests of `method` the stand-in for a module recorded.
fn requests(rig: &Rig, module: &str, method: &str) -> usize {
    std::fs::read_to_string(rig.root.join(format!("{module}.requests")))
        .unwrap_or_default()
        .lines()
        .filter(|line| line.starts_with(method))
        .count()
}

struct Route {
    connection: Arc<SubcConnection>,
    route: RouteHandle,
}

impl Route {
    async fn call(&self, arguments: Value, key: Option<&str>) -> Result<Vec<u8>, Value> {
        let mut body =
            json!({"name":broca_module_serve::llm_tools::CLASSIFY_TOOL_NAME,"arguments":arguments});
        if let Some(key) = key {
            body["call_key"] = json!(key);
        }
        self.connection
            .route_request_terminal_with_deadline(
                &self.route,
                serde_json::to_vec(&body).unwrap(),
                Duration::from_secs(30),
            )
            .await
            .unwrap()
            .map_err(|bytes| serde_json::from_slice(&bytes).unwrap())
    }
}

fn args() -> Value {
    json!({"model":MODEL,"questions":{"q":{"type":"noul","instructions":"Is the invented note urgent?"}},"item":{"state":"invented"},"max_cost_usd":0.01})
}

fn rows(path: &Path, table: &str) -> Vec<String> {
    if !path.exists() {
        return Vec::new();
    }
    let connection =
        rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .unwrap();
    let mut statement = connection
        .prepare(&format!("SELECT * FROM {table}"))
        .unwrap();
    let columns = statement.column_count();
    let mut rows = statement
        .query_map([], |row| {
            Ok((0..columns)
                .map(|i| format!("{:?}", row.get_ref(i).unwrap()))
                .collect::<Vec<_>>()
                .join("|"))
        })
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    rows.sort();
    rows
}

/// Every artifact a refusal must leave untouched: batches, outcomes, outbox rows, exported spend
/// facts, and every grants table including the ones a held call or a card would write.
fn artifacts(rig: &Rig) -> Value {
    let mut value = json!({});
    for (file, tables) in [
        (
            "system-one.db",
            vec!["batch", "item_outcome", "usage_outbox"],
        ),
        ("run-index.db", vec!["export_facts"]),
        (
            "grants.db",
            vec![
                "grants",
                "denies",
                "held_calls",
                "cards",
                "generations",
                "cursors",
            ],
        ),
    ] {
        let path = rig.state.join(file);
        value[file]["exists"] = json!(path.exists());
        for table in tables {
            value[file][table] = json!(rows(&path, table));
        }
    }
    let requests = std::fs::read_to_string(rig.root.join("cingulate.events")).unwrap_or_default();
    value["consent_requests"] = json!(requests
        .lines()
        .filter(|line| line.contains("consent.request"))
        .count());
    value
}

/// A call the policy admits, retried while the request's own reply deadline outlasts the
/// policy's questions: a route binds and asks before the answer is due, so a short client
/// deadline can expire on a call that is in fact proceeding.
async fn admitted_with_retry(rig: &Rig, route: &Route, key: &str) -> Value {
    let mut last = None;
    for attempt in 0..4 {
        match route.call(args(), Some(&format!("{key}-{attempt}"))).await {
            Ok(bytes) => {
                return serde_json::from_slice(&bytes).unwrap();
            }
            Err(error) => last = Some(error),
        }
    }
    panic!(
        "the call was never admitted: {last:?} ({} provider calls)",
        rig.calls()
    );
}

/// One call that is refused, leaving every artifact exactly as it was.
async fn refused_without_writes(rig: &Rig, route: &Route, key: &str) -> Value {
    refused_arguments_without_writes(rig, route, args(), key).await
}

async fn refused_arguments_without_writes(
    rig: &Rig,
    route: &Route,
    arguments: Value,
    key: &str,
) -> Value {
    let before = artifacts(rig);
    let error = route.call(arguments, Some(key)).await.unwrap_err();
    assert_eq!(artifacts(rig), before, "a refusal wrote an artifact");
    error
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unreachable_cerebellum_counts_as_marked_and_asks_again_on_every_call() {
    // This cerebellum serves the question by refusing it, which is the answer broca has to
    // treat as "no answer".
    let Some(rig) = Rig::start(
        "unreachable",
        Stub::Answering(
            r#"{"code":"private_content_unavailable","message":"cerebellum cannot tell"}"#,
        ),
        Stub::Unreachable,
    )
    .await
    else {
        return;
    };
    let route = rig.open("run", false).await;
    let error = refused_without_writes(&rig, &route, "unlisted").await;
    assert_eq!(error["code"], "not_started");
    assert_eq!(error["detail"]["consent"], "private_content_unknown");
    let error = refused_without_writes(&rig, &route, "unlisted-again").await;
    assert_eq!(error["detail"]["consent"], "private_content_unknown");
    assert_eq!(
        rig.cerebellum_queries(),
        2,
        "a failed answer is never cached, so the next call asks again"
    );
    assert_eq!(rig.calls(), 0, "no provider may be called");

    // A listed provider needs no question about private content. This build reads the
    // operator's list when it starts, so the next start of broca serves it.
    rig.list_provider("typesafe");
    let route = rig.restart().await;
    let result: Value =
        serde_json::from_slice(&route.call(args(), Some("listed")).await.unwrap()).unwrap();
    assert_eq!(
        result["cost_usd"].as_f64().unwrap(),
        0.0000042,
        "100 input tokens at the shipped rate of $42 per million"
    );
    assert_eq!(rig.calls(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_unmarked_run_calls_an_unlisted_provider() {
    let Some(rig) = Rig::start(
        "unmarked",
        Stub::Answering(r#"{"private_content":false,"sources":[]}"#),
        Stub::Answering(r#"{"flow_id":"tool-flow","version":"1","code_hash":"00","providers":[]}"#),
    )
    .await
    else {
        return;
    };
    let route = rig.open("run", false).await;
    let result = admitted_with_retry(&rig, &route, "unmarked").await;
    assert!(result["cost_usd"].is_number());
    assert_eq!(rig.cerebellum_queries(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_marked_run_ends_not_started_consent_required_and_files_nothing() {
    let Some(rig) = Rig::start(
        "marked",
        Stub::Answering(r#"{"private_content":true,"sources":["person_browser"]}"#),
        Stub::Answering(r#"{"flow_id":"tool-flow","version":"1","code_hash":"00","providers":[]}"#),
    )
    .await
    else {
        return;
    };
    let route = rig.open("run", false).await;
    for key in ["marked", "marked-again"] {
        let error = refused_without_writes(&rig, &route, key).await;
        assert_eq!(error["code"], "not_started");
        assert_eq!(error["detail"]["consent"], "consent_required");
    }
    assert_eq!(rig.cerebellum_queries(), 1, "a marked answer is cached");
    assert_eq!(rig.calls(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_marked_run_with_an_unknown_source_files_no_card() {
    let Some(rig) = Rig::start(
        "marked-unknown",
        // A marked answer whose only source kind broca does not know, so a card would have no
        // source to name and the call files none.
        Stub::Answering(r#"{"private_content":true,"sources":["future_source"]}"#),
        Stub::Answering(r#"{"flow_id":"tool-flow","version":"1","code_hash":"00","providers":[]}"#),
    )
    .await
    else {
        return;
    };
    let route = rig.open("run", false).await;
    let error = refused_without_writes(&rig, &route, "unknown-source").await;
    assert_eq!(error["detail"]["consent"], "private_content_unknown");
    assert_eq!(rig.calls(), 0);
}
