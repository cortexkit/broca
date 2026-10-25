//! Real daemon, real ck-broca and loopback provider: no refusal assertion bypasses routing.

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
    usage: Mutex<Option<Value>>,
    hold: tokio::sync::Notify,
    blocked: std::sync::atomic::AtomicBool,
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
                let request: Value = serde_json::from_slice(&buffer[body_start..]).unwrap();
                *state.calls.lock().unwrap() += 1;
                let released = state.hold.notified();
                if state.blocked.load(std::sync::atomic::Ordering::SeqCst) {
                    released.await;
                }
                let answers = request["questions"]
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(id, question)| {
                        let answer = match question["type"].as_str().unwrap() {
                            "choice" => json!({"type":"choice","choice":"green"}),
                            "score" => json!({"type":"score","score":0.5}),
                            _ => json!({"type":"noul","noul":0.5}),
                        };
                        (id.clone(), answer)
                    })
                    .collect::<serde_json::Map<_, _>>();
                let usage = state
                    .usage
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| json!({"input_tokens":100,"output_tokens":2}));
                let body =
                    json!({"model":"jev-1.13.0","answers":answers,"usage":usage}).to_string();
                let response = format!("HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    });
    format!("http://{address}")
}

struct Rig {
    _daemon: broca_test_support::RigChild,
    root: PathBuf,
    state: PathBuf,
    provider: Arc<Provider>,
}

impl Rig {
    async fn start(
        name: &str,
        tags: bool,
        trusted: bool,
        input: Option<u64>,
        output: u64,
        cut: bool,
    ) -> Option<Self> {
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
            .join(format!("llm-tools-{name}-{}", std::process::id()));
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
        let mut row = json!({"existence":"present","model.kind":"decision","rate.output":if output == 0 { json!({"state":"stated_zero"}) } else { priced(output) }});
        if let Some(input) = input {
            row["rate.input"] = if input == 0 {
                json!({"state":"stated_zero"})
            } else {
                priced(input)
            };
        }
        std::fs::write(state.join("catalog/fusiform-lkg.json"), json!({"catalog_version":1,"resolved_at_ms":1,"source":"models.dev","models":{MODEL:row}}).to_string()).unwrap();
        let provider_state = Arc::new(Provider::default());
        let url = provider(provider_state.clone()).await;
        let config = root.join("config/cortexkit/broca.jsonc");
        std::fs::write(&config, json!({"providers":{"typesafe":{"base_url":format!("{url}/v1")}},"private_content_providers":if trusted {vec!["typesafe"]} else {vec![]},"llm_tools":{"advertise_tags":tags}}).to_string()).unwrap();
        let stub = broca_test_support::subc_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
        assert!(stub.exists(), "scope authority fixture missing");
        let scopes = root.join("scopes.json");
        std::fs::write(&scopes, json!([
            {"ref":"run","scope_epoch":1,"kind":"head","attributes":{"agent_id":"agent","run_id":"run"},"carriers":[{"principal":{"kind":"reserved","module_id":"basal"}}]},
            {"ref":"flow","scope_epoch":1,"kind":"head","attributes":{"flow_id":"tool-flow"},"carriers":[{"principal":{"kind":"reserved","module_id":"basal"}}]},
            {"ref":"other-run","scope_epoch":2,"kind":"head","attributes":{"agent_id":"agent","run_id":"other-run"}}
        ]).to_string()).unwrap();
        let mut modules = json!({});
        for id in ["owner", "basal", "cingulate"] {
            let mut env = json!({"FAKE_AFT_MODULE_ID":id,"FAKE_AFT_LAUNCH_NONCE_PATH":root.join(format!("{id}.nonce")),"FAKE_AFT_EVENTS_PATH":root.join(format!("{id}.events"))});
            if id == "owner" {
                env["FAKE_AFT_SCOPE_SYNC_PATH"] = json!(scopes);
                env["FAKE_AFT_EVENTS_PATH"] = json!(root.join("owner.events"));
            }
            modules[id] = json!({"program":broca_test_support::ckdev_binary(&stub, broca_test_support::scratch_root()),"args":[],"reserved":true,"enabled":true,"env":env});
        }
        let mut env = json!({"BROCA_STATE_ROOT":state,"BROCA_CONFIG_PATH":config,"BROCA_PROVIDER_BASE_URL":format!("{url}/v1"),"CK_LOG":"info"});
        if cut {
            env["BROCA_TEST_SYSTEM_ONE_ANSWER_GATE"] = json!(root.join("answer-cut"));
        }
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

    async fn open(&self, scoped: bool, basal: bool) -> Route {
        self.open_to("broca", scoped, basal).await
    }

    async fn open_to(&self, target: &str, scoped: bool, basal: bool) -> Route {
        let id = if basal { "basal" } else { "owner" };
        let nonce = self.root.join(format!("{id}.nonce"));
        broca_test_support::wait_until("caller nonce", || async {
            std::fs::read_to_string(&nonce).map_err(|e| e.to_string())
        })
        .await;
        let connection = Arc::new(
            SubcConnection::connect_with_consumer_identity(
                &self.root.join("runtime/subc-connection.json"),
                Some(subc_control::ConsumerIdentity {
                    module_id: id.into(),
                    launch_nonce: std::fs::read_to_string(nonce).unwrap().trim().into(),
                }),
            )
            .await
            .unwrap(),
        );
        let identity = broca_protocol::BindIdentity::new(
            self.root.join("project").canonicalize().unwrap(),
            "test",
            "tools",
        );
        let scope = scoped.then(|| {
            subc_protocol::scope::ScopeSelector::new(
                subc_protocol::Principal::Reserved {
                    module_id: "owner".into(),
                },
                "run",
                Some(1),
            )
        });
        let route = broca_test_support::wait_until("tool-provider route", || {
            let connection = connection.clone();
            let identity = identity.clone();
            let scope = scope.clone();
            async move {
                connection
                    .route_open_tool_provider_v1(target, &identity, scope)
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

    // Only the Unix cross-process claim test starts a second daemon.
    #[cfg(unix)]
    fn spawn_peer(&self, id: &str) -> broca_test_support::RigChild {
        let config = self.root.join("config/cortexkit/broca.jsonc");
        let settings: Value = serde_json::from_slice(&std::fs::read(&config).unwrap()).unwrap();
        let child = broca_test_support::tokio_rig_command(
            broca_test_support::ckdev_binary(
                Path::new(env!("CARGO_BIN_EXE_ck-broca")),
                broca_test_support::scratch_root(),
            ),
            &self.root,
            &format!("peer-{id}"),
        )
        .unwrap()
        .arg("--subc")
        .arg(self.root.join("runtime/subc-connection.json"))
        .env("SUBC_MODULE_ID", id)
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
        .kill_on_drop(true)
        .spawn()
        .unwrap();
        broca_test_support::RigChild::new(child, &self.root, &format!("peer-{id}"))
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_keyed_calls_share_execution_even_after_first_waiter_disconnects() {
    let Some(rig) = Rig::start("single-flight", true, true, Some(42_000_000), 0, false).await
    else {
        return;
    };
    rig.provider
        .blocked
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let first = Arc::new(rig.open(true, false).await);
    let waiter = {
        let first = first.clone();
        tokio::spawn(async move { first.call(args(), Some("single-flight")).await })
    };
    broca_test_support::wait_until("one provider call", || async {
        if rig.calls() == 1 {
            Ok(())
        } else {
            Err("not dispatched".to_string())
        }
    })
    .await;
    let second = Arc::new(rig.open(true, false).await);
    let follower = {
        let second = second.clone();
        tokio::spawn(async move { second.call(args(), Some("single-flight")).await })
    };
    waiter.abort();
    drop(first);
    rig.provider
        .blocked
        .store(false, std::sync::atomic::Ordering::SeqCst);
    rig.provider.hold.notify_waiters();
    let answer = tokio::time::timeout(Duration::from_secs(20), follower)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        second.call(args(), Some("single-flight")).await.unwrap(),
        answer
    );
    assert_eq!(rig.calls(), 1);
    assert_eq!(
        rows(&rig.state.join("run-index.db"), "export_facts").len(),
        1
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keyless_calls_are_new_work_and_known_zero_input_is_priced() {
    let Some(rig) = Rig::start("keyless", true, true, Some(0), 0, false).await else {
        return;
    };
    let route = rig.open(true, false).await;
    for _ in 0..2 {
        let result: Value =
            serde_json::from_slice(&route.call(args(), None).await.unwrap()).unwrap();
        assert_eq!(result["cost_usd"], 0);
    }
    assert_eq!(rig.calls(), 2);
    assert_eq!(rows(&rig.state.join("system-one.db"), "batch").len(), 2);
    assert_eq!(
        rows(&rig.state.join("run-index.db"), "export_facts").len(),
        2
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reported_overrun_is_returned_and_missing_input_cost_is_a_durable_failure() {
    let Some(rig) = Rig::start("cost", true, true, Some(42_000_000), 0, false).await else {
        return;
    };
    let route = rig.open(true, false).await;
    *rig.provider.usage.lock().unwrap() = Some(json!({"input_tokens":1_000_000,"output_tokens":2}));
    let mut bounded = args();
    bounded["max_cost_usd"] = json!(0.001);
    let answer = route.call(bounded.clone(), Some("overrun")).await.unwrap();
    let result: Value = serde_json::from_slice(&answer).unwrap();
    assert_eq!(result["cost_usd"], json!(0.042));
    assert_eq!(route.call(bounded, Some("overrun")).await.unwrap(), answer);
    assert_eq!(rig.calls(), 1);
    *rig.provider.usage.lock().unwrap() = Some(json!({"output_tokens":17}));
    let failure = route.call(args(), Some("unmeasured")).await.unwrap_err();
    assert_eq!(failure["code"], "cost_unmeasured");
    assert_eq!(failure["detail"]["class"], "permanent");
    assert!(failure.get("answers").is_none());
    assert_eq!(
        route.call(args(), Some("unmeasured")).await.unwrap_err(),
        failure
    );
    assert_eq!(rig.calls(), 2);
    let db = rusqlite::Connection::open_with_flags(
        rig.state.join("run-index.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let mut statement = db
        .prepare("SELECT segment_json FROM export_facts ORDER BY segment_json")
        .unwrap();
    let facts = statement
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .map(|row| serde_json::from_str::<Value>(&row.unwrap()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(facts.len(), 2);
    let unknown = facts
        .iter()
        .find(|fact| fact["usage"]["output_tokens"] == 17)
        .unwrap();
    assert!(unknown["usage"].get("input_tokens").is_none());
    assert!(unknown.get("cost_usd").is_none());
}

#[cfg(unix)]
async fn kill_broca(route: &Route) {
    let late: Value = serde_json::from_slice(
        &route
            .request("late_results", json!({"since":null}), None)
            .await
            .unwrap(),
    )
    .unwrap();
    let incarnation = late["cursor"]["provider_incarnation"].as_str().unwrap();
    let pid = incarnation.split('-').next().unwrap();
    assert!(std::process::Command::new("kill")
        .args(["-KILL", pid])
        .status()
        .unwrap()
        .success());
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_precedes_catalog_and_pricing_across_real_process_restarts() {
    let Some(rig) = Rig::start("catalog-replay", true, true, Some(42_000_000), 0, false).await
    else {
        return;
    };
    let mut route = rig.open(true, false).await;
    let first = route.call(args(), Some("frozen")).await.unwrap();
    for remove_row in [false, true] {
        let path = rig.state.join("catalog/fusiform-lkg.json");
        let mut catalog: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        if remove_row {
            catalog["models"].as_object_mut().unwrap().remove(MODEL);
        } else {
            catalog["models"][MODEL]
                .as_object_mut()
                .unwrap()
                .remove("rate.input");
        }
        std::fs::write(path, catalog.to_string()).unwrap();
        kill_broca(&route).await;
        route = rig.open(true, false).await;
        assert_eq!(route.call(args(), Some("frozen")).await.unwrap(), first);
        assert_eq!(rig.calls(), 1);
        assert_eq!(
            rows(&rig.state.join("run-index.db"), "export_facts").len(),
            1
        );
    }
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn provider_answer_crash_cut_retries_once_and_stores_one_outcome_and_fact() {
    let Some(rig) = Rig::start("answer-cut", true, true, Some(42_000_000), 0, true).await else {
        return;
    };
    let route = Arc::new(rig.open(true, false).await);
    let caller = {
        let route = route.clone();
        tokio::spawn(async move { route.call(args(), Some("cut")).await })
    };
    let marker = rig.root.join("answer-cut");
    broca_test_support::wait_until("provider answered before outcome write", || async {
        std::fs::read_to_string(&marker).map_err(|e| e.to_string())
    })
    .await;
    assert_eq!(rig.calls(), 1);
    assert!(rows(&rig.state.join("system-one.db"), "item_outcome").is_empty());
    assert!(rows(&rig.state.join("run-index.db"), "export_facts").is_empty());
    kill_broca(&route).await;
    caller.abort();
    let restarted = rig.open(true, false).await;
    let answer = restarted.call(args(), Some("cut")).await.unwrap();
    assert_eq!(rig.calls(), 2);
    assert_eq!(
        rows(&rig.state.join("system-one.db"), "item_outcome").len(),
        1
    );
    let facts = rows(&rig.state.join("run-index.db"), "export_facts");
    assert_eq!(facts.len(), 1);
    kill_broca(&restarted).await;
    let again = rig.open(true, false).await;
    assert_eq!(again.call(args(), Some("cut")).await.unwrap(), answer);
    assert_eq!(rig.calls(), 2);
    assert_eq!(rows(&rig.state.join("run-index.db"), "export_facts"), facts);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn published_classify_request_vectors_use_the_real_serve_path() {
    let Some(rig) = Rig::start("vectors", true, true, Some(0), 0, false).await else {
        return;
    };
    let route = rig.open(true, false).await;
    let vectors: Value = serde_json::from_str(cortexkit_llm_tools::vectors::CLASSIFY).unwrap();
    for vector in vectors["valid_requests"].as_array().unwrap() {
        let mut arguments = vector["value"].clone();
        arguments["model"] = json!(MODEL);
        let bytes = route
            .call(arguments, Some(vector["name"].as_str().unwrap()))
            .await
            .unwrap();
        let result: cortexkit_llm_tools::classify::Result = serde_json::from_slice(&bytes).unwrap();
        cortexkit_llm_tools::classify::validate_result(&result).unwrap();
    }
    for vector in vectors["invalid_requests"].as_array().unwrap() {
        let mut arguments = vector["value"].clone();
        if arguments.get("model").is_some() {
            arguments["model"] = json!(MODEL);
        }
        refused_without_writes(
            &rig,
            &route,
            arguments,
            vector["name"].as_str().unwrap(),
            "invalid_params",
        )
        .await;
    }
    assert_eq!(rig.calls(), 3);
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_process_claim_returns_transient_without_writes() {
    let Some(rig) = Rig::start("cross-process", true, true, Some(42_000_000), 0, false).await
    else {
        return;
    };
    let _owner_process = rig.spawn_peer("broca-owner-process");
    let first = Arc::new(rig.open_to("broca-owner-process", true, false).await);
    rig.provider
        .blocked
        .store(true, std::sync::atomic::Ordering::SeqCst);
    let executing = {
        let first = first.clone();
        tokio::spawn(async move { first.call(args(), Some("cross-process")).await })
    };
    broca_test_support::wait_until("first process provider dispatch", || async {
        if rig.calls() == 1 {
            Ok(())
        } else {
            Err("not dispatched".to_string())
        }
    })
    .await;
    let before = artifacts(&rig);
    let _later_process = rig.spawn_peer("broca-later-process");
    let second = rig.open_to("broca-later-process", true, false).await;
    let error = second
        .call(args(), Some("cross-process"))
        .await
        .unwrap_err();
    assert_eq!(error["code"], "batch_in_progress");
    assert_eq!(error["detail"]["class"], "transient");
    assert_eq!(error["detail"]["retry_after_ms"], 1000);
    assert_eq!(artifacts(&rig), before);
    executing.abort();
    kill_broca(&first).await;
    let recovery = broca_store::SystemOneStore::open(rig.state.join("system-one.db")).unwrap();
    broca_test_support::wait_until("dead owner lifetime lock release", || async {
        match recovery.release_abandoned().unwrap() {
            1 => Ok(()),
            0 => Err("owner has not exited yet".to_string()),
            count => panic!("unexpected abandoned claim count {count}"),
        }
    })
    .await;
    assert_eq!(recovery.release_abandoned().unwrap(), 0);
    rig.provider
        .blocked
        .store(false, std::sync::atomic::Ordering::SeqCst);
    rig.provider.hold.notify_waiters();
    let answer = second.call(args(), Some("cross-process")).await.unwrap();
    assert_eq!(
        second.call(args(), Some("cross-process")).await.unwrap(),
        answer
    );
    assert_eq!(rig.calls(), 2);
    assert_eq!(
        rows(&rig.state.join("system-one.db"), "item_outcome").len(),
        1
    );
    assert_eq!(
        rows(&rig.state.join("run-index.db"), "export_facts").len(),
        1
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn effective_subscription_override_is_unpriceable() {
    let Some(rig) = Rig::start("subscription", true, true, Some(42_000_000), 0, false).await else {
        return;
    };
    let route = rig.open(true, false).await;
    let path = rig.root.join("config/cortexkit/broca.jsonc");
    let mut config: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    config["charge_basis"] = json!({"typesafe:apikey":"subscription_included"});
    std::fs::write(path, config.to_string()).unwrap();
    kill_broca(&route).await;
    let restarted = rig.open(true, false).await;
    refused_without_writes(&rig, &restarted, args(), "subscription", "cost_unpriceable").await;
}

struct FixedCatalog;
impl broca_module_serve::system_one::BatchCatalog for FixedCatalog {
    fn row(&self, _: &str) -> Option<broca_catalog::ModelBatchRow> {
        Some(broca_catalog::ModelBatchRow {
            kind: Some("decision".into()),
            context_tokens: None,
            attachment: false,
            input_rate_nanos: Some(42_000_000),
            output_rate_nanos: Some(0),
        })
    }
}

#[derive(Default)]
struct CountedProvider(std::sync::atomic::AtomicUsize);
#[async_trait::async_trait]
impl broca_module_serve::system_one::ClassifierProvider for CountedProvider {
    fn egress_host(&self) -> String {
        "localhost".into()
    }
    fn credential_id(&self) -> Option<String> {
        None
    }
    async fn call(&self, _: String) -> broca_module_serve::system_one::ProviderReply {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        broca_module_serve::system_one::ProviderReply::Answered(br#"{"model":"jev-1.13.0","answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":100}}"#.to_vec())
    }
}

fn stamped(epoch: u64, owner: &str) -> broca_protocol::SendScope {
    broca_protocol::SendScope::Stamped(broca_protocol::BindScope {
        scope: broca_protocol::SessionScope {
            owner: broca_protocol::BindPrincipal::Reserved {
                module_id: owner.into(),
            },
            scope_ref: "run".into(),
            scope_epoch: epoch,
            flow_id: None,
            sender: None,
            agent_id: Some("agent".into()),
            run_id: Some("run".into()),
        },
        owner_authorized: true,
    })
}

fn direct_tools(
    clock: Arc<std::sync::atomic::AtomicI64>,
    provider: Arc<CountedProvider>,
    store: Arc<broca_store::SystemOneStore>,
    export: Arc<broca_store::SqliteStore>,
) -> Arc<broca_module_serve::llm_tools::LlmTools> {
    use broca_module_serve::system_one::{Config, ModelPin, SystemOne, LLM_CLASSIFY_STORE_ROLE};
    let system = SystemOne::new(
        store,
        "clock-test",
        Arc::new(FixedCatalog),
        vec![ModelPin::jev(provider)],
        Config::default(),
        Arc::default(),
    )
    .with_store_role(LLM_CLASSIFY_STORE_ROLE)
    .with_clock(Arc::new(move || {
        clock.load(std::sync::atomic::Ordering::SeqCst)
    }))
    .with_export(
        export,
        Arc::new(broca_module_serve::ChargeBasisTable::shipped_defaults()),
    );
    Arc::new(broca_module_serve::llm_tools::LlmTools::new(
        Arc::new(system),
        broca_module_serve::config::LlmToolsConfig {
            advertise_tags: true,
            private_content_providers: vec!["typesafe".into()],
        },
    ))
}

fn direct_body(arguments: Value, key: Option<&str>) -> Vec<u8> {
    let mut body = json!({"name":"llm.classify/v1","arguments":arguments});
    if let Some(key) = key {
        body["call_key"] = json!(key);
    }
    serde_json::to_vec(&body).unwrap()
}

fn owner() -> broca_protocol::BindPrincipal {
    broca_protocol::BindPrincipal::Reserved {
        module_id: "owner".into(),
    }
}

#[tokio::test]
async fn memory_scope_guards_run_before_argument_and_catalog_admission() {
    let provider = Arc::new(CountedProvider::default());
    let tools = direct_tools(
        Arc::new(std::sync::atomic::AtomicI64::new(1000)),
        provider.clone(),
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        Arc::new(broca_store::SqliteStore::open_in_memory().unwrap()),
    );
    let basal = broca_protocol::BindPrincipal::Reserved {
        module_id: "basal".into(),
    };
    let body = direct_body(json!({}), Some("scope"));
    for scope in [broca_protocol::SendScope::default(), stamped(1, "owner")] {
        let error: Value =
            serde_json::from_slice(&tools.answer(&body, Some(&basal), &scope).await.unwrap_err())
                .unwrap();
        assert_eq!(error["code"], "flow_scope_required");
    }
    let error: Value = serde_json::from_slice(
        &tools
            .answer(&body, Some(&owner()), &broca_protocol::SendScope::default())
            .await
            .unwrap_err(),
    )
    .unwrap();
    assert_eq!(error["code"], "scope_required");
    let mut missing_run = stamped(1, "owner");
    if let broca_protocol::SendScope::Stamped(bind) = &mut missing_run {
        bind.scope.run_id = None;
    }
    for scope in [broca_protocol::SendScope::default(), missing_run] {
        let failure: Value = serde_json::from_slice(
            &tools
                .answer(
                    &direct_body(args(), Some("valid-unscoped")),
                    Some(&owner()),
                    &scope,
                )
                .await
                .unwrap_err(),
        )
        .unwrap();
        assert_eq!(failure["code"], "scope_required");
    }
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn memory_unknown_argument_refusal_names_the_field_before_any_execution() {
    let provider = Arc::new(CountedProvider::default());
    let tools = direct_tools(
        Arc::new(std::sync::atomic::AtomicI64::new(1000)),
        provider.clone(),
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        Arc::new(broca_store::SqliteStore::open_in_memory().unwrap()),
    );
    let mut arguments = args();
    arguments["injected"] = json!(true);
    let error: Value = serde_json::from_slice(
        &tools
            .answer(
                &direct_body(arguments, Some("unknown")),
                Some(&owner()),
                &stamped(1, "owner"),
            )
            .await
            .unwrap_err(),
    )
    .unwrap();
    assert_eq!(error["code"], "invalid_params");
    assert_eq!(error["detail"]["field"], "injected");
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 0);
}

struct ChangingCatalog(Mutex<Option<broca_catalog::ModelBatchRow>>);
impl broca_module_serve::system_one::BatchCatalog for ChangingCatalog {
    fn row(&self, _: &str) -> Option<broca_catalog::ModelBatchRow> {
        self.0.lock().unwrap().clone()
    }
}

fn tools_with_catalog(
    catalog: Arc<ChangingCatalog>,
    provider: Arc<CountedProvider>,
    allow: bool,
) -> Arc<broca_module_serve::llm_tools::LlmTools> {
    use broca_module_serve::system_one::{Config, ModelPin, SystemOne, LLM_CLASSIFY_STORE_ROLE};
    let system = SystemOne::new(
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        "memory",
        catalog,
        vec![ModelPin::jev(provider)],
        Config::default(),
        Arc::default(),
    )
    .with_store_role(LLM_CLASSIFY_STORE_ROLE);
    Arc::new(broca_module_serve::llm_tools::LlmTools::new(
        Arc::new(system),
        broca_module_serve::config::LlmToolsConfig {
            advertise_tags: false,
            private_content_providers: if allow {
                vec!["typesafe".into()]
            } else {
                vec![]
            },
        },
    ))
}

#[tokio::test]
async fn memory_frozen_replay_does_not_depend_on_a_live_catalog_row() {
    use broca_module_serve::system_one::BatchCatalog;
    let catalog = Arc::new(ChangingCatalog(Mutex::new(FixedCatalog.row(MODEL))));
    let provider = Arc::new(CountedProvider::default());
    let tools = tools_with_catalog(catalog.clone(), provider.clone(), true);
    let body = direct_body(args(), Some("frozen"));
    let first = tools
        .answer(&body, Some(&owner()), &stamped(1, "owner"))
        .await
        .unwrap();
    *catalog.0.lock().unwrap() = None;
    assert_eq!(
        tools
            .answer(&body, Some(&owner()), &stamped(1, "owner"))
            .await
            .unwrap()
            .get(),
        first.get()
    );
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn memory_tool_pricing_rejects_missing_input_and_positive_output_rates() {
    use broca_module_serve::system_one::BatchCatalog;
    for missing in [false, true] {
        let mut row = FixedCatalog.row(MODEL).unwrap();
        if missing {
            row.input_rate_nanos = None;
        } else {
            row.output_rate_nanos = Some(1);
        }
        let provider = Arc::new(CountedProvider::default());
        let tools = tools_with_catalog(
            Arc::new(ChangingCatalog(Mutex::new(Some(row)))),
            provider.clone(),
            true,
        );
        let error: Value = serde_json::from_slice(
            &tools
                .answer(
                    &direct_body(args(), Some("price")),
                    Some(&owner()),
                    &stamped(1, "owner"),
                )
                .await
                .unwrap_err(),
        )
        .unwrap();
        assert_eq!(error["code"], "cost_unpriceable");
        assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn memory_private_unknown_provider_is_not_dispatched() {
    use broca_module_serve::system_one::BatchCatalog;
    let provider = Arc::new(CountedProvider::default());
    let tools = tools_with_catalog(
        Arc::new(ChangingCatalog(Mutex::new(FixedCatalog.row(MODEL)))),
        provider.clone(),
        false,
    );
    let error: Value = serde_json::from_slice(
        &tools
            .answer(
                &direct_body(args(), Some("private")),
                Some(&owner()),
                &stamped(1, "owner"),
            )
            .await
            .unwrap_err(),
    )
    .unwrap();
    assert_eq!(error["code"], "not_started");
    assert_eq!(error["detail"]["consent"], "private_content_unknown");
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn memory_final_replay_checks_argument_digest_and_scope_before_returning_data() {
    let path = broca_test_support::scratch_root()
        .join(format!("tool-replay-scope-{}.db", std::process::id()));
    let store = Arc::new(broca_store::SystemOneStore::open(&path).unwrap());
    let provider = Arc::new(CountedProvider::default());
    let tools = direct_tools(
        Arc::new(std::sync::atomic::AtomicI64::new(1000)),
        provider,
        store,
        Arc::new(broca_store::SqliteStore::open_in_memory().unwrap()),
    );
    let body = direct_body(args(), Some("replay-guards"));
    tools
        .answer(&body, Some(&owner()), &stamped(1, "owner"))
        .await
        .unwrap();
    let mut changed = args();
    changed["max_cost_usd"] = json!(0.02);
    let error: Value = serde_json::from_slice(
        &tools
            .answer(
                &direct_body(changed, Some("replay-guards")),
                Some(&owner()),
                &stamped(1, "owner"),
            )
            .await
            .unwrap_err(),
    )
    .unwrap();
    assert_eq!(error["code"], "batch_id_reuse");
    let db = rusqlite::Connection::open(path).unwrap();
    db.execute("UPDATE batch SET scope_json=NULL", []).unwrap();
    let error: Value = serde_json::from_slice(
        &tools
            .answer(&body, Some(&owner()), &stamped(1, "owner"))
            .await
            .unwrap_err(),
    )
    .unwrap();
    assert_eq!(error["code"], "batch_scope_mismatch");
}

#[tokio::test]
async fn memory_catalog_tag_switch_is_opt_in_and_preserves_invocation_modes() {
    use broca_module_serve::system_one::{Config, ModelPin, SystemOne, LLM_CLASSIFY_STORE_ROLE};
    for advertise_tags in [false, true] {
        let system = SystemOne::new(
            Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
            "tags",
            Arc::new(FixedCatalog),
            vec![ModelPin::jev(Arc::new(CountedProvider::default()))],
            Config::default(),
            Arc::default(),
        )
        .with_store_role(LLM_CLASSIFY_STORE_ROLE);
        let tools = Arc::new(broca_module_serve::llm_tools::LlmTools::new(
            Arc::new(system),
            broca_module_serve::config::LlmToolsConfig {
                advertise_tags,
                ..Default::default()
            },
        ));
        let body = json!({"name":"tool.catalog","arguments":{"params":{}}}).to_string();
        let answer = tools
            .answer(body.as_bytes(), None, &broca_protocol::SendScope::default())
            .await
            .unwrap();
        let catalog: Value = serde_json::from_str(answer.get()).unwrap();
        assert_eq!(
            catalog["tools"][0]["capabilities"],
            if advertise_tags {
                json!(["llm.classify/v1"])
            } else {
                json!([])
            }
        );
        assert_eq!(catalog["tools"][0]["modes"], json!(["codemode", "flow"]));
        assert_eq!(catalog["capabilities"]["late_results"], true);
    }
}

struct MissingInputProvider(std::sync::atomic::AtomicUsize);
#[async_trait::async_trait]
impl broca_module_serve::system_one::ClassifierProvider for MissingInputProvider {
    fn egress_host(&self) -> String {
        "localhost".into()
    }
    fn credential_id(&self) -> Option<String> {
        None
    }
    async fn call(&self, _: String) -> broca_module_serve::system_one::ProviderReply {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        broca_module_serve::system_one::ProviderReply::Answered(br#"{"model":"jev-1.13.0","answers":{"q":{"type":"noul","noul":0.5}},"usage":{"output_tokens":17}}"#.to_vec())
    }
}

#[tokio::test]
async fn memory_unmeasured_cost_is_a_stored_permanent_failure_without_an_answer() {
    use broca_module_serve::system_one::{Config, ModelPin, SystemOne, LLM_CLASSIFY_STORE_ROLE};
    use sha2::{Digest, Sha256};
    let store = Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap());
    let provider = Arc::new(MissingInputProvider(std::sync::atomic::AtomicUsize::new(0)));
    let system = SystemOne::new(
        store.clone(),
        "unmeasured",
        Arc::new(FixedCatalog),
        vec![ModelPin::jev(provider.clone())],
        Config::default(),
        Arc::default(),
    )
    .with_store_role(LLM_CLASSIFY_STORE_ROLE);
    let tools = Arc::new(broca_module_serve::llm_tools::LlmTools::new(
        Arc::new(system),
        broca_module_serve::config::LlmToolsConfig {
            advertise_tags: true,
            private_content_providers: vec!["typesafe".into()],
        },
    ));
    let body = direct_body(args(), Some("opaque"));
    let first = tools
        .answer(&body, Some(&owner()), &stamped(1, "owner"))
        .await
        .unwrap_err();
    let failure: Value = serde_json::from_slice(&first).unwrap();
    assert_eq!(
        failure["code"],
        cortexkit_llm_tools::errors::COST_UNMEASURED
    );
    assert_eq!(failure["detail"]["class"], "permanent");
    assert!(failure.get("answers").is_none());
    assert!(failure["message"]
        .as_str()
        .unwrap()
        .contains("max_cost_usd"));
    assert_eq!(
        tools
            .answer(&body, Some(&owner()), &stamped(1, "owner"))
            .await
            .unwrap_err(),
        first
    );
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    let id=format!("{:x}",Sha256::digest(br#"{"call_key":"opaque","owner":"reserved:owner","principal":"reserved:owner","scope_epoch":1,"scope_ref":"run"}"#));
    let recorded = store.lookup(LLM_CLASSIFY_STORE_ROLE, &id).unwrap().unwrap();
    assert!(
        matches!(&recorded.outcomes[0].outcome,broca_store::system_one::Outcome::Failed(body) if body.contains("cost_unmeasured"))
    );
    assert_eq!(recorded.outcomes[0].input_tokens, None);
    assert_eq!(recorded.outcomes[0].output_tokens, Some(17));
}

struct GatedProvider {
    calls: std::sync::atomic::AtomicUsize,
    gate: tokio::sync::Semaphore,
}
#[async_trait::async_trait]
impl broca_module_serve::system_one::ClassifierProvider for GatedProvider {
    fn egress_host(&self) -> String {
        "localhost".into()
    }
    fn credential_id(&self) -> Option<String> {
        None
    }
    async fn call(&self, _: String) -> broca_module_serve::system_one::ProviderReply {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let _permit = self.gate.acquire().await.unwrap();
        broca_module_serve::system_one::ProviderReply::Answered(br#"{"model":"jev-1.13.0","answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":100}}"#.to_vec())
    }
}

#[tokio::test]
async fn memory_in_process_single_flight_waiters_receive_the_same_answer() {
    use broca_module_serve::system_one::{Config, ModelPin, SystemOne, LLM_CLASSIFY_STORE_ROLE};
    let provider = Arc::new(GatedProvider {
        calls: std::sync::atomic::AtomicUsize::new(0),
        gate: tokio::sync::Semaphore::new(0),
    });
    let system = SystemOne::new(
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        "single-flight",
        Arc::new(FixedCatalog),
        vec![ModelPin::jev(provider.clone())],
        Config::default(),
        Arc::default(),
    )
    .with_store_role(LLM_CLASSIFY_STORE_ROLE);
    let tools = Arc::new(broca_module_serve::llm_tools::LlmTools::new(
        Arc::new(system),
        broca_module_serve::config::LlmToolsConfig {
            advertise_tags: true,
            private_content_providers: vec!["typesafe".into()],
        },
    ));
    let invoke = |tools: Arc<broca_module_serve::llm_tools::LlmTools>| {
        tokio::spawn(async move {
            tools
                .answer(
                    &direct_body(args(), Some("single-flight")),
                    Some(&owner()),
                    &stamped(1, "owner"),
                )
                .await
        })
    };
    let first = invoke(tools.clone());
    broca_test_support::wait_until("single-flight provider dispatch", || async {
        if provider.calls.load(std::sync::atomic::Ordering::SeqCst) > 0 {
            Ok(())
        } else {
            Err("provider not dispatched".to_string())
        }
    })
    .await;
    let mut second = invoke(tools);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut second)
            .await
            .is_err(),
        "a concurrent keyed caller must wait, not receive batch_in_progress"
    );
    provider.gate.add_permits(1);
    let first = tokio::time::timeout(Duration::from_secs(5), first)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let second = tokio::time::timeout(Duration::from_secs(5), second)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.get(), second.get());
    assert_eq!(provider.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn memory_keyless_calls_get_random_independent_batches() {
    let provider = Arc::new(CountedProvider::default());
    let tools = direct_tools(
        Arc::new(std::sync::atomic::AtomicI64::new(1000)),
        provider.clone(),
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        Arc::new(broca_store::SqliteStore::open_in_memory().unwrap()),
    );
    let body = direct_body(args(), None);
    for _ in 0..2 {
        tools
            .answer(&body, Some(&owner()), &stamped(1, "owner"))
            .await
            .unwrap();
    }
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[tokio::test]
async fn memory_cost_ceiling_prevents_provider_execution() {
    let provider = Arc::new(CountedProvider::default());
    let tools = direct_tools(
        Arc::new(std::sync::atomic::AtomicI64::new(1000)),
        provider.clone(),
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        Arc::new(broca_store::SqliteStore::open_in_memory().unwrap()),
    );
    let mut arguments = args();
    arguments["max_cost_usd"] = json!(0);
    let failure: Value = serde_json::from_slice(
        &tools
            .answer(
                &direct_body(arguments, Some("ceiling")),
                Some(&owner()),
                &stamped(1, "owner"),
            )
            .await
            .unwrap_err(),
    )
    .unwrap();
    assert_eq!(failure["code"], "cost_exceeded");
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn memory_schema_pins_are_checked_before_provider_execution() {
    let provider = Arc::new(CountedProvider::default());
    let tools = direct_tools(
        Arc::new(std::sync::atomic::AtomicI64::new(1000)),
        provider.clone(),
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        Arc::new(broca_store::SqliteStore::open_in_memory().unwrap()),
    );
    let digest = cortexkit_role_tool_provider::catalog::schema_digest(
        &cortexkit_llm_tools::classify::input_schema(),
    )
    .unwrap();
    for (digest, semantics, code) in [
        ("0".repeat(64), 1, "tool_schema_changed"),
        (digest, 2, "tool_semantics_changed"),
    ] {
        let pin = cortexkit_role_tool_provider::call::SchemaPin::new(
            "llm.classify/v1",
            digest,
            semantics,
        )
        .encode()
        .unwrap();
        let body =
            json!({"name":"llm.classify/v1","arguments":args(),"call_key":"pin","schema_pin":pin})
                .to_string();
        let failure: Value = serde_json::from_slice(
            &tools
                .answer(body.as_bytes(), Some(&owner()), &stamped(1, "owner"))
                .await
                .unwrap_err(),
        )
        .unwrap();
        assert_eq!(failure["code"], code);
    }
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 0);
}

#[tokio::test]
async fn memory_late_results_refuse_foreign_and_future_cursors() {
    let tools = direct_tools(
        Arc::new(std::sync::atomic::AtomicI64::new(1000)),
        Arc::new(CountedProvider::default()),
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        Arc::new(broca_store::SqliteStore::open_in_memory().unwrap()),
    );
    for cursor in [
        json!({"provider_incarnation":"foreign","seq":0}),
        json!({"provider_incarnation":broca_module_serve::serve::process_launch_id(),"seq":1}),
    ] {
        for (name, field) in [("late_results", "since"), ("late_results.ack", "through")] {
            let body = json!({"name":name,"arguments":{field:cursor}}).to_string();
            let failure: Value = serde_json::from_slice(
                &tools
                    .answer(body.as_bytes(), Some(&owner()), &stamped(1, "owner"))
                    .await
                    .unwrap_err(),
            )
            .unwrap();
            assert_eq!(failure["code"], "cursor_incarnation_changed");
        }
    }
}

#[tokio::test]
async fn memory_seven_day_boundary_replays_before_and_bills_at_expiry() {
    let clock = Arc::new(std::sync::atomic::AtomicI64::new(1000));
    let provider = Arc::new(CountedProvider::default());
    let store = Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap());
    let export = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
    let tools = direct_tools(
        clock.clone(),
        provider.clone(),
        store.clone(),
        export.clone(),
    );
    let body =
        json!({"name":"llm.classify/v1","arguments":args(),"call_key":"boundary"}).to_string();
    let principal = broca_protocol::BindPrincipal::Reserved {
        module_id: "owner".into(),
    };
    let first = tools
        .answer(body.as_bytes(), Some(&principal), &stamped(1, "owner"))
        .await
        .unwrap();
    clock.store(
        1000 + broca_store::system_one::RETENTION_MS - 1,
        std::sync::atomic::Ordering::SeqCst,
    );
    assert_eq!(
        tools
            .answer(body.as_bytes(), Some(&principal), &stamped(1, "owner"))
            .await
            .unwrap()
            .get(),
        first.get()
    );
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert_eq!(export.read_export_page(None, 100).unwrap().facts.len(), 1);
    clock.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    tools
        .answer(body.as_bytes(), Some(&principal), &stamped(1, "owner"))
        .await
        .unwrap();
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(export.read_export_page(None, 100).unwrap().facts.len(), 2);
}

#[tokio::test]
async fn memory_derived_keys_separate_principals_owners_and_scope_epochs_and_store_argument_digest()
{
    use sha2::{Digest, Sha256};
    let store = Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap());
    let provider = Arc::new(CountedProvider::default());
    let tools = direct_tools(
        Arc::new(std::sync::atomic::AtomicI64::new(1000)),
        provider.clone(),
        store.clone(),
        Arc::new(broca_store::SqliteStore::open_in_memory().unwrap()),
    );
    let body = json!({"name":"llm.classify/v1","arguments":args(),"call_key":"opaque"}).to_string();
    for (caller, owner, epoch) in [
        ("owner", "owner", 1),
        ("other", "owner", 1),
        ("owner", "another", 1),
        ("owner", "owner", 2),
    ] {
        let principal = broca_protocol::BindPrincipal::Reserved {
            module_id: caller.into(),
        };
        tools
            .answer(body.as_bytes(), Some(&principal), &stamped(epoch, owner))
            .await
            .unwrap();
    }
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 4);
    let id = format!("{:x}",Sha256::digest(br#"{"call_key":"opaque","owner":"reserved:owner","principal":"reserved:owner","scope_epoch":1,"scope_ref":"run"}"#));
    let recorded = store
        .lookup("llm.classify", &id)
        .unwrap()
        .expect("JCS-derived batch id");
    let digest = format!("{:x}",Sha256::digest(br#"{"item":{"state":"invented"},"max_cost_usd":0.01,"model":"typesafe/jev-1.13.0","questions":{"q":{"instructions":"Is the invented note urgent?","type":"noul"}}}"#));
    assert_eq!(recorded.args_digest, digest);
}

struct Route {
    connection: Arc<SubcConnection>,
    route: RouteHandle,
}

impl Route {
    async fn request(
        &self,
        name: &str,
        arguments: Value,
        key: Option<&str>,
    ) -> Result<Vec<u8>, Value> {
        let mut body = json!({"name":name,"arguments":arguments});
        if let Some(key) = key {
            body["call_key"] = json!(key);
        }
        self.connection
            .route_request_terminal_with_deadline(
                &self.route,
                serde_json::to_vec(&body).unwrap(),
                Duration::from_secs(20),
            )
            .await
            .unwrap()
            .map_err(|bytes| serde_json::from_slice(&bytes).unwrap())
    }
    async fn call(&self, arguments: Value, key: Option<&str>) -> Result<Vec<u8>, Value> {
        self.request("llm.classify/v1", arguments, key).await
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

/// Compare persisted database contents, including empty tables and absent grant storage.
/// Row counts alone would miss a refusal that silently tightened or rewrote an existing row.
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
    let sessions = rig.state.join("sessions");
    value["sessions"] = json!(std::fs::read_dir(sessions)
        .map(|entries| entries.count())
        .unwrap_or(0));
    let requests = std::fs::read_to_string(rig.root.join("cingulate.events")).unwrap_or_default();
    value["consent_requests"] = json!(requests
        .lines()
        .filter(|line| line.contains("consent.request"))
        .count());
    value
}

async fn refused_without_writes(
    rig: &Rig,
    route: &Route,
    arguments: Value,
    key: &str,
    expected: &str,
) -> Value {
    let before = artifacts(rig);
    let calls = rig.calls();
    let error = route.call(arguments, Some(key)).await.unwrap_err();
    assert_eq!(error["code"], expected, "{error}");
    assert_eq!(error["detail"]["class"], "permanent", "{error}");
    assert_eq!(
        artifacts(rig),
        before,
        "{expected} changed a durable artifact"
    );
    assert_eq!(rig.calls(), calls, "{expected} called the provider");
    error
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catalog_and_every_declared_tool_operation_answer_on_real_daemon() {
    let Some(rig) = Rig::start("catalog", true, true, Some(42_000_000), 0, false).await else {
        return;
    };
    let route = rig.open(true, false).await;
    let describe: Value = serde_json::from_slice(
        &route
            .request("role.describe", json!({}), None)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(describe["majors"][0]["version"], "tool-provider/v1");
    let catalog: Value = serde_json::from_slice(
        &route
            .request("tool.catalog", json!({"params":{}}), None)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(catalog["tools"][0]["name"], "llm.classify/v1");
    assert_eq!(
        catalog["tools"][0]["input_schema"],
        cortexkit_llm_tools::classify::input_schema()
    );
    assert_eq!(catalog["tools"][0]["modes"], json!(["codemode", "flow"]));
    assert_eq!(
        catalog["tools"][0]["capabilities"],
        json!(["llm.classify/v1"])
    );
    let decoded: cortexkit_role_tool_provider::catalog::CatalogAnswer =
        serde_json::from_value(catalog.clone()).unwrap();
    assert!(
        decoded.declares(cortexkit_role_tool_provider::catalog::session_capabilities::LATE_RESULTS)
    );
    let late: Value = serde_json::from_slice(
        &route
            .request("late_results", json!({"since":null}), None)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(late["entries"], json!([]));
    assert_eq!(
        route
            .request("late_results.ack", json!({"through":late["cursor"]}), None)
            .await
            .unwrap(),
        b"{}"
    );
    let call = route.call(args(), Some("first")).await.unwrap();
    let result: cortexkit_llm_tools::classify::Result = serde_json::from_slice(&call).unwrap();
    cortexkit_llm_tools::classify::validate_result(&result).unwrap();
    assert!(result.cost_usd.as_f64().unwrap() > 0.0);
    for name in ["session.send", "classify.run", "permissions.list"] {
        assert_eq!(
            route.request(name, json!({}), None).await.unwrap_err()["code"],
            "unknown_method"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tags_are_absent_when_advertise_tags_is_off() {
    let Some(rig) = Rig::start("tags-off", false, true, Some(42_000_000), 0, false).await else {
        return;
    };
    let route = rig.open(true, false).await;
    let catalog: Value = serde_json::from_slice(
        &route
            .request("tool.catalog", json!({"params":{}}), None)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(catalog["tools"][0]["capabilities"], json!([]));
    assert!(!broca_module_serve::config::LlmToolsConfig::default().advertise_tags);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scope_and_argument_refusals_precede_all_writes() {
    let Some(rig) = Rig::start("refusals", true, true, Some(42_000_000), 0, false).await else {
        return;
    };
    let unscoped = rig.open(false, false).await;
    refused_without_writes(&rig, &unscoped, json!({}), "no-scope", "scope_required").await;
    for scoped in [false, true] {
        let basal = rig.open(scoped, true).await;
        refused_without_writes(&rig, &basal, json!({}), "basal", "flow_scope_required").await;
    }
    let route = rig.open(true, false).await;
    for (field, value) in [("extra", json!(1)), ("max_cost_usd", json!(-1))] {
        let mut arguments = args();
        arguments[field] = value;
        let error = refused_without_writes(&rig, &route, arguments, field, "invalid_params").await;
        assert_eq!(error["detail"]["field"], field);
    }
    let mut arguments = args();
    arguments.as_object_mut().unwrap().remove("max_cost_usd");
    let error =
        refused_without_writes(&rig, &route, arguments, "missing-ceiling", "invalid_params").await;
    assert_eq!(error["detail"]["field"], "max_cost_usd");
    let mut arguments = args();
    arguments["max_cost_usd"] = json!(0);
    refused_without_writes(&rig, &route, arguments, "ceiling", "cost_exceeded").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unpriced_and_output_priced_models_are_refused_without_artifacts() {
    for (label, input, output) in [
        ("unpriced", None, 0),
        ("output-priced", Some(42_000_000), 42_000_000),
    ] {
        let Some(rig) = Rig::start(label, true, true, input, output, false).await else {
            return;
        };
        let route = rig.open(true, false).await;
        refused_without_writes(&rig, &route, args(), "unpriceable", "cost_unpriceable").await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unknown_private_content_never_executes_an_unlisted_provider() {
    let Some(rig) = Rig::start("private", true, false, Some(42_000_000), 0, false).await else {
        return;
    };
    let route = rig.open(true, false).await;
    let error = refused_without_writes(&rig, &route, args(), "private", "not_started").await;
    assert_eq!(error["detail"]["consent"], "private_content_unknown");
    assert_eq!(
        route.call(args(), None).await.unwrap_err()["detail"]["consent"],
        "private_content_unknown"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replay_is_byte_identical_and_argument_reuse_is_write_free() {
    let Some(rig) = Rig::start("replay", true, true, Some(42_000_000), 0, false).await else {
        return;
    };
    let route = rig.open(true, false).await;
    let first = route
        .call(args(), Some("opaque-key/not-hex"))
        .await
        .unwrap();
    let before = artifacts(&rig);
    assert_eq!(
        route
            .call(args(), Some("opaque-key/not-hex"))
            .await
            .unwrap(),
        first
    );
    assert_eq!(artifacts(&rig), before);
    assert_eq!(rig.calls(), 1);
    for field in ["item", "questions", "max_cost_usd"] {
        let mut changed = args();
        changed[field] = match field {
            "item" => json!({"state":"changed"}),
            "questions" => json!({"different":{"type":"noul","instructions":"different"}}),
            _ => json!(0.02),
        };
        refused_without_writes(
            &rig,
            &route,
            changed,
            "opaque-key/not-hex",
            "batch_id_reuse",
        )
        .await;
    }
    let db = rusqlite::Connection::open(rig.state.join("system-one.db")).unwrap();
    db.execute(
        "UPDATE batch SET scope_json=NULL WHERE role='llm.classify'",
        [],
    )
    .unwrap();
    refused_without_writes(
        &rig,
        &route,
        args(),
        "opaque-key/not-hex",
        "batch_scope_mismatch",
    )
    .await;
}

struct TransientThenAnswer(std::sync::atomic::AtomicUsize);
#[async_trait::async_trait]
impl broca_module_serve::system_one::ClassifierProvider for TransientThenAnswer {
    fn egress_host(&self) -> String {
        "localhost".into()
    }
    fn credential_id(&self) -> Option<String> {
        None
    }
    async fn call(&self, _: String) -> broca_module_serve::system_one::ProviderReply {
        if self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
            broca_module_serve::system_one::ProviderReply::Status {
                status: 500,
                retry_after_ms: None,
            }
        } else {
            broca_module_serve::system_one::ProviderReply::Answered(br#"{"model":"jev-1.13.0","answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":100}}"#.to_vec())
        }
    }
}

#[tokio::test]
async fn memory_nonfinal_transient_outcome_resumes_under_its_frozen_admission() {
    use broca_module_serve::system_one::{
        BatchCatalog, Config, ModelPin, SystemOne, LLM_CLASSIFY_STORE_ROLE,
    };
    let catalog = Arc::new(ChangingCatalog(Mutex::new(FixedCatalog.row(MODEL))));
    let provider = Arc::new(TransientThenAnswer(std::sync::atomic::AtomicUsize::new(0)));
    let system = SystemOne::new(
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        "transient",
        catalog.clone(),
        vec![ModelPin::jev(provider.clone())],
        Config {
            max_attempts: 1,
            ..Default::default()
        },
        Arc::default(),
    )
    .with_store_role(LLM_CLASSIFY_STORE_ROLE);
    let tools = Arc::new(broca_module_serve::llm_tools::LlmTools::new(
        Arc::new(system),
        broca_module_serve::config::LlmToolsConfig {
            advertise_tags: true,
            private_content_providers: vec!["typesafe".into()],
        },
    ));
    let body = direct_body(args(), Some("transient"));
    let failure: Value = serde_json::from_slice(
        &tools
            .answer(&body, Some(&owner()), &stamped(1, "owner"))
            .await
            .unwrap_err(),
    )
    .unwrap();
    assert_eq!(failure["code"], "provider_error");
    assert_eq!(failure["detail"]["class"], "transient");
    *catalog.0.lock().unwrap() = None;
    let answer = tools
        .answer(&body, Some(&owner()), &stamped(1, "owner"))
        .await
        .unwrap();
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(
        tools
            .answer(&body, Some(&owner()), &stamped(1, "owner"))
            .await
            .unwrap()
            .get(),
        answer.get()
    );
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 2);
}

#[derive(Default)]
struct RawProvider {
    payload: Mutex<Option<String>>,
    calls: std::sync::atomic::AtomicUsize,
}
#[async_trait::async_trait]
impl broca_module_serve::system_one::ClassifierProvider for RawProvider {
    fn egress_host(&self) -> String {
        "localhost".into()
    }
    fn credential_id(&self) -> Option<String> {
        None
    }
    async fn call(&self, body: String) -> broca_module_serve::system_one::ProviderReply {
        *self.payload.lock().unwrap() = Some(body);
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        broca_module_serve::system_one::ProviderReply::Answered(br#"{"model":"jev-1.13.0","answers":{"z":{"type":"noul","noul":0.12345678901234567890123456789,"provider_extra":{"n":123456789012345678901234567890}},"a":{"type":"noul","noul":0.5}},"usage":{"input_tokens":100}}"#.to_vec())
    }
}

#[tokio::test]
async fn memory_question_order_and_raw_json_survive_the_tool_adapter() {
    use broca_module_serve::system_one::{Config, ModelPin, SystemOne, LLM_CLASSIFY_STORE_ROLE};
    let provider = Arc::new(RawProvider::default());
    let system = SystemOne::new(
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        "raw",
        Arc::new(FixedCatalog),
        vec![ModelPin::jev(provider.clone())],
        Config::default(),
        Arc::default(),
    )
    .with_store_role(LLM_CLASSIFY_STORE_ROLE);
    let tools = Arc::new(broca_module_serve::llm_tools::LlmTools::new(
        Arc::new(system),
        broca_module_serve::config::LlmToolsConfig {
            advertise_tags: true,
            private_content_providers: vec!["typesafe".into()],
        },
    ));
    let body = br#"{"name":"llm.classify/v1","arguments":{"model":"typesafe/jev-1.13.0","questions":{"z":{"type":"noul","instructions":"last alphabetically, first in the request"},"a":{"type":"noul","instructions":"second in the request"}},"item":{"state":{"huge":123456789012345678901234567890}},"max_cost_usd":0.01},"call_key":"raw"}"#;
    let result = tools
        .answer(body, Some(&owner()), &stamped(1, "owner"))
        .await
        .unwrap();
    let payload = provider.payload.lock().unwrap().clone().unwrap();
    assert!(
        payload.find("\"z\"").unwrap() < payload.find("\"a\"").unwrap(),
        "question order was normalized"
    );
    assert!(
        payload.contains("123456789012345678901234567890"),
        "structured state JSON was normalized"
    );
    assert!(
        result.get().contains("0.12345678901234567890123456789"),
        "provider answer number spelling was normalized"
    );
    assert!(
        result.get().contains("123456789012345678901234567890"),
        "unknown provider answer field was normalized"
    );
    assert_eq!(
        tools
            .answer(body, Some(&owner()), &stamped(1, "owner"))
            .await
            .unwrap()
            .get(),
        result.get()
    );
    assert_eq!(provider.calls.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test]
async fn memory_catalog_digest_covers_composition_and_refusals_name_catalog_fields() {
    let tools = direct_tools(
        Arc::new(std::sync::atomic::AtomicI64::new(1000)),
        Arc::new(CountedProvider::default()),
        Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap()),
        Arc::new(broca_store::SqliteStore::open_in_memory().unwrap()),
    );
    let mut replies = Vec::new();
    for (context, digest_only) in [("first", false), ("second", false), ("second", true)] {
        let body = json!({"name":"tool.catalog","arguments":{"params":{},"composition":{"context":context},"digest_only":digest_only}}).to_string();
        replies.push(
            serde_json::from_str::<Value>(
                tools
                    .answer(body.as_bytes(), None, &broca_protocol::SendScope::default())
                    .await
                    .unwrap()
                    .get(),
            )
            .unwrap(),
        );
    }
    assert_eq!(replies[0]["generation"], replies[1]["generation"]);
    assert_ne!(replies[0]["catalog_digest"], replies[1]["catalog_digest"]);
    assert_eq!(replies[1]["catalog_digest"], replies[2]["catalog_digest"]);
    assert_eq!(replies[2].as_object().unwrap().len(), 2);
    for (arguments, field) in [
        (json!({"params":{"unknown":true}}), "params.unknown"),
        (json!({"params":{},"preset":"unknown"}), "preset"),
        (
            json!({"params":{},"system_text":{"preset":"unknown","params":{}}}),
            "system_text",
        ),
    ] {
        let body = json!({"name":"tool.catalog","arguments":arguments}).to_string();
        let failure: Value = serde_json::from_slice(
            &tools
                .answer(body.as_bytes(), None, &broca_protocol::SendScope::default())
                .await
                .unwrap_err(),
        )
        .unwrap();
        assert_eq!(failure["code"], "invalid_request");
        assert_eq!(failure["detail"]["field"], field);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stamped_flow_executes_and_is_billed_to_the_flow_not_its_owner() {
    let Some(rig) = Rig::start("flow", true, true, Some(42_000_000), 0, false).await else {
        return;
    };
    let basal = rig.open(true, true).await;
    let connection = basal.connection.clone();
    let identity = broca_protocol::BindIdentity::new(
        rig.root.join("project").canonicalize().unwrap(),
        "test",
        "flow",
    );
    let handle = connection
        .route_open_tool_provider_v1(
            "broca",
            &identity,
            Some(subc_protocol::scope::ScopeSelector::new(
                subc_protocol::Principal::Reserved {
                    module_id: "owner".into(),
                },
                "flow",
                Some(1),
            )),
        )
        .await
        .unwrap();
    let flow = Route {
        connection,
        route: handle,
    };
    let result: cortexkit_llm_tools::classify::Result =
        serde_json::from_slice(&flow.call(args(), Some("flow")).await.unwrap()).unwrap();
    cortexkit_llm_tools::classify::validate_result(&result).unwrap();
    assert_eq!(rig.calls(), 1);
    let database = rusqlite::Connection::open_with_flags(
        rig.state.join("run-index.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let text: String = database
        .query_row("SELECT segment_json FROM export_facts", [], |row| {
            row.get(0)
        })
        .unwrap();
    let fact: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(fact["flow_id"], "tool-flow");
    assert!(fact.get("run_id").is_none());
    assert!(fact.get("agent_id").is_none());
}

#[tokio::test]
async fn memory_flow_scope_is_admitted_without_inventing_an_agent_run() {
    use sha2::{Digest, Sha256};
    let store = Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap());
    let provider = Arc::new(CountedProvider::default());
    let tools = direct_tools(
        Arc::new(std::sync::atomic::AtomicI64::new(1000)),
        provider.clone(),
        store.clone(),
        Arc::new(broca_store::SqliteStore::open_in_memory().unwrap()),
    );
    let mut scope = stamped(1, "owner");
    if let broca_protocol::SendScope::Stamped(bind) = &mut scope {
        bind.scope.flow_id = Some("flow".into());
        bind.scope.agent_id = None;
        bind.scope.run_id = None;
    }
    let principal = broca_protocol::BindPrincipal::Reserved {
        module_id: "basal".into(),
    };
    tools
        .answer(&direct_body(args(), Some("flow")), Some(&principal), &scope)
        .await
        .unwrap();
    let id = format!("{:x}", Sha256::digest(br#"{"call_key":"flow","owner":"reserved:owner","principal":"reserved:basal","scope_epoch":1,"scope_ref":"run"}"#));
    assert_eq!(
        store.lookup("llm.classify", &id).unwrap().unwrap().scope,
        Some(broca_store::system_one::BatchScope::Flow {
            flow_id: "flow".into()
        })
    );
    assert_eq!(provider.0.load(std::sync::atomic::Ordering::SeqCst), 1);
}
