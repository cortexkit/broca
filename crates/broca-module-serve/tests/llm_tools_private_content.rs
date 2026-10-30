//! Private-content admission through the real serve path: a real daemon, a real ck-broca and a
//! loopback provider.
//!
//! The route asks cerebellum and Basal, so the rig has to answer as both. It does that with
//! `fake-surface`, a stand-in module (its own package under `tests/support/fake-surface`) that
//! declares the two management operations and serves them from a fixture file. `Stub::Answering`
//! gives it a fixture. Cerebellum's answers and outcome refusals are bare response bodies;
//! Basal's error bodies use error frames. The `owner` and `cingulate` stand-ins are
//! `fake-aft-stub`, which declares no management surface, so the daemon refuses a request
//! aimed at either of them.

// The handoff uses /bin/sh and records kernel process identities for cleanup.
// Those identity helpers are implemented only on Linux and macOS.
#![cfg(any(target_os = "linux", target_os = "macos"))]

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use broca_subc::{RouteHandle, SubcConnection};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const MODEL: &str = "typesafe/jev-1.13.0";

// Cerebellum fbcc6766, docs/contracts/private-content-query-v1.md. These reply bodies come from
// the following vectors; SHA-256s identify whole golden-file documents for provenance only:
// crates/cerebellum/tests/vectors/private-content-query-v1/success-marked.json
// whole-file SHA-256 eb4c452cadd0b5d48e8855db366c599c3dcccb3408da1ea403fa5d8703032d1c
// crates/cerebellum/tests/vectors/private-content-query-v1/success-unmarked.json
// whole-file SHA-256 e860efcd7db12298f7af82758e8a72bc66b5e0ede81ada0c7fba40c1aa29acb2
// crates/cerebellum/tests/vectors/private-content-query-v1/unknown-field.json
// whole-file SHA-256 7cc2ec06859a700a91c1e6a1a57643c66ab9af848debcf9b4beb6ecaa7aeb2bc
const CEREBELLUM_MARKED: &str = r#"{"private_content":true,"sources":["signed_in_site"]}"#;
const CEREBELLUM_UNMARKED: &str = r#"{"private_content":false,"sources":[]}"#;
const CEREBELLUM_REFUSAL: &str = r#"{"context":null,"detail":{"field":"owner","reason_code":"unknown_field"},"outcome":"invalid_request","session":null}"#;

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
    /// Give the stand-in no fixture. An unmatched operation then gets `unknown_method`, which is
    /// what a module that does not serve the operation answers, except `private_content.query`,
    /// which gets the stand-in's default answer (see `fake-surface`).
    Unreachable,
    /// Use this JSON as the matching fixture's wire body. Basal and Cingulate wrap `{result}`;
    /// its error bodies use error frames. Cerebellum's answers and refusals use response frames.
    Answering(&'static str),
}

#[derive(Clone, Copy, PartialEq)]
enum StandInHold {
    None,
    Handoff,
    BasalNoncePublication,
}

struct Rig {
    _daemon: broca_test_support::RigChild,
    root: PathBuf,
    state: PathBuf,
    provider: Arc<Provider>,
}

impl Rig {
    async fn start(name: &str, cerebellum: Stub, basal: Stub) -> Option<Self> {
        Self::start_with_hold(name, cerebellum, basal, StandInHold::None).await
    }

    async fn start_with_hold(
        name: &str,
        cerebellum: Stub,
        basal: Stub,
        hold: StandInHold,
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
        if hold == StandInHold::BasalNoncePublication {
            std::fs::write(root.join("basal.hold"), b"").unwrap();
            // A new stand-in must retire the preceding launch's ready credential.
            std::fs::write(root.join("basal.nonce"), b"previous-launch").unwrap();
        }
        let state = root.join("state");
        let priced = |units| json!({"currency":"USD","exponent":9,"state":"priced","units":units,"unit_provenance":{"kind":"stated"}});
        let row = json!({"existence":"present","model.kind":"decision","model.name":"Jev 1.13","provider.name":"TypeSafe","rate.output":{"state":"stated_zero"},"rate.input":priced(42_000_000)});
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
            {"ref":"run","scope_epoch":1,"kind":"head","attributes":{"agent_id":"agent","run_id":"run"},"carriers":[{"principal":{"kind":"reserved","module_id":"basal"}},{"principal":{"kind":"reserved","module_id":"broca"}}]},
            {"ref":"project-a-first","scope_epoch":1,"kind":"head","project_id":"project-a","workdir":root.join("project").canonicalize().unwrap(),"attributes":{"agent_id":"agent","run_id":"project-a-first"},"carriers":[{"principal":{"kind":"reserved","module_id":"broca"}}]},
            {"ref":"project-a-later","scope_epoch":1,"kind":"head","project_id":"project-a","workdir":root.join("project").canonicalize().unwrap(),"attributes":{"agent_id":"agent","run_id":"project-a-later"},"carriers":[{"principal":{"kind":"reserved","module_id":"broca"}}]},
            {"ref":"project-b","scope_epoch":1,"kind":"head","project_id":"project-b","workdir":root.join("project").canonicalize().unwrap(),"attributes":{"agent_id":"agent","run_id":"project-b"},"carriers":[{"principal":{"kind":"reserved","module_id":"broca"}}]},
            {"ref":"flow","scope_epoch":1,"kind":"head","attributes":{"flow_id":"tool-flow"},"carriers":[{"principal":{"kind":"reserved","module_id":"basal"}}]}
        ]).to_string()).unwrap();
        // Cerebellum, Basal and Cingulate use the `fake-surface` stand-in, which declares
        // their management operations and serves them from fixture files. The
        // `owner` module above is the scope authority owner and publishes the scopes; the
        // stand-ins hold none of their own.
        //
        // The daemon names the module on the command line, so the handoff passes on only what
        // the daemon does not know: the fixture path and the request log.
        let mut modules = json!({});
        for id in ["owner", "cerebellum"] {
            let mut env = json!({"FAKE_AFT_MODULE_ID":id,"FAKE_AFT_LAUNCH_NONCE_PATH":root.join(format!("{id}.nonce")),"FAKE_AFT_EVENTS_PATH":json!(root.join(format!("{id}.events")))});
            if id == "owner" {
                env["FAKE_AFT_SCOPE_SYNC_PATH"] = json!(scopes);
                env["FAKE_AFT_EVENTS_PATH"] = json!(root.join("owner.events"));
            }
            modules[id] = json!({"program":broca_test_support::ckdev_binary(&stub, broca_test_support::scratch_root()),"args":[],"reserved":true,"enabled":true,"env":env});
        }
        for (module_id, answer) in [
            ("cerebellum", cerebellum),
            ("basal", basal),
            (
                "cingulate",
                Stub::Answering(r#"{"result":{"elicitation_id":"fake-card"}}"#),
            ),
        ] {
            let fixture = root.join(format!("{module_id}.fixtures.jsonl"));
            let method = match module_id {
                "cerebellum" => "private_content.query",
                "cingulate" => "consent.request",
                _ => "flow.private_content_providers",
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
                    "#!/bin/sh\nwhile [ \"$#\" -gt 0 ]; do\n  case \"$1\" in\n    --subc) SUBC_CONNECTION=\"$2\"; shift 2 ;;\n    *) shift ;;\n  esac\ndone\nexport SUBC_CONNECTION\nexport PRIVATE_CONTENT_ROOT='{root}'\nexport PRIVATE_CONTENT_HOLD_HANDOFF={hold}\nexport SURFACE_MODULE_ID={module_id}\nexport PRIVATE_CONTENT_SURFACE='{surface}'\nexport PRIVATE_CONTENT_FIXTURES={fixtures}\nexport PRIVATE_CONTENT_REQUESTS={requests}\nexec '{helper}' --exact private_content_handoff --nocapture >> '{ready}' 2>> '{stderr}'\n",
                    root = root.display(),
                    hold = u8::from(hold == StandInHold::Handoff),
                    helper = broca_test_support::ckdev_binary(std::env::current_exe().unwrap(), broca_test_support::scratch_root()).display(),
                    surface = fake_surface_bin().display(),
                    module_id = module_id,
                    fixtures = hex(&fixture),
                    requests = hex(&root.join(format!("{module_id}.requests"))),
                    ready = root.join(format!("{module_id}.stand-in.out")).display(),
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
        modules["broca"] = json!({"program":broca_test_support::ckdev_binary(Path::new(env!("CARGO_BIN_EXE_ck-broca")), broca_test_support::scratch_root()),"args":[],"reserved":true,"enabled":true,"env":env});
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
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let lock = spawn_lock(&rig.root).unwrap();
            lock.lock().unwrap();
            record_process(&rig.root, "daemon", rig._daemon.id().unwrap());
        }
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
        self.open_with_bind_project(target, scope_ref, flow, None, false)
            .await
    }

    async fn open_with_bind_project(
        &self,
        target: &str,
        scope_ref: &str,
        flow: bool,
        project_id: Option<&str>,
        held: bool,
    ) -> Route {
        // A run scope is opened by its owner; the flow carrier is Basal, whose calls carry the
        // flow's scope.
        let caller = if flow { "basal" } else { "owner" };
        let nonce = self.root.join(format!("{caller}.nonce"));
        let launch_nonce = consumer_nonce(&nonce).await;
        let connection = Arc::new(
            SubcConnection::connect_with_consumer_identity(
                &self.root.join("runtime/subc-connection.json"),
                Some(subc_control::ConsumerIdentity {
                    module_id: caller.into(),
                    launch_nonce,
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
        served_route_with_bind_project(
            &connection,
            target,
            &identity,
            Some(scope),
            project_id,
            held,
        )
        .await
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

    /// Restart the daemon's broca over the same state and open a fresh route to the new
    /// process. Only one broca may serve a state root at a time, so the running one is killed
    /// first, and the daemon starts it again with the config as it is now.
    async fn restart(&self) -> Route {
        let roster = self.root.join("data/cortexkit/run/live-children.json");
        let record: Value = serde_json::from_slice(&std::fs::read(&roster).unwrap()).unwrap();
        let pid = record["children"]
            .as_array()
            .unwrap()
            .iter()
            .find(|child| child["module_id"] == "broca")
            .and_then(|child| child["pid"].as_u64())
            .map(|pid| u32::try_from(pid).unwrap())
            .expect("the rig daemon supervises a live broca");
        subc_os::Process::open(pid)
            .unwrap()
            .expect("the daemon's broca is live")
            .signal(subc_os::Signal::Kill)
            .unwrap();
        // Gone means the daemon has reaped it, so a route opened next cannot reach the old
        // process.
        broca_test_support::wait_until("the old broca reaped", || async {
            match subc_os::Process::open(pid) {
                Ok(None) => Ok(()),
                _ => Err(format!("broca {pid} still exists")),
            }
        })
        .await;
        self.open_to("broca", "run", false).await
    }

    /// How many `private_content.query` requests reached cerebellum.
    fn cerebellum_queries(&self) -> usize {
        requests(self, "cerebellum", "private_content.query")
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let cleanup = cleanup_recorded_processes(&self.root);
            broca_test_support::kill_and_reap(&mut self._daemon);
            if let Err(error) = cleanup {
                let message = format!(
                    "private-content rig cleanup {}: {error}",
                    self.root.display()
                );
                if std::thread::panicking() {
                    eprintln!("{message}");
                } else {
                    panic!("{message}");
                }
            }
        }
        #[cfg(not(any(target_os = "linux", target_os = "macos")))]
        broca_test_support::kill_and_reap(&mut self._daemon);
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn cleanup_recorded_processes(root: &Path) -> std::io::Result<()> {
    use std::time::Instant;
    const LOCK_GRACE: Duration = Duration::from_secs(5);

    // Helpers check this marker under the same lock used for spawning and
    // recording. Once locked, no stand-in can slip between spawn and publication.
    std::fs::write(root.join("stopping"), b"")?;
    let lock = spawn_lock(root)?;
    let deadline = Instant::now() + LOCK_GRACE;
    let locked = loop {
        if lock.try_lock().is_ok() {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::sleep(Duration::from_millis(10));
    };

    let mut processes = recorded_processes(root);
    for (role, process) in &processes {
        if role == "daemon" {
            if let Err(error) = process.stop() {
                eprintln!("private-content rig: could not stop {process:?}: {error}");
            }
        }
    }
    // Freezing the daemon prevents restart races. The daemon's own ledger owns
    // direct modules; the append-only helper ledger also owns already-orphaned children.
    processes.extend(recorded_processes(root));
    for (role, process) in &processes {
        if role != "stand-in" {
            if let Err(error) = process.stop() {
                eprintln!("private-content rig: could not stop {process:?}: {error}");
            }
        }
    }
    processes.sort_by_key(|(role, _)| match role.as_str() {
        "stand-in" => 0,
        "daemon" => 2,
        _ => 1,
    });
    for (_, process) in &processes {
        // Every signal rechecks the kernel start time, never a command name or
        // process group. A stale ledger entry is not authority to kill a reused PID.
        if let Err(error) = process.kill() {
            eprintln!("private-content rig: could not kill {process:?}: {error}");
        }
    }
    let identities: Vec<_> = processes.into_iter().map(|(_, process)| process).collect();
    broca_test_support::RecordedProcess::await_exit(&identities)?;
    if !locked {
        return Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "helper spawn lock remained held; cleaned all published identities",
        ));
    }
    Ok(())
}

async fn consumer_nonce(path: &Path) -> String {
    broca_test_support::wait_until("caller nonce", || async {
        let contents = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
        let nonce = contents.trim();
        if nonce.is_empty() {
            Err("the consumer nonce is still empty".to_owned())
        } else {
            Ok(nonce.to_owned())
        }
    })
    .await
}

#[tokio::test]
async fn an_empty_consumer_nonce_is_not_readiness() {
    use std::future::Future;
    use std::task::Poll;

    let path = broca_test_support::scratch_root().join("empty-consumer.nonce");
    for empty in ["", " \n\t"] {
        std::fs::write(&path, empty).unwrap();
        let mut reader = Box::pin(consumer_nonce(&path));
        let observed = std::future::poll_fn(|cx| Poll::Ready(reader.as_mut().poll(cx))).await;
        assert!(
            observed.is_pending(),
            "an empty credential completed the reader"
        );
        std::fs::write(&path, " captured-nonce\n").unwrap();
        assert_eq!(reader.await, "captured-nonce");
    }
    std::fs::remove_file(path).unwrap();
}

/// Open a tool-provider route to `target` and wait until that process answers a catalog.
///
/// A daemon admits a route to a module while the module is still starting, and that module may
/// then see the route bind before it has registered its own providers. The first call on such
/// a route is answered by whatever guards it has, which for this route means it fails closed.
/// Waiting for the catalog keeps the first tool call on a served route. Returns the route.
async fn served_route_with_bind_project(
    connection: &Arc<SubcConnection>,
    target: &str,
    identity: &broca_protocol::BindIdentity,
    scope: Option<subc_protocol::scope::ScopeSelector>,
    project_id: Option<&str>,
    held: bool,
) -> Route {
    // A daemon admits a route to a module whose registration it has not finished handling,
    // and once that route exists it does not open a second one. A module that blinks during
    // its first registration would therefore lose its only route for the rest of the rig, so
    // keep asking until the daemon agrees to open one.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    let route = loop {
        let opened: Result<RouteHandle, String> =
            broca_test_support::wait_until("a tool-provider route", || {
                let connection = connection.clone();
                let identity = identity.clone();
                let scope = scope.clone();
                let target = target.to_owned();
                async move {
                    #[cfg(debug_assertions)]
                    let opened = connection
                        .route_open_tool_provider_with_bind_project_for_tests(
                            &target, &identity, scope, project_id, held,
                        )
                        .await;
                    #[cfg(not(debug_assertions))]
                    let opened = {
                        assert!(project_id.is_none() && !held);
                        connection
                            .route_open_tool_provider_v1(&target, &identity, scope)
                            .await
                    };
                    Ok(match opened {
                        Ok(route) => Ok(route),
                        Err(error) => Err(error.to_string()),
                    })
                }
            })
            .await;
        match opened {
            Ok(route) => break route,
            Err(error) if std::time::Instant::now() < deadline => {
                eprintln!("tool-provider route to {target} not ready yet: {error}");
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            Err(error) => panic!("tool-provider route to {target} never opened: {error}"),
        }
    };
    let body = serde_json::to_vec(&json!({"name":"tool.catalog","arguments":{}})).unwrap();
    broca_test_support::wait_until("a served tool catalog", || {
        let connection = connection.clone();
        let route = &route;
        let body = body.clone();
        async move {
            connection
                .route_request_terminal_with_deadline(route, body, Duration::from_secs(10))
                .await
                .map_err(|error| error.to_string())?
                .map(|_| ())
                .map_err(|body| String::from_utf8_lossy(&body).into_owned())
        }
    })
    .await;
    Route {
        connection: connection.clone(),
        route,
    }
}

/// The stand-in management surface this test points broca at for cerebellum and Basal.
///
/// That stand-in is another module, so it speaks the subc wire and the manifest shapes, and it
/// is built as a separate package (`tests/support/fake-surface`) whose manifest pins exactly
/// the releases broca links. CI runs prebuilt test archives without a complete Cargo registry,
/// so it supplies `FAKE_SURFACE_BIN`. Runs with `BROCA_REQUIRE_SUBC=1` never compile fixtures;
/// other local runs may build on demand. The rig always launches the `ckdev_binary` published
/// copy, not the build output: on macOS a daemon-spawned module can block indefinitely in the
/// loader on a removable build volume before it can register.
fn fake_surface_bin() -> PathBuf {
    if let Some(binary) = std::env::var_os("FAKE_SURFACE_BIN") {
        return broca_test_support::ckdev_binary(binary, broca_test_support::scratch_root());
    }
    assert!(
        std::env::var("BROCA_REQUIRE_SUBC").as_deref() != Ok("1"),
        "required private-content stand-in missing: set FAKE_SURFACE_BIN to a prebuilt fake-surface binary; strict runs never compile fixtures"
    );
    let stand_in = build_dir("fake-surface");
    let built = stand_in.join("debug/fake-surface");
    let ready = stand_in.join("fake-surface.ready");
    if !(ready.exists() && built.exists() && built_after_source(&built)) {
        let package = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/fake-surface");
        let status = std::process::Command::new(env!("CARGO"))
            .arg("build")
            .arg("--offline")
            .arg("--quiet")
            // A build, not a broca process: it writes only under the `--target-dir` below. The
            // data home is pinned to the same scratch area and an inherited state root is
            // dropped, so nothing this build runs can reach the operator's state.
            .env("XDG_DATA_HOME", build_dir("fake-surface/data"))
            .env_remove("BROCA_STATE_ROOT")
            .arg("--manifest-path")
            .arg(package.join("Cargo.toml"))
            .arg("--target-dir")
            .arg(&stand_in)
            .status()
            .unwrap();
        assert!(status.success(), "the stand-in surface must compile");
        std::fs::write(&ready, "").unwrap();
    }
    // Launch the published copy, never the build output. The build tree may sit on a
    // removable volume, and a module the daemon spawns under its own privacy identity can
    // block in the loader there indefinitely, before `main` runs, so it never registers.
    broca_test_support::ckdev_binary(&built, broca_test_support::scratch_root())
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
            // The route's own dispatch budget: a call may open routes and ask cerebellum and
            // Basal before it answers, and a client deadline is a liveness probe, not a work
            // budget.
            .route_request_terminal_with_deadline(
                &self.route,
                serde_json::to_vec(&body).unwrap(),
                Duration::from_secs(120),
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

/// A call the policy admits, retried until it is.
///
/// The first call on a fresh route can meet a daemon that admitted the route before the module
/// served its providers; that call fails closed by design, and a retry reaches a served route.
/// Each attempt uses the caller's key, so a retry is the same work and joins whatever the
/// first attempt left running rather than starting a second provider call.
async fn admitted_with_retry(rig: &Rig, route: &Route, key: &str) -> Value {
    let observed = std::sync::Arc::new(std::sync::Mutex::new(None));
    let seen = observed.clone();
    let admitted = broca_test_support::wait_until("an admitted model-tool call", || {
        let seen = seen.clone();
        async move {
            match route.call(args(), Some(key)).await {
                Ok(bytes) => Ok(Some(serde_json::from_slice::<Value>(&bytes).ok())),
                // Nothing reached a provider, so the call was refused before dispatch; the
                // route may simply not have been served yet.
                Err(error) if rig.calls() == 0 => {
                    *seen.lock().unwrap() = Some(error);
                    Err("the route has not been admitted yet".to_string())
                }
                // A provider call was made and its answer was an error: that is the outcome.
                Err(_) => Ok(None),
            }
        }
    })
    .await;
    match admitted.flatten() {
        Some(result) => result,
        None => {
            let last = observed.lock().unwrap().clone();
            panic!("the call was never admitted: {last:?}")
        }
    }
}

/// A call that the policy refuses, waited for until the route can actually reach it.
///
/// A daemon admits a route to a module that has not yet served its providers, and a call in
/// that window is refused before the policy runs. That is the intended production behaviour,
/// and it reports the scope as unreachable. This helper is for tests about the policy itself,
/// so it keeps asking until the policy answers, and checks every artifact stayed untouched on
/// every attempt.
async fn refused_once_the_route_is_served(rig: &Rig, route: &Route, key: &str) -> Value {
    broca_test_support::wait_until("a policy refusal", || async {
        let error = refused_without_writes(rig, route, key).await;
        if error["detail"]["consent"] == "private_content_unreachable" {
            return Err(format!("the route is not served yet: {error}"));
        }
        Ok(error)
    })
    .await
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
        Stub::Answering(CEREBELLUM_REFUSAL),
        Stub::Unreachable,
    )
    .await
    else {
        return;
    };
    let route = rig.open("run", false).await;
    let error = refused_without_writes(&rig, &route, "unlisted").await;
    assert_eq!(error["code"], "not_started");
    assert_eq!(error["detail"]["consent"], "private_content_unreachable");
    let error = refused_without_writes(&rig, &route, "unlisted-again").await;
    assert_eq!(error["detail"]["consent"], "private_content_unreachable");
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
        Stub::Answering(CEREBELLUM_UNMARKED),
        Stub::Answering(r#"{"result":{"flow_id":"tool-flow","version":7,"code_hash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","providers":[]}}"#),
    )
    .await
    else {
        return;
    };
    let route = rig.open("run", false).await;
    let result = admitted_with_retry(&rig, &route, "unmarked").await;
    assert!(result["cost_usd"].is_number());
    assert_eq!(rig.cerebellum_queries(), 1);
    let requests = std::fs::read_to_string(rig.root.join("cerebellum.requests")).unwrap();
    let params: Value = serde_json::from_str(
        requests
            .lines()
            .find_map(|line| line.strip_prefix("private_content.query "))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(params, json!({"scope_ref": "run"}));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(debug_assertions)]
async fn always_grant_uses_the_daemon_project_not_the_bind_project_or_workdir() {
    let Some(rig) = Rig::start(
        "always-project",
        Stub::Answering(CEREBELLUM_MARKED),
        Stub::Unreachable,
    )
    .await
    else {
        return;
    };
    let first = rig.open("project-a-first", false).await;
    let store = broca_store::grants::GrantsStore::open(rig.state.join("grants.db")).unwrap();
    let identity = broca_store::grants::ScopeIdentity {
        owner: "reserved:owner".into(),
        scope_ref: "project-a-first".into(),
        scope_epoch: 1,
    };
    store
        .put_card(&broca_store::grants::Card {
            project_id: Some("project-a".into()),
            dedup_key: "seed".into(),
            scope: identity.clone(),
            agent_id: "agent".into(),
            provider: "typesafe".into(),
            content: "{}".into(),
            card_id: Some("always-card".into()),
            state: broca_store::grants::CardState::Pending,
        })
        .unwrap();
    store
        .apply_answer(
            &identity,
            broca_store::grants::CardAnswer {
                dedup_key: "seed".into(),
                card_id: "always-card".into(),
                decision: broca_store::grants::Decision::AllowAlways,
                run_live: false,
            },
            1,
        )
        .unwrap();
    assert!(admitted_with_retry(&rig, &first, "first-a").await["cost_usd"].is_number());
    let later = rig
        .open_with_bind_project(
            "broca",
            "project-a-later",
            false,
            Some("client-project-b"),
            false,
        )
        .await;
    assert!(admitted_with_retry(&rig, &later, "later-a").await["cost_usd"].is_number());
    assert_eq!(rig.calls(), 2);
    for run in ["project-b", "run"] {
        let route = rig
            .open_with_bind_project("broca", run, false, Some("project-a"), false)
            .await;
        let error = refused_once_the_route_is_served(&rig, &route, run).await;
        assert_eq!(error["detail"]["consent"], "consent_required");
    }
    assert_eq!(
        rig.calls(),
        2,
        "a shared workdir or client bind project must not match"
    );
    assert!(store.revoke("always-card").unwrap());
    assert_eq!(store.generation("agent", "typesafe").unwrap(), 1);
    let error = refused_once_the_route_is_served(&rig, &later, "revoked-a").await;
    assert_eq!(error["detail"]["consent"], "consent_required");
    assert_eq!(rig.calls(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg(debug_assertions)]
async fn consent_card_content_uses_only_the_daemon_stamped_project() {
    let Some(rig) = Rig::start(
        "project-card",
        Stub::Answering(r#"{"private_content":true,"sources":["person_browser"]}"#),
        Stub::Unreachable,
    )
    .await
    else {
        return;
    };
    for (run, claim, fixture, expected_project) in [
        (
            "run",
            "project-a",
            include_str!("../../broca-subc/tests/fixtures/consent/broca/private-content-card.json"),
            None,
        ),
        (
            "project-a-first",
            "client-project-b",
            include_str!(
                "../../broca-subc/tests/fixtures/consent/broca/private-content-card-project.json"
            ),
            Some("project-a"),
        ),
    ] {
        let route = rig
            .open_with_bind_project("broca", run, false, Some(claim), true)
            .await;
        broca_test_support::wait_until("Cingulate's consent capability", || async {
            let ready = route
                .connection
                .catalog_list()
                .await
                .unwrap()
                .iter()
                .any(|entry| {
                    entry.module_id == "cingulate"
                        && entry.capabilities.as_ref().is_some_and(|caps| {
                            caps.provides
                                .iter()
                                .any(|cap| cap == "consent-no-expiry/v1")
                        })
                });
            if ready {
                Ok(())
            } else {
                Err("Cingulate has not registered".to_owned())
            }
        })
        .await;
        let reply = admitted_with_retry(&rig, &route, run).await;
        assert_eq!(reply["held"]["reason"], "consent");
        let identity = broca_store::grants::ScopeIdentity {
            owner: "reserved:owner".into(),
            scope_ref: run.into(),
            scope_epoch: 1,
        };
        let conn = rusqlite::Connection::open_with_flags(
            rig.state.join("grants.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let mut stmt = conn.prepare("SELECT body FROM cards").unwrap();
        let cards = stmt.query_map([], |row| row.get::<_, String>(0)).unwrap();
        let card = cards
            .map(|body| serde_json::from_str::<broca_store::grants::Card>(&body.unwrap()).unwrap())
            .find(|card| card.scope == identity)
            .unwrap();
        assert_eq!(card.project_id.as_deref(), expected_project);
        let mut expected: Value = serde_json::from_str(fixture).unwrap();
        expected["dedup_key"] = json!(card.dedup_key);
        expected["requester_ref"] = expected["dedup_key"].clone();
        assert_eq!(
            serde_json::from_str::<Value>(&card.content).unwrap(),
            expected
        );
    }
    assert_eq!(rig.calls(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_marked_run_ends_not_started_consent_required_and_files_nothing() {
    let Some(rig) = Rig::start(
        "marked",
        Stub::Answering(CEREBELLUM_MARKED),
        Stub::Answering(r#"{"result":{"flow_id":"tool-flow","version":7,"code_hash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","providers":[]}}"#),
    )
    .await
    else {
        return;
    };
    let route = rig.open("run", false).await;
    for key in ["marked", "marked-again"] {
        let error = refused_once_the_route_is_served(&rig, &route, key).await;
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
        Stub::Answering(r#"{"result":{"flow_id":"tool-flow","version":7,"code_hash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","providers":[]}}"#),
    )
    .await
    else {
        return;
    };
    let route = rig.open("run", false).await;
    // Cerebellum answered, and named no source kind broca knows: a card could name none.
    let error = refused_once_the_route_is_served(&rig, &route, "unknown-source").await;
    assert_eq!(error["detail"]["consent"], "private_content_unknown");
    assert_eq!(rig.calls(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_marked_flow_uses_basals_approved_version_and_ownerless_request() {
    let Some(rig) = Rig::start(
        "approved-flow",
        Stub::Answering(CEREBELLUM_MARKED),
        Stub::Answering(r#"{"result":{"flow_id":"tool-flow","version":7,"code_hash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","providers":["typesafe","openai"]}}"#),
    ).await else { return; };
    let route = rig.open("flow", true).await;
    let result = admitted_with_retry(&rig, &route, "approved-flow").await;
    assert!(result["cost_usd"].is_number());
    assert_eq!(rig.calls(), 1);
    let requests = std::fs::read_to_string(rig.root.join("basal.requests")).unwrap();
    let params: Value = serde_json::from_str(
        requests
            .lines()
            .find_map(|line| line.strip_prefix("flow.private_content_providers "))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(params, json!({"flow_id": "tool-flow"}));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_marked_flow_with_no_declared_providers_gets_no_extra_provider() {
    let Some(rig) = Rig::start(
        "empty-flow",
        Stub::Answering(CEREBELLUM_MARKED),
        Stub::Answering(r#"{"result":{"flow_id":"tool-flow","version":7,"code_hash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","providers":[]}}"#),
    ).await else { return; };
    let route = rig.open("flow", true).await;
    for key in ["empty-flow", "empty-flow-again", "empty-flow-cached"] {
        let error = refused_without_writes(&rig, &route, key).await;
        assert_eq!(error["code"], "provider_not_allowed");
    }
    assert_eq!(
        requests(&rig, "basal", "flow.private_content_providers"),
        1,
        "the empty approved provider set is a cacheable policy, not a lookup error"
    );
    assert_eq!(rig.calls(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flow_waits_for_basals_acknowledged_nonce_publication() {
    use std::future::Future;
    use std::task::Poll;

    let Some(rig) = Rig::start_with_hold(
        "held-basal-nonce",
        Stub::Answering(CEREBELLUM_MARKED),
        Stub::Unreachable,
        StandInHold::BasalNoncePublication,
    )
    .await
    else {
        return;
    };
    let nonce_path = rig.root.join("basal.nonce");
    broca_test_support::wait_until("Basal paused before credential publication", || async {
        if rig.root.join("basal.created").exists() {
            Ok(())
        } else {
            Err("Basal has not created its unpublished nonce file".to_owned())
        }
    })
    .await;

    let mut reader = Box::pin(consumer_nonce(&nonce_path));
    let observed = std::future::poll_fn(|cx| Poll::Ready(reader.as_mut().poll(cx))).await;
    assert!(
        observed.is_pending(),
        "an unpublished nonce completed the reader"
    );
    assert!(
        !nonce_path.exists(),
        "a stale or partial credential was visible"
    );
    assert!(std::fs::read_to_string(rig.root.join("basal.requests"))
        .unwrap()
        .contains("hello-ack "));

    std::fs::remove_file(rig.root.join("basal.hold")).unwrap();
    let captured = reader.await;
    assert!(!captured.is_empty());
    let route = rig.open("flow", true).await;
    assert_eq!(route.connection.consumer_module_id(), Some("basal"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn basal_error_frames_never_authorize_extra_providers_or_cache_a_failure() {
    for (name, fixture, expected) in [
        (
            "not-approved",
            r#"{"code":"flow_not_approved","message":"flow has no approved version"}"#,
            "flow_not_approved",
        ),
        (
            "not-permitted",
            r#"{"code":"not_permitted","message":"caller is not permitted"}"#,
            "provider_not_allowed",
        ),
        (
            "invalid-params",
            r#"{"code":"invalid_params","message":"unknown parameter"}"#,
            "provider_not_allowed",
        ),
        (
            "warming",
            r#"{"code":"module_warming","message":"module is warming"}"#,
            "provider_not_allowed",
        ),
        (
            "storage",
            r#"{"code":"storage_unavailable","message":"storage is unavailable"}"#,
            "provider_not_allowed",
        ),
    ] {
        let Some(rig) = Rig::start(
            name,
            Stub::Answering(CEREBELLUM_MARKED),
            Stub::Answering(fixture),
        )
        .await
        else {
            return;
        };
        let route = rig.open("flow", true).await;
        let error = broca_test_support::wait_until("a Basal error frame", || async {
            let error = refused_without_writes(&rig, &route, "flow-refused").await;
            if requests(&rig, "basal", "flow.private_content_providers") == 0 {
                Err("Basal has not served its management route yet".to_string())
            } else {
                Ok(error)
            }
        })
        .await;
        assert_eq!(error["code"], expected);
        let before = requests(&rig, "basal", "flow.private_content_providers");
        let error = refused_without_writes(&rig, &route, "flow-refused-again").await;
        assert_eq!(error["code"], expected);
        assert_eq!(
            requests(&rig, "basal", "flow.private_content_providers"),
            before + 1
        );
        assert_eq!(rig.calls(), 0);
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn spawn_lock(root: &Path) -> std::io::Result<std::fs::File> {
    std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join("processes.lock"))
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn record_process(root: &Path, role: &str, pid: u32) {
    use std::io::Write;
    let process = broca_test_support::RecordedProcess::capture(pid).unwrap();
    // The handoff helper holds processes.lock while it spawns and records its stand-in, so
    // cleanup never misses one. One append publishes the complete identity before the helper
    // waits for its child.
    let line = format!("{role} {} {}\n", process.pid, process.start_time);
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(root.join("processes.txt"))
        .unwrap()
        .write_all(line.as_bytes())
        .unwrap();
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn recorded_processes(root: &Path) -> Vec<(String, broca_test_support::RecordedProcess)> {
    let mut processes: Vec<_> = std::fs::read_to_string(root.join("processes.txt"))
        .unwrap_or_default()
        .lines()
        .filter_map(|line| {
            let mut fields = line.split_whitespace();
            Some((
                fields.next()?.to_owned(),
                broca_test_support::RecordedProcess {
                    pid: fields.next()?.parse().ok()?,
                    start_time: fields.next()?.parse().ok()?,
                },
            ))
        })
        .collect();
    // The daemon records the identity of every directly supervised module. Its
    // start-time units are microseconds on macOS and kernel clock ticks on Linux.
    if let Ok(bytes) = std::fs::read(root.join("data/cortexkit/run/live-children.json")) {
        if let Ok(ledger) = serde_json::from_slice::<Value>(&bytes) {
            for child in ledger["children"].as_array().into_iter().flatten() {
                if let (Some(pid), Some(start_time)) = (
                    child["pid"]
                        .as_u64()
                        .and_then(|pid| u32::try_from(pid).ok()),
                    child["start_time"].as_u64(),
                ) {
                    processes.push((
                        "module".into(),
                        broca_test_support::RecordedProcess { pid, start_time },
                    ));
                }
            }
        }
    }
    processes
}

/// Run the registration retry loop in a Rust helper so each stand-in's identity
/// survives even if the daemon kills the helper before the stand-in exits.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_content_handoff() {
    let Some(root) = std::env::var_os("PRIVATE_CONTENT_ROOT") else {
        return;
    };
    let root = PathBuf::from(root);
    let module = std::env::var("SURFACE_MODULE_ID").unwrap();
    let mut recorded_helper = false;
    broca_test_support::wait_until_sync(&format!("{module} stand-in registration"), || {
        let lock = spawn_lock(&root).unwrap();
        lock.lock().unwrap();
        if root.join("stopping").exists() {
            return Ok(());
        }
        if !recorded_helper {
            record_process(&root, "helper", std::process::id());
            recorded_helper = true;
        }
        // Retire readiness before starting another stand-in, including a helper's retries.
        match std::fs::remove_file(root.join(format!("{module}.nonce"))) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("clear {module} consumer nonce: {error}"),
        }
        let hold = std::env::var("PRIVATE_CONTENT_HOLD_HANDOFF").as_deref() == Ok("1");
        let mut command = if hold {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args(["--exact", "private_content_held_standin", "--nocapture"])
                .env("PRIVATE_CONTENT_HELD_STANDIN", "1");
            command
        } else {
            let mut command =
                std::process::Command::new(std::env::var_os("PRIVATE_CONTENT_SURFACE").unwrap());
            command
                .args(["--module-id", &module])
                .arg(format!(
                    "--SURFACE_FIXTURES={}",
                    std::env::var("PRIVATE_CONTENT_FIXTURES").unwrap()
                ))
                .arg(format!(
                    "--SURFACE_REQUESTS={}",
                    std::env::var("PRIVATE_CONTENT_REQUESTS").unwrap()
                ))
                .arg(format!(
                    "--SURFACE_NONCE_PATH={}",
                    hex(&root.join(format!("{module}.nonce")))
                ));
            command
        };
        let mut child = command
            .env("BROCA_STATE_ROOT", root.join("state"))
            .spawn()
            .unwrap();
        record_process(&root, "stand-in", child.id());
        drop(lock);
        let status = child.wait().unwrap();
        // A daemon may launch a module before it can acknowledge HELLO. Retry
        // an early exit under the shared registration hang guard.
        if status.success() {
            Ok(())
        } else {
            Err(format!("{module} stand-in exited with {status}"))
        }
    });
}

/// Keep the cleanup probes independent of registration and connection-close timing.
/// A stopped orphan may be automatically resumed or terminated when its helper
/// dies. Parking leaves the process running, so only rig cleanup can end it.
#[cfg(any(target_os = "linux", target_os = "macos"))]
#[test]
fn private_content_held_standin() {
    if std::env::var_os("PRIVATE_CONTENT_HELD_STANDIN").is_some() {
        loop {
            std::thread::park();
        }
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
struct CleanupProbe {
    root: PathBuf,
    lookalike: std::process::Child,
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
impl Drop for CleanupProbe {
    fn drop(&mut self) {
        // A broken cleanup must fail without making the regression test itself
        // leak processes. Observe survivors before this independent backstop runs.
        for (_, process) in recorded_processes(&self.root) {
            let _ = process.kill();
        }
        let _ = self.lookalike.kill();
        let _ = self.lookalike.wait();
    }
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
async fn cleanup_probe(name: &str, timeout: bool, orphan: bool) {
    use broca_test_support::RecordedProcess;
    use std::os::unix::fs::PermissionsExt;
    let Some(rig) = Rig::start_with_hold(
        name,
        Stub::Unreachable,
        Stub::Unreachable,
        StandInHold::Handoff,
    )
    .await
    else {
        return;
    };
    let root = rig.root.clone();
    eprintln!("cleanup probe root: {}", root.display());
    let other_root = root.with_extension("lookalike");
    std::fs::create_dir_all(&other_root).unwrap();
    let script = other_root.join("cerebellum.handoff.sh");
    std::fs::write(&script, "#!/bin/sh\nread ignored\n").unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    let mut probe = CleanupProbe {
        root: root.clone(),
        lookalike: std::process::Command::new("/bin/sh")
            .arg(&script)
            .stdin(std::process::Stdio::piped())
            .spawn()
            .unwrap(),
    };
    let lookalike = RecordedProcess::capture(probe.lookalike.id()).unwrap();
    broca_test_support::wait_until("recorded stand-ins", || async {
        let count = recorded_processes(&root)
            .iter()
            .filter(|(role, process)| role == "stand-in" && process.is_running())
            .count();
        if count >= 2 {
            Ok(())
        } else {
            Err(format!("only {count} live stand-ins"))
        }
    })
    .await;
    if orphan {
        let helpers: Vec<_> = recorded_processes(&root)
            .into_iter()
            .filter(|(role, _)| role == "helper")
            .map(|(_, process)| process)
            .collect();
        for helper in &helpers {
            helper.kill().unwrap();
        }
        broca_test_support::wait_until("helpers killed before rig drop", || async {
            if helpers.iter().all(|process| !process.is_running()) {
                Ok(())
            } else {
                Err("helper still running".to_owned())
            }
        })
        .await;
        broca_test_support::wait_until("stand-ins reparented before rig drop", || async {
            let orphaned = recorded_processes(&root)
                .iter()
                .filter(|(role, process)| {
                    role == "stand-in" && process.parent_pid().is_ok_and(|pid| pid == 1)
                })
                .count();
            if orphaned >= 2 {
                Ok(())
            } else {
                Err(format!("only {orphaned} stand-ins reparented to pid 1"))
            }
        })
        .await;
    }
    let task = tokio::spawn(async move {
        let _rig = rig;
        if timeout {
            // Readiness uses real time; pause only the deliberately stuck wait.
            tokio::time::pause();
            broca_test_support::wait_until("deliberately stuck private-content rig", || async {
                std::future::pending::<Result<(), String>>().await
            })
            .await;
        }
        panic!("deliberate private-content rig failure");
    });
    let failure = task.await.expect_err("the rig owner must unwind");
    if timeout {
        tokio::time::resume();
    }
    assert!(failure.is_panic());
    let survivors: Vec<_> = recorded_processes(&root)
        .into_iter()
        .filter(|(_, process)| process.is_running())
        .collect();
    let unrelated_survived =
        lookalike.is_running() && probe.lookalike.try_wait().unwrap().is_none();
    drop(probe);
    assert!(
        unrelated_survived,
        "rig cleanup killed the unrelated look-alike"
    );
    assert!(
        survivors.is_empty(),
        "rig processes survived unwinding drop: {survivors:?}"
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn private_content_rig_cleans_up_on_panic_and_preserves_lookalikes() {
    cleanup_probe("cleanup-panic", false, false).await;
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn private_content_rig_cleans_up_after_wait_until_times_out() {
    cleanup_probe("cleanup-timeout", true, false).await;
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn private_content_rig_cleans_up_standins_orphaned_before_drop() {
    cleanup_probe("cleanup-orphan", false, true).await;
}
