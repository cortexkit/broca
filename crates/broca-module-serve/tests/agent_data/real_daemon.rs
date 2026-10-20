//! Two real supervised broca processes, a daemon-attested fake Entorhinal driver,
//! and a fake session door. A cutId fixes the WAL's byte length and SHA-256;
//! reads stop at that length, and broca installs only verified bytes. A returning
//! Mac's old bytes must equal the beginning of the incoming WAL. Wire source:
//! engram AGENT_SYNC.md (around line 279), commit 50d65fc. This tests broca
//! against that contract, not live Engram interop.
use super::*;
use broca_subc::{RouteHandle, RouteOpenRetry, SubcConnection};
use broca_test_support::{event_timeout, wait_until, EVENT_HANG_GUARD};
use std::{io::Write, os::unix::fs::PermissionsExt, path::Path, time::Duration};
use subc_control::ConsumerIdentity;
use subc_protocol::manifest::{InternalTransport, ModuleManifest, ProviderRole};

struct DoorProvider(PathBuf);
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn unhex(bytes: &str) -> Vec<u8> {
    (0..bytes.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&bytes[i..i + 2], 16).unwrap())
        .collect()
}
#[async_trait]
impl subc_client_rs::ModuleHandler for DoorProvider {
    async fn on_bind(
        &self,
        bind: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        assert_eq!(
            bind.principal,
            Some(subc_protocol::Principal::Reserved {
                module_id: "broca".into()
            })
        );
        std::fs::write(self.0.join("broca-principal-observed"), b"reserved:broca").unwrap();
        subc_client_rs::BindDecision::accept()
    }
    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        bytes: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        use subc_client_rs::HandlerOutcome;
        let request: Value = serde_json::from_slice(&bytes).unwrap();
        let params = &request["params"];
        let reference = params["sessionRef"].as_str().unwrap();
        assert!(contract::refs::is_session_ref(reference));
        assert!(
            params.get("stream").is_none(),
            "the door derives stream from the principal"
        );
        let path = self.0.join("streams").join(reference);
        let mut stream = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => vec![],
            Err(e) => panic!("{e}"),
        };
        let mut answer = match request["method"].as_str().unwrap() {
            "session.cover" => json!({"durableOffset":stream.len(),"cloudOffset":stream.len()}),
            "session.admit" => {
                assert_eq!(params["fromOffset"], stream.len() as u64);
                let chunk = unhex(params["bytes"].as_str().unwrap());
                assert!(chunk.len() <= broca_subc::session_door::MAX_CALL_BYTES);
                let mut log = std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.0.join("chunks"))
                    .unwrap();
                writeln!(log, "{} {}", stream.len(), chunk.len()).unwrap();
                stream.extend_from_slice(&chunk);
                std::fs::create_dir_all(path.parent().unwrap()).unwrap();
                let mut file = std::fs::File::create(path).unwrap();
                file.write_all(&stream).unwrap();
                file.sync_all().unwrap();
                json!({"durableOffset":stream.len(),"cloudOffset":stream.len()})
            }
            "session.read" => {
                let cut = params["cutId"].as_str().unwrap();
                contract::refs::CutId::new(cut).unwrap();
                let published = self.0.join("sealed").join(cut);
                if published.join("verify-failed").exists() {
                    return HandlerOutcome::Error {
                        code: "verify_failed".into(),
                        message: "published manifest failed verification".into(),
                    };
                }
                let sealed = published.join(reference);
                let bytes = match std::fs::read(sealed) {
                    Ok(bytes) => bytes,
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        return HandlerOutcome::Error {
                            code: if published.exists() {
                                "cut_item_missing"
                            } else {
                                "not_found"
                            }
                            .into(),
                            message: "cut or broca stream is not published".into(),
                        }
                    }
                    Err(e) => panic!("{e}"),
                };
                let from = params["fromOffset"].as_u64().unwrap() as usize;
                let limit = params["limit"].as_u64().unwrap() as usize;
                if !(1..=MAX_CALL_BYTES).contains(&limit) || from > bytes.len() {
                    return HandlerOutcome::Error {
                        code: "unavailable".into(),
                        message: "sealed range cannot be produced in full".into(),
                    };
                }
                let end = bytes.len().min(from.saturating_add(limit));
                json!({"bytes":hex(&bytes[from..end]),"fromOffset":from,"endOffset":end,"streamEnd":bytes.len(),"digest":broca_wal::migration::digest(&bytes)})
            }
            other => panic!("unexpected door op {other}"),
        };
        if request["method"] == "session.read" && self.0.join("omit-digest").exists() {
            answer.as_object_mut().unwrap().remove("digest");
        }
        HandlerOutcome::Response(serde_json::to_vec(&json!({"result":answer})).unwrap())
    }
}

#[test]
#[ignore = "supervised fake door process, launched only by the real-daemon test"]
fn supervised_fake_session_door() {
    let _ = subc_os::launch_nonce();
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let connection = PathBuf::from(std::env::var_os("BROCA_TEST_CONNECTION_FILE").unwrap());
        let state = PathBuf::from(std::env::var_os("BROCA_TEST_DOOR_STATE").unwrap());
        let manifest = ModuleManifest::builder("engram", "0.0.0")
            .provides(vec![ProviderRole::InternalService {
                service_id: "session-door".into(),
                transport: InternalTransport::Bulk,
                agent_facing: false,
                operations: vec![
                    "session.cover".into(),
                    "session.admit".into(),
                    "session.read".into(),
                ],
            }])
            .build();
        let (_, serving) =
            subc_client_rs::serve_with_handle(&connection, manifest, DoorProvider(state))
                .await
                .unwrap();
        serving.await.unwrap();
    });
}

fn live_logs(state_root: &Path) -> String {
    let supervisor = state_root.join("supervisor");
    let data_home = supervisor.join("data/cortexkit");
    let mut logs = broca_test_support::rig_process_logs(&supervisor, "daemon");
    logs.push_str(&broca_test_support::rig_log_diagnostics(
        &data_home, state_root,
    ));
    for module in ["engram", "entorhinal"] {
        broca_test_support::append_file_tail(
            &mut logs,
            &format!("supervised {module} stderr"),
            &data_home.join(format!("run/logs/{module}.stderr.log")),
        );
    }
    logs
}

struct LiveLogGuard([PathBuf; 2]);
impl LiveLogGuard {
    fn diagnostics(&self) -> String {
        self.0.iter().map(|root| live_logs(root)).collect()
    }
}
impl Drop for LiveLogGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            eprintln!("{}", self.diagnostics());
        }
    }
}

#[test]
fn real_daemon_failure_diagnostics_include_each_module_and_daemon() {
    let logs = LiveLogGuard([root("source-logs"), root("receiver-logs")]);
    for (index, state_root) in logs.0.iter().enumerate() {
        let directory = state_root.join("supervisor/data/cortexkit/run/logs");
        std::fs::create_dir_all(&directory).unwrap();
        for module in ["broca", "engram", "entorhinal"] {
            std::fs::write(
                directory.join(format!("{module}.stderr.log")),
                format!("{module} startup failure {index}"),
            )
            .unwrap();
        }
        std::fs::write(
            directory.join("subc.2026-10-16.log"),
            format!("daemon spawn failure {index}"),
        )
        .unwrap();
    }
    let diagnostics = logs.diagnostics();
    for index in 0..2 {
        for module in ["broca", "engram", "entorhinal"] {
            assert!(
                diagnostics.contains(&format!("{module} startup failure {index}")),
                "{diagnostics}"
            );
        }
        assert!(
            diagnostics.contains(&format!("daemon spawn failure {index}")),
            "{diagnostics}"
        );
    }
}

struct Live {
    daemon: broca_test_support::RigChild,
    conn: Arc<SubcConnection>,
    route: RouteHandle,
    logs: PathBuf,
}
impl Live {
    async fn start(rig: &Rig, door: &Path, daemon_bin: &Path, fake_bin: &Path) -> Self {
        let root = rig.root.join("supervisor");
        let config = root.join("config");
        let runtime = root.join("runtime");
        let data = root.join("data");
        let home = root.join("home");
        for path in [&config.join("cortexkit"), &runtime, &data, &home] {
            std::fs::create_dir_all(path).unwrap();
        }
        let connection = runtime.join("subc-connection.json");
        let nonce = root.join("entorhinal.nonce");
        let script = root.join("fake-engram.sh");
        // The build directory may be on removable storage, where macOS can block exec
        // before the fake module can register or write any diagnostics.
        let executable = broca_test_support::ckdev_binary(
            std::env::current_exe().unwrap(),
            broca_test_support::scratch_root(),
        );
        let quoted = executable.to_string_lossy().replace('\'', "'\\''");
        std::fs::write(&script,format!("#!/bin/sh\nexec '{quoted}' --exact real_daemon::supervised_fake_session_door --ignored --nocapture\n")).unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
        let config_file = config.join("cortexkit/subc.jsonc");
        // Launch broca under its dev name, as every test rig does, never the production
        // `ck-broca` name that operator tools and process lists treat as the live module.
        let broca = broca_test_support::ckdev_binary(
            env!("CARGO_BIN_EXE_ck-broca"),
            broca_test_support::scratch_root(),
        );
        std::fs::write(&config_file,serde_json::to_vec_pretty(&json!({"version":1,"modules":{
            "broca":{"program":broca,"args":[],"enabled":true,"reserved":true,"env":{
                "BROCA_STATE_ROOT":rig.root,"BROCA_PROVIDER_BASE_URL":"http://127.0.0.1:9","XDG_DATA_HOME":data}},
            "entorhinal":{"program":fake_bin,"args":[],"enabled":true,"reserved":true,"env":{
                "FAKE_AFT_MODULE_ID":"entorhinal","FAKE_AFT_LAUNCH_NONCE_PATH":nonce}},
            "engram":{"program":script,"args":[],"enabled":true,"reserved":true,"env":{
                "BROCA_TEST_CONNECTION_FILE":connection,"BROCA_TEST_DOOR_STATE":door}}
        }})).unwrap()).unwrap();
        let child = broca_test_support::tokio_rig_command(daemon_bin, &root, "daemon")
            .unwrap()
            .env("XDG_CONFIG_HOME", &config)
            .env("XDG_RUNTIME_DIR", &runtime)
            .env("XDG_DATA_HOME", &data)
            .env("HOME", &home)
            .env("SUBC_PORT", "0")
            .env_remove("BROCA_STATE_ROOT")
            .env_remove("SUBC_MODULE_ID")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut daemon = broca_test_support::RigChild::new(child, &root, "daemon");
        wait_until("agent-data daemon connection and Entorhinal nonce", || {
            assert!(
                daemon.try_wait().unwrap().is_none(),
                "daemon exited during boot: {}",
                live_logs(&rig.root)
            );
            std::future::ready(if connection.exists() && nonce.exists() {
                Ok(())
            } else {
                Err(format!(
                    "connection={}, nonce={}; {}",
                    connection.exists(),
                    nonce.exists(),
                    live_logs(&rig.root)
                ))
            })
        })
        .await;
        let identity = ConsumerIdentity {
            module_id: "entorhinal".into(),
            launch_nonce: std::fs::read_to_string(&nonce).unwrap().trim().into(),
        };
        let conn = Arc::new(
            SubcConnection::connect_with_consumer_identity(&connection, Some(identity))
                .await
                .unwrap(),
        );
        let route = conn
            .route_open_internal_service_within(
                "broca",
                "agent-data",
                &BindIdentity::new("/driver", "fake-entorhinal", "move"),
                RouteOpenRetry {
                    budget: EVENT_HANG_GUARD,
                    max_interval: Duration::from_millis(200),
                },
            )
            .await
            .unwrap_or_else(|error| panic!("{error}: {}", live_logs(&rig.root)));
        Self {
            daemon,
            conn,
            route,
            logs: rig.root.clone(),
        }
    }
    async fn call(&self, method: &str, params: Value) -> Result<Value, Value> {
        let request =
            serde_json::to_vec(&json!({"method":format!("agent_data.{method}"),"params":params}))
                .unwrap();
        let answer = self
            .conn
            .route_unary_reply(&self.route, request, EVENT_HANG_GUARD)
            .await
            .unwrap_or_else(|e| panic!("{e}: {}", live_logs(&self.logs)));
        match answer {
            Ok(bytes) => Ok(serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()),
            Err(error) => Err(serde_json::to_value(error).unwrap()),
        }
    }
    async fn stop(mut self) {
        let _ = self.conn.route_close(&self.route).await;
        self.daemon.kill().await.unwrap();
    }
}
fn seal_door(door: &Path, cut: &str, reference: &str) {
    let destination = door.join("sealed").join(cut).join(reference);
    std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
    std::fs::copy(door.join("streams").join(reference), destination).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_real_daemons_move_and_move_back_through_the_attested_door() {
    let daemon_bin = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
    let fake_bin = broca_test_support::subc_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
    if !daemon_bin.exists() || !fake_bin.exists() {
        assert!(
            std::env::var_os("BROCA_REQUIRE_SUBC").is_none(),
            "real agent-data lane requires ck-subc and fake-aft-stub"
        );
        eprintln!("SKIP real agent-data lane: daemon rigs are absent");
        return;
    }
    let shared = root("real-door");
    std::fs::create_dir_all(&shared).unwrap();
    let source = Rig::new("real-source", Arc::new(FakeDoor::default()));
    let receiver = Rig::new("real-receiver", Arc::new(FakeDoor::default()));
    // Keep the log paths outside the timed future: cancelling startup must not
    // discard the only diagnostics for a module that never registered.
    let logs = LiveLogGuard([source.root.clone(), receiver.root.clone()]);
    event_timeout(
        "two real agent-data daemons move and move back",
        async {
            let session = source.session("real-move");
            source.seed_tool_run(&session, "source-tool-run");
            let params = source.params(CUT, std::slice::from_ref(&session));
            let reference = params["refs"][0]["ref"].as_str().unwrap();
            let before = source.bytes(&session);
            let from = Live::start(&source, &shared, &daemon_bin, &fake_bin).await;
            let to = Live::start(&receiver, &shared, &daemon_bin, &fake_bin).await;
            from.call("prepare", params.clone()).await.unwrap();
            let exported = from.call("export", params.clone()).await.unwrap();
            assert_eq!(from.call("export", params.clone()).await.unwrap(), exported);
            assert_eq!(
                std::fs::read(shared.join("streams").join(reference)).unwrap(),
                before
            );
            seal_door(&shared, CUT, reference);
            std::fs::write(
                shared.join("omit-digest"),
                b"missing required sealed digest",
            )
            .unwrap();
            assert_code(to.call("import", params.clone()).await, "unavailable");
            assert!(!receiver.root.join("wal-staging").exists());
            std::fs::remove_file(shared.join("omit-digest")).unwrap();
            to.call("import", params.clone()).await.unwrap();
            to.call("activate", params.clone()).await.unwrap();
            assert_eq!(receiver.bytes(&session), before);
            let mut release = params.clone();
            release["holder"] = json!("receiver-mac");
            from.call("release", release).await.unwrap();
            assert!(source.wal.check_move(&session, false).is_err());
            receiver.seed_tool_run(&session, "receiver-tool-run");
            let extended = receiver.bytes(&session);
            assert!(extended.starts_with(&before));
            let back = receiver.params(BACK, std::slice::from_ref(&session));
            to.call("prepare", back.clone()).await.unwrap();
            to.call("export", back.clone()).await.unwrap();
            seal_door(&shared, BACK, reference);
            assert_eq!(
                from.call("diverged", back.clone()).await.unwrap(),
                json!({"refs":[{"state":"at_cut"}]})
            );
            from.call("import", back.clone()).await.unwrap();
            from.call("activate", back).await.unwrap();
            assert_eq!(source.bytes(&session), extended);
            assert!(source.wal.check_move(&session, true).is_ok());
            source.seed_tool_run(&session, "source-fork-after-rejoin");
            let fork_cut = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
            seal_door(&shared, fork_cut, reference);
            let fork = source.params(fork_cut, std::slice::from_ref(&session));
            let fork_bytes = source.bytes(&session);
            assert_eq!(
                from.call("diverged", fork.clone()).await.unwrap(),
                json!({"refs":[{"state":"advanced"}]})
            );
            assert_code(from.call("import", fork).await, "local_advanced");
            assert_eq!(source.bytes(&session), fork_bytes);
            assert_eq!(
                std::fs::read_to_string(shared.join("broca-principal-observed")).unwrap(),
                "reserved:broca"
            );
            from.stop().await;
            to.stop().await;
        },
        || logs.diagnostics(),
    )
    .await
    .expect("agent-data real-daemon lane exceeded its own deadline");
}

#[tokio::test]
async fn required_daemon_lane_refuses_to_skip_missing_binaries() {
    let missing = root("required-missing-rigs");
    let output = event_timeout(
        "required agent-data lane rejects missing rigs",
        tokio::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "real_daemon::two_real_daemons_move_and_move_back_through_the_attested_door",
                "--nocapture",
            ])
            .env("BROCA_STATE_ROOT", missing.join("state"))
            .env("BROCA_REQUIRE_SUBC", "1")
            .env("SUBC_CORE_BIN", missing.join("no-daemon"))
            .env("FAKE_AFT_STUB_BIN", missing.join("no-fake"))
            .kill_on_drop(true)
            .output(),
        || format!("missing-rig fixture {}", missing.display()),
    )
    .await
    .expect("required-lane self-test deadline")
    .unwrap();
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        !output.status.success(),
        "missing required rigs silently passed: {text}"
    );
    assert!(
        text.contains("real agent-data lane requires ck-subc and fake-aft-stub"),
        "{text}"
    );
}
