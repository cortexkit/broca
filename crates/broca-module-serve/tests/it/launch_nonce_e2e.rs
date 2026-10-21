//! The real daemon spawns the real broca with ONLY the launch-nonce descriptor, and both of
//! broca's nonce readers pass the daemon's own checks.
//!
//! The daemon hands a module its launch nonce twice: as an inherited pipe named by
//! `SUBC_LAUNCH_NONCE_FD`, and as the environment copy `SUBC_LAUNCH_NONCE` it keeps setting while
//! modules move to the pipe. Here a wrapper removes the environment copy before exec'ing broca, so
//! the pipe is the only place the nonce can come from. Then:
//!
//! - HELLO: broca is configured `reserved`, so the daemon registers it only if its HELLO echoes
//!   the nonce it spawned it with. Broca becoming live is that check passing.
//! - Route open: broca opens a route to the tool module after HELLO, for as long as it runs. The
//!   daemon attests the route as `reserved:broca` only if the route open presents the same nonce,
//!   and relays that principal to the tool module, whose stub records it. A route open that read
//!   the environment would present nothing here, and arrive as `direct`.
//!
//! UNIX ONLY: the daemon passes the descriptor on macOS and Linux; Windows keeps only the
//! environment copy.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::process::Child;
use tokio::time::{sleep, Instant};

/// A sibling-checkout subc binary, env-overridable, matching the other rigs here.
fn sibling_bin(name: &str, env_key: &str) -> PathBuf {
    broca_test_support::subc_bin(name, env_key)
}

/// A skipped test reports `ok` and would say nothing about the property it exists for.
/// `scripts/rust-test-gate.sh` exports `BROCA_REQUIRE_SUBC`, so in a gated run a missing daemon
/// is a failure that names itself; a bare local run may still skip, out loud.
fn require_subc() -> bool {
    std::env::var_os("BROCA_REQUIRE_SUBC").is_some()
}

struct Rig {
    child: Child,
    dir: PathBuf,
    connection_file: PathBuf,
    aft_events: PathBuf,
    fd_record: PathBuf,
}

impl Drop for Rig {
    fn drop(&mut self) {
        broca_test_support::kill_and_reap_tree(&mut self.child);
        broca_test_support::dump_rig_process_logs_on_failure(&self.dir, "daemon");
        broca_test_support::remove_rig_dir(&self.dir);
    }
}

impl Rig {
    async fn start() -> Option<Self> {
        let subc_core = sibling_bin("ck-subc", "SUBC_CORE_BIN");
        let fake_aft = sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
        for bin in [&subc_core, &fake_aft] {
            if !bin.exists() {
                assert!(
                    !require_subc(),
                    "launch_nonce_e2e: {bin:?} is missing, but this run is REQUIRED by \
                     BROCA_REQUIRE_SUBC"
                );
                println!(
                    "SKIP launch_nonce_e2e: {bin:?} is missing. This reports `ok` -- set \
                     BROCA_REQUIRE_SUBC=1 to make it a failure instead."
                );
                return None;
            }
        }
        let subc_core =
            broca_test_support::ckdev_binary(&subc_core, broca_test_support::scratch_root());

        // This test's own client must connect as a plain key holder. A test process running
        // under a daemon inherits that daemon's module id, and presenting it to this rig's
        // daemon would be refused; without a module id no identity is presented at all.
        std::env::remove_var("SUBC_MODULE_ID");

        let dir = broca_test_support::scratch_root()
            .join(format!("broca-launch-nonce-e2e-{}", std::process::id()));
        broca_test_support::remove_rig_dir(&dir);
        let config_home = dir.join("config");
        let runtime_dir = dir.join("runtime");
        // Pinned under the rig: the daemon derives its module log capture directory and the
        // module derives its own log directory from the data home, and neither may land in the
        // operator's real one.
        let data_home = runtime_dir.join("data-home");
        let config_path = config_home.join("cortexkit").join("subc.jsonc");
        std::fs::create_dir_all(config_path.parent().unwrap()).expect("create subc config dir");
        std::fs::create_dir_all(&runtime_dir).expect("create runtime dir");

        // The wrapper records what the daemon named as the descriptor, so a daemon that stopped
        // passing it fails this test by name instead of leaving broca with no nonce at all, then
        // removes the environment copy and becomes broca. The descriptor survives both execs:
        // the daemon cleared its close-on-exec flag, and neither the shell nor `env` closes it.
        let wrapper = dir.join("broca-without-env-nonce.sh");
        std::fs::write(
            &wrapper,
            "#!/bin/sh\n\
             printf '%s' \"${SUBC_LAUNCH_NONCE_FD:-}\" > \"$CK_TEST_FD_RECORD\"\n\
             exec env -u SUBC_LAUNCH_NONCE \"$CK_REAL_BROCA_BIN\" \"$@\"\n",
        )
        .expect("write the broca wrapper");
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700))
                .expect("make the broca wrapper executable");
        }

        let aft_events = dir.join("aft-events.jsonl");
        let fd_record = dir.join("broca-launch-nonce-fd.txt");
        let module_bin = broca_test_support::ckdev_binary(
            env!("CARGO_BIN_EXE_ck-broca"),
            broca_test_support::scratch_root(),
        );
        let config = serde_json::json!({
            "version": 1,
            "modules": {
                "broca": {
                    "program": wrapper,
                    "args": [],
                    // Reserved: the daemon registers this id only for a HELLO that echoes the
                    // nonce it spawned the process with.
                    "reserved": true,
                    "env": {
                        "CK_REAL_BROCA_BIN": module_bin,
                        "CK_TEST_FD_RECORD": fd_record,
                        // A base_url override puts every run on the AuthMode::None path, so the
                        // rig needs no vault. The run fails at the unreachable URL, after the
                        // tool route this test watches has been opened.
                        "BROCA_PROVIDER_BASE_URL": "http://127.0.0.1:1/unused",
                    },
                    "enabled": true
                },
                "aft": {
                    "program": fake_aft,
                    "args": [],
                    "env": {
                        "FAKE_AFT_MODULE_ID": "aft",
                        "FAKE_AFT_TOOLS": "read",
                        "FAKE_AFT_EVENTS_PATH": aft_events,
                    },
                    "enabled": true
                }
            }
        });
        std::fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap())
            .expect("write subc config");

        let connection_file = runtime_dir.join("subc-connection.json");
        let child = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(&subc_core, &dir, "daemon")?
                .env("XDG_CONFIG_HOME", &config_home)
                .env("XDG_RUNTIME_DIR", &runtime_dir)
                .env("XDG_DATA_HOME", &data_home)
                // broca's state root defaults to the data home pinned above; an inherited
                // BROCA_STATE_ROOT would override it and put this rig's WAL in the operator's state.
                .env_remove("BROCA_STATE_ROOT")
                .env("SUBC_PORT", "0")
                .env_remove("SUBC_LAUNCH_NONCE")
                .env_remove("SUBC_LAUNCH_NONCE_FD")
                .env_remove("SUBC_MODULE_ID")
                .kill_on_drop(true)
                .spawn()
        })
        .expect("spawn subc-core");
        let rig = Self {
            child,
            dir,
            connection_file,
            aft_events,
            fd_record,
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while !rig.connection_file.exists() {
            assert!(
                Instant::now() < deadline,
                "daemon never published its connection file"
            );
            sleep(Duration::from_millis(25)).await;
        }
        Some(rig)
    }

    /// Whether broca registered: its management surface accepts a route within the deadline.
    async fn broca_is_live(&self) -> bool {
        let probe = broca_protocol::BindIdentity::new(&self.dir, "probe", "launch-nonce");
        let deadline = Instant::now() + Duration::from_secs(20);
        while Instant::now() < deadline {
            if let Ok(conn) = broca_subc::SubcConnection::connect(&self.connection_file).await {
                if conn.route_open_management("broca", &probe).await.is_ok() {
                    return true;
                }
            }
            sleep(Duration::from_millis(50)).await;
        }
        false
    }
}

/// Every route.bind the tool stub recorded.
fn attach_events(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
        .filter(|event| event["kind"] == "attach")
        .collect()
}

/// WITH ONLY THE DESCRIPTOR, THE DAEMON ACCEPTS BROCA'S HELLO AND ATTESTS ITS LATER TOOL ROUTE AS
/// `reserved:broca`.
#[tokio::test(flavor = "multi_thread")]
async fn with_only_the_descriptor_hello_registers_and_a_later_route_is_attested_as_broca() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    let live = rig.broca_is_live().await;
    let named = std::fs::read_to_string(&rig.fd_record).unwrap_or_default();
    assert!(
        !named.is_empty(),
        "the daemon named no launch-nonce descriptor for broca, so this rig tests nothing: \
         broca had neither the pipe nor the environment copy"
    );
    assert!(
        live,
        "broca never registered. It is reserved, so the daemon refuses a HELLO that does not \
         echo the nonce it was handed over descriptor {named}"
    );

    // A tool-bearing send makes broca open its tool route (a detached warm bind), after HELLO.
    use broca_module_serve::client::{SendOutcome, SendSpec, SessionClient};
    let identity = broca_protocol::BindIdentity::new(&rig.dir, "probe", "tool-route");
    let conn = broca_subc::SubcConnection::connect(&rig.connection_file)
        .await
        .expect("connect to the rig daemon");
    let client = SessionClient::bind_with_timeout(conn, "broca", identity, Duration::from_secs(20))
        .await
        .unwrap_or_else(|error| {
            let data_home = rig.dir.join("runtime/data-home/cortexkit");
            eprintln!(
                "{}",
                super::session_client::rig_log_diagnostics(&data_home, &data_home.join("broca"))
            );
            panic!("bind broca's management surface: {error:?}");
        });
    let tool = serde_json::from_value(serde_json::json!({
        "name": "read",
        "description": "Read a file from disk",
        "input_schema": {"type": "object", "properties": {}}
    }))
    .expect("tool schema");
    let outcome = client
        .send(SendSpec {
            prompt: "hi",
            system: None,
            provider: "mock",
            model: "mock-model",
            variant: None,
            service_tier: None,
            tools: Some(vec![tool]),
            send_id: None,
            max_output_tokens: None,
            cache: None,
            budget: None,
            auth: None,
            one_shot: None,
            context_management: None,
        })
        .await;
    assert!(
        matches!(outcome, Ok(SendOutcome::Active { .. })),
        "the tool-bearing send was not admitted: {outcome:?}"
    );

    let attaches =
        broca_test_support::wait_until("tool route attach after admitted send", || async {
            let attaches = attach_events(&rig.aft_events);
            if !attaches.is_empty() {
                Ok(attaches)
            } else {
                Err(format!(
                    "attach_events=0, path={}",
                    rig.aft_events.display()
                ))
            }
        })
        .await;
    assert!(
        !attaches.is_empty(),
        "broca opened no route to the tool module after HELLO"
    );
    for attach in &attaches {
        assert_eq!(
            attach["principal"],
            serde_json::json!({"kind": "reserved", "module_id": "broca"}),
            "the daemon attests a route as reserved:broca only when the route open presents the \
             nonce broca was spawned with; this one did not: {attach}"
        );
    }
}
