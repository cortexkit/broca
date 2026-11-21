//! broca under the real daemon's settings gate: an old-shape `broca.jsonc` in broca's own folder
//! is copied, converted, to the path the daemon reads, and the settings in it reach broca through
//! the daemon's view.
//!
//! It needs a `ck-subc` built with subc's `test-support` feature, named by `SUBC_GATE_CORE_BIN`:
//! that build lets `SUBC_TEST_CONFIG_GATE=1` turn the gate on with an isolated config store, never
//! the operator's. The rig ck-subc the other suites use is built without it, so under them broca
//! reads its file as under any daemon that doesn't manage its settings. CI's fixtures don't build
//! that daemon, so the test is required only where `BROCA_REQUIRE_SUBC_GATE` is set (the native
//! release suites), not by `BROCA_REQUIRE_SUBC` alone.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use tokio::process::Child;
use tokio::time::{sleep, Instant};

struct Rig {
    child: Child,
    dir: PathBuf,
    connection_file: PathBuf,
    config_home: PathBuf,
    data_home: PathBuf,
}

impl Drop for Rig {
    fn drop(&mut self) {
        broca_test_support::kill_and_reap_tree(&mut self.child);
        broca_test_support::dump_rig_process_logs_on_failure(&self.dir, "daemon");
        broca_test_support::remove_rig_dir(&self.dir);
    }
}

impl Rig {
    /// Starts the gated daemon with broca enabled and `old` as broca's file in its own folder.
    /// `None` when no gate build of the daemon is named and the run doesn't require one.
    async fn start(name: &str, old: &[u8]) -> Option<Self> {
        let Some(subc_core) = std::env::var_os("SUBC_GATE_CORE_BIN").map(PathBuf::from) else {
            assert!(
                std::env::var_os("BROCA_REQUIRE_SUBC_GATE").is_none(),
                "settings_gate_real_daemon: SUBC_GATE_CORE_BIN is unset, but this run is \
                 REQUIRED by BROCA_REQUIRE_SUBC_GATE"
            );
            println!(
                "SKIP settings_gate_real_daemon: SUBC_GATE_CORE_BIN is unset. This reports \
                 `ok`; set BROCA_REQUIRE_SUBC_GATE=1 to make it a failure instead."
            );
            return None;
        };
        let subc_core =
            broca_test_support::ckdev_binary(&subc_core, broca_test_support::scratch_root());
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-settings-gate-{name}-{}", std::process::id()));
        broca_test_support::remove_rig_dir(&dir);
        let config_home = dir.join("config");
        let runtime_dir = dir.join("runtime");
        let data_home = runtime_dir.join("data-home");
        let old_file = config_home.join("cortexkit/broca/broca.jsonc");
        std::fs::create_dir_all(old_file.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&runtime_dir).unwrap();
        std::fs::write(&old_file, old).unwrap();
        // No BROCA_CONFIG_PATH: broca finds its old file in its own folder under the config
        // home, as on an operator's machine.
        let config = serde_json::json!({
            "version": 1,
            "modules": {
                "broca": {
                    "program": broca_test_support::ckdev_binary(
                        env!("CARGO_BIN_EXE_ck-broca"),
                        broca_test_support::scratch_root(),
                    ),
                    "args": [],
                    "env": {"BROCA_PROVIDER_BASE_URL": "http://127.0.0.1:1/unused"},
                    "enabled": true
                }
            }
        });
        std::fs::write(
            config_home.join("cortexkit/subc.jsonc"),
            serde_json::to_vec_pretty(&config).unwrap(),
        )
        .unwrap();
        let connection_file = runtime_dir.join("subc-connection.json");
        let child = broca_test_support::tokio_rig_command(&subc_core, &dir, "daemon")
            .expect("create daemon log files")
            .arg("--config-store-root")
            .arg(dir.join("config-store"))
            .env("SUBC_TEST_CONFIG_GATE", "1")
            .env("XDG_CONFIG_HOME", &config_home)
            .env("XDG_RUNTIME_DIR", &runtime_dir)
            .env("XDG_DATA_HOME", &data_home)
            .env_remove("BROCA_STATE_ROOT")
            .env_remove("BROCA_CONFIG_PATH")
            .env("SUBC_PORT", "0")
            .env("SUBC_CGROUP_PLACEMENT", "disabled")
            .kill_on_drop(true)
            .spawn()
            .expect("spawn the gated daemon");
        let rig = Self {
            child,
            dir,
            connection_file,
            config_home,
            data_home,
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

    /// Whether broca's management route opens within 30 s. broca stays unready, refusing it,
    /// until the daemon's settings view arrives and broca is built from it.
    async fn ready(&self) -> bool {
        let probe = broca_protocol::BindIdentity::new(&self.dir, "probe", "settings-gate");
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Ok(conn) = broca_subc::SubcConnection::connect(&self.connection_file).await {
                if conn.route_open_management("broca", &probe).await.is_ok() {
                    return true;
                }
            }
            sleep(Duration::from_millis(100)).await;
        }
        false
    }

    fn logs(&self) -> String {
        let data_home = self.data_home.join("cortexkit");
        super::session_client::rig_log_diagnostics(&data_home, &data_home.join("broca"))
    }
}

/// Every file under `dir` whose bytes are `bytes`.
fn files_holding(dir: &Path, bytes: &[u8]) -> Vec<PathBuf> {
    let mut found = Vec::new();
    let Ok(entries) = std::fs::read_dir(dir) else {
        return found;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            found.extend(files_holding(&path, bytes));
        } else if std::fs::read(&path).is_ok_and(|held| held == bytes) {
            found.push(path);
        }
    }
    found
}

/// broca's old file holds `max_same_account_wait` as a bare number, the shape broca's schema no
/// longer accepts. It is copied to `<config home>/cortexkit/broca.jsonc` with that value rewritten
/// to the tagged form the schema declares, its old bytes are saved in broca's state root, and the
/// old file is left as it was. The send that follows is the proof the settings reached broca
/// through the daemon: the settings turn session encryption off, and with the defaults (encrypted
/// storage, and no vault in this rig) broca would refuse the send for want of the storage key.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_old_settings_file_moves_and_its_settings_reach_broca_through_the_gate() {
    let mut old = super::stored_log::plain_storage();
    old["max_same_account_wait"] = serde_json::json!(30);
    let old = serde_json::to_vec_pretty(&old).unwrap();
    let Some(rig) = Rig::start("move", &old).await else {
        return;
    };
    assert!(
        rig.ready().await,
        "broca never became ready under the gate\n{}",
        rig.logs()
    );
    let new_file = rig.config_home.join("cortexkit/broca.jsonc");
    let moved: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&new_file).expect("the moved file")).unwrap();
    assert_eq!(
        moved["max_same_account_wait"],
        serde_json::json!({"kind": "minutes", "minutes": 30})
    );
    assert_eq!(moved["session_storage"]["encryption"], "off");
    assert_eq!(
        std::fs::read(rig.config_home.join("cortexkit/broca/broca.jsonc")).unwrap(),
        old,
        "the old file is left as it was"
    );
    let saved = files_holding(&rig.data_home, &old);
    assert_eq!(saved.len(), 1, "the old bytes are saved once: {saved:?}");

    use broca_module_serve::client::{SendSpec, SessionClient};
    let identity = broca_protocol::BindIdentity::new(&rig.dir, "probe", "after-gate");
    let conn = broca_subc::SubcConnection::connect(&rig.connection_file)
        .await
        .expect("connect to the rig daemon");
    let client = SessionClient::bind_with_timeout(conn, "broca", identity, Duration::from_secs(20))
        .await
        .unwrap_or_else(|error| panic!("bind broca: {error:?}\n{}", rig.logs()));
    let outcome = client
        .send(SendSpec {
            prompt: "hi",
            system: None,
            provider: "mock",
            model: "mock-model",
            variant: None,
            service_tier: None,
            tools: None,
            send_id: None,
            max_output_tokens: None,
            cache: None,
            budget: None,
            auth: None,
            one_shot: None,
            context_management: None,
            session_class: None,
            plan: None,
        })
        .await;
    assert!(
        outcome.is_ok(),
        "the send was refused: {outcome:?}\n{}",
        rig.logs()
    );
}
