//! Shared dogfood rig: brings up the real `subc-core` daemon + `fake-aft-stub`
//! (built from the subconscious checkout) in a per-test temp dir, and connects
//! as a consumer. Used by both the stub-provider exit gate (`dogfood.rs`) and
//! the live-Anthropic end-to-end demo (`live_e2e.rs`).
//!
//! Binary paths come from SUBC_CORE_BIN / FAKE_AFT_STUB_BIN, else the sibling
//! checkout's target/debug. If the binaries are absent the rig returns None so
//! the caller SKIPS with a clear message (never a silent pass).

#![allow(dead_code)]

use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use broca_subc::{SubcConnection, SubcToolPlane};
use serde_json::json;
use tokio::time::sleep;

pub fn checkout_bin(name: &str, env_key: &str) -> PathBuf {
    broca_test_support::subc_bin(name, env_key)
}

/// Whether a missing subc binary (`subc-core` / `fake-aft-stub`) must FAIL the test
/// rather than skip it. True in CI (GitHub Actions sets `CI=true`) or when
/// `BROCA_REQUIRE_SUBC` is set. The skip-clean path is a local-dev convenience (run
/// without first building the sibling subc binaries); in CI their absence is a real
/// failure, because a silent skip would give the whole real-subc spine suite ZERO
/// coverage while showing green.
pub fn subc_required() -> bool {
    std::env::var_os("BROCA_REQUIRE_SUBC").is_some()
        || std::env::var("CI").map(|v| v == "true").unwrap_or(false)
}

/// The chokepoint that turns "a required subc binary is missing" into either a loud
/// failure (when required, i.e. CI) or a clean skip (local dev). The env read
/// (`subc_required`) is kept separate so this decision is unit-testable without touching
/// the process environment. Only `subc-core` and `fake-aft-stub` — the binaries CI
/// builds from the sibling checkout — route through here; a caller-supplied module
/// program (e.g. the real `aft` binary, which CI does NOT build) is NOT guarded and
/// always skips cleanly when absent.
pub fn decide_subc_bin(found: Option<PathBuf>, required: bool, name: &str) -> Option<PathBuf> {
    match found {
        Some(p) => Some(p),
        None if required => panic!(
            "{name} is missing, but the real-subc tests are REQUIRED (CI or \
             BROCA_REQUIRE_SUBC). Build the sibling subc binaries with \
             `cd ../subconscious && cargo build -p subc-core --bins`. Skipping here would \
             give the real-subc spine suite zero coverage while showing green."
        ),
        None => {
            eprintln!(
                "SKIP: {name} missing. Build it: \
                 (cd ../subconscious && cargo build -p subc-core --bins)"
            );
            None
        }
    }
}

/// Resolve a REQUIRED subc binary (`subc-core` / `fake-aft-stub`) through the
/// anti-masking guard: present → its path; absent + required (CI) → panic; absent + not
/// required (local) → clean skip.
fn required_subc_bin(name: &str, env_key: &str) -> Option<PathBuf> {
    let path = checkout_bin(name, env_key);
    let found = path.exists().then_some(path);
    decide_subc_bin(found, subc_required(), name).map(|path| {
        if name.starts_with("ck-") {
            broca_test_support::ckdev_binary(path, broca_test_support::scratch_root())
        } else {
            path
        }
    })
}

pub struct Rig {
    _child: tokio::process::Child,
    pub dir: PathBuf,
    pub connection_file: PathBuf,
}

impl Drop for Rig {
    fn drop(&mut self) {
        broca_test_support::kill_and_reap_tree(&mut self._child);
        broca_test_support::dump_rig_process_logs_on_failure(&self.dir, "daemon");
        broca_test_support::remove_rig_dir(&self.dir);
    }
}

/// One module the daemon should supervise: its subc module id, the program to run,
/// its args, and its environment.
pub struct ModuleSpec {
    pub module_id: String,
    pub program: PathBuf,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
}

impl Rig {
    /// Spawn the daemon with the `aft` stub configured by `stub_env`. Returns
    /// None if the binaries are absent (caller should skip). This is the
    /// fake-aft-stub convenience path used by the stub/live tool-plane tests.
    pub async fn start(tag: &str, stub_env: &[(&str, &str)]) -> Option<Rig> {
        // fake-aft-stub is built by CI from the sibling subc-core crate, so its absence is
        // a loud failure in CI (anti-masking) and a clean skip locally.
        let fake_aft = required_subc_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN")?;
        let module = ModuleSpec {
            module_id: "aft".to_string(),
            program: fake_aft,
            args: vec![],
            env: stub_env
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        };
        Rig::start_with_module(tag, module).await
    }

    /// Spawn the daemon supervising an ARBITRARY module binary — used to drive the
    /// REAL `aft` binary (point `module.program` at it) instead of the stub.
    /// Returns None if subc-core or the module program is absent (caller skips).
    pub async fn start_with_module(tag: &str, module: ModuleSpec) -> Option<Rig> {
        Rig::start_with_modules(tag, vec![module]).await
    }

    /// Spawn the daemon supervising multiple module binaries in one subc-core process.
    /// Returns None if subc-core or any module program is absent (caller skips).
    pub async fn start_with_modules(tag: &str, modules: Vec<ModuleSpec>) -> Option<Rig> {
        // When the test process itself runs under subc supervision (an operator shell
        // spawned by a daemon-supervised tool), it inherits that daemon's attestation
        // env (SUBC_MODULE_ID / SUBC_LAUNCH_NONCE). The in-process consumer connection
        // would present that foreign identity to THIS rig's fresh daemon, which rejects
        // the unknown launch nonce (`bad_consumer_identity`). The rig talks to its own
        // daemon as a plain Direct client, so scrub the inherited attestation once,
        // before any connection is opened (Once: env mutation is not thread-safe and
        // tests in one binary run concurrently).
        static SCRUB_ATTESTATION: std::sync::Once = std::sync::Once::new();
        SCRUB_ATTESTATION.call_once(|| {
            std::env::remove_var(subc_protocol::SUBC_MODULE_ID_ENV);
            std::env::remove_var(subc_protocol::SUBC_LAUNCH_NONCE_ENV);
        });
        // subc-core is built by CI from the sibling checkout → guarded (loud in CI).
        let subc_core = required_subc_bin("ck-subc", "SUBC_CORE_BIN")?;
        // The module program is caller-supplied. The real `aft` binary is NOT built by
        // CI, so its absence is always a clean skip (never a CI failure) — only the subc
        // binaries CI provides are anti-masking-guarded above.
        for module in &modules {
            if !module.program.exists() {
                eprintln!("SKIP: module binary missing ({})", module.program.display());
                return None;
            }
        }

        let dir = broca_test_support::scratch_root().join(format!(
            "broca-rig-{}-{}",
            std::process::id(),
            tag
        ));
        broca_test_support::remove_rig_dir(&dir);
        let config_home = dir.join("config");
        let runtime_dir = dir.join("runtime");
        let config_path = config_home.join("cortexkit").join("subc.jsonc");
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&runtime_dir).unwrap();

        let modules_json: serde_json::Map<String, serde_json::Value> = modules
            .iter()
            .map(|module| {
                let env_json: serde_json::Map<String, serde_json::Value> = module
                    .env
                    .iter()
                    .map(|(k, v)| (k.clone(), json!(v)))
                    .collect();
                let args_json: Vec<serde_json::Value> =
                    module.args.iter().map(|a| json!(a)).collect();
                (
                    module.module_id.clone(),
                    json!({
                        "program": module.program.to_string_lossy(),
                        "args": args_json,
                        "env": env_json,
                        "enabled": true,
                    }),
                )
            })
            .collect();
        let config = json!({
            "version": 1,
            "modules": modules_json
        });
        std::fs::write(&config_path, serde_json::to_string_pretty(&config).unwrap()).unwrap();

        let connection_file = runtime_dir.join("subc-connection.json");
        let child = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(&subc_core, &dir, "daemon")?
                // The daemon derives its per-module capture directory from the DATA home
                // (`<data-home>/cortexkit/run/logs/<module_id>.stderr.log`), not from
                // XDG_RUNTIME_DIR. A rig that leaves it unset appends its own "broca" module's
                // stderr into the OPERATOR'S production capture file, where placement gates read
                // seal-line counts. Pinning it under the rig keeps the count about production.
                .env("XDG_CONFIG_HOME", &config_home)
                .env("XDG_RUNTIME_DIR", &runtime_dir)
                .env("XDG_DATA_HOME", runtime_dir.join("data-home"))
                .env("SUBC_PORT", "0")
                .kill_on_drop(true)
                .spawn()
        })
        .expect("spawn subc-core");

        Some(Rig {
            _child: child,
            dir,
            connection_file,
        })
    }

    /// The supervising subc-core daemon's PID (for tests that kill supervised
    /// grandchildren via `pgrep -P`).
    pub fn daemon_pid(&self) -> Option<u32> {
        self._child.id()
    }

    /// Crash every module process the daemon supervises (the daemon's direct
    /// children), simulating an unexpected module crash mid-session. subc-core's
    /// supervisor then respawns them under its restart policy. Unix-only (uses
    /// `pgrep`/`kill`); a no-op elsewhere. Returns the count killed.
    pub async fn crash_supervised_modules(&self) -> usize {
        let Some(pid) = self.daemon_pid() else {
            return 0;
        };
        let out = tokio::process::Command::new("pgrep")
            .arg("-P")
            .arg(pid.to_string())
            .output()
            .await;
        let Ok(out) = out else { return 0 };
        let mut killed = 0;
        for line in String::from_utf8_lossy(&out.stdout).lines() {
            if let Ok(child_pid) = line.trim().parse::<u32>() {
                let _ = tokio::process::Command::new("kill")
                    .arg("-9")
                    .arg(child_pid.to_string())
                    .status()
                    .await;
                killed += 1;
            }
        }
        killed
    }

    /// Connect once the daemon publishes its connection file + accepts auth.
    pub async fn connect(&self) -> SubcConnection {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if self.connection_file.exists() {
                if let Ok(conn) = SubcConnection::connect(&self.connection_file).await {
                    return conn;
                }
            }
            assert!(Instant::now() < deadline, "daemon never became ready");
            sleep(Duration::from_millis(25)).await;
        }
    }
}

/// Wait until the catalog reports the `aft` provider exposing `tool` (the stub
/// registers asynchronously after the daemon binds).
pub async fn wait_for_aft(plane: &SubcToolPlane, tool: &str) {
    wait_for_module_tool(plane, "aft", tool).await;
}

/// Wait until the catalog reports `module_id` exposing `tool`.
pub async fn wait_for_module_tool(plane: &SubcToolPlane, module_id: &str, tool: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(catalog) = plane.catalog().await {
            let has = catalog.iter().any(|e| {
                e.module_id == module_id
                    && e.roles.iter().any(|r| {
                        matches!(r,
                            subc_protocol::manifest::ProviderRole::ToolProvider { tools, .. }
                            if tools.iter().any(|t| t.name == tool))
                    })
            });
            if has {
                return;
            }
        }
        assert!(
            Instant::now() < deadline,
            "{module_id} provider/tool {tool} never registered"
        );
        sleep(Duration::from_millis(25)).await;
    }
}
