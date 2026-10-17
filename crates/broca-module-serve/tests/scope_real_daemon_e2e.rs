//! The scope carrier's inbound half against the REAL subc daemon and the REAL ck-broca.
//!
//! In-process tests build `route.bind` themselves; only a real daemon shows what it actually
//! stamps. The daemon supervises, beside broca, two copies of subconscious's `fake-aft-stub`:
//! - the scope OWNER, which syncs whatever scopes this test writes to its sync file, and
//!   writes out its launch nonce so this test can connect as the owner (the daemon attests a
//!   connection presenting a supervised module's nonce as that module, `reserved:<id>`);
//! - a CARRIER, listed on the scope so the daemon admits its scoped open, standing in for a
//!   second identity that is not the scope's owner.
//!
//! The test, as the owner, opens broca's management surface under the synced scope and sends.
//! Broca must read the daemon's stamp and freeze `(owner, ref, scope_epoch)` on the first
//! `RunStarted`, in a gated frame requiring `scope/v1`, after the WAL feature floor lists it.
//! The carrier's send over the same scope is refused `scope_owner_mismatch`. And a session an
//! unscoped (direct) caller already drove cannot be adopted: the owner's scoped `route.open` is
//! answered with broca's `scope_adoption_refused`, relayed by the daemon from the bind.
#![cfg(unix)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use broca_subc::{RouteOpenRetry, SubcConnection};
use subc_control::ConsumerIdentity;
use subc_protocol::scope::ScopeSelector;
use subc_protocol::Principal;
use tokio::process::{Child, Command};
use tokio::time::{sleep, Instant};

const OWNER: &str = "prefrontal-core";
const CARRIER: &str = "basal";
const OTHER_CARRIER: &str = "broca-e2e-other-carrier";
const SCOPE_REF: &str = "head-e2e";
const SCOPE_EPOCH: u64 = 7;
/// Must equal `SCOPE_SYNC_CORR_BASE` in subconscious's fake-aft-stub: its `n`th scope.sync goes
/// out with corr `SCOPE_SYNC_CORR_BASE + n`, and its reply is recorded under that corr.
const SCOPE_SYNC_CORR_BASE: u64 = 1_000_000;

fn sibling_bin(name: &str, env_key: &str) -> PathBuf {
    broca_test_support::subc_bin(name, env_key)
}

/// `scripts/rust-test-gate.sh` exports `BROCA_REQUIRE_SUBC`, so in a gated run a missing
/// daemon is a failure that names itself; a bare local run may still skip, out loud.
fn require_subc() -> bool {
    std::env::var_os("BROCA_REQUIRE_SUBC").is_some()
}

struct Rig {
    child: Child,
    dir: PathBuf,
    connection_file: PathBuf,
    state_root: PathBuf,
    sync_path: PathBuf,
    owner_events: PathBuf,
    owner_nonce: PathBuf,
    carrier_nonce: PathBuf,
    other_carrier_nonce: PathBuf,
}

impl Drop for Rig {
    fn drop(&mut self) {
        // Let the supervisor drain its modules before killing it. Otherwise the placed
        // release processes can briefly outlive the rig and contaminate the pid snapshot.
        if let Some(pid) = self.child.id() {
            if let Ok(Some(process)) = subc_os::Process::open(pid) {
                let _ = process.signal(subc_os::Signal::Terminate);
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(15);
            while matches!(self.child.try_wait(), Ok(None)) && std::time::Instant::now() < deadline
            {
                std::thread::sleep(Duration::from_millis(25));
            }
        }
        broca_test_support::kill_and_reap(&mut self.child);
        broca_test_support::dump_rig_process_logs_on_failure(&self.dir, "daemon");
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl Rig {
    async fn start() -> Option<Self> {
        Self::start_configured(None, None, None).await
    }

    async fn start_configured(
        binary: Option<&Path>,
        state: Option<&Path>,
        model_url: Option<&str>,
    ) -> Option<Self> {
        let subc_core = sibling_bin("ck-subc", "SUBC_CORE_BIN");
        let stub = sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
        for bin in [&subc_core, &stub] {
            if !bin.exists() {
                assert!(
                    !require_subc(),
                    "scope_real_daemon_e2e: {bin:?} is missing, but this run is REQUIRED by \
                     BROCA_REQUIRE_SUBC (build it: cd ../subconscious && cargo build -p \
                     subc-core --bins)"
                );
                println!(
                    "SKIP scope_real_daemon_e2e: {bin:?} is missing. This reports `ok` -- set \
                     BROCA_REQUIRE_SUBC=1 to make it a failure instead."
                );
                return None;
            }
        }
        let subc_core =
            broca_test_support::ckdev_binary(&subc_core, broca_test_support::scratch_root());

        static NEXT_RIG: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-scope-e2e-{}-{}",
            std::process::id(),
            NEXT_RIG.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let config_home = dir.join("config");
        let runtime_dir = dir.join("runtime");
        // Pinned under the rig: the daemon derives its module log capture directory, and the
        // modules their own log directories, from the data home; none may land in the
        // operator's real one.
        let data_home = runtime_dir.join("data-home");
        let state_root = state.map_or_else(|| dir.join("broca-state"), Path::to_path_buf);
        let config_path = config_home.join("cortexkit").join("subc.jsonc");
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&runtime_dir).unwrap();

        let sync_path = dir.join("owner-scopes.json");
        let owner_events = dir.join("owner-events.jsonl");
        let owner_nonce = dir.join("owner.nonce");
        let carrier_nonce = dir.join("carrier.nonce");
        let other_carrier_nonce = dir.join("other-carrier.nonce");
        let module_bin = broca_test_support::ckdev_binary(
            binary
                .map(Path::to_path_buf)
                .or_else(|| std::env::var_os("BROCA_E2E_MODULE_BIN").map(PathBuf::from))
                .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_ck-broca")))
                .canonicalize()
                .expect("the requested module binary exists"),
            broca_test_support::scratch_root(),
        );
        let module_bytes = std::fs::read(&module_bin).unwrap();
        use sha2::{Digest, Sha256};
        let module_sha = format!("{:x}", Sha256::digest(&module_bytes));
        let version_child = broca_test_support::retry_executable_file_busy(|| {
            Command::new(&module_bin)
                .arg("--version")
                .env_remove("BROCA_STATE_ROOT")
                .env("HOME", &dir)
                .env("XDG_CONFIG_HOME", &config_home)
                .env("XDG_RUNTIME_DIR", &runtime_dir)
                .env("XDG_DATA_HOME", &data_home)
                // `spawn` inherits stdio, unlike `output`; the version is read from stdout.
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
        })
        .expect("spawn the selected module's version probe");
        let version = version_child
            .wait_with_output()
            .await
            .expect("read the selected module's version");
        assert!(
            version.status.success(),
            "module --version failed: {version:?}"
        );
        let version = String::from_utf8(version.stdout).unwrap();
        // An override must name what it expects to run, so a report about "the release bytes"
        // can't silently describe some other file at that path.
        if binary.is_none() && std::env::var_os("BROCA_E2E_MODULE_BIN").is_some() {
            let want_version = std::env::var("BROCA_E2E_MODULE_VERSION").expect(
                "BROCA_E2E_MODULE_BIN needs BROCA_E2E_MODULE_VERSION (e.g. `ck-broca 0.3.176`)",
            );
            let want_sha = std::env::var("BROCA_E2E_MODULE_SHA256")
                .expect("BROCA_E2E_MODULE_BIN needs BROCA_E2E_MODULE_SHA256");
            assert_eq!(version.trim(), want_version, "requested module version");
            assert_eq!(module_sha, want_sha, "requested module bytes");
        }
        println!(
            "MODULE binary={} version={} sha256={module_sha}",
            module_bin.display(),
            version.trim()
        );
        let stub_module = |module_id: &str, env: serde_json::Value| {
            let mut env = env;
            env["FAKE_AFT_MODULE_ID"] = module_id.into();
            serde_json::json!({
                "program": stub,
                "args": [],
                "env": env,
                "enabled": true,
                // A scope's owner and carriers are named as reserved principals.
                "reserved": true,
            })
        };
        let config = serde_json::json!({
            "version": 1,
            "scope_authority_owners": [OWNER],
            "modules": {
                "broca": {
                    "program": module_bin,
                    "args": [],
                    "reserved": true,
                    "env": {
                        "BROCA_STATE_ROOT": state_root,
                        "XDG_DATA_HOME": data_home,
                        // The override selects AuthMode::None, so the rig needs no vault.
                        // Scope-only tests stop at admission; billing tests use a loopback model.
                        "BROCA_PROVIDER_BASE_URL": model_url.unwrap_or("http://127.0.0.1:1/unused"),
                    },
                    "enabled": true
                },
                OWNER: stub_module(OWNER, serde_json::json!({
                    "FAKE_AFT_SCOPE_SYNC_PATH": sync_path,
                    "FAKE_AFT_EVENTS_PATH": owner_events,
                    "FAKE_AFT_LAUNCH_NONCE_PATH": owner_nonce,
                })),
                CARRIER: stub_module(CARRIER, serde_json::json!({
                    "FAKE_AFT_LAUNCH_NONCE_PATH": carrier_nonce,
                })),
                OTHER_CARRIER: stub_module(OTHER_CARRIER, serde_json::json!({
                    "FAKE_AFT_LAUNCH_NONCE_PATH": other_carrier_nonce,
                })),
            }
        });
        std::fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();

        let connection_file = runtime_dir.join("subc-connection.json");
        // The daemon launches each module through a trampoline: it re-executes its own binary,
        // which then replaces itself with the module image. The roster names the pid before that
        // replacement, so for a moment the pid's image is the daemon's, not broca's.
        let daemon_image = subc_os::file_identity(&subc_core);
        let child = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(&subc_core, &dir, "daemon")?
                .env("HOME", &dir)
                .env("XDG_CONFIG_HOME", &config_home)
                .env("XDG_RUNTIME_DIR", &runtime_dir)
                .env("XDG_DATA_HOME", &data_home)
                // The daemon passes its environment to broca, so an operator's exported
                // BROCA_STATE_ROOT must not reach it; the module config above names the rig's own.
                .env_remove("BROCA_STATE_ROOT")
                .env("SUBC_PORT", "0")
                .env_remove("SUBC_LAUNCH_NONCE")
                .env_remove("SUBC_LAUNCH_NONCE_FD")
                .env_remove("SUBC_MODULE_ID")
                .kill_on_drop(true)
                .spawn()
        })
        .expect("spawn ck-subc");
        let rig = Self {
            child,
            dir,
            connection_file,
            state_root,
            sync_path,
            owner_events,
            owner_nonce,
            carrier_nonce,
            other_carrier_nonce,
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        while !rig.connection_file.exists() {
            assert!(
                Instant::now() < deadline,
                "the daemon never published its connection file"
            );
            sleep(Duration::from_millis(25)).await;
        }
        // Check the kernel's running image, not just the configured program path. The daemon
        // records its supervised pid in this isolated data home's live-child roster.
        let roster = data_home.join("cortexkit/run/live-children.json");
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            let child = std::fs::read(&roster)
                .ok()
                .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                .and_then(|record| {
                    record["children"]
                        .as_array()?
                        .iter()
                        .find(|child| child["module_id"] == "broca")
                        .cloned()
                });
            if let Some(child) = child {
                let pid = u32::try_from(child["pid"].as_u64().unwrap()).unwrap();
                let process = subc_os::Process::open(pid)
                    .unwrap()
                    .expect("supervised broca is live");
                let observed = process.observe().expect("observe supervised broca");
                if observed.executable.is_some() && observed.executable == daemon_image {
                    assert!(
                        Instant::now() < deadline,
                        "pid {pid} was still the daemon's launch trampoline after 30 s, so the \
                         module image never replaced it"
                    );
                    sleep(Duration::from_millis(25)).await;
                    continue;
                }
                assert_eq!(
                    observed.executable,
                    subc_os::file_identity(&module_bin),
                    "the running broca image must be the requested binary"
                );
                assert_eq!(
                    std::fs::read(&module_bin).unwrap(),
                    module_bytes,
                    "the versioned and hashed file must not change during launch"
                );
                println!(
                    "MODULE running pid={pid} image={:?} verified requested binary",
                    observed.executable
                );
                break;
            }
            assert!(
                Instant::now() < deadline,
                "broca never appeared in {}",
                roster.display()
            );
            sleep(Duration::from_millis(25)).await;
        }
        Some(rig)
    }

    async fn wait_for_file(&self, path: &Path) -> String {
        broca_test_support::wait_until("scope rig marker file", || async {
            if let Ok(text) = std::fs::read_to_string(path) {
                if !text.is_empty() {
                    return Ok(text);
                }
            }
            Err(format!("{} absent or empty", path.display()))
        })
        .await
    }

    /// Have the owner sync one head scope listing the carrier, and wait for the daemon to
    /// accept it.
    async fn sync_scope(&self) {
        self.sync_scope_with(None, 1).await;
    }

    async fn sync_scope_with(&self, flow_id: Option<&str>, generation: u64) {
        self.sync_attributes(flow_id.map_or_else(|| serde_json::json!({}), |id| serde_json::json!({"flow_id": id, "agent_id": "owner-agent", "delegates": true})), generation).await;
    }

    async fn sync_attributes(&self, attributes: serde_json::Value, generation: u64) {
        let record = serde_json::json!([{
            "ref": SCOPE_REF,
            "scope_epoch": SCOPE_EPOCH,
            "kind": "head",
            "attributes": attributes,
            "carriers": [
                { "principal": { "kind": "reserved", "module_id": CARRIER } },
                { "principal": { "kind": "reserved", "module_id": OTHER_CARRIER } }
            ],
        }]);
        self.sync_scopes(record, generation).await;
    }

    async fn sync_scopes(&self, record: serde_json::Value, generation: u64) {
        let staging = self.sync_path.with_extension("staging");
        std::fs::write(&staging, serde_json::to_vec(&record).unwrap()).unwrap();
        std::fs::rename(&staging, &self.sync_path).unwrap();
        let corr = SCOPE_SYNC_CORR_BASE + generation;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let reply = std::fs::read_to_string(&self.owner_events)
                .unwrap_or_default()
                .lines()
                .filter_map(|line| serde_json::from_str::<serde_json::Value>(line).ok())
                .find(|event| {
                    event["corr"] == corr
                        && (event["kind"] == "scope_sync_response" || event["kind"] == "error")
                });
            if let Some(reply) = reply {
                assert_eq!(
                    reply["kind"], "scope_sync_response",
                    "scope.sync refused: {reply}"
                );
                for result in reply["body_json"]["results"].as_array().unwrap() {
                    assert_ne!(
                        result["outcome"], "refused",
                        "a scope was refused: {result}"
                    );
                }
                return;
            }
            assert!(
                Instant::now() < deadline,
                "the owner's scope.sync never answered"
            );
            sleep(Duration::from_millis(25)).await;
        }
    }

    async fn connect_as(&self, module_id: &str, nonce_path: &Path) -> SubcConnection {
        let identity = ConsumerIdentity {
            module_id: module_id.into(),
            launch_nonce: self.wait_for_file(nonce_path).await.trim().to_owned(),
        };
        SubcConnection::connect_with_consumer_identity(&self.connection_file, Some(identity))
            .await
            .expect("connect to the rig daemon")
    }

    fn session(&self, name: &str) -> broca_protocol::BindIdentity {
        // Canonical, as broca keys it: on macOS the scratch root sits under /var, a symlink to
        // /private/var, and the WAL address is derived from the resolved project root.
        broca_protocol::BindIdentity::new(self.dir.canonicalize().unwrap(), "scope-e2e", name)
    }

    fn wal_path(&self, session: &broca_protocol::BindIdentity) -> PathBuf {
        broca_core::FileWalStore::new(self.state_root.join("wal")).wal_path(session)
    }
}

/// Every file under `dir`, for a failure message.
fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk(&path));
            } else {
                out.push(path);
            }
        }
    }
    out
}

fn selector() -> ScopeSelector {
    ScopeSelector::new(
        Principal::Reserved {
            module_id: OWNER.into(),
        },
        SCOPE_REF,
        Some(SCOPE_EPOCH),
    )
}

/// Open broca's management surface under the synced scope, retrying while broca is still
/// registering (the daemon refuses an open to a module that is not live yet).
async fn open_scoped(
    conn: &SubcConnection,
    session: &broca_protocol::BindIdentity,
) -> Result<broca_subc::RouteHandle, broca_subc::ConnError> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let retry = RouteOpenRetry {
            deadline: Instant::now() + Duration::from_secs(2),
            max_interval: Duration::from_millis(100),
        };
        match conn
            .route_open_management_scoped_within("broca", session, selector(), retry)
            .await
        {
            Ok(route) => return Ok(route),
            Err(error) if error.to_string().contains("scope_") => return Err(error),
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "broca's scoped route never opened: {error}"
                );
                sleep(Duration::from_millis(100)).await;
            }
        }
    }
}

async fn send(
    conn: &SubcConnection,
    route: &broca_subc::RouteHandle,
    send_id: &str,
) -> Result<serde_json::Value, broca_subc::ConnError> {
    let body = serde_json::json!({"method": "session.send", "params": {
        "prompt": "hi",
        "send_id": send_id,
        "model": {"provider": "mock", "model": "mock-model"},
    }});
    conn.route_request(route, serde_json::to_vec(&body).unwrap())
        .await
}

/// The episodes and queued sends a session's WAL holds: what an admitted send writes. The
/// owner's run keeps retrying the unreachable provider in the background, so the file itself
/// may still grow; a refused send must add to neither count.
fn admitted_sends(path: &Path) -> (usize, usize) {
    let records = broca_wal::replay_file(path, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
    let count = |want: fn(&broca_wal::WalRecord) -> bool| {
        records.iter().filter(|framed| want(&framed.record)).count()
    };
    (
        count(|record| matches!(record, broca_wal::WalRecord::RunStarted { .. })),
        count(|record| matches!(record, broca_wal::WalRecord::Queued { .. })),
    )
}

#[tokio::test(flavor = "multi_thread")]
async fn the_real_daemons_stamp_freezes_the_scope_and_refuses_another_identity() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    rig.sync_scope().await;
    let owner = rig.connect_as(OWNER, &rig.owner_nonce).await;
    let session = rig.session("scoped");

    // The owner's first send over a route the daemon stamped freezes the scope.
    let route = open_scoped(&owner, &session)
        .await
        .expect("the owner's scoped open to a fresh session is admitted");
    let reply = send(&owner, &route, "first")
        .await
        .expect("the owner's send");
    assert_eq!(reply["result"]["state"], "active", "{reply}");

    let wal = std::fs::read(rig.wal_path(&session)).unwrap_or_else(|error| {
        panic!(
            "the session's WAL {}: {error}; the rig holds {:?}",
            rig.wal_path(&session).display(),
            walk(&rig.dir)
        )
    });
    let records = broca_wal::replay_file(rig.wal_path(&session), broca_wal::DEFAULT_MAX_READ_BYTES)
        .expect("the WAL reads");
    let frozen: Vec<_> = records
        .iter()
        .filter_map(|framed| match &framed.record {
            broca_wal::WalRecord::RunStarted {
                scope, principal, ..
            } => Some((scope.clone(), principal.clone())),
            _ => None,
        })
        .collect();
    let owner_principal = broca_protocol::BindPrincipal::Reserved {
        module_id: OWNER.into(),
    };
    assert_eq!(
        frozen,
        [(
            Some(broca_protocol::SessionScope {
                owner: owner_principal.clone(),
                scope_ref: SCOPE_REF.into(),
                scope_epoch: SCOPE_EPOCH,
                flow_id: None,
                sender: None,
                agent_id: None,
                run_id: None,
            }),
            Some(owner_principal),
        )],
        "broca froze exactly the (owner, ref, scope_epoch) the daemon stamped"
    );
    let gated = br#"{"requires":["scope/v1"],"record":{"type":"run_started""#;
    assert_eq!(
        wal.windows(gated.len())
            .filter(|window| *window == gated)
            .count(),
        1,
        "the scoped RunStarted is a gated frame requiring scope/v1"
    );
    assert_eq!(
        std::fs::read_to_string(rig.state_root.join(broca_wal::FEATURE_FLOOR_FILENAME))
            .ok()
            .as_deref(),
        Some("scope/v1\n"),
        "the feature floor lists scope/v1"
    );

    // A second identity, admitted by the daemon as a listed carrier of the same scope, is not
    // the scope's owner: broca refuses its send by name and writes nothing.
    let carrier = rig
        .connect_as(OTHER_CARRIER, &rig.other_carrier_nonce)
        .await;
    let carrier_route = open_scoped(&carrier, &session)
        .await
        .expect("the daemon admits a listed carrier's scoped open");
    let before = admitted_sends(&rig.wal_path(&session));
    let refused = send(&carrier, &carrier_route, "intruder")
        .await
        .expect_err("a second identity's send is refused");
    assert!(
        refused.to_string().contains("scope_owner_mismatch"),
        "{refused}"
    );
    assert_eq!(
        admitted_sends(&rig.wal_path(&session)),
        before,
        "the refused send admitted nothing"
    );

    // A session a direct caller already drove records `direct` on its episode, so the owner's
    // scope cannot adopt it: the daemon relays broca's bind refusal as the route.open answer.
    let direct = SubcConnection::connect_with_consumer_identity(&rig.connection_file, None)
        .await
        .expect("connect as a direct caller");
    let unscoped = rig.session("unscoped");
    let direct_route = direct
        .route_open_management("broca", &unscoped)
        .await
        .expect("an unscoped open");
    let reply = direct
        .route_request(
            &direct_route,
            serde_json::to_vec(&serde_json::json!({"method": "session.send", "params": {
                "prompt": "hi", "send_id": "direct-1",
                "model": {"provider": "mock", "model": "mock-model"},
            }}))
            .unwrap(),
        )
        .await
        .expect("the direct send");
    assert_eq!(reply["result"]["state"], "active", "{reply}");
    let refused = open_scoped(&owner, &unscoped)
        .await
        .expect_err("the owner cannot adopt a session a direct caller drove");
    assert!(
        refused.to_string().contains("scope_adoption_refused — ")
            && refused.to_string().contains("under principal 'direct'"),
        "the route.open answer is broca's bind refusal: {refused}"
    );
}

/// A real daemon allows a module other than the owner to send under a scope with a flow id.
/// Broca records the scope's owner, ref and epoch, the flow id, and that sender's identity;
/// it does not let the sender adopt the owner agent's existing session history.
#[tokio::test(flavor = "multi_thread")]
async fn real_flow_carrier_freezes_identity_refuses_tools_changes_and_adoption() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    rig.sync_scope_with(Some("flow-a"), 1).await;
    let carrier = rig.connect_as(CARRIER, &rig.carrier_nonce).await;
    let owner = rig.connect_as(OWNER, &rig.owner_nonce).await;
    let session = rig.session("flow");
    let route = open_scoped(&carrier, &session)
        .await
        .expect("daemon admits flow carrier");

    // Refused tool requests must not create a WAL, even before the first episode.
    let tools_session = rig.session("flow-tools");
    let tools_route = open_scoped(&carrier, &tools_session).await.unwrap();
    let wal_path = rig.wal_path(&tools_session);
    let bytes_before = std::fs::metadata(&wal_path).map_or(0, |m| m.len());
    for (field, extra) in [
        (
            "tools",
            serde_json::json!({"tools": [{"name": "read", "description": "Read", "input_schema": {"type": "object"}}]}),
        ),
        (
            "plan.tool_items",
            serde_json::json!({"plan": {"tool_items": [{"provider": "aft", "preset": "main"}]}}),
        ),
    ] {
        let mut params = serde_json::json!({"prompt": "hi", "model": {"provider": "mock", "model": "mock-model"}});
        params
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        let body = serde_json::json!({"method": "session.send", "params": params});
        let error = carrier
            .route_request(&tools_route, serde_json::to_vec(&body).unwrap())
            .await
            .expect_err("flow tools refused");
        println!("FLOW sent reserved:basal session.send with {field}; returned {error}; WAL bytes before={bytes_before} after={}", std::fs::metadata(&wal_path).map_or(0, |m| m.len()));
        assert!(
            error.to_string().contains("invalid_params") && error.to_string().contains(field),
            "{error}"
        );
        assert_eq!(
            std::fs::metadata(&wal_path).map_or(0, |m| m.len()),
            bytes_before,
            "tool refusal changes no WAL bytes"
        );
    }

    let reply = send(&carrier, &route, "flow-first")
        .await
        .expect("flow carrier send accepted");
    println!("FLOW sent reserved:basal session.send flow-a owner=reserved:prefrontal-core agent_id=owner-agent delegates=true; returned {reply}");
    assert_eq!(reply["result"]["state"], "active", "{reply}");
    let records =
        broca_wal::replay_file(rig.wal_path(&session), broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
    let sender = broca_protocol::BindPrincipal::Reserved {
        module_id: CARRIER.into(),
    };
    let scopes: Vec<_> = records
        .iter()
        .filter_map(|r| match &r.record {
            broca_wal::WalRecord::RunStarted {
                scope, principal, ..
            } => Some((scope.clone(), principal.clone())),
            _ => None,
        })
        .collect();
    assert_eq!(
        scopes,
        [(
            Some(broca_protocol::SessionScope {
                owner: broca_protocol::BindPrincipal::Reserved {
                    module_id: OWNER.into()
                },
                scope_ref: SCOPE_REF.into(),
                scope_epoch: SCOPE_EPOCH,
                flow_id: Some("flow-a".into()),
                sender: Some(sender.clone()),
                agent_id: None,
                run_id: None,
            }),
            Some(sender)
        )]
    );
    println!("FLOW durable RunStarted scope/principal={scopes:?}");
    let other = rig
        .connect_as(OTHER_CARRIER, &rig.other_carrier_nonce)
        .await;
    let other_route = open_scoped(&other, &session).await.unwrap();
    let before = admitted_sends(&rig.wal_path(&session));
    let error = send(&other, &other_route, "wrong-carrier")
        .await
        .expect_err("wrong_carrier_refused: a non-Basal carrier cannot drive the frozen flow");
    println!("FLOW sent reserved:{OTHER_CARRIER} session.send on basal's flow-a; returned {error}");
    assert!(
        error.to_string().contains("scope_sender_mismatch"),
        "wrong_carrier_refused: {error}"
    );
    assert_eq!(admitted_sends(&rig.wal_path(&session)), before);

    let owner_route = open_scoped(&owner, &session).await.unwrap();
    let before = admitted_sends(&rig.wal_path(&session));
    let error = send(&owner, &owner_route, "changed-sender")
        .await
        .expect_err("sender cannot switch to owner");
    println!(
        "FLOW sent owner reserved:prefrontal-core session.send on basal's flow-a; returned {error}"
    );
    assert!(
        error.to_string().contains("scope_sender_mismatch"),
        "{error}"
    );
    assert_eq!(admitted_sends(&rig.wal_path(&session)), before);

    // Even history belonging entirely to the owner must not become an automation's history.
    let unscoped = rig.session("owner-unscoped");
    let unscoped_route = owner
        .route_open_management("broca", &unscoped)
        .await
        .unwrap();
    send(&owner, &unscoped_route, "unscoped-first")
        .await
        .unwrap();
    let error = open_scoped(&owner, &unscoped)
        .await
        .expect_err("flow adoption refused");
    println!("FLOW opened flow-a on owner-driven unscoped session; returned {error}");
    assert!(
        error.to_string().contains("scope_adoption_refused")
            && error.to_string().contains("flow scope"),
        "{error}"
    );

    rig.sync_scope_with(Some("flow-b"), 2).await;
    let route = open_scoped(&carrier, &session).await.unwrap();
    let before = admitted_sends(&rig.wal_path(&session));
    let error = send(&carrier, &route, "changed-flow")
        .await
        .expect_err("flow id cannot change");
    println!("FLOW sent reserved:basal session.send with changed flow-b; returned {error}");
    assert!(
        error.to_string().contains("scope_flow_id_mismatch"),
        "{error}"
    );
    assert_eq!(admitted_sends(&rig.wal_path(&session)), before);

    rig.sync_scope_with(None, 3).await;
    let plain = rig.session("plain-basal");
    let plain_route = open_scoped(&carrier, &plain).await.unwrap();
    let plain_wal = rig.wal_path(&plain);

    let error = send(&carrier, &plain_route, "plain-basal")
        .await
        .expect_err("non-owner Basal without a flow stamp is refused");
    println!("FLOW sent reserved:basal session.send without flow_id on non-owner scope; returned {error}");
    assert!(error.to_string().contains("flow_scope_required"), "{error}");
    assert_eq!(
        admitted_sends(&plain_wal),
        (0, 0),
        "plain Basal refusal admits no run or queued send"
    );
}

/// Basal cannot start unscoped work, but empty-session reads cannot start work either.
#[tokio::test(flavor = "multi_thread")]
async fn real_basal_unscoped_send_refused_without_wal_and_reads_allowed() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    let basal = rig.connect_as(CARRIER, &rig.carrier_nonce).await;
    let session = rig.session("basal-empty");
    let route = basal
        .route_open_management("broca", &session)
        .await
        .unwrap();
    let path = rig.wal_path(&session);
    let before = std::fs::read(&path).unwrap_or_default();
    for delivery in ["queue", "steer", "interrupt"] {
        let request = serde_json::json!({"method":"session.send", "params": {
            "prompt":"hi", "delivery":delivery,
            "model":{"provider":"mock", "model":"mock-model"}
        }});
        let error = basal
            .route_request(&route, serde_json::to_vec(&request).unwrap())
            .await
            .expect_err("unscoped Basal cannot send");
        assert!(error.to_string().contains("flow_scope_required"), "{error}");
        assert_eq!(std::fs::read(&path).unwrap_or_default(), before);
        assert_eq!(admitted_sends(&path), (0, 0));
    }
    for method in ["run.status", "session.head", "session.read"] {
        let response = basal
            .route_request(
                &route,
                serde_json::to_vec(&serde_json::json!({"method":method, "params":{}})).unwrap(),
            )
            .await
            .unwrap();
        assert!(response.get("result").is_some(), "{response}");
    }
}

/// A route without a stamp must not let a flow observe or stop another sender's work.
#[tokio::test(flavor = "multi_thread")]
async fn real_basal_sender_guard_protects_other_runs_and_allows_own_flow() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    rig.sync_scope_with(Some("flow-a"), 1).await;
    let owner = rig.connect_as(OWNER, &rig.owner_nonce).await;
    let basal = rig.connect_as(CARRIER, &rig.carrier_nonce).await;
    let foreign = rig.session("foreign-sender");
    let owner_route = owner
        .route_open_management("broca", &foreign)
        .await
        .unwrap();
    let reply = send(&owner, &owner_route, "foreign-first").await.unwrap();
    let run_id = reply["result"]["run_id"].as_str().unwrap();
    let basal_route = basal
        .route_open_management("broca", &foreign)
        .await
        .unwrap();
    for method in [
        "session.read",
        "session.head",
        "run.status",
        "run.result",
        "run.cancel",
        "session.retract",
        "session.baseline",
        "session.subscribe",
        "vault.audit",
    ] {
        let request = serde_json::json!({"method":method,"params":{"run_id":run_id,"submission_id":"not-pending"}});
        let error = basal
            .route_request(&basal_route, serde_json::to_vec(&request).unwrap())
            .await
            .expect_err("foreign sender refused before effects");
        let code = if matches!(method, "run.cancel" | "session.retract") {
            "scope_owner_mismatch"
        } else {
            "sender_not_owner"
        };
        assert!(error.to_string().contains(code), "{method}: {error}");
    }
    let status = owner
        .route_request(
            &owner_route,
            serde_json::to_vec(&serde_json::json!({"method":"run.status","params":{}})).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        status["result"]["state"], "active",
        "foreign run was not cancelled: {status}"
    );
    let records =
        broca_wal::replay_file(rig.wal_path(&foreign), broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
    assert!(
        !records
            .iter()
            .any(|frame| matches!(&frame.record, broca_wal::WalRecord::RunFinished { .. })),
        "foreign run stays running"
    );

    let own = rig.session("basal-owned-flow");
    let own_route = open_scoped(&basal, &own).await.unwrap();
    let reply = send(&basal, &own_route, "basal-first").await.unwrap();
    let own_run = reply["result"]["run_id"].as_str().unwrap();
    for method in ["session.read", "session.head", "run.status"] {
        let response = basal
            .route_request(
                &own_route,
                serde_json::to_vec(&serde_json::json!({"method":method,"params":{}})).unwrap(),
            )
            .await
            .unwrap();
        assert!(response.get("result").is_some(), "{response}");
    }
    let response = basal
        .route_request(
            &own_route,
            serde_json::to_vec(
                &serde_json::json!({"method":"run.cancel","params":{"run_id":own_run}}),
            )
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response["result"], "ack", "{response}");
}

/// A successful, fixed chat-completion response keeps billing checks independent of a vault
/// or an external provider. The request is consumed before replying, like the production HTTP path.
struct ScopeModel {
    url: String,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for ScopeModel {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl ScopeModel {
    async fn start() -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1", listener.local_addr().unwrap());
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut buffer = Vec::new();
                    let mut chunk = [0u8; 8192];
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
                    let headers =
                        String::from_utf8_lossy(&buffer[..body_start]).to_ascii_lowercase();
                    let length: usize = headers
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
                    let events = [
                        serde_json::json!({"id":"scope-model","object":"chat.completion.chunk","model":"mock-model","choices":[{"index":0,"delta":{"role":"assistant","content":"scope answer"},"finish_reason":null}]}),
                        serde_json::json!({"id":"scope-model","object":"chat.completion.chunk","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}),
                        serde_json::json!({"id":"scope-model","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}),
                    ];
                    let mut body = events
                        .iter()
                        .map(|event| format!("data: {event}\n\n"))
                        .collect::<String>();
                    body.push_str("data: [DONE]\n\n");
                    let reply = format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
                    stream.write_all(reply.as_bytes()).await.unwrap();
                });
            }
        });
        Self { url, task }
    }
}

async fn scope_request(
    conn: &SubcConnection,
    route: &broca_subc::RouteHandle,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value, broca_subc::ConnError> {
    conn.route_request(
        route,
        serde_json::to_vec(&serde_json::json!({"method":method,"params":params})).unwrap(),
    )
    .await
}

async fn completed_run(conn: &SubcConnection, route: &broca_subc::RouteHandle, run: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = scope_request(conn, route, "run.status", serde_json::json!({"run_id":run}))
            .await
            .unwrap();
        if status["result"]["state"] != "active" {
            assert_eq!(status["result"]["state"], "completed", "{status}");
            return;
        }
        assert!(Instant::now() < deadline, "run did not finish: {status}");
        sleep(Duration::from_millis(25)).await;
    }
}

fn fact_bytes(rig: &Rig, run: &str) -> Vec<String> {
    let db = rusqlite::Connection::open_with_flags(
        rig.state_root.join("run-index.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let mut statement = db
        .prepare("SELECT segment_json FROM export_facts WHERE run_id = ?1 ORDER BY fact_id")
        .unwrap();
    let facts = statement
        .query_map([run], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    facts
}

async fn wait_for_export(rig: &Rig, run: &str) {
    // The terminal WAL state is observable before the asynchronous index projection finishes.
    // Wait for that projection before taking the no-write snapshots of index rows and facts.
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let db = rusqlite::Connection::open_with_flags(
            rig.state_root.join("run-index.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let state: String = db
            .query_row(
                "SELECT state FROM run_index WHERE run_id = ?1",
                [run],
                |row| row.get(0),
            )
            .unwrap();
        if state == "completed" && !fact_bytes(rig, run).is_empty() {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "completed run {run} was not projected/exported"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

fn admission_index_rows(rig: &Rig) -> (Vec<(String, String, String)>, Vec<String>) {
    let db = rusqlite::Connection::open_with_flags(
        rig.state_root.join("run-index.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let mut runs = db
        .prepare("SELECT run_id, session, state FROM run_index ORDER BY run_id")
        .unwrap();
    let runs = runs
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let mut facts = db
        .prepare("SELECT segment_json FROM export_facts ORDER BY fact_id")
        .unwrap();
    let facts = facts
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    (runs, facts)
}

async fn run_scoped_send(
    rig: &Rig,
) -> (
    SubcConnection,
    broca_subc::RouteHandle,
    broca_protocol::BindIdentity,
    String,
) {
    rig.sync_attributes(
        serde_json::json!({"agent_id":"agent-a","run_id":"parent-run"}),
        1,
    )
    .await;
    let basal = rig.connect_as(CARRIER, &rig.carrier_nonce).await;
    let session = rig.session("basal:codemode-parent-run:0");
    let route = open_scoped(&basal, &session).await.unwrap();
    let reply = send(&basal, &route, "first").await.unwrap();
    let run = reply["result"]["run_id"].as_str().unwrap().to_owned();
    completed_run(&basal, &route, &run).await;
    wait_for_export(rig, &run).await;
    (basal, route, session, run)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_agent_run_scope_freezes_bills_and_refuses_every_invalid_send_without_writes() {
    let model = ScopeModel::start().await;
    let Some(rig) = Rig::start_configured(None, None, Some(&model.url)).await else {
        return;
    };
    let (basal, route, session, run) = run_scoped_send(&rig).await;
    let path = rig.wal_path(&session);
    let records = broca_wal::replay_file(&path, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
    let scopes = records
        .iter()
        .filter_map(|frame| match &frame.record {
            broca_wal::WalRecord::RunStarted {
                scope, principal, ..
            } => Some((scope.clone().unwrap(), principal.clone().unwrap())),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(scopes.len(), 1);
    let (scope, principal) = &scopes[0];
    assert_eq!(scope.agent_id.as_deref(), Some("agent-a"));
    assert_eq!(scope.run_id.as_deref(), Some("parent-run"));
    assert_eq!(scope.owner.as_owner_string(), "reserved:prefrontal-core");
    assert_eq!(scope.scope_ref, SCOPE_REF);
    assert_eq!(scope.scope_epoch, SCOPE_EPOCH);
    assert_eq!(scope.sender.as_ref(), Some(principal));
    assert_eq!(principal.as_owner_string(), "reserved:basal");
    let wal = std::fs::read(&path).unwrap();
    assert!(wal
        .windows(b"agent-run-scopes/v1".len())
        .any(|w| w == b"agent-run-scopes/v1"));
    let facts = fact_bytes(&rig, &run);
    assert_eq!(facts.len(), 1);
    let fact: serde_json::Value = serde_json::from_str(&facts[0]).unwrap();
    assert_eq!(fact["agent_id"], "agent-a");
    assert_eq!(fact["agent_run_id"], "parent-run");
    assert_eq!(fact["run_id"], run);

    // A finished run makes byte comparison meaningful: no provider is still appending records.
    for (field, extra) in [
        (
            "tools",
            serde_json::json!({"tools":[{"name":"read","description":"Read","input_schema":{"type":"object"}}]}),
        ),
        (
            "plan.tool_items",
            serde_json::json!({"plan":{"tool_items":[{"provider":"aft","preset":"main"}]}}),
        ),
    ] {
        for (test_session, test_route) in [
            (&session, &route),
            (
                &rig.session(&format!("run-{field}")),
                &open_scoped(&basal, &rig.session(&format!("run-{field}")))
                    .await
                    .unwrap(),
            ),
        ] {
            let path = rig.wal_path(test_session);
            let before = std::fs::read(&path).unwrap_or_default();
            let index_before = admission_index_rows(&rig);
            let mut params = serde_json::json!({"prompt":"not written","model":{"provider":"mock","model":"mock-model"}});
            params
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            let error = scope_request(&basal, test_route, "session.send", params)
                .await
                .unwrap_err();
            assert!(
                error.to_string().contains("invalid_params") && error.to_string().contains(field),
                "{error}"
            );
            assert_eq!(
                std::fs::read(&path).unwrap_or_default(),
                before,
                "{field} refusal wrote WAL bytes"
            );
            if test_session != &session {
                assert!(!path.exists(), "fresh {field} refusal created a WAL");
            }
            assert_eq!(
                admission_index_rows(&rig),
                index_before,
                "{field} refusal wrote index rows"
            );
        }
    }
    for (module, nonce) in [
        (OTHER_CARRIER, &rig.other_carrier_nonce),
        (OWNER, &rig.owner_nonce),
    ] {
        let conn = rig.connect_as(module, nonce).await;
        for key in [&session, &rig.session(&format!("run-{module}"))] {
            let other_route = open_scoped(&conn, key).await.unwrap();
            let path = rig.wal_path(key);
            let before = std::fs::read(&path).unwrap_or_default();
            let index_before = admission_index_rows(&rig);
            let error = send(&conn, &other_route, "wrong-sender").await.unwrap_err();
            assert!(
                error.to_string().contains("scope_owner_mismatch"),
                "{module}: {error}"
            );
            assert_eq!(
                std::fs::read(&path).unwrap_or_default(),
                before,
                "{module} refusal wrote WAL bytes"
            );
            if key != &session {
                assert!(!path.exists(), "fresh {module} refusal created a WAL");
            }
            assert_eq!(
                admission_index_rows(&rig),
                index_before,
                "{module} refusal wrote index rows"
            );
        }
    }
    for (generation, attributes, field) in [
        (
            2,
            serde_json::json!({"agent_id":"agent-a","run_id":"changed-run"}),
            "run_id",
        ),
        (
            3,
            serde_json::json!({"agent_id":"changed-agent","run_id":"parent-run"}),
            "agent_id",
        ),
    ] {
        rig.sync_attributes(attributes, generation).await;
        let changed = open_scoped(&basal, &session).await.unwrap();
        let before = std::fs::read(&path).unwrap();
        let index_before = admission_index_rows(&rig);
        let error = send(&basal, &changed, "changed-identity")
            .await
            .unwrap_err();
        assert!(
            error.to_string().contains("scope_run_mismatch") && error.to_string().contains(field),
            "{error}"
        );
        assert_eq!(
            std::fs::read(&path).unwrap(),
            before,
            "{field} refusal wrote WAL bytes"
        );
        assert_eq!(
            admission_index_rows(&rig),
            index_before,
            "{field} refusal wrote index rows"
        );
    }
    assert_eq!(
        fact_bytes(&rig, &run),
        facts,
        "refusals changed billing facts"
    );

    let owner = rig.connect_as(OWNER, &rig.owner_nonce).await;
    let ordinary = rig.session("ordinary-facts");
    let ordinary_route = owner
        .route_open_management("broca", &ordinary)
        .await
        .unwrap();
    let reply = send(&owner, &ordinary_route, "ordinary").await.unwrap();
    let ordinary_run = reply["result"]["run_id"].as_str().unwrap();
    completed_run(&owner, &ordinary_route, ordinary_run).await;
    wait_for_export(&rig, ordinary_run).await;
    let ordinary_facts = fact_bytes(&rig, ordinary_run);
    assert_eq!(ordinary_facts.len(), 1);
    for bytes in &ordinary_facts {
        let fact: broca_core::billing::SpendSegment = serde_json::from_str(bytes).unwrap();
        assert_eq!(
            serde_json::to_string(&fact).unwrap(),
            *bytes,
            "ordinary fact bytes round-trip unchanged"
        );
        let value: serde_json::Value = serde_json::from_str(bytes).unwrap();
        assert!(value.get("agent_id").is_none() && value.get("agent_run_id").is_none());
    }
}

/// A separately built live-release reader must read the writer's session, but may not
/// append another episode. Reading the old release independently also exercises its WAL gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn released_reader_reads_agent_run_writer_session_and_refuses_advancement() {
    let Some(reader) = std::env::var_os("BROCA_AGENT_RUN_READER_BIN").map(PathBuf::from) else {
        assert!(
            std::env::var_os("BROCA_REQUIRE_AGENT_RUN_READER").is_none(),
            "released agent-run reader is required"
        );
        println!("SKIP agent-run rollback: BROCA_AGENT_RUN_READER_BIN is not set");
        return;
    };
    let version = std::process::Command::new(&reader)
        .arg("--version")
        .env_clear()
        .output()
        .unwrap();
    assert!(version.status.success());
    assert_eq!(
        String::from_utf8(version.stdout).unwrap().trim(),
        "ck-broca 0.3.194"
    );
    let model = ScopeModel::start().await;
    let Some(mut writer) = Rig::start_configured(None, None, Some(&model.url)).await else {
        return;
    };
    assert_ne!(
        std::fs::read(&reader).unwrap(),
        std::fs::read(broca_test_support::ckdev_binary(
            env!("CARGO_BIN_EXE_ck-broca"),
            &writer.dir
        ))
        .unwrap(),
        "the reader must not be a renamed writer"
    );
    let (basal, route, session, run) = run_scoped_send(&writer).await;
    let page = scope_request(&basal, &route, "session.read", serde_json::json!({}))
        .await
        .unwrap();
    assert!(page.to_string().contains("scope answer"));
    drop(basal);
    if let Some(pid) = writer.child.id() {
        subc_os::Process::open(pid)
            .unwrap()
            .unwrap()
            .signal(subc_os::Signal::Terminate)
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(30), writer.child.wait())
        .await
        .unwrap()
        .unwrap();
    let before = std::fs::read(writer.wal_path(&session)).unwrap();
    let facts = fact_bytes(&writer, &run);
    let rollback = Rig::start_configured(Some(&reader), Some(&writer.state_root), Some(&model.url))
        .await
        .unwrap();
    let owner = rollback.connect_as(OWNER, &rollback.owner_nonce).await;
    let route = owner
        .route_open_management("broca", &session)
        .await
        .unwrap();
    assert_eq!(
        scope_request(&owner, &route, "session.read", serde_json::json!({}))
            .await
            .unwrap(),
        page
    );
    let status = scope_request(
        &owner,
        &route,
        "run.status",
        serde_json::json!({"run_id":run}),
    )
    .await
    .unwrap();
    assert_eq!(status["result"]["state"], "completed");
    let error = send(&owner, &route, "reader-next").await.unwrap_err();
    assert!(
        error.to_string().contains("agent_run_scopes_unsupported"),
        "{error}"
    );
    assert_eq!(
        std::fs::read(writer.wal_path(&session)).unwrap(),
        before,
        "reader refusal appended WAL bytes"
    );
    assert_eq!(
        fact_bytes(&writer, &run),
        facts,
        "reader changed export bytes"
    );
    assert_eq!(
        scope_request(&owner, &route, "session.read", serde_json::json!({}))
            .await
            .unwrap(),
        page,
        "reader still serves after refusing advancement"
    );
}

/// Hold real HTTP requests without a response so authorization checks cannot race provider
/// retries or completions. The handshake establishes that the run's start is already durable.
struct HeldOwnerModel {
    url: String,
    requests: std::sync::Arc<tokio::sync::Semaphore>,
    content: tokio::sync::mpsc::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for HeldOwnerModel {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl HeldOwnerModel {
    async fn start() -> Self {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1", listener.local_addr().unwrap());
        let requests = std::sync::Arc::new(tokio::sync::Semaphore::new(0));
        let received = requests.clone();
        let (content, mut writes) = tokio::sync::mpsc::channel(1);
        let task = tokio::spawn(async move {
            let mut held: Vec<tokio::net::TcpStream> = Vec::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((mut stream, _)) = accepted else { break };
                        let mut buf = [0; 8192];
                        if stream.read(&mut buf).await.unwrap_or(0) > 0 {
                            held.push(stream);
                            received.add_permits(1);
                        }
                    }
                    Some(()) = writes.recv() => {
                        for stream in &mut held {
                            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: 100000\r\n\r\ndata: {\"id\":\"held-model\",\"choices\":[{\"index\":0,\"delta\":{\"content\":\"hello\"}}]}\n\n").await.unwrap();
                        }
                    }
                }
            }
        });
        Self {
            url,
            requests,
            content,
            task,
        }
    }

    async fn wait_request(&self) {
        tokio::time::timeout(Duration::from_secs(20), self.requests.acquire())
            .await
            .expect("provider request reached the held model")
            .unwrap()
            .forget();
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn active_status_reports_held_model_step_and_content() {
    let model = HeldOwnerModel::start().await;
    let Some(rig) = Rig::start_configured(None, None, Some(&model.url)).await else {
        return;
    };
    rig.sync_scope_with(Some("flow-a"), 1).await;
    let basal = rig.connect_as(CARRIER, &rig.carrier_nonce).await;
    let session = rig.session("active-progress");
    let route = open_scoped(&basal, &session).await.unwrap();
    let started = send(&basal, &route, "first").await.unwrap();
    let run_id = started["result"]["run_id"].as_str().unwrap();
    model.wait_request().await;
    let status = scope_request(
        &basal,
        &route,
        "run.status",
        serde_json::json!({"run_id":run_id}),
    )
    .await
    .unwrap();
    assert_eq!(status["result"]["state"], "active");
    let step = &status["result"]["step"];
    assert_eq!(step["step_id"], 1, "{status}");
    assert_eq!(step["attempt"], 0, "{status}");
    let started_at = step["started_at_ms"]
        .as_u64()
        .expect("step start was observed");
    let attempt_at = step["attempt_started_at_ms"]
        .as_u64()
        .expect("attempt start was observed");
    assert!(started_at <= attempt_at);
    assert!(step.get("last_content_at_ms").is_none());
    assert!(step.get("max_content_gap_ms").is_none());
    assert!(step.get("max_content_gap_ended_at_ms").is_none());

    model.content.send(()).await.unwrap();
    let status =
        broca_test_support::wait_until("live actor sees content during held HTTP call", || async {
            let status = scope_request(
                &basal,
                &route,
                "run.status",
                serde_json::json!({"run_id":run_id}),
            )
            .await
            .unwrap();
            if status["result"]["step"]["last_content_at_ms"].is_u64() {
                Ok(status)
            } else {
                Err(status.to_string())
            }
        })
        .await;
    let step = &status["result"]["step"];
    assert_eq!(step["started_at_ms"], started_at);
    assert_eq!(step["attempt_started_at_ms"], attempt_at);
    let first_content_at = step["last_content_at_ms"].as_u64().unwrap();
    assert!(first_content_at >= attempt_at);
    assert!(step.get("max_content_gap_ms").is_none());

    model.content.send(()).await.unwrap();
    let status =
        broca_test_support::wait_until("live actor sees the interior content gap", || async {
            let status = scope_request(
                &basal,
                &route,
                "run.status",
                serde_json::json!({"run_id":run_id}),
            )
            .await
            .unwrap();
            let step = &status["result"]["step"];
            if step["max_content_gap_ms"].is_u64() && step["max_content_gap_ended_at_ms"].is_u64() {
                Ok(status)
            } else {
                Err(status.to_string())
            }
        })
        .await;
    let step = &status["result"]["step"];
    assert!(step["max_content_gap_ms"].as_u64().is_some());
    assert_eq!(
        step["max_content_gap_ended_at_ms"], step["last_content_at_ms"],
        "the longest gap ends at the latest content event in this two-event stream"
    );
    assert!(step["max_content_gap_ended_at_ms"].as_u64().unwrap() >= first_content_at);
    assert_eq!(step["started_at_ms"], started_at);
    assert_eq!(step["attempt_started_at_ms"], attempt_at);
    let mut golden = status["result"].clone();
    golden["step"]["started_at_ms"] = 1790000000000u64.into();
    golden["step"]["attempt_started_at_ms"] = 1790000000001u64.into();
    golden["step"]["last_content_at_ms"] = 1790000000002u64.into();
    golden["step"]["max_content_gap_ms"] = 1u64.into();
    golden["step"]["max_content_gap_ended_at_ms"] = 1790000000002u64.into();
    let bytes = serde_json::to_vec(&golden).unwrap();
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/run_status.active_with_step.json");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, &bytes).unwrap();
    }
    assert_eq!(
        bytes,
        std::fs::read(path).expect("active-step golden generated through serve")
    );
    let cancelled = scope_request(
        &basal,
        &route,
        "run.cancel",
        serde_json::json!({"run_id":run_id}),
    )
    .await
    .unwrap();
    assert_eq!(cancelled["result"], "ack", "{cancelled}");
    broca_test_support::wait_until("cancelled active run retires its live step", || async {
        let status = scope_request(
            &basal,
            &route,
            "run.status",
            serde_json::json!({"run_id":run_id}),
        )
        .await
        .unwrap();
        if status["result"]["state"] != "active" {
            assert!(status["result"].get("step").is_none());
            return Ok(());
        }
        Err(status.to_string())
    })
    .await;
}

async fn owner_op(
    conn: &SubcConnection,
    route: &broca_subc::RouteHandle,
    method: &str,
    params: serde_json::Value,
) -> Result<serde_json::Value, Vec<u8>> {
    let terminal = tokio::time::timeout(
        Duration::from_secs(20),
        conn.route_request_terminal(
            route,
            serde_json::to_vec(&serde_json::json!({"method":method,"params":params})).unwrap(),
        ),
    )
    .await
    .expect("owner operation answered before deadline")
    .unwrap_or_else(|error| panic!("{method} route transport works: {error}"));
    terminal.map(|body| serde_json::from_slice(&body).unwrap())
}

async fn pending_owner_send(
    conn: &SubcConnection,
    route: &broca_subc::RouteHandle,
    send_id: &str,
) -> String {
    let queued = send(conn, route, send_id).await.unwrap();
    assert_eq!(queued["result"]["state"], "pending", "{queued}");
    queued["result"]["submission_id"]
        .as_str()
        .unwrap()
        .to_owned()
}

async fn assert_owner_refusal_writes_nothing(
    rig: &Rig,
    session: &broca_protocol::BindIdentity,
    conn: &SubcConnection,
    route: &broca_subc::RouteHandle,
    method: &str,
    params: serde_json::Value,
) -> Vec<u8> {
    let before = std::fs::read(rig.wal_path(session)).unwrap();
    let body = owner_op(conn, route, method, params)
        .await
        .expect_err("non-owner is refused");
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(error["code"], "scope_owner_mismatch", "{error}");
    assert_eq!(
        std::fs::read(rig.wal_path(session)).unwrap(),
        before,
        "{method} refusal writes nothing"
    );
    body
}

#[tokio::test(flavor = "multi_thread")]
async fn real_owner_cleanup_after_scope_end_and_cancel_refusal_golden() {
    let model = HeldOwnerModel::start().await;
    let Some(rig) = Rig::start_configured(None, None, Some(&model.url)).await else {
        return;
    };
    rig.sync_scope_with(Some("flow-a"), 1).await;
    let basal = rig.connect_as(CARRIER, &rig.carrier_nonce).await;
    let owner = rig.connect_as(OWNER, &rig.owner_nonce).await;
    let foreign = rig
        .connect_as(OTHER_CARRIER, &rig.other_carrier_nonce)
        .await;
    let direct = SubcConnection::connect(&rig.connection_file).await.unwrap();
    let session = rig.session("owner-cleanup");
    let sending = open_scoped(&basal, &session).await.unwrap();
    let started = send(&basal, &sending, "first").await.unwrap();
    let run_id = started["result"]["run_id"].as_str().unwrap();
    model.wait_request().await;
    let submission_id = pending_owner_send(&basal, &sending, "queued-owner-send").await;

    let foreign_route = foreign
        .route_open_management("broca", &session)
        .await
        .unwrap();
    let golden = assert_owner_refusal_writes_nothing(
        &rig,
        &session,
        &foreign,
        &foreign_route,
        "run.cancel",
        serde_json::json!({"run_id":run_id}),
    )
    .await;
    assert_eq!(
        golden,
        include_bytes!("../../broca-wire/tests/golden/scope_owner_mismatch.cancel.json").as_slice(),
        "real serve cancel refusal golden"
    );
    assert_owner_refusal_writes_nothing(
        &rig,
        &session,
        &foreign,
        &foreign_route,
        "session.retract",
        serde_json::json!({"submission_id":submission_id}),
    )
    .await;
    let direct_route = direct
        .route_open_management("broca", &session)
        .await
        .unwrap();
    assert_owner_refusal_writes_nothing(
        &rig,
        &session,
        &direct,
        &direct_route,
        "run.cancel",
        serde_json::json!({"run_id":run_id}),
    )
    .await;
    assert_owner_refusal_writes_nothing(
        &rig,
        &session,
        &direct,
        &direct_route,
        "session.retract",
        serde_json::json!({"submission_id":submission_id}),
    )
    .await;

    rig.sync_scopes(serde_json::json!([]), 2).await;
    assert!(
        open_scoped(&owner, &session).await.is_err(),
        "ended scope cannot open routes"
    );
    // Core performs cleanup on its unscoped management route after the stamped scope ends.
    let cleanup = owner
        .route_open_management("broca", &session)
        .await
        .unwrap();
    let response = owner_op(
        &owner,
        &cleanup,
        "session.retract",
        serde_json::json!({"submission_id":submission_id}),
    )
    .await
    .unwrap();
    assert_eq!(response["result"], "retracted", "{response}");
    let response = owner_op(
        &owner,
        &cleanup,
        "run.cancel",
        serde_json::json!({"run_id":run_id}),
    )
    .await
    .unwrap();
    assert_eq!(response["result"], "ack", "{response}");
}

#[tokio::test(flavor = "multi_thread")]
async fn real_frozen_carrier_and_scope_owner_cleanup_across_epochs() {
    let model = HeldOwnerModel::start().await;
    let Some(rig) = Rig::start_configured(None, None, Some(&model.url)).await else {
        return;
    };
    rig.sync_scope_with(Some("flow-a"), 1).await;
    let basal = rig.connect_as(CARRIER, &rig.carrier_nonce).await;
    let owner = rig.connect_as(OWNER, &rig.owner_nonce).await;
    let session = rig.session("carrier-cleanup");
    let sending = open_scoped(&basal, &session).await.unwrap();
    let started = send(&basal, &sending, "first").await.unwrap();
    let run_id = started["result"]["run_id"].as_str().unwrap();
    model.wait_request().await;
    let first = pending_owner_send(&basal, &sending, "queued-first").await;
    let second = pending_owner_send(&basal, &sending, "queued-second").await;
    let unscoped = basal
        .route_open_management("broca", &session)
        .await
        .unwrap();
    let response = owner_op(
        &basal,
        &unscoped,
        "session.retract",
        serde_json::json!({"submission_id":first}),
    )
    .await
    .unwrap();
    assert_eq!(response["result"], "retracted", "{response}");
    rig.sync_scopes(
        serde_json::json!([{
            "ref": SCOPE_REF, "scope_epoch": SCOPE_EPOCH + 2, "kind": "head",
            "attributes": {"flow_id":"flow-a"},
            "carriers": [{"principal":{"kind":"reserved","module_id":CARRIER}}]
        }]),
        2,
    )
    .await;
    let mut reopened = selector();
    reopened.scope_epoch = Some(SCOPE_EPOCH + 2);
    let owner_route = owner
        .route_open_management_scoped_within(
            "broca",
            &session,
            reopened.clone(),
            RouteOpenRetry::within(Duration::from_secs(20), Duration::from_millis(100)),
        )
        .await
        .unwrap();
    let response = owner_op(
        &owner,
        &owner_route,
        "session.retract",
        serde_json::json!({"submission_id":second}),
    )
    .await
    .unwrap();
    assert_eq!(response["result"], "retracted", "{response}");
    let carrier_route = basal
        .route_open_management_scoped_within(
            "broca",
            &session,
            reopened,
            RouteOpenRetry::within(Duration::from_secs(20), Duration::from_millis(100)),
        )
        .await
        .unwrap();
    let response = owner_op(
        &basal,
        &carrier_route,
        "run.cancel",
        serde_json::json!({"run_id":run_id}),
    )
    .await
    .unwrap();
    assert_eq!(response["result"], "ack", "{response}");
}

#[tokio::test(flavor = "multi_thread")]
async fn real_unscoped_session_uses_only_attested_first_sender() {
    let model = HeldOwnerModel::start().await;
    let Some(rig) = Rig::start_configured(None, None, Some(&model.url)).await else {
        return;
    };
    let owner = rig.connect_as(OWNER, &rig.owner_nonce).await;
    let basal = rig.connect_as(CARRIER, &rig.carrier_nonce).await;
    let foreign = rig
        .connect_as(OTHER_CARRIER, &rig.other_carrier_nonce)
        .await;
    let direct = SubcConnection::connect(&rig.connection_file).await.unwrap();
    let session = rig.session("unscoped-owner-cleanup");
    let sending = owner
        .route_open_management("broca", &session)
        .await
        .unwrap();
    let started = send(&owner, &sending, "first").await.unwrap();
    let run_id = started["result"]["run_id"].as_str().unwrap();
    model.wait_request().await;
    let submission_id = pending_owner_send(&owner, &sending, "queued").await;
    for caller in [&basal, &foreign, &direct] {
        let route = caller
            .route_open_management("broca", &session)
            .await
            .unwrap();
        let body = assert_owner_refusal_writes_nothing(
            &rig,
            &session,
            caller,
            &route,
            "run.cancel",
            serde_json::json!({"run_id":run_id,"principal":{"kind":"reserved","module_id":OWNER}}),
        )
        .await;
        let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(error["detail"]["first"], format!("reserved:{OWNER}"));
        assert_owner_refusal_writes_nothing(
            &rig,
            &session,
            caller,
            &route,
            "session.retract",
            serde_json::json!({"submission_id":submission_id}),
        )
        .await;
    }
    let response = owner_op(
        &owner,
        &sending,
        "session.retract",
        serde_json::json!({"submission_id":submission_id}),
    )
    .await
    .unwrap();
    assert_eq!(response["result"], "retracted", "{response}");
    let response = owner_op(
        &owner,
        &sending,
        "run.cancel",
        serde_json::json!({"run_id":run_id}),
    )
    .await
    .unwrap();
    assert_eq!(response["result"], "ack", "{response}");
}

#[tokio::test(flavor = "multi_thread")]
async fn real_legacy_and_direct_histories_remain_unprotected() {
    use broca_core::WalStore;
    let model = HeldOwnerModel::start().await;
    let Some(rig) = Rig::start_configured(None, None, Some(&model.url)).await else {
        return;
    };
    let owner = rig.connect_as(OWNER, &rig.owner_nonce).await;
    let basal = rig.connect_as(CARRIER, &rig.carrier_nonce).await;
    let foreign = rig
        .connect_as(OTHER_CARRIER, &rig.other_carrier_nonce)
        .await;
    let direct = SubcConnection::connect(&rig.connection_file).await.unwrap();
    let source = rig.session("legacy-config-source");
    let source_route = owner.route_open_management("broca", &source).await.unwrap();
    send(&owner, &source_route, "first").await.unwrap();
    model.wait_request().await;
    let records =
        broca_wal::replay_file(rig.wal_path(&source), broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
    let start = records
        .iter()
        .find(|frame| matches!(&frame.record, broca_wal::WalRecord::RunStarted { .. }))
        .unwrap()
        .record
        .clone();
    let store = broca_core::FileWalStore::new(rig.state_root.join("wal"));
    for (index, (first, sender, caller)) in [
        (None, &owner, &foreign),
        (None, &owner, &basal),
        (None, &owner, &direct),
        (
            Some(broca_protocol::BindPrincipal::Direct),
            &direct,
            &foreign,
        ),
    ]
    .into_iter()
    .enumerate()
    {
        // Seed a completed episode in a session whose actor has never opened. New work then
        // enters through the real daemon; a later attested sender must not protect legacy
        // history retroactively. Unique episode ids avoid collisions in durable run lookups.
        let session = rig.session(&format!("legacy-{index}"));
        let mut start = start.clone();
        let broca_wal::WalRecord::RunStarted {
            session: recorded_session,
            principal,
            run_id,
            send_id,
            request_identity_digest,
            ..
        } = &mut start
        else {
            unreachable!()
        };
        *recorded_session = session.clone();
        *principal = first;
        *run_id = broca_protocol::RunId(format!("legacy-origin-{index}"));
        *send_id = None;
        *request_identity_digest = None;
        {
            let mut wal = store.open(&session).unwrap();
            wal.append(1, &start, true).unwrap();
            wal.append(
                1,
                &broca_wal::WalRecord::RunFinished {
                    reason: broca_protocol::RunFinishReason::Cancelled,
                    ts_ms: None,
                    usage: broca_protocol::Usage::default(),
                    error: None,
                    retries_used: 0,
                },
                true,
            )
            .unwrap();
        }
        let sending = sender
            .route_open_management("broca", &session)
            .await
            .unwrap();
        let started = send(sender, &sending, "new-work").await.unwrap();
        assert_eq!(started["result"]["state"], "active", "{started}");
        model.wait_request().await;
        let run_id = started["result"]["run_id"].as_str().unwrap();
        let submission_id = pending_owner_send(sender, &sending, "queued").await;
        let route = caller
            .route_open_management("broca", &session)
            .await
            .unwrap();
        let response = owner_op(
            caller,
            &route,
            "session.retract",
            serde_json::json!({"submission_id":submission_id}),
        )
        .await
        .unwrap();
        assert_eq!(
            response["result"], "retracted",
            "legacy {index}: {response}"
        );
        let response = owner_op(
            caller,
            &route,
            "run.cancel",
            serde_json::json!({"run_id":run_id}),
        )
        .await
        .unwrap();
        assert_eq!(response["result"], "ack", "legacy {index}: {response}");
    }
}
