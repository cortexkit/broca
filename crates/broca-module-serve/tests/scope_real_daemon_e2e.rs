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

#[path = "../../broca-wire/tests/support/run_usage.rs"]
mod usage_compat;

#[path = "common/stored_log.rs"]
mod stored_log;
use broca_session_store::record::Record;
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

#[path = "scope_real_daemon_e2e/compaction_routes.rs"]
mod compaction_routes;

#[path = "scope_real_daemon_e2e/compose_plan.rs"]
mod compose_plan;

#[derive(Default)]
struct AcceptanceCatalog {
    fetched: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl subc_client_rs::ModuleHandler for AcceptanceCatalog {
    async fn handle(
        &self,
        _ctx: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        use sha2::{Digest, Sha256};
        let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(request["name"], "tool.catalog");
        assert_eq!(request["arguments"]["preset"], "reader");
        let composition = &request["arguments"]["composition"];
        let schema = serde_json::json!({"type": "object"});
        let mut answer = serde_json::json!({
            "generation": "1",
            "catalog_digest": format!("{:x}", Sha256::digest(b"acceptance-catalog")),
            "capabilities": {"late_results": true},
            "tools": [{
                "name": "read", "description": "Read a file",
                "input_schema": schema,
                "schema_digest": cortexkit_role_tool_provider::catalog::schema_digest(&schema).unwrap(),
                "semantics": 1, "capabilities": ["code.read/v1"],
                "result_format": "content/v1"
            }]
        });
        // A provider release between preflight and admission must recompose the plan, not
        // rotate models or bypass the WAL feature checks.
        if self.fetched.swap(true, std::sync::atomic::Ordering::SeqCst) {
            answer["catalog_digest"] = serde_json::json!(format!(
                "{:x}",
                Sha256::digest(b"acceptance-catalog-updated")
            ));
            answer["tools"].as_array_mut().unwrap().push(serde_json::json!({
                "name": "search", "description": "Search files", "input_schema": schema,
                "schema_digest": cortexkit_role_tool_provider::catalog::schema_digest(&schema).unwrap(),
                "semantics": 1, "capabilities": ["code.search/v1"], "result_format": "content/v1"
            }));
        }
        if !composition.is_null() {
            answer["composition_digest"] = serde_json::json!(
                cortexkit_role_tool_provider::catalog::composition_digest(composition).unwrap()
            );
        }
        subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&answer).unwrap())
    }
}

fn worker_acceptance_scope(checkout: &Path) -> serde_json::Value {
    serde_json::json!([{
        "ref": SCOPE_REF, "scope_epoch": SCOPE_EPOCH, "kind": "head",
        "workdir": checkout, "attributes": {},
        "carriers": [{
            "principal": {"kind": "reserved", "module_id": "broca"},
            "targets": ["aft"],
        }],
    }])
}

#[test]
fn worker_acceptance_scope_authorizes_broca_for_aft() {
    let scopes = worker_acceptance_scope(Path::new("/fixture/checkout"));
    let _: Vec<subc_protocol::scope::ScopeRecord> = serde_json::from_value(scopes.clone())
        .expect("the fixture follows the daemon's scope schema");
    assert_eq!(
        scopes[0]["carriers"],
        serde_json::json!([{
            "principal": {"kind": "reserved", "module_id": "broca"},
            "targets": ["aft"],
        }]),
        "broca must carry the owner's scope to the plan's AFT provider"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_admission_acceptance_reads_real_run_started_gates() {
    use subc_protocol::manifest::{Concurrency, ExecutionMode, IdentityScope, ProviderRole, Tool};
    let model = ScopeModel::start().await;
    // A mason starts in the new storage, which refuses a new session until its deployment key
    // loads; plain sessions need no key, so the rig needs no vault.
    let storage = serde_json::json!({"session_storage": {"encryption": "off"}});
    let Some(rig) = Rig::start_with_broca_config(None, None, Some(&model.url), Some(storage)).await
    else {
        return;
    };
    let manifest =
        subc_protocol::manifest::ModuleManifest::builder("aft", env!("CARGO_PKG_VERSION"))
            .provides(vec![ProviderRole::ToolProvider {
                tools: vec![Tool {
                    name: "read".into(),
                    description: Some("Read a file".into()),
                    execution_mode: ExecutionMode::Pure,
                    schema: serde_json::json!({"type": "object"}),
                }],
                identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
                concurrency: Concurrency::ModuleManaged,
                emits_push: false,
                sub_supervises: false,
            }])
            .build();
    let (_, serve) = subc_client_rs::serve_with_handle(
        &rig.connection_file,
        manifest,
        AcceptanceCatalog::default(),
    )
    .await
    .expect("register declaring AFT catalog on private daemon");
    let serving = tokio::spawn(serve);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();

    let now_ms = || {
        u64::try_from(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis(),
        )
        .unwrap()
    };
    let restart_observed_at_ms = now_ms();
    let placement_report = rig.dir.join("placement.json");
    std::fs::write(
        &placement_report,
        serde_json::to_vec(&serde_json::json!({
            "restart_observed_at_ms": restart_observed_at_ms,
        }))
        .unwrap(),
    )
    .unwrap();
    broca_test_support::wait_until(
        "an admission timestamp after the restart boundary",
        || async {
            if now_ms() > restart_observed_at_ms {
                Ok(())
            } else {
                Err("the clock is still at the restart boundary".into())
            }
        },
    )
    .await;

    // The rig's authenticated owner registers a scope with a real checkout.
    // The acceptance script only observes the worker the owner admits below.
    let checkout = rig.dir.join("acceptance-checkout");
    std::fs::create_dir_all(&checkout).unwrap();
    let checkout = checkout.canonicalize().unwrap();
    rig.sync_scopes(worker_acceptance_scope(&checkout), 1).await;
    let owner = std::sync::Arc::new(rig.connect_as(OWNER, &rig.owner_nonce).await);
    let session = rig.session("worker-admission-acceptance");
    let route = open_scoped(&owner, &session).await.unwrap();
    let scope = broca_protocol::SessionScope {
        owner: broca_protocol::BindPrincipal::Reserved {
            module_id: OWNER.into(),
        },
        agent_id_frozen: false,
        scope_ref: SCOPE_REF.into(),
        scope_epoch: SCOPE_EPOCH,
        flow_id: None,
        sender: None,
        agent_id: None,
        run_id: None,
    };
    // The daemon validates the same registered scope on catalog and management routes.
    let fetcher =
        broca_subc::SubcPlanFetcher::new(owner.clone(), session.clone(), Some(scope.clone()));
    let items = serde_json::from_slice(
        &std::fs::read(root.join("scripts/fixtures/acceptance-reader-plan.json")).unwrap(),
    )
    .unwrap();
    let plan = broca_fetch_plan::compose(&items, &fetcher, broca_fetch_plan::ADMISSION_DEADLINE)
        .await
        .expect("compose through the owner's real scoped AFT route");
    fetcher.close().await;
    let mut params = serde_json::json!({
        "prompt": "Reply only OK. Do not call tools.",
        "model": {"provider": "mock", "model": "mock-model"},
        "session_class": "mason", "plan": plan,
    });
    let stale = scope_request(&owner, &route, "session.send", params.clone())
        .await
        .expect_err("the changed AFT catalog refuses the stale composition before admission");
    assert!(stale.to_string().contains("plan_stale"), "{stale}");
    params["plan"] =
        broca_fetch_plan::compose(&items, &fetcher, broca_fetch_plan::ADMISSION_DEADLINE)
            .await
            .expect("recompose once after the catalog change");
    fetcher.close().await;
    let admitted = scope_request(&owner, &route, "session.send", params)
        .await
        .expect("admit the scoped worker with the refreshed AFT composition");
    assert_eq!(admitted["result"]["state"], "active");
    let run = admitted["result"]["run_id"].as_str().unwrap();
    completed_run(&owner, &route, run).await;

    // A mason runs in the new storage; its run start is read back through `run.admission`, the
    // same read the acceptance script makes.
    let admission = scope_request(
        &owner,
        &route,
        "run.admission",
        serde_json::json!({"run_id": run}),
    )
    .await
    .expect("read the worker run's admission");
    let admission = &admission["result"];
    assert_eq!(admission["storage"], "store", "{admission}");
    assert_eq!(admission["session_class"], "mason", "{admission}");
    let frozen = serde_json::to_value(&scope).unwrap();
    assert_eq!(admission["scope"]["owner"], frozen["owner"], "{admission}");
    assert_eq!(admission["scope"]["ref"], frozen["ref"], "{admission}");
    assert_eq!(
        admission["scope"]["epoch"], frozen["scope_epoch"],
        "{admission}"
    );
    let aft = admission["fetches"]
        .as_array()
        .and_then(|fetches| fetches.iter().find(|fetch| fetch["provider"] == "aft"))
        .expect("admission froze the AFT fetch");
    let declarations = &aft["declarations_frozen"];
    assert_eq!(declarations["late_results"], true, "{admission}");
    assert!(
        declarations["result_formats"]
            .as_object()
            .is_some_and(|formats| !formats.is_empty()),
        "{admission}"
    );
    let _ = checkout;

    let output = Command::new("python3")
        .arg(root.join("scripts/acceptance-worker-turn.py"))
        .arg("--session-bin")
        .arg(env!("CARGO_BIN_EXE_broca-session"))
        .arg("--subc")
        .arg(&rig.connection_file)
        .arg("--state-root")
        .arg(&rig.state_root)
        .arg("--project-root")
        .arg(&rig.dir)
        .arg("--placement-report")
        .arg(&placement_report)
        .args(["--timeout", "30"])
        .env("BROCA_BIN", env!("CARGO_BIN_EXE_ck-broca"))
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .await
        .unwrap();
    serving.abort();
    println!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    if !output.status.success() {
        for path in walk(&rig.dir) {
            if let Ok(text) = std::fs::read_to_string(&path) {
                for line in text
                    .lines()
                    .filter(|line| line.contains("panicked") || line.contains("precomputed"))
                    .take(4)
                {
                    println!("{}: {line}", path.display());
                }
            }
        }
    }
    assert!(
        output.status.success(),
        "worker admission acceptance failed"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("PASS -- admitted, completed"));
    assert!(stdout.contains("class=mason"));
    assert!(stdout.contains("candidates=1"));
    assert!(stdout.contains("terminal_counts=completed:1"));
    assert!(!stdout.contains("scope answer"));
}

fn sibling_bin(name: &str, env_key: &str) -> PathBuf {
    broca_test_support::subc_bin(name, env_key)
}

/// `scripts/rust-test-gate.sh` exports `BROCA_REQUIRE_SUBC`, so in a gated run a missing
/// daemon is a failure that names itself; a bare local run may still skip, out loud.
fn require_subc() -> bool {
    std::env::var_os("BROCA_REQUIRE_SUBC").is_some()
}

async fn broca_management_ready<T, Open, Opened>(mut open: Open) -> T
where
    Open: FnMut() -> Opened,
    Opened: std::future::Future<Output = Result<T, broca_subc::ConnError>>,
{
    broca_test_support::wait_until("broca's sessionless management bind", || {
        let opened = open();
        async move {
            match opened.await {
                Ok(route) => Ok(route),
                Err(error) if error.code() == Some("module_warming") => Err(error.to_string()),
                Err(error) => panic!("broca startup readiness failed: {error}"),
            }
        }
    })
    .await
}

fn warming_refusal(code: &str) -> broca_subc::ConnError {
    let body = serde_json::to_vec(&serde_json::json!({
        "code": code, "message": "broca is supervised but not available",
    }))
    .unwrap();
    broca_subc::ConnError::Rejected(
        broca_subc::connection::RejectedMessage::from_error_frame_body_for_test(&body),
    )
}

#[tokio::test(start_paused = true)]
async fn rig_readiness_waits_for_a_live_management_bind() {
    let attempts = std::cell::Cell::new(0);
    let opened = broca_management_ready(|| {
        attempts.set(attempts.get() + 1);
        std::future::ready(if attempts.get() == 3 {
            Ok("management route")
        } else {
            Err(warming_refusal("module_warming"))
        })
    })
    .await;
    assert_eq!(opened, "management route");
    assert_eq!(attempts.get(), 3);
}

#[tokio::test(start_paused = true)]
async fn rig_readiness_refuses_nonwarming_errors_without_retrying() {
    use futures::FutureExt;
    for error in [
        warming_refusal("permission_denied"),
        warming_refusal("module_reloading"),
        broca_subc::ConnError::Rejected("module_warming in plain text".into()),
        broca_subc::ConnError::Transport("connection lost".into()),
    ] {
        let expected = error.to_string();
        let error = std::cell::RefCell::new(Some(error));
        let attempts = std::cell::Cell::new(0);
        let start = tokio::time::Instant::now();
        let panic = std::panic::AssertUnwindSafe(broca_management_ready(|| {
            attempts.set(attempts.get() + 1);
            std::future::ready(Err::<(), _>(
                error
                    .borrow_mut()
                    .take()
                    .expect("must not retry a terminal error"),
            ))
        }))
        .catch_unwind()
        .await
        .expect_err("a non-warming refusal must fail readiness");
        let report = panic
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                panic
                    .downcast_ref::<&str>()
                    .map(|message| (*message).to_owned())
            })
            .expect("readiness panic is text");
        assert!(report.contains(&expected), "{report}");
        assert_eq!(attempts.get(), 1);
        assert_eq!(start.elapsed(), Duration::ZERO);
    }
}

async fn project_identity_open(
    connection: &SubcConnection,
    identity: &broca_protocol::BindIdentity,
    scoped: bool,
) -> broca_subc::RouteHandle {
    if scoped {
        open_scoped(connection, identity).await.unwrap()
    } else {
        connection
            .route_open_management_within(
                "broca",
                identity,
                RouteOpenRetry {
                    budget: Duration::from_secs(30),
                    max_interval: Duration::from_millis(100),
                },
            )
            .await
            .unwrap()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn project_identity_existing_unscoped_and_scoped_resume_on_legacy_addresses() {
    for (scoped, old_folder) in [
        (false, "legacy-one"),
        (false, "legacy-two"),
        (true, "legacy-one"),
        (true, "legacy-two"),
    ] {
        let Some(rig) = Rig::start().await else {
            return;
        };
        if scoped {
            rig.sync_scopes(
                serde_json::json!([{
                    "ref": SCOPE_REF, "scope_epoch": SCOPE_EPOCH, "kind":"head",
                    "workdir": rig.dir.canonicalize().unwrap(), "attributes":{}, "carriers":[],
                }]),
                1,
            )
            .await;
        }
        let owner = rig.connect_as(OWNER, &rig.owner_nonce).await;
        let mut legacy = rig.session(if scoped {
            "scoped-resume"
        } else {
            "athena-seat"
        });
        let old_root = rig.dir.join(old_folder);
        std::fs::create_dir_all(&old_root).unwrap();
        legacy.project_root = old_root.canonicalize().unwrap();
        // Reserve a pre-existing directory-root identity independently of today's wire bind.
        let inventory =
            broca_store::SqliteStore::open(rig.state_root.join("run-index.db")).unwrap();
        broca_core::SessionRefIndex::record_session(
            &inventory,
            &broca_core::session_addr(&legacy),
            &legacy,
        )
        .unwrap();
        drop(inventory);
        let route = project_identity_open(&owner, &legacy, scoped).await;
        let first = send(&owner, &route, "first-consult-round").await.unwrap();
        let run_id = first["result"]["run_id"].as_str().unwrap().to_owned();
        let prefix = rig.debug_records(&legacy).await;
        owner.route_close(&route).await.unwrap();
        let roster: serde_json::Value = serde_json::from_slice(
            &std::fs::read(
                rig.dir
                    .join("runtime/data-home/cortexkit/run/live-children.json"),
            )
            .unwrap(),
        )
        .unwrap();
        let pid = roster["children"]
            .as_array()
            .unwrap()
            .iter()
            .find(|child| child["module_id"] == "broca")
            .unwrap()["pid"]
            .as_u64()
            .unwrap();
        subc_os::Process::open(u32::try_from(pid).unwrap())
            .unwrap()
            .unwrap()
            .signal(subc_os::Signal::Kill)
            .unwrap();

        let other_root = rig.dir.join("different-client-folder");
        std::fs::create_dir_all(&other_root).unwrap();
        let mut rebound = legacy.clone();
        rebound.project_root = other_root.canonicalize().unwrap();
        let reopened = project_identity_open(&owner, &rebound, scoped).await;
        let second = send(&owner, &reopened, "first-consult-round")
            .await
            .unwrap();
        assert_eq!(second["result"]["run_id"], run_id);
        // The session resumed under its recorded folder: its records continue there, and
        // nothing is stored under the folder the client bound with.
        let resumed = rig.debug_records(&legacy).await;
        assert_eq!(&resumed[..prefix.len()], prefix);
        assert!(!rig.exists(&rebound));
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn project_identity_new_scoped_session_uses_stamped_workdir() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    let checkout = rig.dir.join("stamped-checkout");
    std::fs::create_dir_all(&checkout).unwrap();
    let checkout = checkout.canonicalize().unwrap();
    rig.sync_scopes(
        serde_json::json!([{
            "ref":SCOPE_REF, "scope_epoch":SCOPE_EPOCH, "kind":"head",
            "workdir":checkout, "attributes":{}, "carriers":[],
        }]),
        1,
    )
    .await;
    let owner = rig.connect_as(OWNER, &rig.owner_nonce).await;
    let route_identity = rig.session("new-scoped-workdir");
    let route = project_identity_open(&owner, &route_identity, true).await;
    send(&owner, &route, "first").await.unwrap();
    let mut storage = route_identity.clone();
    storage.project_root = checkout;
    assert!(rig.exists(&storage));
    assert!(!rig.exists(&route_identity));
}

#[tokio::test(flavor = "multi_thread")]
async fn project_identity_new_unscoped_session_uses_namespace_under_state_root() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    let owner = rig.connect_as(OWNER, &rig.owner_nonce).await;
    let mut claimed = rig.session("new-unscoped");
    claimed.project_root = rig.dir.canonicalize().unwrap();
    let route = project_identity_open(&owner, &claimed, false).await;
    send(&owner, &route, "first").await.unwrap();
    let expected =
        broca_protocol::BindIdentity::new("broca-unscoped-v1", &claimed.harness, &claimed.session);
    assert!(rig.exists(&expected));
    assert!(!rig.exists(&claimed));
    let inventory =
        broca_store::SqliteStore::open_read_only(rig.state_root.join("run-index.db")).unwrap();
    let reference = broca_core::agent_session_ref(&expected);
    let snapshot = inventory
        .session_refs(std::iter::once(reference.as_str()))
        .unwrap();
    assert_eq!(
        snapshot.sessions[&reference],
        (broca_core::session_addr(&expected), expected)
    );
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
        broca_test_support::kill_and_reap_tree(&mut self.child);
        broca_test_support::dump_rig_process_logs_on_failure(&self.dir, "daemon");
        broca_test_support::remove_rig_dir(&self.dir);
    }
}

impl Rig {
    async fn start() -> Option<Self> {
        Self::start_configured(None, None, None).await
    }

    async fn start_with_compaction_writer() -> Option<Self> {
        Self::start_configured_with_compaction_writer(None, None, None, true).await
    }

    fn write_compaction_config(config_home: &Path) -> PathBuf {
        Self::write_broca_config(config_home, &Self::compaction_config())
    }

    /// The `broca.jsonc` that lets broca call compaction providers: only with bus events
    /// enabled does admission ask a provider about a compaction item instead of refusing it.
    fn compaction_config() -> serde_json::Value {
        serde_json::json!({"bus_run_events_enabled": true})
    }

    /// Writes `config` as broca's `broca.jsonc` under `config_home`, and returns its path.
    fn write_broca_config(config_home: &Path, config: &serde_json::Value) -> PathBuf {
        let path = config_home.join("cortexkit/broca.jsonc");
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, serde_json::to_vec(config).unwrap()).unwrap();
        path
    }

    async fn start_configured(
        binary: Option<&Path>,
        state: Option<&Path>,
        model_url: Option<&str>,
    ) -> Option<Self> {
        Self::start_configured_with_compaction_writer(binary, state, model_url, false).await
    }

    async fn start_configured_with_compaction_writer(
        binary: Option<&Path>,
        state: Option<&Path>,
        model_url: Option<&str>,
        compaction_writer: bool,
    ) -> Option<Self> {
        let broca_config = compaction_writer.then(Self::compaction_config);
        Self::start_with_broca_config(binary, state, model_url, broca_config).await
    }

    /// A rig whose broca runs `binary` (else this build's) over the state root `state` (else a
    /// fresh one under the rig), calls the model at `model_url`, and reads `broca_config`, when
    /// given, as its `broca.jsonc`.
    async fn start_with_broca_config(
        binary: Option<&Path>,
        state: Option<&Path>,
        model_url: Option<&str>,
        broca_config: Option<serde_json::Value>,
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
        broca_test_support::remove_rig_dir(&dir);
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
        let mut config = serde_json::json!({
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
        // No deployment-key vault here: sessions are stored plain, so this test reads them back
        // from disk (`stored_log`).
        let mut broca_config = broca_config.unwrap_or_else(|| serde_json::json!({}));
        broca_config["session_storage"] = serde_json::json!({"encryption": "off"});
        config["modules"]["broca"]["env"]["BROCA_CONFIG_PATH"] =
            serde_json::json!(Self::write_broca_config(&config_home, &broca_config));
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
        // A running process can still be warming. Wait until broca answers a bind, so no test
        // opens a route while the daemon still refuses it with `module_warming`.
        rig.wait_for_broca_ready().await;
        Some(rig)
    }

    async fn wait_for_broca_ready(&self) {
        // The process image and connection file can precede store initialization and
        // HELLO. A permissions-role bind observes readiness without creating a session.
        let connection =
            SubcConnection::connect_with_consumer_identity(&self.connection_file, None)
                .await
                .expect("connect for broca readiness");
        let identity = broca_protocol::BindIdentity::new("broca-unscoped-v1", "", "");
        let route = broca_management_ready(|| {
            connection.route_open_management_roles_within(
                "broca",
                &identity,
                std::collections::BTreeMap::from([("permissions".into(), "v1".into())]),
                RouteOpenRetry::within(Duration::ZERO, Duration::ZERO),
            )
        })
        .await;
        connection
            .route_close(&route)
            .await
            .expect("close readiness route");
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
        broca_protocol::BindIdentity::new("broca-unscoped-v1", "scope-e2e", name)
    }

    /// `session` as the daemon keys it: a folder as it resolves it (on macOS /var is
    /// /private/var); the unscoped namespace as it is.
    fn keyed(&self, session: &broca_protocol::BindIdentity) -> broca_protocol::BindIdentity {
        let mut keyed = session.clone();
        if let Ok(resolved) = std::fs::canonicalize(&session.project_root) {
            keyed.project_root = resolved;
        }
        keyed
    }

    /// `session`'s records, each as its debug text, for comparing what a later step kept.
    async fn debug_records(&self, session: &broca_protocol::BindIdentity) -> Vec<String> {
        self.records(session)
            .await
            .iter()
            .map(|record| format!("{record:?}"))
            .collect()
    }

    /// Whether the new storage holds `session`.
    fn exists(&self, session: &broca_protocol::BindIdentity) -> bool {
        broca_wal::session_storage_class(
            &self.state_root,
            &broca_core::session_addr(&self.keyed(session)),
        )
        .unwrap()
        .is_some()
    }

    /// `session`'s records in the new storage, in log order; empty while it doesn't exist.
    async fn records(&self, session: &broca_protocol::BindIdentity) -> Vec<Record> {
        stored_log::stored_records(&self.state_root, &self.keyed(session)).await
    }

    /// What each run `session` started was admitted with, in the order the runs started.
    async fn admissions(
        &self,
        session: &broca_protocol::BindIdentity,
    ) -> Vec<broca_session_loop::run::Admission> {
        stored_log::stored_admissions(&self.state_root, &self.keyed(session)).await
    }

    /// The runs `session` started and the sends it queued: what an admitted send writes. The
    /// owner's run keeps retrying the unreachable provider in the background, so the session
    /// may still grow; a refused send must add to neither count.
    async fn admitted_sends(&self, session: &broca_protocol::BindIdentity) -> (usize, usize) {
        let records = self.records(session).await;
        (
            records
                .iter()
                .filter(|record| matches!(record, Record::RunStart(_)))
                .count(),
            records
                .iter()
                .filter(|record| matches!(record, Record::QueuedSend(_)))
                .count(),
        )
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
            budget: Duration::from_secs(2),
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

    assert!(
        rig.exists(&session),
        "the session is stored; the rig holds {:?}",
        walk(&rig.dir)
    );
    let frozen: Vec<_> = rig
        .admissions(&session)
        .await
        .into_iter()
        .map(|admission| (admission.send.scope, admission.send.principal))
        .collect();
    let owner_principal = broca_protocol::BindPrincipal::Reserved {
        module_id: OWNER.into(),
    };
    assert_eq!(
        frozen,
        [(
            Some(broca_protocol::SessionScope {
                agent_id_frozen: false,
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
    // A second identity, admitted by the daemon as a listed carrier of the same scope, is not
    // the scope's owner: broca refuses its send by name and writes nothing.
    let carrier = rig
        .connect_as(OTHER_CARRIER, &rig.other_carrier_nonce)
        .await;
    let carrier_route = open_scoped(&carrier, &session)
        .await
        .expect("the daemon admits a listed carrier's scoped open");
    let before = rig.admitted_sends(&session).await;
    let refused = send(&carrier, &carrier_route, "intruder")
        .await
        .expect_err("a second identity's send is refused");
    assert!(
        refused.to_string().contains("scope_owner_mismatch"),
        "{refused}"
    );
    assert_eq!(
        rig.admitted_sends(&session).await,
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

/// A new session's first send with an ordered `delivery` list starts its run from the queue during
/// its own admission. That run must freeze the stamped scope exactly like a plain send's: the
/// queue path used to drop it, so the first `RunStarted` carried no scope and the run's tools
/// reached their providers on unscoped routes, unconfined.
#[tokio::test(flavor = "multi_thread")]
async fn an_ordered_first_send_freezes_the_stamped_scope() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    rig.sync_scope().await;
    let owner = rig.connect_as(OWNER, &rig.owner_nonce).await;
    let session = rig.session("ordered");
    let route = open_scoped(&owner, &session)
        .await
        .expect("the owner's scoped open to a fresh session is admitted");
    let reply = owner
        .route_request(
            &route,
            serde_json::to_vec(&serde_json::json!({"method": "session.send", "params": {
                "prompt": "hi",
                "send_id": "ordered-first",
                "delivery": ["steer", "idle_start"],
                "model": {"provider": "mock", "model": "mock-model"},
            }}))
            .unwrap(),
        )
        .await
        .expect("the owner's ordered send");
    assert_eq!(reply["result"]["state"], "active", "{reply}");
    let scopes: Vec<_> = rig
        .admissions(&session)
        .await
        .into_iter()
        .map(|admission| admission.send.scope)
        .collect();
    assert_eq!(
        scopes,
        [Some(broca_protocol::SessionScope {
            agent_id_frozen: false,
            owner: broca_protocol::BindPrincipal::Reserved {
                module_id: OWNER.into(),
            },
            scope_ref: SCOPE_REF.into(),
            scope_epoch: SCOPE_EPOCH,
            flow_id: None,
            sender: None,
            agent_id: None,
            run_id: None,
        })],
        "the ordered first send's run froze the scope the daemon stamped"
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

    // Refused tool requests must not create the session, even before the first episode.
    let tools_session = rig.session("flow-tools");
    let tools_route = open_scoped(&carrier, &tools_session).await.unwrap();
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
        println!("FLOW sent reserved:basal session.send with {field}; returned {error}");
        assert!(
            error.to_string().contains("invalid_params") && error.to_string().contains(field),
            "{error}"
        );
        assert!(!rig.exists(&tools_session), "a tool refusal stores nothing");
    }

    let reply = send(&carrier, &route, "flow-first")
        .await
        .expect("flow carrier send accepted");
    println!("FLOW sent reserved:basal session.send flow-a owner=reserved:prefrontal-core agent_id=owner-agent delegates=true; returned {reply}");
    assert_eq!(reply["result"]["state"], "active", "{reply}");
    let sender = broca_protocol::BindPrincipal::Reserved {
        module_id: CARRIER.into(),
    };
    let scopes: Vec<_> = rig
        .admissions(&session)
        .await
        .into_iter()
        .map(|admission| (admission.send.scope, admission.send.principal))
        .collect();
    assert_eq!(
        scopes,
        [(
            Some(broca_protocol::SessionScope {
                agent_id_frozen: true,
                owner: broca_protocol::BindPrincipal::Reserved {
                    module_id: OWNER.into()
                },
                scope_ref: SCOPE_REF.into(),
                scope_epoch: SCOPE_EPOCH,
                flow_id: Some("flow-a".into()),
                sender: Some(sender.clone()),
                agent_id: Some("owner-agent".into()),
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
    let before = rig.admitted_sends(&session).await;
    let error = send(&other, &other_route, "wrong-carrier")
        .await
        .expect_err("wrong_carrier_refused: a non-Basal carrier cannot drive the frozen flow");
    println!("FLOW sent reserved:{OTHER_CARRIER} session.send on basal's flow-a; returned {error}");
    assert!(
        error.to_string().contains("scope_sender_mismatch"),
        "wrong_carrier_refused: {error}"
    );
    assert_eq!(rig.admitted_sends(&session).await, before);

    let owner_route = open_scoped(&owner, &session).await.unwrap();
    let before = rig.admitted_sends(&session).await;
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
    assert_eq!(rig.admitted_sends(&session).await, before);

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
    let before = rig.admitted_sends(&session).await;
    let error = send(&carrier, &route, "changed-flow")
        .await
        .expect_err("flow id cannot change");
    println!("FLOW sent reserved:basal session.send with changed flow-b; returned {error}");
    assert!(
        error.to_string().contains("scope_flow_id_mismatch"),
        "{error}"
    );
    assert_eq!(rig.admitted_sends(&session).await, before);

    rig.sync_scope_with(None, 3).await;
    let plain = rig.session("plain-basal");
    let plain_route = open_scoped(&carrier, &plain).await.unwrap();

    let error = send(&carrier, &plain_route, "plain-basal")
        .await
        .expect_err("non-owner Basal without a flow stamp is refused");
    println!("FLOW sent reserved:basal session.send without flow_id on non-owner scope; returned {error}");
    assert!(error.to_string().contains("flow_scope_required"), "{error}");
    assert_eq!(
        rig.admitted_sends(&plain).await,
        (0, 0),
        "plain Basal refusal admits no run or queued send"
    );
}

/// Basal cannot start unscoped work, but empty-session reads cannot start work either.
#[tokio::test(flavor = "multi_thread")]
async fn real_basal_unscoped_send_refused_writing_nothing_and_reads_allowed() {
    let Some(rig) = Rig::start().await else {
        return;
    };
    let basal = rig.connect_as(CARRIER, &rig.carrier_nonce).await;
    let session = rig.session("basal-empty");
    let route = basal
        .route_open_management("broca", &session)
        .await
        .unwrap();
    let before = rig.debug_records(&session).await;
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
        assert_eq!(rig.debug_records(&session).await, before);
        assert_eq!(rig.admitted_sends(&session).await, (0, 0));
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
    let records = rig.records(&foreign).await;
    assert!(
        !records
            .iter()
            .any(|record| matches!(record, Record::RunEnd(_))),
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
        .unwrap_err();
    assert!(
        response.to_string().contains("scope_owner_mismatch"),
        "{response}"
    );
    let cleanup = owner.route_open_management("broca", &own).await.unwrap();
    let cancelled = owner_op(
        &owner,
        &cleanup,
        "run.cancel",
        serde_json::json!({"run_id":own_run}),
    )
    .await
    .unwrap();
    assert_eq!(cancelled["result"], "ack", "{cancelled}");
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

/// The run's segment facts, as stored. Its per-request `model_attempt` facts are
/// [`attempt_facts`].
fn fact_bytes(rig: &Rig, run: &str) -> Vec<String> {
    all_fact_bytes(rig, run)
        .into_iter()
        .filter(|bytes| serde_json::from_str::<serde_json::Value>(bytes).unwrap()["kind"].is_null())
        .collect()
}

/// The run's `model_attempt` facts: one per provider request it sent.
fn attempt_facts(rig: &Rig, run: &str) -> Vec<serde_json::Value> {
    all_fact_bytes(rig, run)
        .iter()
        .map(|bytes| serde_json::from_str::<serde_json::Value>(bytes).unwrap())
        .filter(|fact| fact["kind"] == "model_attempt")
        .collect()
}

fn all_fact_bytes(rig: &Rig, run: &str) -> Vec<String> {
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
    // The run's end is observable before its usage fact is written. Wait for the fact before
    // taking the no-write snapshots of the facts.
    let deadline = Instant::now() + Duration::from_secs(30);
    while fact_bytes(rig, run).is_empty() {
        assert!(
            Instant::now() < deadline,
            "completed run {run} was not exported"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

/// Every usage fact in the index, in fact order: what a refused send must not add to.
fn admission_index_rows(rig: &Rig) -> Vec<String> {
    let db = rusqlite::Connection::open_with_flags(
        rig.state_root.join("run-index.db"),
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )
    .unwrap();
    let mut facts = db
        .prepare("SELECT segment_json FROM export_facts ORDER BY fact_id")
        .unwrap();
    let facts = facts
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    facts
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
    let scopes = rig
        .admissions(&session)
        .await
        .into_iter()
        .map(|admission| {
            (
                admission.send.scope.unwrap(),
                admission.send.principal.unwrap(),
            )
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
    let facts = fact_bytes(&rig, &run);
    assert_eq!(facts.len(), 1);
    let fact: serde_json::Value = serde_json::from_str(&facts[0]).unwrap();
    assert_eq!(fact["agent_id"], "agent-a");
    assert_eq!(fact["agent_run_id"], "parent-run");
    assert_eq!(fact["run_id"], run);
    let attempts = attempt_facts(&rig, &run);
    assert!(
        !attempts.is_empty(),
        "the run's request has its attempt fact"
    );
    for attempt in &attempts {
        assert_eq!(attempt["agent_id"], "agent-a");
        assert_eq!(attempt["agent_run_id"], "parent-run");
    }

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
            let before = rig.debug_records(test_session).await;
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
                rig.debug_records(test_session).await,
                before,
                "{field} refusal wrote records"
            );
            if test_session != &session {
                assert!(
                    !rig.exists(test_session),
                    "fresh {field} refusal created the session"
                );
            }
            assert_eq!(
                admission_index_rows(&rig),
                index_before,
                "{field} refusal wrote usage facts"
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
            let before = rig.debug_records(key).await;
            let index_before = admission_index_rows(&rig);
            let error = send(&conn, &other_route, "wrong-sender").await.unwrap_err();
            let expected = if key == &session {
                "scope_sender_mismatch"
            } else {
                "scope_owner_mismatch"
            };
            assert_eq!(error.code(), Some(expected), "{module}: {error}");
            assert_eq!(
                rig.debug_records(key).await,
                before,
                "{module} refusal wrote records"
            );
            if key != &session {
                assert!(
                    !rig.exists(key),
                    "fresh {module} refusal created the session"
                );
            }
            assert_eq!(
                admission_index_rows(&rig),
                index_before,
                "{module} refusal wrote usage facts"
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
        let before = rig.debug_records(&session).await;
        let index_before = admission_index_rows(&rig);
        let body = serde_json::json!({"method":"session.send","params":{
            "prompt":"hi","send_id":"changed-identity","model":{"provider":"mock","model":"mock-model"}
        }});
        let error = basal
            .route_unary_reply(
                &changed,
                serde_json::to_vec(&body).unwrap(),
                Duration::from_secs(20),
            )
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code, format!("scope_{field}_mismatch"));
        assert_eq!(error.detail.unwrap()["field"], field);
        assert_eq!(
            rig.debug_records(&session).await,
            before,
            "{field} refusal wrote records"
        );
        assert_eq!(
            admission_index_rows(&rig),
            index_before,
            "{field} refusal wrote usage facts"
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

/// A head's creating send, over a route the daemon stamped with its owner's scope, starts the
/// head in the durable root of the new storage, not on today's storage: the real `ck-broca`,
/// supervised by the real `ck-subc`, creating plain sessions. Its first turn completes there.
/// After the daemon and broca restart over the same state, the owner's next send runs as the
/// head's second turn, and the head's history reads both turns back, with no log of the head
/// on today's storage at any point.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_head_runs_two_turns_across_a_restart_in_the_new_storage() {
    let model = ScopeModel::start().await;
    // Plain sessions are served without the deployment key, so the rig needs no vault.
    let storage = serde_json::json!({"session_storage": {"encryption": "off"}});
    let Some(mut first) =
        Rig::start_with_broca_config(None, None, Some(&model.url), Some(storage.clone())).await
    else {
        return;
    };
    let session = first.session("alfonso:head:agent_0123456789abcdef");
    // Where the head is kept: the new storage's root holding its head file, if any.
    let kept = |rig: &Rig| {
        broca_wal::session_storage_class(&rig.state_root, &broca_core::session_addr(&session))
            .unwrap()
    };
    let in_the_new_storage = Some(broca_wal::StorageClass::Durable);
    let turn = |prompt: &str, send_id: &str, class: Option<&str>| {
        let mut params = serde_json::json!({
            "prompt": prompt,
            "send_id": send_id,
            "model": {"provider": "mock", "model": "mock-model"},
        });
        if let Some(class) = class {
            params["session_class"] = class.into();
        }
        params
    };

    first.sync_scope().await;
    let owner = first.connect_as(OWNER, &first.owner_nonce).await;
    let route = open_scoped(&owner, &session).await.unwrap();
    let reply = scope_request(
        &owner,
        &route,
        "session.send",
        turn("Start the head.", "head-first", Some("head")),
    )
    .await
    .unwrap();
    assert_eq!(reply["result"]["state"], "active", "{reply}");
    let run = reply["result"]["run_id"].as_str().unwrap().to_owned();
    completed_run(&owner, &route, &run).await;
    assert_eq!(kept(&first), in_the_new_storage, "after the first turn");
    drop(owner);

    // The daemon stops, draining broca, and a new daemon starts broca over the same state.
    if let Some(pid) = first.child.id() {
        subc_os::Process::open(pid)
            .unwrap()
            .unwrap()
            .signal(subc_os::Signal::Terminate)
            .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(30), first.child.wait())
        .await
        .unwrap()
        .unwrap();
    let second = Rig::start_with_broca_config(
        None,
        Some(&first.state_root),
        Some(&model.url),
        Some(storage),
    )
    .await
    .unwrap();
    second.sync_scope().await;
    let owner = second.connect_as(OWNER, &second.owner_nonce).await;
    let route = open_scoped(&owner, &session).await.unwrap();
    let reply = scope_request(
        &owner,
        &route,
        "session.send",
        turn("Carry on.", "head-second", None),
    )
    .await
    .unwrap();
    assert_eq!(reply["result"]["state"], "active", "{reply}");
    let run = reply["result"]["run_id"].as_str().unwrap().to_owned();
    completed_run(&owner, &route, &run).await;
    let history = scope_request(&owner, &route, "session.history", serde_json::json!({}))
        .await
        .unwrap();
    let texts: Vec<(String, String)> = history["result"]["messages"]
        .as_array()
        .unwrap_or_else(|| panic!("a history page: {history}"))
        .iter()
        .map(|message| {
            (
                message["message"]["role"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
                message["message"]["content"][0]["text"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
            )
        })
        .collect();
    let said = |role: &str, text: &str| (role.to_owned(), text.to_owned());
    assert_eq!(
        texts,
        [
            said("user", "Start the head."),
            said("assistant", "scope answer"),
            said("user", "Carry on."),
            said("assistant", "scope answer"),
        ],
        "{history}"
    );
    assert_eq!(kept(&second), in_the_new_storage, "after the restart");
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
    let expected = std::fs::read(path).expect("active-step golden generated through serve");
    assert_eq!(usage_compat::legacy_bytes(&bytes, &expected), expected);
    let cleanup = basal
        .route_open_management("broca", &session)
        .await
        .unwrap();
    let cancelled = scope_request(
        &basal,
        &cleanup,
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
    let before = rig.debug_records(session).await;
    let body = owner_op(conn, route, method, params)
        .await
        .expect_err("non-owner is refused");
    let error: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(error["code"], "scope_owner_mismatch", "{error}");
    assert_eq!(
        rig.debug_records(session).await,
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
    .unwrap_err();
    let error: serde_json::Value = serde_json::from_slice(&response).unwrap();
    assert_eq!(error["code"], "scope_owner_mismatch", "{error}");
    let cleanup = owner
        .route_open_management("broca", &session)
        .await
        .unwrap();
    let response = owner_op(
        &owner,
        &cleanup,
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
    .unwrap_err();
    let error: serde_json::Value = serde_json::from_slice(&response).unwrap();
    assert_eq!(error["code"], "scope_owner_mismatch", "{error}");
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
    let sending = broca_test_support::wait_for(
        "live broca management route for unscoped owner",
        project_identity_open(&owner, &session, false),
        || broca_test_support::rig_process_logs(&rig.dir, "daemon"),
    )
    .await;
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
