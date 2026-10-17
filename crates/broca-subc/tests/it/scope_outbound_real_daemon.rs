//! The scope carrier's outbound half against the REAL subc daemon: broca's tool plane opening
//! provider routes under a run's frozen scope, and what happens to a call when the scope's
//! owner changes the scope under it.
//!
//! Only a real daemon shows what it stamps on the provider's bind, which refusal it answers an
//! open with, how a scope change closes the carrier's routes (a GOODBYE on each), and what
//! `scope.describe` says afterwards. When a scoped route closes, the plane reads the scope's
//! current state with `scope.describe` and resolves the call from that and from what the call
//! had already done:
//! - a call already sent has an unknown outcome, and the run stops;
//! - a call not yet sent is drained (never sent) when the scope or its parent is gone, or when a
//!   fresh route would carry a different agent; it is re-sent on a fresh route when the scope is
//!   live under the same epoch and agent (the call was composed for that session and that agent,
//!   so only a route carrying both may take it);
//! - with nothing in flight, a gone scope or parent stops the rest of the run, and a live scope
//!   is ordinary route loss.
//!
//! This test proves that against the daemon's real answers.
//!
//! `scope.describe` is answered only on a registered module connection. In production that is
//! broca's own serve connection; here the test registers a small module connection of its own
//! (DESCRIBER) and gives the plane a describer over it.
//!
//! The daemon supervises three copies of subconscious's `fake-aft-stub`:
//! - the scope OWNER, which syncs whatever scope set this test writes to its sync file;
//! - a CARRIER, whose launch nonce this test presents so the plane's connection is attested as
//!   `reserved:<carrier>`, a listed carrier of every scope here (broca's role in production);
//! - the tool provider `aft`, which records every bind (with the daemon's scope stamp) and
//!   every request it receives.
//!
//! A fourth, OWNER_UNSYNCED, is configured but never syncs, so the daemon describes its scopes as
//! not live with the owner not yet synced, for as long as the test cares to hold.
//!
//! Not covered here: a daemon without `scopes/v1`. This daemon advertises it; the refusal on a
//! daemon that does not is pinned against the plane's connection seam in `subc_plane`'s tests.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use broca_core::tool_plane::{BatchCtx, DispatchResult, ToolPlane, ToolPlaneError};
use broca_core::ToolManifest;
use broca_protocol::{
    request::{ExecutionMode, Tool},
    BatchId, BindIdentity, BindPrincipal, RunId, SessionScope, ToolCall, ToolCallId,
};
use broca_subc::{ModuleScopeDescriber, SubcConnection, SubcToolPlane};
use subc_control::ConsumerIdentity;
use tokio::process::Child;
use tokio::time::{sleep, Instant};

const OWNER: &str = "broca-b7-owner";
const OWNER_UNSYNCED: &str = "broca-b7-owner-unsynced";
const CARRIER: &str = "broca-b7-carrier";
const PROVIDER: &str = "aft";
const TOOL: &str = "fake_read";
/// Must equal `SCOPE_SYNC_CORR_BASE` in subconscious's fake-aft-stub: its `n`th scope.sync goes
/// out with corr `SCOPE_SYNC_CORR_BASE + n`, and its reply is recorded under that corr.
const SCOPE_SYNC_CORR_BASE: u64 = 1_000_000;

/// The unregistered, self-connecting module the test describes scopes over.
const DESCRIBER: &str = "broca-b7-describer";

/// The scope changes driven below, each as its own owner sync.
const CHANGES: [&str; 4] = [
    "delegation_narrowed",
    "agent_rebound",
    "scope_ended",
    "scope_parent_ended",
];

fn sibling_bin(name: &str, env_key: &str) -> PathBuf {
    broca_test_support::subc_bin(name, env_key)
}

/// `scripts/gate.sh` sets `BROCA_REQUIRE_SUBC`, so in a gated run a missing daemon fails this
/// test naming the missing binary; a local run without it skips and prints that it skipped.
fn require_subc() -> bool {
    std::env::var_os("BROCA_REQUIRE_SUBC").is_some()
        || std::env::var("CI").map(|v| v == "true").unwrap_or(false)
}

struct Rig {
    child: Child,
    dir: PathBuf,
    connection_file: PathBuf,
    sync_path: PathBuf,
    owner_events: PathBuf,
    provider_events: PathBuf,
    carrier_nonce: PathBuf,
    syncs: u64,
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
        let stub = sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
        for bin in [&subc_core, &stub] {
            if !bin.exists() {
                assert!(
                    !require_subc(),
                    "scope_outbound_real_daemon: {bin:?} is missing, but this run is REQUIRED \
                     (build it: cd ../subconscious && cargo build -p subc-core --bins)"
                );
                println!(
                    "SKIP scope_outbound_real_daemon: {bin:?} is missing. This reports `ok` -- \
                     set BROCA_REQUIRE_SUBC=1 to make it a failure instead."
                );
                return None;
            }
        }
        let subc_core =
            broca_test_support::ckdev_binary(&subc_core, broca_test_support::scratch_root());
        // The test process may itself run under a subc daemon; its inherited attestation would
        // be presented to this rig's daemon, which does not know that nonce.
        std::env::remove_var(subc_protocol::SUBC_MODULE_ID_ENV);
        std::env::remove_var(subc_protocol::SUBC_LAUNCH_NONCE_ENV);

        // One state root per rig: the tests of this file run in parallel in one process.
        static NEXT_RIG: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-b7-scope-{}-{}",
            std::process::id(),
            NEXT_RIG.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        broca_test_support::remove_rig_dir(&dir);
        let config_home = dir.join("config");
        let runtime_dir = dir.join("runtime");
        let data_home = runtime_dir.join("data-home");
        let config_path = config_home.join("cortexkit").join("subc.jsonc");
        std::fs::create_dir_all(config_path.parent().unwrap()).unwrap();
        std::fs::create_dir_all(&runtime_dir).unwrap();

        let sync_path = dir.join("owner-scopes.json");
        let owner_events = dir.join("owner-events.jsonl");
        let provider_events = dir.join("provider-events.jsonl");
        let carrier_nonce = dir.join("carrier.nonce");
        let stub_module = |module_id: &str, env: serde_json::Value| {
            let mut env = env;
            env["FAKE_AFT_MODULE_ID"] = module_id.into();
            serde_json::json!({
                "program": stub,
                "args": [],
                "env": env,
                "enabled": true,
                // Scope owners and carriers are named as reserved principals.
                "reserved": true,
            })
        };
        let config = serde_json::json!({
            "version": 1,
            // Lets the owner set `agent_id` and `delegates`: the test changes them to make the
            // daemon close the routes under a scope whose delegation or agent changed.
            "scope_authority_owners": [OWNER],
            "modules": {
                OWNER: stub_module(OWNER, serde_json::json!({
                    "FAKE_AFT_SCOPE_SYNC_PATH": sync_path,
                    "FAKE_AFT_EVENTS_PATH": owner_events,
                })),
                OWNER_UNSYNCED: stub_module(OWNER_UNSYNCED, serde_json::json!({})),
                CARRIER: stub_module(CARRIER, serde_json::json!({
                    "FAKE_AFT_LAUNCH_NONCE_PATH": carrier_nonce,
                })),
                PROVIDER: stub_module(PROVIDER, serde_json::json!({
                    "FAKE_AFT_EVENTS_PATH": provider_events,
                    "FAKE_AFT_TOOLS": TOOL,
                })),
            }
        });
        std::fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();

        let connection_file = runtime_dir.join("subc-connection.json");
        let child = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(&subc_core, &dir, "daemon")?
                .env("XDG_CONFIG_HOME", &config_home)
                .env("XDG_RUNTIME_DIR", &runtime_dir)
                .env("XDG_DATA_HOME", &data_home)
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
            sync_path,
            owner_events,
            provider_events,
            carrier_nonce,
            syncs: 0,
        };
        wait_until("the daemon's connection file", || {
            rig.connection_file.exists().then_some(())
        })
        .await;
        Some(rig)
    }

    /// Have the owner sync exactly `records` (its whole set) and wait for the daemon to accept
    /// every one of them.
    async fn sync(&mut self, records: &[serde_json::Value]) {
        let staging = self.sync_path.with_extension("staging");
        std::fs::write(&staging, serde_json::to_vec(records).unwrap()).unwrap();
        std::fs::rename(&staging, &self.sync_path).unwrap();
        self.syncs += 1;
        let corr = SCOPE_SYNC_CORR_BASE + self.syncs;
        let reply = wait_until("the owner's scope.sync reply", || {
            events(&self.owner_events).into_iter().find(|event| {
                event["corr"] == corr
                    && (event["kind"] == "scope_sync_response" || event["kind"] == "error")
            })
        })
        .await;
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
    }

    /// The tool plane broca runs, on a connection attested as the carrier.
    async fn carrier_plane(&self, hold: Duration) -> SubcToolPlane {
        let nonce = wait_until("the carrier's launch nonce", || {
            std::fs::read_to_string(&self.carrier_nonce)
                .ok()
                .filter(|text| !text.trim().is_empty())
        })
        .await;
        let identity = ConsumerIdentity {
            module_id: CARRIER.into(),
            launch_nonce: nonce.trim().to_owned(),
        };
        let conn =
            SubcConnection::connect_with_consumer_identity(&self.connection_file, Some(identity))
                .await
                .expect("connect as the carrier");
        let plane = SubcToolPlane::new(conn, PROVIDER).with_scope_sync_hold_for_test(hold);
        plane.set_scope_describer(self.describer().await);
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let registered = plane.catalog().await.ok().is_some_and(|catalog| {
                catalog
                    .iter()
                    .any(|entry| entry.module_id == PROVIDER && entry.ready)
            });
            if registered {
                return plane;
            }
            assert!(Instant::now() < deadline, "the provider never registered");
            sleep(Duration::from_millis(50)).await;
        }
    }

    /// Register DESCRIBER on a module connection of its own and return a describer over it. The
    /// connection answers the daemon's pings and hands every channel-0 reply to the describer.
    async fn describer(&self) -> Arc<ModuleScopeDescriber> {
        let info = subc_transport::connection_file::read(&self.connection_file)
            .expect("the daemon's connection file");
        let endpoint = info.endpoints.first().expect("an endpoint");
        let mut stream =
            tokio::net::TcpStream::connect(format!("{}:{}", endpoint.host, endpoint.port))
                .await
                .expect("connect the describer module");
        subc_transport::authenticate_client(&mut stream, &info, Duration::from_secs(10))
            .await
            .expect("authenticate the describer module");
        let (mut reader, mut writer) = stream.into_split();
        let hello = subc_protocol::ModuleHelloBody {
            manifest: subc_protocol::manifest::ModuleManifest::builder(DESCRIBER, "0.0.0").build(),
            protocol_ver: subc_protocol::PROTOCOL_VERSION,
            control_ops: None,
            launch_nonce: None,
        };
        let flags = subc_protocol::Flags::new(false, subc_protocol::Priority::Passive, false);
        let hello = subc_protocol::Frame::build(
            subc_protocol::FrameType::Hello,
            flags,
            0,
            0,
            1,
            serde_json::to_vec(&hello).unwrap(),
        )
        .unwrap();
        subc_transport::write_frame(&mut writer, &hello)
            .await
            .unwrap();
        let ack = subc_transport::read_frame(&mut reader)
            .await
            .unwrap()
            .expect("a HELLO reply");
        assert_eq!(
            ack.header.ty,
            subc_protocol::FrameType::HelloAck,
            "the describer registered: {}",
            String::from_utf8_lossy(&ack.body)
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel::<subc_protocol::Frame>(64);
        tokio::spawn(async move {
            while let Some(frame) = rx.recv().await {
                if subc_transport::write_frame(&mut writer, &frame)
                    .await
                    .is_err()
                {
                    return;
                }
            }
        });
        let describer = Arc::new(ModuleScopeDescriber::new());
        describer.attach(tx.clone());
        let routed = Arc::clone(&describer);
        tokio::spawn(async move {
            while let Ok(Some(frame)) = subc_transport::read_frame(&mut reader).await {
                if routed.on_frame(&frame) {
                    continue;
                }
                if frame.header.ty == subc_protocol::FrameType::Ping && frame.header.channel == 0 {
                    let pong = subc_protocol::Frame::build_with_version(
                        frame.header.ver,
                        subc_protocol::FrameType::Pong,
                        frame.header.flags,
                        0,
                        0,
                        frame.header.corr,
                        Vec::new(),
                    )
                    .unwrap();
                    if tx.send(pong).await.is_err() {
                        return;
                    }
                }
            }
        });
        describer
    }

    fn session(&self, name: &str) -> BindIdentity {
        BindIdentity::new(&self.dir, "b7-scope", name)
    }

    /// The provider's binds for `session`, each with the scope stamp the daemon put on it.
    fn binds_for(&self, session: &str) -> Vec<serde_json::Value> {
        events(&self.provider_events)
            .into_iter()
            .filter(|event| event["kind"] == "attach" && event["identity"]["session"] == session)
            .map(|event| event["scope"].clone())
            .collect()
    }

    /// Whether the provider received a request whose arguments carry `marker`.
    fn received(&self, marker: &str) -> bool {
        events(&self.provider_events).into_iter().any(|event| {
            event["kind"] == "request_received"
                && event["body_json"]["arguments"]["marker"] == marker
        })
    }
}

fn events(path: &Path) -> Vec<serde_json::Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}

async fn wait_until<T>(what: &str, mut probe: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if let Some(found) = probe() {
            return found;
        }
        assert!(Instant::now() < deadline, "{what} never appeared");
        sleep(Duration::from_millis(25)).await;
    }
}

fn scope(owner: &str, scope_ref: &str) -> SessionScope {
    SessionScope {
        owner: BindPrincipal::Reserved {
            module_id: owner.into(),
        },
        scope_ref: scope_ref.into(),
        scope_epoch: 1,
        flow_id: None,
        sender: None,
        agent_id: None,
        run_id: None,
    }
}

fn carriers() -> serde_json::Value {
    serde_json::json!([{ "principal": { "kind": "reserved", "module_id": CARRIER } }])
}

/// A head scope under the owner at epoch 1, listing the carrier.
fn head(scope_ref: &str) -> serde_json::Value {
    serde_json::json!({
        "ref": scope_ref,
        "scope_epoch": 1,
        "kind": "head",
        "carriers": carriers(),
    })
}

fn ctx(bind: &BindIdentity, batch: &str, run: &str, scope: &SessionScope) -> BatchCtx {
    BatchCtx {
        batch_id: BatchId(batch.into()),
        bind: bind.clone(),
        run_id: RunId(run.into()),
        scope: Some(scope.clone()),
    }
}

fn manifest() -> ToolManifest {
    ToolManifest::from_tools(vec![Tool {
        name: TOOL.into(),
        description: String::new(),
        input_schema: serde_json::json!({"type": "object"}),
        module: Some(PROVIDER.into()),
        execution_mode: ExecutionMode::Parallel,
    }])
}

fn call(marker: &str, delay_ms: u64) -> ToolCall {
    ToolCall {
        thought_signature: None,
        tool_call_id: ToolCallId(format!("call-{marker}")),
        tool_name: TOOL.into(),
        input: serde_json::json!({ "marker": marker, "delay_ms": delay_ms }),
    }
}

/// The scope set for one change: the scopes the three cases run under, as the owner syncs
/// them before the change (`before`) and after it (`after`).
struct ChangeScopes {
    before: Vec<serde_json::Value>,
    after: Vec<serde_json::Value>,
    /// The refs the three cases run under: not sent, sent, nothing in flight.
    refs: [String; 3],
}

fn change_scopes(change: &str) -> ChangeScopes {
    let refs = ["unsent", "sent", "idle"].map(|case| format!("{change}-{case}"));
    let with_attributes = |r: &String, attributes: serde_json::Value| {
        let mut record = head(r);
        record["attributes"] = attributes;
        record
    };
    let (before, after) = match change {
        "delegation_narrowed" => (
            refs.iter()
                .map(|r| {
                    with_attributes(
                        r,
                        serde_json::json!({ "agent_id": "agent-b7", "delegates": true }),
                    )
                })
                .collect(),
            refs.iter()
                .map(|r| with_attributes(r, serde_json::json!({ "agent_id": "agent-b7" })))
                .collect(),
        ),
        "agent_rebound" => (
            refs.iter()
                .map(|r| with_attributes(r, serde_json::json!({ "agent_id": "agent-b7" })))
                .collect(),
            refs.iter()
                .map(|r| with_attributes(r, serde_json::json!({ "agent_id": "agent-b8" })))
                .collect(),
        ),
        "scope_ended" => (refs.iter().map(|r| head(r)).collect(), Vec::new()),
        "scope_parent_ended" => {
            let parent = format!("{change}-parent");
            let child = |r: &String| {
                let mut record = head(r);
                record["kind"] = "worker".into();
                record["parent"] = serde_json::json!({
                    "owner": { "kind": "reserved", "module_id": OWNER },
                    "ref": parent,
                    "scope_epoch": 1,
                });
                record
            };
            let mut before = vec![head(&parent)];
            before.extend(refs.iter().map(child));
            (before, refs.iter().map(child).collect())
        }
        other => unreachable!("not a change this test drives: {other}"),
    };
    ChangeScopes {
        before,
        after,
        refs,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn broca_carries_the_frozen_scope_and_resolves_each_close_by_the_scopes_current_state() {
    let Some(mut rig) = Rig::start().await else {
        return;
    };
    rig.sync(&[head("head-a"), head("head-b")]).await;
    let plane = Arc::new(rig.carrier_plane(Duration::from_secs(2)).await);

    // --- Each run's provider route opens under that run's scope, not another run's. ---
    let head_a = scope(OWNER, "head-a");
    let head_b = scope(OWNER, "head-b");
    let (bind_a, bind_b) = (rig.session("session-a"), rig.session("session-b"));
    for (bind, scope, marker) in [(&bind_a, &head_a, "a"), (&bind_b, &head_b, "b")] {
        let ctx = ctx(
            bind,
            &format!("{marker}-b1"),
            &format!("run-{marker}"),
            scope,
        );
        let read = call(marker, 0);
        plane
            .batch_started(&ctx, std::slice::from_ref(&read), &manifest())
            .await
            .expect("a scoped batch starts");
        match plane.dispatch(&ctx, &read).await {
            DispatchResult::Completed(result) => assert!(!result.is_error, "{result:?}"),
            other => panic!("the scoped call completes: {other:?}"),
        }
    }
    for (session, expected_ref) in [("session-a", "head-a"), ("session-b", "head-b")] {
        let stamps = rig.binds_for(session);
        assert_eq!(stamps.len(), 1, "{session}: {stamps:?}");
        assert_eq!(stamps[0]["ref"], expected_ref, "{session}: {stamps:?}");
        assert_eq!(stamps[0]["scope_epoch"], 1, "{session}: {stamps:?}");
        assert_eq!(
            stamps[0]["owner"]["module_id"], OWNER,
            "{session}: {stamps:?}"
        );
    }
    assert_eq!(
        plane.cached_route_channel_for_scope(&bind_b, Some(&head_a), PROVIDER),
        None,
        "session B has no route under session A's scope"
    );

    // --- Each change, with a call not yet sent, a call sent, and nothing in flight. ---
    for change in CHANGES {
        let scopes = change_scopes(change);
        let mut before = vec![head("head-a"), head("head-b")];
        before.extend(scopes.before.iter().cloned());
        rig.sync(&before).await;
        let [unsent_ref, sent_ref, idle_ref] = &scopes.refs;
        let case = |r: &String| {
            let bind = rig.session(r);
            let run = format!("run-{r}");
            ctx(&bind, "b1", &run, &scope(OWNER, r))
        };

        // Not sent: the batch has started (its route open, its intent durable), the call not.
        let unsent_ctx = case(unsent_ref);
        let unsent_call = call(unsent_ref, 0);
        plane
            .batch_started(&unsent_ctx, std::slice::from_ref(&unsent_call), &manifest())
            .await
            .unwrap_or_else(|error| panic!("{change} unsent: {error}"));

        // Sent: a slow call the provider has received and not answered.
        let sent_ctx = case(sent_ref);
        let slow = call(sent_ref, 20_000);
        plane
            .batch_started(&sent_ctx, std::slice::from_ref(&slow), &manifest())
            .await
            .unwrap_or_else(|error| panic!("{change} sent: {error}"));
        let in_flight = {
            let plane = Arc::clone(&plane);
            let ctx = sent_ctx.clone();
            tokio::spawn(async move { plane.dispatch(&ctx, &slow).await })
        };
        wait_until("the provider receiving the slow call", || {
            rig.received(sent_ref).then_some(())
        })
        .await;

        // Nothing in flight: a batch ran to completion; its route stays cached for the next.
        let idle_ctx = case(idle_ref);
        let fast = call(idle_ref, 0);
        plane
            .batch_started(&idle_ctx, std::slice::from_ref(&fast), &manifest())
            .await
            .unwrap_or_else(|error| panic!("{change} idle: {error}"));
        assert!(matches!(
            plane.dispatch(&idle_ctx, &fast).await,
            DispatchResult::Completed(_)
        ));
        plane.batch_finished(&idle_ctx).await.unwrap();

        // The owner makes the change; the daemon closes the routes under it.
        let mut after = vec![head("head-a"), head("head-b")];
        after.extend(scopes.after.iter().cloned());
        rig.sync(&after).await;
        for ctx in [&unsent_ctx, &idle_ctx] {
            wait_until(&format!("{change}: the route closing"), || {
                (plane.test_cached_route_is_open(&ctx.bind, ctx.scope.as_ref(), PROVIDER)
                    == Some(false))
                .then_some(())
            })
            .await;
        }

        // Not sent.
        let unsent = plane.dispatch(&unsent_ctx, &unsent_call).await;
        match change {
            "delegation_narrowed" => {
                // Same epoch, same agent: sent once more, on a fresh route whose stamp is the
                // current one, with delegation turned off.
                assert!(
                    matches!(&unsent, DispatchResult::Completed(result) if !result.is_error),
                    "a narrowing re-sends the unsent call: {unsent:?}"
                );
                wait_until("the provider receiving the re-sent call", || {
                    rig.received(unsent_ref).then_some(())
                })
                .await;
                let stamps = rig.binds_for(unsent_ref);
                assert_eq!(stamps.len(), 2, "re-sent on a fresh route: {stamps:?}");
                assert_eq!(stamps[1]["attributes"]["agent_id"], "agent-b7");
                assert!(
                    stamps[1]["attributes"].get("delegates").is_none(),
                    "the fresh route carries the narrowed stamp: {stamps:?}"
                );
            }
            _ => {
                let expected = match change {
                    "agent_rebound" => "scope_agent_changed",
                    other => other,
                };
                match &unsent {
                    DispatchResult::ScopeDrained {
                        reason,
                        sent: false,
                    } => assert_eq!(reason, expected, "{change}"),
                    other => panic!("{change}: the unsent call must be drained, got {other:?}"),
                }
                sleep(Duration::from_millis(200)).await;
                assert!(
                    !rig.received(unsent_ref),
                    "{change}: the drained call was dispatched anyway"
                );
            }
        }

        // Sent: the outcome is unknown whatever the change.
        match (change, in_flight.await.unwrap()) {
            ("delegation_narrowed", DispatchResult::SentButOutcomeUnknown { .. }) => {}
            ("agent_rebound", DispatchResult::ScopeDrained { reason, sent: true }) => {
                assert_eq!(reason, "scope_agent_changed")
            }
            (gone, DispatchResult::ScopeDrained { reason, sent: true })
                if gone == "scope_ended" || gone == "scope_parent_ended" =>
            {
                assert_eq!(reason, gone)
            }
            (change, other) => {
                panic!("{change}: a sent call must be outcome_unknown, got {other:?}")
            }
        }

        // Nothing in flight: the run's next batch either stops (scope or parent gone) or
        // re-opens its route (scope still live).
        let next = BatchCtx {
            batch_id: BatchId("b2".into()),
            ..idle_ctx.clone()
        };
        let started = plane
            .batch_started(&next, &[call("after-change", 0)], &manifest())
            .await;
        match change {
            "scope_ended" | "scope_parent_ended" => {
                match started {
                    Err(ToolPlaneError::ScopeDrained { reason }) => assert_eq!(reason, change),
                    other => panic!("{change}: the rest of the run must stop, got {other:?}"),
                }
                assert_eq!(
                    rig.binds_for(idle_ref).len(),
                    1,
                    "{change}: the stopped run opened nothing more"
                );
            }
            _ => {
                // The scope is live: ordinary route loss, and the next batch re-opens.
                started.unwrap_or_else(|error| panic!("{change}: {error}"));
                assert_eq!(rig.binds_for(idle_ref).len(), 2, "{change}: re-opened once");
            }
        }
    }

    // --- An unsynced owner held past the bound: this owner never syncs, so the daemon keeps
    // describing its scopes as not live with the owner not yet synced. The plane waits for the
    // sync up to its (here shortened) scope hold, then refuses the call `scope_unverifiable`
    // with nothing opened. ---
    let unsynced = scope(OWNER_UNSYNCED, "never-synced");
    let held_ctx = ctx(&rig.session("held"), "b1", "run-held", &unsynced);
    let started = Instant::now();
    let refused = plane
        .batch_started(&held_ctx, &[call("held", 0)], &manifest())
        .await
        .expect_err("the hold runs out");
    assert!(
        matches!(&refused, ToolPlaneError::ScopeRefused { code, .. } if code == "scope_unverifiable"),
        "{refused:?}"
    );
    assert!(
        started.elapsed() >= Duration::from_secs(2),
        "the call was held"
    );
    assert!(rig.binds_for("held").is_empty(), "nothing was opened");
    assert!(!rig.received("held"), "nothing was dispatched");

    // --- A ref missing from the scope set its owner synced is gone: the call is refused
    // `scope_not_live` at once, with no wait. ---
    let not_live = scope(OWNER, "not-in-the-owners-set");
    let not_live_ctx = ctx(&rig.session("not-live"), "b1", "run-not-live", &not_live);
    let started = Instant::now();
    let refused = plane
        .batch_started(&not_live_ctx, &[call("not-live", 0)], &manifest())
        .await
        .expect_err("the scope is not live");
    assert!(
        matches!(&refused, ToolPlaneError::ScopeRefused { code, .. } if code == "scope_not_live"),
        "{refused:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "a scope that is gone is never held"
    );
    assert!(rig.binds_for("not-live").is_empty(), "nothing was opened");
}

/// A session whose scope its owner reopened at a later epoch runs its next episode under the
/// new epoch, and that episode's first tool call opens a fresh route stamped with it: the route
/// cached for the ended epoch is never reused. The cache keys a route by session, scope and
/// module, and the scope includes its epoch.
#[tokio::test(flavor = "multi_thread")]
async fn the_first_call_after_a_scope_reopens_opens_a_fresh_route_under_the_new_epoch() {
    let Some(mut rig) = Rig::start().await else {
        return;
    };
    rig.sync(&[head("reopened")]).await;
    let plane = rig.carrier_plane(Duration::from_secs(2)).await;
    let bind = rig.session("reopened");

    let ended = scope(OWNER, "reopened");
    let first = ctx(&bind, "b1", "run-epoch-1", &ended);
    let read = call("epoch-1", 0);
    plane
        .batch_started(&first, std::slice::from_ref(&read), &manifest())
        .await
        .expect("the epoch-1 batch starts");
    assert!(matches!(
        plane.dispatch(&first, &read).await,
        DispatchResult::Completed(result) if !result.is_error
    ));
    plane.batch_finished(&first).await.unwrap();
    assert!(
        plane
            .cached_route_channel_for_scope(&bind, Some(&ended), PROVIDER)
            .is_some(),
        "the epoch-1 route stays cached after its batch"
    );

    // The owner reopens the ref at the next epoch, which ends epoch 1.
    let mut next_epoch = head("reopened");
    next_epoch["scope_epoch"] = 2.into();
    rig.sync(&[next_epoch]).await;
    let reopened = SessionScope {
        scope_epoch: 2,
        ..ended.clone()
    };
    assert_eq!(
        plane.cached_route_channel_for_scope(&bind, Some(&reopened), PROVIDER),
        None,
        "nothing is cached under the new epoch before its first call"
    );

    let next = ctx(&bind, "b1", "run-epoch-2", &reopened);
    let read = call("epoch-2", 0);
    plane
        .batch_started(&next, std::slice::from_ref(&read), &manifest())
        .await
        .expect("the epoch-2 batch starts");
    match plane.dispatch(&next, &read).await {
        DispatchResult::Completed(result) => assert!(!result.is_error, "{result:?}"),
        other => panic!("the epoch-2 call completes: {other:?}"),
    }
    let stamps = rig.binds_for("reopened");
    assert_eq!(
        stamps.len(),
        2,
        "a fresh route for the new epoch: {stamps:?}"
    );
    assert_eq!(stamps[0]["scope_epoch"], 1, "{stamps:?}");
    assert_eq!(stamps[1]["scope_epoch"], 2, "{stamps:?}");
    assert_eq!(stamps[1]["ref"], "reopened", "{stamps:?}");
    assert!(rig.received("epoch-2"));
    assert!(
        plane
            .cached_route_channel_for_scope(&bind, Some(&reopened), PROVIDER)
            .is_some(),
        "the new epoch's route is the one cached for it"
    );
}
