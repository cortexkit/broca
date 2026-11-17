//! Run the shared `llm-runner/v1` contract tests against real `ck-broca` daemon routes.
//! The suite comes from the pinned cortexkit/commons repository.
//!
//! Each temporary directory owns a private `ck-subc` daemon, the supervised runner's
//! WAL and index, and a stand-in module that supplies a second caller identity. The
//! HTTP model and SDK tool/compaction providers stay in this test process, so their
//! observations survive killing and restarting the runner.
//!
//! Compaction model requests carry a unique session-specific model id and use the
//! number of emitted answers to choose their scripted turn. Legacy requests identify
//! their session by a system marker and count assistant messages already in the input.
//!
//! Before SIGKILL, the harness verifies the WAL records required by the named crash
//! checkpoint. CompactionApplied and FoldRecorded pause inside the runner before HTTP
//! can receive the corresponding model input. Older checkpoints hold the subsequent
//! model or tool operation. StepRecorded additionally truncates away dispatch intents
//! and honestly reports LogTruncation instead of claiming a pure process-signal cut.
//!
//! `SUBC_CORE_BIN` and `FAKE_AFT_STUB_BIN` name the staged daemon/stand-in fixtures.
//! `BROCA_REQUIRE_SUBC=1` makes missing fixtures fail rather than silently pass a skip.
#![cfg(unix)]

mod delivery_rig;
mod step_transform_rig;
#[path = "common/stored_log.rs"]
mod stored_log;

use broca_session_store::record::Record;
use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_subc::{RouteHandle, SubcConnection};
use cortexkit_role_harness::{
    Harness, HarnessError, KillMechanism, KillPoint, KillReport, PointDeclaration, RouteStamp,
    Trigger,
};
use cortexkit_role_llm_runner::points;
use cortexkit_role_llm_runner_conformance::{
    run_suite, Capability, CaseOutcome, LlmRunnerSubject, Reply, RouteFailure, RunnerRoute, Script,
    ScriptedLateEntry, ScriptedPart, ScriptedUsage, SubscribeOutcome, SCRIPTED_TOOL,
};
use serde_json::{json, Map, Value};
use subc_control::ConsumerIdentity;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Notify;
use tokio::time::{sleep, Instant};

/// The scripted tool provider's module id: what broca records as each call's `dispatched_to`.
const TOOL_MODULE: &str = "conformance-tools";
/// The stand-in module whose launch nonce gives the suite a second caller identity.
const OWNER_MODULE: &str = "conformance-owner";
const SENDER_MODULE: &str = "conformance-sender";
const HEAD_SENDER_MODULE: &str = "conformance-head-sender";
const STRANGER_MODULE: &str = "conformance-stranger";
const PLAIN_SCOPE_REF: &str = "conformance-plain";
const FLOW_SCOPE_REF: &str = "conformance-flow";
// The supervised stub's scope.sync correlations start at 1_000_001.
const FIRST_SCOPE_SYNC_CORR: u64 = 1_000_001;

fn scope_fixture_records() -> Value {
    // The plain-scope plan fetches the scripted catalog through broca's attested route.
    // Grant that exact provider target; the tool-free flow fixture needs no extra carrier.
    json!([
        {"ref":PLAIN_SCOPE_REF,"scope_epoch":1,"kind":"head","attributes":{},
         "carriers":[
             {"principal":{"kind":"reserved","module_id":SENDER_MODULE},"targets":["broca"]},
             {"principal":{"kind":"reserved","module_id":"broca"},"targets":[TOOL_MODULE]}
         ]},
        {"ref":FLOW_SCOPE_REF,"scope_epoch":1,"kind":"head",
         "attributes":{"flow_id":"conformance-flow","agent_id":"conformance-agent"},
         "carriers":[{"principal":{"kind":"reserved","module_id":SENDER_MODULE},"targets":["broca"]}]}
    ])
}

fn scope_fixture_stamps(reference: &str) -> (RouteStamp, RouteStamp) {
    let scope = cortexkit_role_harness::ScopeStamp {
        owner: format!("reserved:{OWNER_MODULE}"),
        scope_ref: reference.into(),
        scope_epoch: 1,
    };
    (
        RouteStamp {
            principal: format!("reserved:{SENDER_MODULE}"),
            scope: Some(scope.clone()),
        },
        RouteStamp {
            principal: scope.owner.clone(),
            scope: Some(scope),
        },
    )
}

/// Publish both records through the supervised owner's real scope.sync, then require
/// its correlated daemon acknowledgement before opening any scope fixture route.
async fn publish_scope_fixtures(path: &Path, events: &Path) -> Result<(), HarnessError> {
    let staging = path.with_extension("staging");
    std::fs::write(
        &staging,
        serde_json::to_vec(&scope_fixture_records()).unwrap(),
    )
    .and_then(|_| std::fs::rename(&staging, path))
    .map_err(|error| HarnessError::new(format!("publishing scope fixtures: {error}")))?;
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let reply = std::fs::read_to_string(events)
            .unwrap_or_default()
            .lines()
            .filter_map(|line| serde_json::from_str::<Value>(line).ok())
            .find(|event| {
                event["corr"] == FIRST_SCOPE_SYNC_CORR
                    && (event["kind"] == "scope_sync_response" || event["kind"] == "error")
            });
        if let Some(reply) = reply {
            let accepted = reply["kind"] == "scope_sync_response"
                && reply["body_json"]["results"]
                    .as_array()
                    .is_some_and(|results| {
                        results.len() == 2
                            && results.iter().all(|result| {
                                result["outcome"]
                                    .as_str()
                                    .is_some_and(|outcome| outcome != "refused")
                            })
                    });
            return if accepted {
                Ok(())
            } else {
                Err(HarnessError::new(format!(
                    "scope fixture sync refused: {reply}"
                )))
            };
        }
        if Instant::now() >= deadline {
            return Err(HarnessError::new(
                "the scope fixture owner's initial sync never answered",
            ));
        }
        sleep(Duration::from_millis(25)).await;
    }
}

fn caller_module_nonce<'a>(
    principal: &'a str,
    owner_nonce: &Path,
) -> Result<Option<(&'a str, PathBuf)>, HarnessError> {
    if principal == "direct" {
        return Ok(None);
    }
    let module = principal
        .strip_prefix("reserved:")
        .ok_or_else(|| HarnessError::new("expected a reserved module principal"))?;
    let nonce = match module {
        OWNER_MODULE => owner_nonce.to_path_buf(),
        SENDER_MODULE => owner_nonce.with_file_name("sender.nonce"),
        HEAD_SENDER_MODULE => owner_nonce.with_file_name("head-sender.nonce"),
        STRANGER_MODULE => owner_nonce.with_file_name("stranger.nonce"),
        "magic-context" => owner_nonce.with_file_name("reader.nonce"),
        _ => return Err(HarnessError::new("unknown conformance module principal")),
    };
    Ok(Some((module, nonce)))
}

fn reset_caller_nonce_files(owner_nonce: &Path) -> Result<(), HarnessError> {
    // Every stand-in is supervised and receives a new launch nonce after restart. A file
    // left by the previous process must not satisfy the next caller's readiness check.
    for module in [
        OWNER_MODULE,
        SENDER_MODULE,
        HEAD_SENDER_MODULE,
        STRANGER_MODULE,
        "magic-context",
    ] {
        let principal = format!("reserved:{module}");
        let (_, path) = caller_module_nonce(&principal, owner_nonce)?.expect("known caller module");
        match std::fs::remove_file(path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(HarnessError::new(format!(
                    "resetting {module} caller nonce before launch: {error}"
                )))
            }
        }
    }
    Ok(())
}

#[test]
fn caller_nonce_reset_discards_every_previous_supervised_launch() {
    let root = broca_test_support::scratch_root()
        .join(format!("caller-nonce-reset-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&root);
    std::fs::create_dir_all(&root).unwrap();
    let owner_nonce = root.join("owner.nonce");
    let mut paths = Vec::new();
    for module in [
        OWNER_MODULE,
        SENDER_MODULE,
        HEAD_SENDER_MODULE,
        STRANGER_MODULE,
        "magic-context",
    ] {
        let (_, path) = caller_module_nonce(&format!("reserved:{module}"), &owner_nonce)
            .unwrap()
            .unwrap();
        std::fs::write(&path, "previous-supervised-launch").unwrap();
        paths.push(path);
    }
    std::fs::write(root.join("unrelated.nonce"), "keep").unwrap();
    reset_caller_nonce_files(&owner_nonce).unwrap();
    for path in paths {
        assert!(
            !path.exists(),
            "a restarted rig could read an old caller identity from {}",
            path.display()
        );
    }
    assert_eq!(
        std::fs::read(root.join("unrelated.nonce")).unwrap(),
        b"keep"
    );
    reset_caller_nonce_files(&owner_nonce).unwrap();
    broca_test_support::remove_rig_dir(&root);
}

#[test]
fn caller_nonce_reset_fails_before_launch_if_a_publication_cannot_be_removed() {
    let root = broca_test_support::scratch_root()
        .join(format!("caller-nonce-reset-error-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&root);
    std::fs::create_dir_all(root.join("sender.nonce")).unwrap();
    let error = reset_caller_nonce_files(&root.join("owner.nonce")).unwrap_err();
    assert!(error.to_string().contains(SENDER_MODULE), "{error}");
    broca_test_support::remove_rig_dir(&root);
}
const PLAN_MODEL_PREFIX: &str = "conformance-plan-";
/// The system-prompt marker naming the session a model request belongs to.
const SESSION_MARKER: &str = "conformance-session:";

/// Only the kit's dedicated history/display families omit the extra stored system entry.
/// Match complete minted names, not arbitrary sessions whose labels contain those words.
fn history_display_kit_session(session: &str) -> bool {
    let counter = |value: &str| {
        value
            .parse::<u64>()
            .is_ok_and(|number| number > 0 && number.to_string() == value)
    };
    if let Some(tail) = session.strip_prefix("cf-cf-history-") {
        return tail
            .split_once('-')
            .is_some_and(|(label, session)| counter(label) && counter(session));
    }
    ["cf-display-", "cf-display-sender-", "cf-display-model-"]
        .iter()
        .any(|prefix| session.strip_prefix(*prefix).is_some_and(counter))
}

fn message_marks_kit_session(session: &str) -> bool {
    ["cf-message-marks-", "cf-marks-retry-"]
        .iter()
        .any(|prefix| {
            session.strip_prefix(*prefix).is_some_and(|counter| {
                counter
                    .parse::<u64>()
                    .is_ok_and(|number| number > 0 && number.to_string() == counter)
            })
        })
}

/// A session the kit's media_refs_* and media_descriptors_* cases open: `cf-media-<counter>`.
fn media_fixture_session(session: &str) -> bool {
    session.strip_prefix("cf-media-").is_some_and(|counter| {
        counter
            .parse::<u64>()
            .is_ok_and(|number| number > 0 && number.to_string() == counter)
    })
}

fn message_marks_route_stamp(session: &str, stamp: &RouteStamp) -> RouteStamp {
    if message_marks_kit_session(session)
        && stamp.scope.is_none()
        && stamp.principal == format!("reserved:{OWNER_MODULE}")
    {
        scope_fixture_stamps(PLAIN_SCOPE_REF).1
    } else {
        stamp.clone()
    }
}

/// Refresh fixtures start with a plan so later sends omit legacy `tools` and `system` fields.
/// Use the same model id for every send: switching models rebuilds the prompt prefix and would
/// apply an `on_prefix_rebuild` refresh before the fixture's intended trigger.
fn session_change_kit_session(session: &str) -> bool {
    [
        "cf-change-next-run-",
        "cf-change-rung-",
        "cf-change-held-",
        "cf-change-flush-",
        "cf-change-repeat-",
        "cf-change-stale-",
        "cf-change-host-set-",
    ]
    .iter()
    .any(|prefix| {
        session.strip_prefix(*prefix).is_some_and(|counter| {
            counter
                .parse::<u64>()
                .is_ok_and(|number| number > 0 && number.to_string() == counter)
        })
    })
}
/// The harness every session the suite opens is bound under.
const HARNESS: &str = "conformance";
/// The member of a scripted tool result that holds the content blocks the scripted tool
/// provider answers with as they are, instead of the result's JSON as one text block.
const OWN_CONTENT: &str = "$broca_content";
const OWN_STRUCTURED: &str = "$broca_structured";
/// The model the kit's media fixture sessions use. It is scripted through the Messages
/// stream, like the output-refs fixture, and its catalog row differs from that fixture's only
/// in a context window large enough to send the frame-budget case's two 2.2 MB tool-call
/// arguments back to the model.
const MEDIA_FIXTURE_MODEL: &str = "conformance-media-blocks";
#[cfg(feature = "test-clock")]
#[path = "llm_runner_conformance/bus.rs"]
mod bus;
#[path = "llm_runner_conformance/catalog.rs"]
mod catalog;

#[path = "llm_runner_conformance/boundary_cancel.rs"]
mod boundary_cancel;
#[path = "llm_runner_conformance/evidence.rs"]
mod evidence;
#[path = "llm_runner_conformance/fork_answer.rs"]
mod fork_answer;
#[path = "llm_runner_conformance/held_late_results.rs"]
mod held_late_results;
#[path = "llm_runner_conformance/output_refs.rs"]
mod output_refs;
#[path = "llm_runner_conformance/reader_identity.rs"]
mod reader_identity;
/// Retention deletion scenarios on the same real-daemon rig. They move the daemon's clock
/// through its `test-clock` control file, which only a `test-clock` build reads.
#[cfg(feature = "test-clock")]
#[path = "llm_runner_conformance/retention_clock.rs"]
mod retention_clock;
#[path = "llm_runner_conformance/routed_accounts.rs"]
mod routed_accounts;
#[path = "llm_runner_conformance/session_class.rs"]
mod session_class;
/// The `it` target's real-daemon diagnostics (log tails, watchdog stall files, decoded session
/// WALs), included by path so this rig prints the same evidence when a suite case fails.
#[allow(dead_code)]
#[path = "it/session_client.rs"]
mod session_client;
#[path = "llm_runner_conformance/step_events.rs"]
mod step_events;

fn client_log_dir() -> PathBuf {
    broca_test_support::scratch_root()
        .join(format!("broca-conformance-client-{}", std::process::id()))
        .join("logs")
}

fn enable_client_diagnostics() {
    // All subjects in this process share the tracing subscriber. Keep it alive through rig
    // teardown, when late terminals and connection failures can still arrive.
    static LOG: std::sync::OnceLock<cortexkit_log::Handle> = std::sync::OnceLock::new();
    LOG.get_or_init(|| {
        let mut config = cortexkit_log::Config::in_dir("conformance", client_log_dir());
        config.spec = Some("info,conformance.subc.wire=debug".into());
        cortexkit_log::init(config).expect("install conformance client diagnostics")
    });
}

/// Read only the ends of the log files, limiting diagnostic output. Failure diagnostics must
/// read them before the suite deletes the directories that hold them.
fn diagnostic_logs(dir: &Path) -> std::io::Result<String> {
    use std::io::{Read, Seek, SeekFrom};

    let mut segments: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|extension| extension == "log"))
        .collect();
    segments.sort();
    let mut output = String::new();
    // Logs rotate, so the last two files catch entries split across a rotation while capping output.
    for path in segments.iter().rev().take(2).rev() {
        let mut file = std::fs::File::open(path)?;
        let length = file.metadata()?.len();
        file.seek(SeekFrom::Start(length.saturating_sub(64 * 1024)))?;
        let mut bytes = Vec::new();
        file.take(64 * 1024).read_to_end(&mut bytes)?;
        output.push_str(&format!(
            "diagnostic segment={} bytes={}\n",
            path.display(),
            bytes.len()
        ));
        output.push_str(&String::from_utf8_lossy(&bytes));
    }
    Ok(output)
}

fn sibling_bin(name: &str, env_key: &str) -> PathBuf {
    broca_test_support::subc_bin(name, env_key)
}

fn require_subc() -> bool {
    std::env::var_os("BROCA_REQUIRE_SUBC").is_some()
}

// ---- the shared state the model and the tool provider read ------------------------------------

/// A kill the harness has armed: the point, and where to say the runner reached it.
struct Armed {
    point: &'static str,
    reached: Arc<Notify>,
    /// The session that reached the point.
    session: Arc<Mutex<Option<String>>>,
}

#[derive(Default)]
struct Shared {
    fork_head_keys: Mutex<HashMap<String, String>>,
    fork_asks: Mutex<fork_answer::PendingAsks>,
    fork_raw_requests: Mutex<fork_answer::RawRequests>,
    routed_account_peers:
        Mutex<HashMap<String, cortexkit_role_llm_runner_conformance::RecordingRoutingPeer>>,
    routed_http_requests: Mutex<HashMap<String, Vec<routed_accounts::HttpRequestEvidence>>>,
    service_tier_sessions: Mutex<BTreeSet<String>>,
    head_selection_sessions: Mutex<BTreeSet<String>>,
    service_tier_keys: Mutex<HashMap<String, String>>,
    service_tier_rides: Mutex<HashMap<String, Vec<Value>>>,
    service_tier_request_rides: Mutex<HashMap<String, Vec<u64>>>,
    fixture_connections: FixtureConnections,
    default_text_module: Mutex<Option<String>>,
    default_text_tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    default_text_seen:
        Mutex<HashMap<String, cortexkit_role_llm_runner_conformance::SystemTextDefaultObservation>>,
    failure_turns: Mutex<HashMap<String, usize>>,
    dispatched_pins: Mutex<HashMap<String, Option<String>>>,
    /// Turns served so far for each session whose scripted model counts its own turns. The
    /// real Magic Context rig needs this: an applied view can drop messages, so the number of
    /// assistant messages in a request no longer says which turn comes next.
    #[cfg(feature = "test-clock")]
    counted_turns: Mutex<HashMap<String, usize>>,
    /// The `mid` of every message a `session.read` answered, keyed by the message's JSON: broca
    /// keys a part `<mid>#<index>`, so naming a message's parts needs the message's `mid`.
    read_mids: Mutex<HashMap<String, String>>,
    read_part_keys: Mutex<HashMap<String, Vec<String>>>,
    read_entries: Mutex<HashMap<(String, String), Value>>,
    plan_sessions: Mutex<BTreeSet<String>>,
    catalog_requests: Mutex<HashMap<String, Vec<Value>>>,
    flow_scope_sessions: Mutex<BTreeSet<String>>,
    output_refs_sessions: Mutex<BTreeSet<String>>,
    model_id_marker_sessions: Mutex<BTreeSet<String>>,
    model_holds: Mutex<HashMap<String, Arc<HoldGate>>>,
    stream_holds: Mutex<HashMap<String, usize>>,
    raw_bodies: Mutex<HashMap<String, Vec<Vec<u8>>>>,
    attachment_models: Mutex<HashMap<String, bool>>,
    session_roots: Mutex<HashMap<String, PathBuf>>,
    session_bindings: Mutex<HashMap<String, broca_protocol::BindIdentity>>,
    /// Each session's scripted model.
    scripts: Mutex<HashMap<String, Script>>,
    /// Each scripted call's result, by its arguments' JSON text.
    results: Mutex<HashMap<String, Value>>,
    /// How often the tool provider was called with each arguments' JSON text, across every kill.
    invocations: Mutex<HashMap<String, usize>>,
    armed: Mutex<Option<Armed>>,
    /// The session the model last answered a request for.
    last_session: Mutex<Option<String>>,
    /// Every request's messages the model received, by session, in order.
    requests: Mutex<HashMap<String, Vec<Vec<Value>>>>,
    /// Every request's whole body the model received, by session, in order.
    bodies: Mutex<HashMap<String, Vec<Value>>>,
    /// Woken when a kill is done, so every request the rig was holding gives up.
    released: Notify,
    /// The scripted calls the suite asked to hold, by their arguments' JSON text, and each one's
    /// gate. A held call answers only once the suite releases it.
    holds: Mutex<HashMap<String, Arc<HoldGate>>>,
    /// Sessions whose model requests the model refuses with HTTP 400, and the JSON error body
    /// it answers with. The retention scenarios use it to put text into provider errors.
    errors: Mutex<HashMap<String, String>>,
    /// Adapter clock writes and `settle_compaction` outcomes in the order they happened, each
    /// with its wall-clock time. Printed only when a suite case fails.
    evidence: Mutex<Vec<String>>,
    /// Per run id: how many `run.result` answers in a row were not terminal, and the latest one.
    run_polls: Mutex<HashMap<String, (usize, Value)>>,
    /// Live snapshots of runs that stayed non-terminal for [`evidence::STALL_POLLS`] polls in a
    /// row, taken while the daemon still answers. Printed only when a suite case fails.
    stalls: Mutex<Vec<String>>,
    /// What each structured-output session's scripted route reports about schema-constrained
    /// output, as a catalog row would: `Some(true)`, `Some(false)` or `None` (not stated).
    structured_output_support: Mutex<HashMap<String, Option<bool>>>,
    /// The scripted provider's held calls and its `late_results` feed (see `held_late_results`).
    held_feed: Mutex<held_late_results::FeedState>,
}

/// One held call's two edges: the tool provider has received it, and the suite has released it.
/// Each edge is a flag plus a `Notify`, so a waiter that arrives after the edge sees the flag
/// rather than waiting for a wakeup that already happened.
#[derive(Default)]
struct HoldGate {
    /// Taken by the one request a model hold applies to.
    claimed: std::sync::atomic::AtomicBool,
    arrived: std::sync::atomic::AtomicBool,
    arrived_wake: Notify,
    released: std::sync::atomic::AtomicBool,
    released_wake: Notify,
}

impl HoldGate {
    /// True for the first caller only. A model hold holds the next request of its session,
    /// not every request made while it is held: a fork's ask runs while its head's step is
    /// held, and must not wait behind it.
    fn claim(&self) -> bool {
        !self.claimed.swap(true, std::sync::atomic::Ordering::SeqCst)
    }

    fn set(flag: &std::sync::atomic::AtomicBool, wake: &Notify) {
        flag.store(true, std::sync::atomic::Ordering::SeqCst);
        wake.notify_waiters();
    }

    /// Resolve once `flag` is set. The wakeup is registered before the flag is read, so a set
    /// that lands between the two still wakes this waiter.
    async fn wait(flag: &std::sync::atomic::AtomicBool, wake: &Notify) {
        loop {
            let woken = wake.notified();
            tokio::pin!(woken);
            woken.as_mut().enable();
            if flag.load(std::sync::atomic::Ordering::SeqCst) {
                return;
            }
            woken.await;
        }
    }
}

impl Shared {
    /// If `point` is armed, take it and say it was reached by `session`. The caller then holds
    /// whatever it was about to answer until the kill releases it.
    fn reach(&self, point: &str, session: &str) -> bool {
        let mut armed = self.armed.lock().unwrap();
        if armed.as_ref().is_some_and(|armed| armed.point == point) {
            let armed = armed.take().expect("checked");
            *armed.session.lock().unwrap() = Some(session.to_owned());
            armed.reached.notify_one();
            true
        } else {
            false
        }
    }
}

// ---- the scripted model -------------------------------------------------------------------------

/// Serve scripted Chat and Messages responses on a loopback port; returns the base URL.
async fn start_model(shared: Arc<Shared>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let shared = shared.clone();
            tokio::spawn(async move {
                if let Err(error) = serve_model_request(stream, shared.clone()).await {
                    shared
                        .evidence
                        .lock()
                        .unwrap()
                        .push(format!("scripted model transport: {error}"));
                }
            });
        }
    });
    format!("http://{address}/v1")
}

/// The scripted model speaks Chat, so the structured-output fixtures use xAI model identities
/// explicitly pinned to Chat rather than inheriting xAI's Responses default. Their capability
/// facts below are synthetic test inputs, not claims about those production models.
const STRUCTURED_OUTPUT_PROVIDER: &str = "xai";

/// The catalog model a structured-output session resolves against, by what its route reports.
fn structured_output_model(support: Option<bool>) -> &'static str {
    match support {
        Some(true) => "conformance-structured-supported",
        Some(false) => "conformance-structured-unsupported",
        None => "conformance-structured-unknown",
    }
}

/// Seed the scripted catalog in broca's state root: Chat rows with tri-state structured-output
/// support, plus the Messages route that preserves independent output-reference samples.
/// broca reaches these rows at
/// the scripted model's address, because `BROCA_PROVIDER_BASE_URL` overrides the endpoint of
/// a catalog model. Kept across restarts of the same root.
fn seed_structured_output_catalog(state_root: &Path) -> std::io::Result<()> {
    let dir = state_root.join("catalog");
    let path = dir.join("fusiform-lkg.json");
    if path.exists() {
        return Ok(());
    }
    std::fs::create_dir_all(&dir)?;
    let row = |support: Option<bool>| {
        let mut row = json!({
            "capability.attachment": false,
            "capability.reasoning": false,
            "capability.tool_call": true,
            "existence": "present",
            "limit.context": 128000
        });
        // The scripted provider speaks Chat Completions. Declare that synthetic transport
        // explicitly so schema support depends on the authored tri-state capability, not on
        // whichever real model happens to share a name with the rig.
        row["wire.npm"] = json!({"value":"@ai-sdk/openai-compatible", "from":"fixture"});
        row["wire.api"] = json!({"value":"https://conformance.invalid/v1", "from":"fixture"});
        if let Some(support) = support {
            row["capability.structured_output"] = json!(support);
        }
        row
    };
    let mut models = Map::new();
    let mut temperature = row(None);
    temperature["capability.temperature"] = json!(true);
    temperature["limit.temperature_min"] = json!(0.0);
    temperature["limit.temperature_max"] = json!(2.0);
    models.insert(
        format!("{STRUCTURED_OUTPUT_PROVIDER}/conformance-temperature"),
        temperature,
    );
    for support in [Some(true), Some(false), None] {
        models.insert(
            format!(
                "{STRUCTURED_OUTPUT_PROVIDER}/{}",
                structured_output_model(support)
            ),
            row(support),
        );
    }
    for accepts in [true, false] {
        let mut facts = row(None);
        facts["capability.input_modalities"] = if accepts {
            json!(["text", "image"])
        } else {
            json!(["text"])
        };
        models.insert(format!("xai/conformance-media-{accepts}"), facts);
    }
    let mut tier = row(None);
    tier["rate.input.mode.priority"] =
        json!({"currency":"USD","exponent":9,"state":"priced","units":2_000_000_000_u64});
    tier["rate.output.mode.priority"] =
        json!({"currency":"USD","exponent":9,"state":"priced","units":4_000_000_000_u64});
    models.insert("xai/conformance-service-tier".into(), tier);
    models.insert(
        format!("{}/{}", output_refs::PROVIDER, output_refs::MODEL),
        output_refs::catalog_row(),
    );
    let mut media = output_refs::catalog_row();
    media["limit.context"] = json!(16_000_000);
    models.insert(
        format!("{}/{MEDIA_FIXTURE_MODEL}", output_refs::PROVIDER),
        media,
    );
    let lkg = json!({
        "catalog_version": 1,
        "models": models,
        "resolved_at_ms": 1,
        "source": "models.dev"
    });
    std::fs::write(path, serde_json::to_vec_pretty(&lkg).unwrap())
}

#[test]
fn structured_output_catalogs_preserve_admission_decisions_without_a_daemon() {
    let root = broca_test_support::scratch_root()
        .join(format!("structured-output-catalog-{}", std::process::id()));
    seed_structured_output_catalog(&root).unwrap();
    let mut response: broca_catalog::FusiformCatalogResponse =
        serde_json::from_slice(&std::fs::read(root.join("catalog/fusiform-lkg.json")).unwrap())
            .unwrap();
    let real: broca_catalog::FusiformCatalogResponse =
        serde_json::from_str(include_str!("fixtures/fusiform_cache_warm_admission.json")).unwrap();
    response
        .models
        .insert("openai/gpt-5".into(), real.models["openai/gpt-5"].clone());
    let cache = std::sync::Arc::new(broca_catalog::CatalogCache::empty());
    cache.refresh(response).unwrap();
    broca_catalog::install_global_catalog_cache(cache);
    let request = serde_json::from_value(json!({
        "mode":"final_text", "name":"answer",
        "schema":{"type":"object", "properties":{"answer":{"type":"string"}}, "required":["answer"], "additionalProperties":false}
    }))
    .unwrap();
    for (model, enforcement, reason) in [
        (
            format!("xai/{}", structured_output_model(Some(true))),
            "enforced",
            None,
        ),
        (
            format!("xai/{}", structured_output_model(Some(false))),
            "best_effort",
            Some("route_unsupported"),
        ),
        (
            format!("xai/{}", structured_output_model(None)),
            "best_effort",
            Some("catalog_unknown"),
        ),
        ("openai/gpt-5".into(), "enforced", None),
    ] {
        let resolved = broca_catalog::live::resolve_provider_with_catalog_base_url(
            &model,
            "http://127.0.0.1:1",
            None,
            None,
            &broca_catalog::live::AuthMode::None,
            &broca_catalog::live::ResolveOptions {
                structured_output: Some(&request),
                ..Default::default()
            },
            broca_provider::build_provider_http_client(),
        )
        .unwrap();
        assert_eq!(
            resolved.family_id,
            if model.starts_with("xai/") {
                broca_catalog::live::WireFamilyId::OpenAiChat
            } else {
                broca_catalog::live::WireFamilyId::OpenAiResponses
            },
            "{model} must use its declared wire package"
        );
        let blob = resolved.provider.render_config_blob();
        assert_eq!(
            blob["structured_output"]["enforcement"], enforcement,
            "{model}"
        );
        assert_eq!(
            blob["structured_output"]["reason"].as_str(),
            reason,
            "{model}"
        );
    }
}

/// What one OpenAI Chat request carried, as the scripted model received it: the offered tool
/// names in order, the final-text schema (`response_format.json_schema.schema`), and the
/// `submit_result` tool's parameters.
fn observe_model_request(
    request: &Value,
) -> cortexkit_role_llm_runner_conformance::ModelRequestObservation {
    let tools = request["tools"].as_array().cloned().unwrap_or_default();
    let names = tools
        .iter()
        .filter_map(|tool| tool["function"]["name"].as_str().map(str::to_owned))
        .collect();
    let mut observation =
        cortexkit_role_llm_runner_conformance::ModelRequestObservation::new(names);
    if let Some(temperature) = request.get("temperature").and_then(Value::as_f64) {
        observation = observation.with_temperature(temperature);
    }
    if let Some(cap) = request
        .get("max_tokens")
        .or_else(|| request.get("max_completion_tokens"))
        .and_then(Value::as_u64)
    {
        observation = observation.with_max_output_tokens(cap);
    }
    if request["response_format"]["type"] == "json_schema" {
        observation = observation
            .with_final_text_schema(request["response_format"]["json_schema"]["schema"].clone());
    }
    if let Some(submit) = tools.iter().find(|tool| {
        tool["function"]["name"] == broca_protocol::structured_output::SUBMIT_RESULT_TOOL
    }) {
        observation =
            observation.with_submit_result_schema(submit["function"]["parameters"].clone());
    }
    // Every text the model received, in request order, as it received it (a rendered tag
    // prefix included): message content strings and text parts, tool results included.
    let mut rendered = Vec::new();
    let mut system_text = Vec::new();
    for message in request["messages"].as_array().into_iter().flatten() {
        let start = rendered.len();
        match &message["content"] {
            Value::String(text) => rendered.push(text.clone()),
            Value::Array(parts) => rendered.extend(
                parts
                    .iter()
                    .filter_map(|part| part["text"].as_str().map(str::to_owned)),
            ),
            _ => {}
        }
        if matches!(message["role"].as_str(), Some("system" | "developer")) {
            system_text.extend_from_slice(&rendered[start..]);
        }
    }
    observation.system_text = Some(system_text);
    observation.with_rendered(rendered)
}

async fn serve_model_request(
    mut stream: tokio::net::TcpStream,
    shared: Arc<Shared>,
) -> std::io::Result<()> {
    let mut first = [0u8; 1];
    if stream.peek(&mut first).await? == 1 && first[0] == b'G' {
        return routed_accounts::serve_websocket(stream, shared).await;
    }
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 8192];
    let body_start = loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&chunk[..n]);
        if let Some(at) = buffer.windows(4).position(|window| window == b"\r\n\r\n") {
            break at + 4;
        }
    };
    let headers = String::from_utf8_lossy(&buffer[..body_start]).to_ascii_lowercase();
    let length: usize = headers
        .lines()
        .find_map(|line| line.strip_prefix("content-length:"))
        .and_then(|value| value.trim().parse().ok())
        .unwrap_or(0);
    while buffer.len() < body_start + length {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..n]);
    }
    let request: Value = serde_json::from_slice(&buffer[body_start..]).unwrap_or(Value::Null);
    let messages = request["messages"].as_array().cloned().unwrap_or_default();
    let model = request["model"].as_str().unwrap_or_default();
    let model = model.strip_prefix("mock/").unwrap_or(model);
    let tier_session = request["prompt_cache_key"]
        .as_str()
        .and_then(|key| shared.service_tier_keys.lock().unwrap().get(key).cloned());
    let session = if let Some(session) = tier_session {
        Some(session)
    } else if let Some(session) = model.strip_prefix(PLAN_MODEL_PREFIX) {
        shared
            .scripts
            .lock()
            .unwrap()
            .contains_key(session)
            .then(|| session.to_owned())
    } else {
        std::iter::once(&request["system"])
            .chain(
                messages
                    .iter()
                    .filter(|message| message["role"] == "system" || message["role"] == "user")
                    .map(|message| &message["content"]),
            )
            .find_map(|message| {
                let text = message.to_string();
                let at = text.find(SESSION_MARKER)? + SESSION_MARKER.len();
                let name: String = text[at..]
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                    .collect();
                Some(name)
            })
    };
    if let Some(session) = &session {
        shared.record_model_ingress(session, &request, &buffer[body_start..body_start + length]);
    }
    let refusal = session
        .as_ref()
        .and_then(|session| shared.errors.lock().unwrap().get(session).cloned());
    if let Some(body) = refusal {
        let reply = format!(
            "HTTP/1.1 400 Bad Request\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(reply.as_bytes()).await?;
        return Ok(());
    }
    // Legacy sessions are matched to their scripted turn by the number of assistant messages
    // already in the request. A step cut at StepRecorded stays in the transcript (the seal keeps
    // it with a never-executed result), so this count is the number of turns already served.
    #[cfg(feature = "test-clock")]
    let counted = session
        .as_ref()
        .and_then(|session| shared.counted_turns.lock().unwrap().get(session).copied());
    #[cfg(not(feature = "test-clock"))]
    let counted: Option<usize> = None;
    let turn_index = session
        .as_ref()
        .and_then(|session| shared.failure_turns.lock().unwrap().get(session).copied())
        .or(counted)
        .unwrap_or_else(|| {
            // A Google Code Assist request carries its turns under `request.contents`, the
            // model's own as role `model`.
            let contents = request["request"]["contents"].as_array();
            match contents {
                Some(contents) if messages.is_empty() => contents
                    .iter()
                    .filter(|content| content["role"] == "model")
                    .count(),
                _ => messages
                    .iter()
                    .filter(|message| message["role"] == "assistant")
                    .count(),
            }
        });
    let turn = session.as_ref().and_then(|session| {
        shared
            .scripts
            .lock()
            .unwrap()
            .get(session)
            .and_then(|script| script.turns.get(turn_index).cloned())
    });
    if let Some(session) = &session {
        if shared
            .routed_account_peers
            .lock()
            .unwrap()
            .contains_key(session)
        {
            let evidence = routed_accounts::http_request_evidence(&headers);
            shared
                .routed_http_requests
                .lock()
                .unwrap()
                .entry(session.clone())
                .or_default()
                .push(evidence);
        }
    }
    let Some(turn) = turn else {
        eprintln!(
            "unmatched conformance model request: model={} session={session:?}",
            request["model"]
        );
        let body = format!(
            "no scripted turn {turn_index} for session {session:?} in the conformance model"
        );
        let reply = format!(
            "HTTP/1.1 400 Bad Request\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(reply.as_bytes()).await?;
        return Ok(());
    };
    let session = session.expect("a turn was found for it");
    if shared
        .service_tier_sessions
        .lock()
        .unwrap()
        .contains(&session)
    {
        let rides = shared
            .service_tier_rides
            .lock()
            .unwrap()
            .get(&session)
            .map_or(0, |rides| rides.len() as u64);
        shared
            .service_tier_request_rides
            .lock()
            .unwrap()
            .entry(session.clone())
            .or_default()
            .push(rides);
    }
    if let Some(served) = shared.failure_turns.lock().unwrap().get_mut(&session) {
        *served += 1;
    }
    #[cfg(feature = "test-clock")]
    if let Some(served) = shared.counted_turns.lock().unwrap().get_mut(&session) {
        *served += 1;
    }
    shared
        .requests
        .lock()
        .unwrap()
        .entry(session.clone())
        .or_default()
        .push(messages.clone());
    shared
        .bodies
        .lock()
        .unwrap()
        .entry(session.clone())
        .or_default()
        .push(request.clone());
    if let Some(failure) = &turn.model_failure {
        let (status, code) = match failure.class.as_str() {
            "auth_required" | "auth" => (401, "authentication_error"),
            "context_overflow" => (400, "context_length_exceeded"),
            "permanent" => (400, "invalid_request_error"),
            _ => (429, "rate_limit_error"),
        };
        let body = json!({"error":{"code":code,"message":failure.message}}).to_string();
        let retry = failure
            .retry_after_secs
            .filter(|value| value.is_finite() && *value >= 0.0)
            .map_or_else(String::new, |value| format!("retry-after: {value}\r\n"));
        // A scripted terminal occupies one turn. Retrying this HTTP failure
        // would consume the recovery turn and turn a failure fixture into success.
        let response = format!("HTTP/1.1 {status} Scripted failure\r\ncontent-type: application/json\r\nx-should-retry: false\r\n{retry}content-length: {}\r\nconnection: close\r\n\r\n{body}", body.len());
        stream.write_all(response.as_bytes()).await?;
        return Ok(());
    }
    let model_gate = shared
        .model_holds
        .lock()
        .unwrap()
        .get(&session)
        .cloned()
        .filter(|gate| gate.claim());
    // A stream hold belongs to the request its model hold holds.
    let stream_parts = model_gate
        .as_ref()
        .and_then(|_| shared.stream_holds.lock().unwrap().remove(&session));
    if let Some(gate) = model_gate.as_ref().filter(|_| stream_parts.is_none()) {
        HoldGate::set(&gate.arrived, &gate.arrived_wake);
        tokio::select! {
            () = HoldGate::wait(&gate.released, &gate.released_wake) => {},
            () = shared.released.notified() => return Ok(()),
        }
    }
    // The first model request of a run comes right after its start record is durable, and the
    // one after a tool call right after that call's result is.
    let point = if turn_index == 0 {
        [points::ADMITTED, points::SEND_RECORDED]
            .into_iter()
            .find(|point| shared.reach(point, &session))
    } else {
        shared
            .reach(points::TOOL_RESULT_RECORDED, &session)
            .then_some(points::TOOL_RESULT_RECORDED)
    };
    if point.is_some() {
        shared.released.notified().await;
        return Ok(());
    }

    *shared.last_session.lock().unwrap() = Some(session.clone());
    let id = format!("chatcmpl-{session}-{turn_index}");
    // A local endpoint override does not change the provider's native stream grammar.
    let messages_endpoint = headers
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .is_some_and(|target| {
            target
                .split('?')
                .next()
                .is_some_and(|path| path.ends_with("/messages"))
        });
    let native_messages = messages_endpoint
        && shared
            .routed_account_peers
            .lock()
            .unwrap()
            .contains_key(&session);
    let code_assist = headers
        .lines()
        .next()
        .is_some_and(|line| line.contains(":streamgeneratecontent"));
    if code_assist {
        let body = routed_accounts::code_assist_stream(&turn);
        let headers = format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len());
        stream.write_all(headers.as_bytes()).await?;
        stream.write_all(body.as_bytes()).await?;
        return stream.shutdown().await;
    }
    let media_fixture = model.ends_with(MEDIA_FIXTURE_MODEL);
    if model.ends_with(output_refs::MODEL) || native_messages || media_fixture {
        let body = if native_messages || media_fixture {
            output_refs::stream_for_model(&id, model, &turn)
        } else {
            output_refs::stream(&id, &turn)
        };
        let headers = format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len());
        stream.write_all(headers.as_bytes()).await?;
        stream.write_all(body.as_bytes()).await?;
        return stream.shutdown().await;
    }
    let chunk = |delta: Value, finish: Option<&str>| {
        json!({"id": id, "object": "chat.completion.chunk", "model": "mock-model",
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]})
    };
    // The scripted model honours the cap it actually received. Its word-sized tokens
    // make bounded-output cases deterministic without borrowing the runner's cap logic.
    let output_cap = request
        .get("max_tokens")
        .or_else(|| request.get("max_completion_tokens"))
        .and_then(Value::as_u64)
        .unwrap_or(u64::MAX);
    let mut remaining = output_cap;
    let mut truncated = false;
    let mut events = vec![chunk(json!({"role": "assistant"}), None)];
    let mut calls = 0;
    for part in &turn.parts {
        events.push(match part {
            ScriptedPart::Text(text) => {
                let words: Vec<_> = text.split_whitespace().collect();
                let content = if words.len() as u64 > remaining {
                    truncated = true;
                    words[..remaining as usize].join(" ")
                } else {
                    text.clone()
                };
                remaining = remaining.saturating_sub(words.len() as u64);
                chunk(json!({"content": content}), None)
            }
            ScriptedPart::Reasoning(text) => chunk(json!({"reasoning_content": text}), None),
            ScriptedPart::ToolCall(call) => {
                calls += 1;
                chunk(
                    json!({"tool_calls": [{"index": calls - 1, "id": call.tool_call_id,
                        "type": "function", "function": {"name": call.tool,
                        "arguments": call.arguments.to_string()}}]}),
                    None,
                )
            }
        });
    }
    events.push(chunk(
        json!({}),
        if truncated {
            Some("length")
        } else {
            turn.finish_reason.as_deref()
        },
    ));
    if let Some(usage) = &turn.usage {
        events.push(scripted_usage_chunk(&id, usage, output_cap));
    }
    let mut body = String::new();
    let mut prefix_bytes = 0;
    for (index, event) in events.into_iter().enumerate() {
        body.push_str(&format!("data: {event}\n\n"));
        if stream_parts.is_some_and(|parts| index == parts) {
            prefix_bytes = body.len();
        }
    }
    body.push_str("data: [DONE]\n\n");
    let headers = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await?;
    if let Some(gate) = model_gate.as_ref().filter(|_| stream_parts.is_some()) {
        stream.write_all(&body.as_bytes()[..prefix_bytes]).await?;
        stream.flush().await?;
        HoldGate::set(&gate.arrived, &gate.arrived_wake);
        tokio::select! {
            () = HoldGate::wait(&gate.released, &gate.released_wake) => {},
            () = shared.released.notified() => return Ok(()),
        }
        stream.write_all(&body.as_bytes()[prefix_bytes..]).await?;
    } else {
        stream.write_all(body.as_bytes()).await?;
    }
    stream.shutdown().await
}

// ---- the scripted tool provider -----------------------------------------------------------------

struct ToolProvider {
    shared: Arc<Shared>,
    /// The session each bound route channel serves, from its bind.
    routes: Mutex<HashMap<(u16, u32), String>>,
}

impl ToolProvider {
    fn catalog_for_route(&self, route: (u16, u32), request: &Value) -> Value {
        let session = self
            .routes
            .lock()
            .unwrap()
            .get(&route)
            .cloned()
            .unwrap_or_default();
        self.shared
            .catalog_requests
            .lock()
            .unwrap()
            .entry(session.clone())
            .or_default()
            .push(request.clone());
        let mut answer = catalog::catalog(request);
        held_late_results::declare_in_catalog(&self.shared, &session, &mut answer);
        answer
    }
}

#[async_trait]
impl subc_client_rs::ModuleHandler for ToolProvider {
    async fn handle(
        &self,
        ctx: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        if let Some(outcome) = held_late_results::serve_feed(&self.shared, &request).await {
            return outcome;
        }
        if request["name"] == "tool.catalog" {
            let handle = ctx.route_handle();
            let answer = self.catalog_for_route((handle.channel, handle.epoch), &request);
            return subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&answer).unwrap());
        }
        let arguments = request["arguments"].to_string();
        let handle = ctx.route_handle();
        if let Some(outcome) =
            held_late_results::answer_yield(&self.shared, (handle.channel, handle.epoch), &request)
        {
            return outcome;
        }
        if let Some(outcome) = held_late_results::answer_waiting_yield(
            &self.shared,
            (handle.channel, handle.epoch),
            &arguments,
            &request,
        )
        .await
        {
            return outcome;
        }
        if let Some(outcome) = held_late_results::answer_held(
            &self.shared,
            (handle.channel, handle.epoch),
            &arguments,
            &request,
        ) {
            return outcome;
        }
        let session = self
            .routes
            .lock()
            .unwrap()
            .get(&(handle.channel, handle.epoch))
            .cloned()
            .unwrap_or_default();
        *self
            .shared
            .invocations
            .lock()
            .unwrap()
            .entry(arguments.clone())
            .or_default() += 1;
        self.shared.dispatched_pins.lock().unwrap().insert(
            arguments.clone(),
            request["schema_pin"].as_str().map(str::to_owned),
        );
        // The call is sent only once its dispatch intent is durable.
        if self.shared.reach(points::DISPATCH_INTENT, &session) {
            self.shared.released.notified().await;
            return subc_client_rs::HandlerOutcome::Error {
                code: "conformance_killed".into(),
                message: "the runner was killed while this call was held".into(),
            };
        }
        // A call the suite asked to hold waits here, after it is counted, until the suite
        // releases it; a kill releases it as an error. Once released, a later call with the same
        // arguments answers at once.
        let gate = self.shared.holds.lock().unwrap().get(&arguments).cloned();
        if let Some(gate) = gate {
            if !gate.released.load(std::sync::atomic::Ordering::SeqCst) {
                HoldGate::set(&gate.arrived, &gate.arrived_wake);
                tokio::select! {
                    () = HoldGate::wait(&gate.released, &gate.released_wake) => {}
                    () = self.shared.released.notified() => {
                        return subc_client_rs::HandlerOutcome::Error {
                            code: "conformance_killed".into(),
                            message: "the runner was killed while this call was held".into(),
                        };
                    }
                }
            }
        }
        let result = self.shared.results.lock().unwrap().get(&arguments).cloned();
        // A scripted result naming its own content blocks (an image-only result) is answered
        // with them as they are.
        if let Some(content) = result.as_ref().and_then(|result| result.get(OWN_CONTENT)) {
            return subc_client_rs::HandlerOutcome::Response(
                serde_json::to_vec(&json!({ "content": content })).unwrap(),
            );
        }
        if let Some(value) = result
            .as_ref()
            .and_then(|result| result.get(OWN_STRUCTURED))
        {
            return subc_client_rs::HandlerOutcome::Response(
                serde_json::to_vec(&json!({ "structuredContent": value })).unwrap(),
            );
        }
        let text = result.map_or_else(
            || format!("no scripted result for arguments {arguments}"),
            |result| match result {
                Value::String(text) => text,
                value => value.to_string(),
            },
        );
        subc_client_rs::HandlerOutcome::Response(
            serde_json::to_vec(&json!({"content": [{"type": "text", "text": text}]})).unwrap(),
        )
    }

    async fn on_bind(
        &self,
        req: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        self.routes.lock().unwrap().insert(
            (req.handle.channel, req.handle.epoch),
            req.identity.session.clone(),
        );
        held_late_results::note_bind(
            &self.shared,
            (req.handle.channel, req.handle.epoch),
            req.consumer_capabilities.as_deref(),
        );
        subc_client_rs::BindDecision::accept()
    }
}

fn tool_manifest() -> subc_protocol::manifest::ModuleManifest {
    use subc_protocol::manifest::{Concurrency, ExecutionMode, IdentityScope, ProviderRole, Tool};
    subc_protocol::manifest::ModuleManifest::builder(TOOL_MODULE, env!("CARGO_PKG_VERSION"))
        .capabilities(Some(subc_protocol::manifest::CapabilityDeclarations {
            provides: vec!["call-yield/v1".into()],
            requires: vec![],
            must_never_reach: vec![],
        }))
        .provides(vec![ProviderRole::ToolProvider {
            tools: vec![Tool {
                name: SCRIPTED_TOOL.to_string(),
                description: Some("Answers each call with its scripted result.".into()),
                execution_mode: ExecutionMode::Pure,
                schema: json!({"type": "object"}),
            }],
            identity_scope: vec![IdentityScope::Project, IdentityScope::Session],
            concurrency: Concurrency::ModuleManaged,
            emits_push: false,
            sub_supervises: false,
        }])
        .build()
}

struct DefaultTextProvider {
    shared: Arc<Shared>,
    session: String,
    rich_ui: Option<cortexkit_role_llm_runner::rich_ui::RichUiStamp>,
}

#[async_trait]
impl subc_client_rs::ModuleHandler for DefaultTextProvider {
    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        #[derive(serde::Deserialize)]
        struct Envelope {
            arguments: Box<serde_json::value::RawValue>,
        }
        #[derive(serde::Deserialize)]
        struct Arguments {
            system_text: Option<Box<serde_json::value::RawValue>>,
        }
        let envelope: Envelope = serde_json::from_slice(&body).unwrap();
        let arguments: Arguments = serde_json::from_str(envelope.arguments.get()).unwrap();
        {
            let mut all = self.shared.default_text_seen.lock().unwrap();
            let seen = all.entry(self.session.clone()).or_default();
            seen.provider_calls += 1;
            if let Some(item) = arguments.system_text {
                seen.provider_items.push(item.get().as_bytes().to_vec());
            }
        }
        let request: Value = serde_json::from_slice(&body).unwrap();
        let composition = &request["arguments"]["composition"];
        let digest =
            cortexkit_role_tool_provider::catalog::composition_digest(composition).unwrap();
        let text = "synthetic provider default system text";
        let mut answer = json!({"generation":"1","catalog_digest":"default-text","composition_digest":digest,"tools":[],
            "system_text":{"text":text,"item_digest":cortexkit_role_tool_provider::catalog::system_text_digest(text),
                "preflight_digest":"default-text","composition_digest":digest,"tool_names":[]}});
        if let Some(stamp) = &self.rich_ui {
            answer["system_text"]["rich_ui"] = serde_json::to_value(stamp).unwrap();
        }
        subc_client_rs::HandlerOutcome::Response(serde_json::to_vec(&answer).unwrap())
    }
}

/// The connection files of the rigs whose daemons fixtures build against (rich UI stamps,
/// default system text), newest last. A case that starts its own daemon, as a restart case
/// does, becomes the current one while its rig lives; when that rig drops, the daemon started
/// before it is current again, so later cases never build against a stopped daemon.
#[derive(Default)]
struct FixtureConnections(Arc<Mutex<FixtureEntries>>);

#[derive(Default)]
struct FixtureEntries {
    next: u64,
    live: Vec<(u64, PathBuf)>,
}

impl FixtureConnections {
    /// Makes `connection_file` current until the returned registration drops.
    fn register(&self, connection_file: PathBuf) -> FixtureRegistration {
        let mut entries = self.0.lock().unwrap();
        let id = entries.next;
        entries.next += 1;
        entries.live.push((id, connection_file));
        FixtureRegistration {
            entries: Arc::clone(&self.0),
            id,
        }
    }

    /// The connection file of the newest rig still alive.
    fn current(&self) -> Option<PathBuf> {
        let entries = self.0.lock().unwrap();
        entries.live.last().map(|(_, path)| path.clone())
    }
}

/// A rig's place in [`FixtureConnections`]. Dropping it removes only this rig's entry, even when
/// a restarted rig on the same root registered the same connection file after it.
struct FixtureRegistration {
    entries: Arc<Mutex<FixtureEntries>>,
    id: u64,
}

impl Drop for FixtureRegistration {
    fn drop(&mut self) {
        if let Ok(mut entries) = self.entries.lock() {
            entries.live.retain(|(id, _)| *id != self.id);
        }
    }
}

#[test]
fn a_model_hold_holds_only_the_request_that_claims_it() {
    let gate = HoldGate::default();
    assert!(gate.claim(), "the next request is held");
    assert!(!gate.claim(), "a request made while it is held passes");
    assert!(!gate.claim());
}

#[test]
fn a_dropped_rig_hands_fixtures_back_to_the_daemon_started_before_it() {
    let connections = FixtureConnections::default();
    assert_eq!(connections.current(), None);
    let suite = connections.register(PathBuf::from("suite/runtime/subc-connection.json"));
    let case_file = PathBuf::from("suite/restart-case/runtime/subc-connection.json");
    // A restart case: its first rig, then the restarted rig on the same root, which publishes
    // the same connection file before the first one is gone.
    let first = connections.register(case_file.clone());
    let restarted = connections.register(case_file.clone());
    drop(first);
    assert_eq!(connections.current(), Some(case_file));
    drop(restarted);
    assert_eq!(
        connections.current(),
        Some(PathBuf::from("suite/runtime/subc-connection.json"))
    );
    drop(suite);
    assert_eq!(connections.current(), None);
}

/// Whether `session` is the head of one of the kit's plan_view cases (`cf-plan-view-<n>`).
/// `session.plan_view` serves only heads kept in the new storage, and the case that sends
/// without a plan must still reach its `no_plan` answer there, so these heads start in the new
/// storage whatever their first send carries.
fn plan_view_session(session: &str) -> bool {
    session.starts_with("cf-plan-view-")
}

#[test]
fn plan_view_heads_start_as_heads_with_or_without_a_plan() {
    let subject = held_call_tests::subject(Arc::new(Shared::default()));
    let session = "cf-plan-view-12";
    let mut first = subject.send_fields(session, true);
    assert_eq!(first["session_class"], "head");
    assert!(first.get("plan").is_some_and(Value::is_object));
    // The no_plan case removes the plan and keeps the class.
    first.remove("plan");
    assert_eq!(first["session_class"], "head");
    assert!(subject
        .send_fields(session, false)
        .get("session_class")
        .is_none());
    assert!(subject
        .send_fields("cf-session-class-9", true)
        .get("session_class")
        .is_none());
}

// ---- the harness ----------------------------------------------------------------------------------

/// One live rig on one state root: the daemon (supervising `ck-broca` and the stand-in), the
/// in-process tool provider, and the caller connections opened on it.
struct Live {
    /// Behind a lock so a rig another owner still holds can be stopped ([`Live::shut_down`]).
    daemon: Mutex<Option<broca_test_support::RigChild>>,
    connection_file: PathBuf,
    owner_nonce: PathBuf,
    state_root: PathBuf,
    logs_dir: PathBuf,
    tools: tokio::task::JoinHandle<()>,
    #[cfg(feature = "test-clock")]
    bus_credentials: Option<tokio::task::JoinHandle<()>>,
    connections: tokio::sync::Mutex<HashMap<String, Arc<SubcConnection>>>,
    _routed_services: Option<routed_accounts::Services>,
    /// This rig's place among the daemons fixtures build against, released when it drops.
    _fixture: Option<FixtureRegistration>,
}

impl Live {
    /// Every process the daemon supervises, by pid and command name.
    fn children(&self) -> Vec<(i32, String)> {
        let Some(pid) = self
            .daemon
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|child| child.id())
        else {
            return Vec::new();
        };
        let output = std::process::Command::new("pgrep")
            .arg("-P")
            .arg(pid.to_string())
            .output();
        let Ok(output) = output else {
            return Vec::new();
        };
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter_map(|line| line.trim().parse::<i32>().ok())
            .map(|pid| {
                let name = std::process::Command::new("ps")
                    .args(["-o", "comm=", "-p", &pid.to_string()])
                    .output()
                    .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_owned())
                    .unwrap_or_default();
                (pid, name)
            })
            .collect()
    }

    /// `SIGKILL` every supervised process, broca first, then the daemon, so nothing respawns
    /// broca and no module outlives its rig.
    fn kill_all(&mut self) -> bool {
        self.kill_supervised()
    }

    /// [`Self::kill_all`] through a shared reference, for a rig another owner still holds.
    fn kill_supervised(&self) -> bool {
        let children = self.children();
        let mut killed_broca = false;
        for (pid, _) in children
            .iter()
            .filter(|(_, name)| name.contains("ckdev-broca"))
        {
            killed_broca |= sigkill(*pid);
        }
        for (pid, _) in children
            .iter()
            .filter(|(_, name)| !name.contains("ckdev-broca"))
        {
            sigkill(*pid);
        }
        let daemon = self.daemon.lock().unwrap().take();
        if let Some(mut daemon) = daemon {
            // The daemon may already have respawned a module killed above. The tree kill stops
            // the daemon before listing and killing its remaining processes, so no module
            // outlives the rig and writes into the scratch directory while it is removed.
            broca_test_support::kill_and_reap_tree(&mut daemon);
        }
        self.tools.abort();
        #[cfg(feature = "test-clock")]
        if let Some(task) = &self.bus_credentials {
            task.abort();
        }
        killed_broca
    }

    /// Stops the rig as dropping it does: an orderly shutdown, then [`Self::kill_supervised`]
    /// for anything left. Callable on a rig another owner still holds, such as a routed-account
    /// runner a session route keeps alive; stopping it again does nothing.
    fn shut_down(&self) {
        // Wait for the daemon's shutdown notification to let the coordinator record a planned
        // stop before fallback cleanup can leave the run for crash recovery.
        let mut daemon = self.daemon.lock().unwrap();
        if let Some(daemon) = daemon.as_mut() {
            if let Some(pid) = daemon.id() {
                let sent = std::process::Command::new("kill")
                    .args(["-TERM", &pid.to_string()])
                    .status()
                    .is_ok_and(|status| status.success());
                if sent {
                    let _ = tokio::task::block_in_place(|| {
                        tokio::runtime::Handle::current().block_on(
                            broca_test_support::event_timeout(
                                "orderly conformance daemon exit",
                                daemon.wait(),
                                || format!("daemon pid={pid}"),
                            ),
                        )
                    });
                }
            }
        }
        drop(daemon);
        self.kill_supervised();
    }
}

/// Send `SIGKILL` to `pid` with the `kill` utility. True when it was delivered.
fn sigkill(pid: i32) -> bool {
    std::process::Command::new("kill")
        .args(["-9", &pid.to_string()])
        .status()
        .is_ok_and(|status| status.success())
}

impl Drop for Live {
    fn drop(&mut self) {
        self.shut_down();
    }
}

#[derive(Clone)]
struct BrocaSubject {
    shared: Arc<Shared>,
    routed_instances: Arc<tokio::sync::Mutex<HashMap<String, std::sync::Weak<Live>>>>,
    model_url: String,
    /// The project root for legacy seeds; routed identities use the session-ref inventory.
    project_root: PathBuf,
    /// Where `ck-broca` writes rendered request dumps (`BROCA_DUMP_REQUEST_DIR`), when a test
    /// turns dumps on. The retention scenarios scan it for content that must never be dumped.
    dump_dir: Option<PathBuf>,
    binary: Option<PathBuf>,
    backup_classes: Option<Vec<String>>,
    bus_events_enabled: bool,
    process_kills_disabled: bool,
    service_tier_enabled: bool,
    reserved_reader: bool,
    #[cfg(feature = "test-clock")]
    bus: Option<Arc<bus::BusRig>>,
}

impl BrocaSubject {
    async fn new(scratch: &Path) -> Self {
        enable_client_diagnostics();
        let shared = Arc::new(Shared::default());
        let model_url = start_model(shared.clone()).await;
        let project_root = scratch.join("project");
        std::fs::create_dir_all(&project_root).unwrap();
        Self {
            shared,
            routed_instances: Default::default(),
            model_url,
            project_root: project_root.canonicalize().unwrap(),
            dump_dir: None,
            binary: None,
            backup_classes: None,
            bus_events_enabled: true,
            process_kills_disabled: false,
            service_tier_enabled: std::env::var_os("BROCA_CONFORMANCE_SERVICE_TIER").is_some(),
            reserved_reader: false,
            #[cfg(feature = "test-clock")]
            bus: None,
        }
    }

    fn raw_bind_identity(&self, session: &str) -> broca_protocol::BindIdentity {
        broca_protocol::BindIdentity::new(self.project_root.clone(), HARNESS, session)
    }

    fn bind_identity(&self, session: &str) -> broca_protocol::BindIdentity {
        let identity = self.raw_bind_identity(session);
        let root = self
            .shared
            .session_roots
            .lock()
            .unwrap()
            .get(session)
            .cloned();
        match root {
            Some(root) => broca_test_support::resolve_session_address(&root, &identity)
                .expect("resolve the conformance session's storage identity"),
            // Before routing, fixtures may author a legacy seed at this identity.
            None => identity,
        }
    }

    async fn start(&self, root: &Path) -> Result<Live, HarnessError> {
        self.start_configured(root, None).await
    }

    async fn start_configured(
        &self,
        root: &Path,
        routed_session: Option<&str>,
    ) -> Result<Live, HarnessError> {
        let routed = routed_session.is_some();
        let fail = |what: String| HarnessError::new(what);
        let config_home = root.join("config");
        let runtime_dir = root.join("runtime");
        let data_home = root.join("data-home");
        let home = root.join("home");
        std::fs::create_dir_all(&home).map_err(|e| fail(e.to_string()))?;
        let state_root = root.join("broca-state");
        seed_structured_output_catalog(&state_root).map_err(|e| fail(e.to_string()))?;
        if let Some(session) = routed_session {
            routed_accounts::seed_catalog(&state_root, &self.model_url, session)
                .map_err(|e| fail(e.to_string()))?;
        }
        let config_path = config_home.join("cortexkit").join("subc.jsonc");
        std::fs::create_dir_all(config_path.parent().unwrap()).map_err(|e| fail(e.to_string()))?;
        std::fs::create_dir_all(&runtime_dir).map_err(|e| fail(e.to_string()))?;
        let owner_nonce = root.join("owner.nonce");
        reset_caller_nonce_files(&owner_nonce)?;
        let scope_sync = root.join("owner-scopes.json");
        let scope_events = root.join("owner-scope-events.jsonl");
        for path in [&scope_sync, &scope_events] {
            match std::fs::remove_file(path) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(fail(format!("resetting scope fixture state: {error}"))),
            }
        }
        let connection_file = runtime_dir.join("subc-connection.json");
        let _ = std::fs::remove_file(&connection_file);
        let config = json!({
            "version": 1,
            "scope_authority_owners": [OWNER_MODULE],
            "modules": {
                "broca": {
                    "program": broca_test_support::ckdev_binary(
                        self.binary.as_deref().unwrap_or_else(|| Path::new(env!("CARGO_BIN_EXE_ck-broca"))),
                        broca_test_support::scratch_root(),
                    ),
                    "args": [],
                    "env": {
                        "BROCA_STATE_ROOT": state_root,
                        "XDG_DATA_HOME": data_home,
                        "BROCA_PROVIDER_BASE_URL": self.model_url,
                        "BROCA_TOOL_MODULE": TOOL_MODULE,
                        "CK_LOG": "info,broca.wire.describe=debug,broca.wire.ingress=debug,broca.wire.egress=debug,broca.step-events.evidence=debug",
                    },
                    "enabled": true
                },
                OWNER_MODULE: {
                    "program": sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
                    "args": [],
                    "env": {
                        "FAKE_AFT_MODULE_ID": OWNER_MODULE,
                        "FAKE_AFT_LAUNCH_NONCE_PATH": owner_nonce,
                        "FAKE_AFT_SCOPE_SYNC_PATH": scope_sync,
                        "FAKE_AFT_EVENTS_PATH": scope_events,
                    },
                    "enabled": true,
                    "reserved": true,
                },
                SENDER_MODULE: {
                    "program": sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
                    "args": [],
                    "env": {
                        "FAKE_AFT_MODULE_ID": SENDER_MODULE,
                        "FAKE_AFT_LAUNCH_NONCE_PATH": root.join("sender.nonce"),
                    },
                    "enabled": true,
                    "reserved": true,
                },
                HEAD_SENDER_MODULE: {
                    "program": sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
                    "args": [],
                    "env": {
                        "FAKE_AFT_MODULE_ID": HEAD_SENDER_MODULE,
                        "FAKE_AFT_LAUNCH_NONCE_PATH": root.join("head-sender.nonce"),
                    },
                    "enabled": true,
                    "reserved": true,
                },
                STRANGER_MODULE: {
                    "program": sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
                    "args": [],
                    "env": {
                        "FAKE_AFT_MODULE_ID": STRANGER_MODULE,
                        "FAKE_AFT_LAUNCH_NONCE_PATH": root.join("stranger.nonce"),
                    },
                    "enabled": true,
                    "reserved": true,
                },
            }
        });
        let mut config = config;
        if self.reserved_reader {
            config["modules"]["magic-context"] = json!({
                "program": sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
                "args": [],
                "env": {
                    "FAKE_AFT_MODULE_ID": "magic-context",
                    "FAKE_AFT_LAUNCH_NONCE_PATH": root.join("reader.nonce"),
                },
                "enabled": true,
                "reserved": true,
            });
        }
        let broca_config = config_home.join("cortexkit/broca.jsonc");
        let mut settings = json!({"bus_run_events_enabled":self.bus_events_enabled && !routed,
            "service_tier":{"enabled":self.service_tier_enabled}});
        if let Some(classes) = &self.backup_classes {
            settings["backup_classes"] = json!(classes);
        }
        // The daemons here have no deployment-key vault, so every session is stored plain: the
        // harness reads its records from disk (`stored_log`).
        settings["session_storage"] = json!({"encryption": "off"});
        if routed {
            routed_accounts::configure(root, &self.model_url, &mut config, &mut settings)?;
        }
        std::fs::write(&broca_config, serde_json::to_vec(&settings).unwrap())
            .map_err(|error| fail(error.to_string()))?;
        config["modules"]["broca"]["env"]["BROCA_CONFIG_PATH"] = json!(broca_config);
        if let Some(dump_dir) = &self.dump_dir {
            config["modules"]["broca"]["env"]["BROCA_DUMP_REQUEST_DIR"] = json!(dump_dir);
        }
        std::fs::write(&config_path, serde_json::to_vec_pretty(&config).unwrap())
            .map_err(|e| fail(e.to_string()))?;
        let daemon = broca_test_support::retry_executable_file_busy(|| {
            broca_test_support::tokio_rig_command(
                broca_test_support::ckdev_binary(
                    sibling_bin("ck-subc", "SUBC_CORE_BIN"),
                    broca_test_support::scratch_root(),
                ),
                root,
                "daemon",
            )?
            .env("XDG_CONFIG_HOME", &config_home)
            .env("XDG_RUNTIME_DIR", &runtime_dir)
            .env("XDG_DATA_HOME", &data_home)
            .env("HOME", &home)
            .env_remove("BROCA_STATE_ROOT")
            .env_remove("BROCA_PROVIDER_BASE_URL")
            .env("SUBC_PORT", "0")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env_remove("SUBC_MODULE_ID")
            .kill_on_drop(true)
            .spawn()
        })
        .map_err(|e| fail(format!("spawning ck-subc: {e}")))?;
        let daemon = broca_test_support::RigChild::new(daemon, root, "daemon");
        let deadline = Instant::now() + Duration::from_secs(20);
        while !connection_file.exists() {
            if Instant::now() >= deadline {
                return Err(fail(
                    "the daemon never published its connection file".into(),
                ));
            }
            sleep(Duration::from_millis(25)).await;
        }
        let (_, serve) = subc_client_rs::serve_with_handle(
            &connection_file,
            tool_manifest(),
            ToolProvider {
                shared: self.shared.clone(),
                routes: Mutex::new(HashMap::new()),
            },
        )
        .await
        .map_err(|e| fail(format!("registering the scripted tool provider: {e}")))?;
        let tools = tokio::spawn(async move {
            let _ = serve.await;
        });
        let routed_services = match routed_session {
            Some(session) => Some(
                routed_accounts::register(&connection_file, self.shared.clone(), session).await?,
            ),
            None => None,
        };
        #[cfg(feature = "test-clock")]
        let bus_credentials = match self
            .bus
            .as_ref()
            .filter(|_| !routed || routed_session.is_some_and(step_events::fixture_session))
        {
            Some(bus) => Some(bus.register(&connection_file).await),
            None => None,
        };
        let fixture = (!routed).then(|| {
            self.shared
                .fixture_connections
                .register(connection_file.clone())
        });
        publish_scope_fixtures(&scope_sync, &scope_events).await?;
        Ok(Live {
            daemon: Mutex::new(Some(daemon)),
            connection_file,
            owner_nonce,
            state_root,
            logs_dir: data_home.join("cortexkit/broca/logs"),
            tools,
            #[cfg(feature = "test-clock")]
            bus_credentials,
            connections: tokio::sync::Mutex::new(HashMap::new()),
            _routed_services: routed_services,
            _fixture: fixture,
        })
    }

    /// The caller connection for `stamp`, opened once per rig.
    async fn connection(
        &self,
        live: &Live,
        stamp: &RouteStamp,
    ) -> Result<Arc<SubcConnection>, HarnessError> {
        let mut connections = live.connections.lock().await;
        if let Some(connection) = connections.get(&stamp.principal) {
            return Ok(connection.clone());
        }
        let identity = if let Some((module, nonce_path)) =
            caller_module_nonce(&stamp.principal, &live.owner_nonce)?
        {
            let deadline = Instant::now() + Duration::from_secs(20);
            let nonce = loop {
                if let Ok(text) = std::fs::read_to_string(&nonce_path) {
                    if !text.trim().is_empty() {
                        break text.trim().to_owned();
                    }
                }
                if Instant::now() >= deadline {
                    return Err(HarnessError::new(
                        "the stand-in module never wrote its nonce",
                    ));
                }
                sleep(Duration::from_millis(25)).await;
            };
            Some(ConsumerIdentity {
                module_id: module.into(),
                launch_nonce: nonce,
            })
        } else {
            None
        };
        let connection = Arc::new(
            SubcConnection::connect_with_consumer_identity(&live.connection_file, identity)
                .await
                .map_err(|e| {
                    HarnessError::new(format!("connecting as {}: {e}", stamp.principal))
                })?,
        );
        connections.insert(stamp.principal.clone(), connection.clone());
        Ok(connection)
    }

    /// Open broca's management route for `session`, retrying while broca is still registering.
    async fn open(
        &self,
        connection: &SubcConnection,
        session: &broca_protocol::BindIdentity,
    ) -> Result<RouteHandle, HarnessError> {
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match connection.route_open_management("broca", session).await {
                Ok(route) => return Ok(route),
                Err(error) if Instant::now() >= deadline => {
                    return Err(HarnessError::new(format!(
                        "broca's management route never opened: {error}"
                    )))
                }
                Err(_) => sleep(Duration::from_millis(100)).await,
            }
        }
    }

    async fn open_scope(
        &self,
        connection: &SubcConnection,
        session: &broca_protocol::BindIdentity,
        scope: &cortexkit_role_harness::ScopeStamp,
    ) -> Result<RouteHandle, HarnessError> {
        let module = scope
            .owner
            .strip_prefix("reserved:")
            .ok_or_else(|| HarnessError::new("scope fixtures require a reserved owner"))?;
        let selector = subc_protocol::scope::ScopeSelector::new(
            subc_protocol::Principal::Reserved {
                module_id: module.into(),
            },
            &scope.scope_ref,
            Some(scope.scope_epoch),
        );
        connection
            .route_open_management_scoped_within(
                "broca",
                session,
                selector,
                broca_subc::RouteOpenRetry {
                    budget: Duration::from_secs(30),
                    max_interval: Duration::from_millis(100),
                },
            )
            .await
            .map_err(|error| {
                HarnessError::new(format!("opening the attested scope fixture: {error}"))
            })
    }

    /// The records of `session`'s open log on `live`'s state root.
    async fn records(&self, live: &Live, session: &str) -> Vec<Record> {
        stored_log::stored_records(&live.state_root, &self.bind_identity(session)).await
    }

    /// Whether `session`'s log holds exactly the durable state `point` names: what must be
    /// there, and nothing the next step would have written.
    ///
    /// For a point in a run's tool loop the counts are of the cut run alone: the records from
    /// the session's last `RunStarted` on. A scenario may run earlier turns on the same session
    /// first (the StepRecorded crash case runs one finished turn to read its lineage before the
    /// kill), and those runs must each have ended before the cut one started. The compaction
    /// points keep counting the whole WAL, as their scenarios run one turn per session.
    async fn holds_point(&self, live: &Live, session: &str, point: &str) -> Result<(), String> {
        use Record as R;
        let all = self.records(live, session).await;
        let current = all
            .iter()
            .rposition(|r| matches!(r, R::RunStart(_)))
            .unwrap_or(0);
        let (earlier, records) = all.split_at(current);
        let earlier_started = earlier
            .iter()
            .filter(|r| matches!(r, R::RunStart(_)))
            .count();
        let earlier_finished = earlier.iter().filter(|r| matches!(r, R::RunEnd(_))).count();
        if earlier_started != earlier_finished {
            return Err(format!(
                "session {session}'s log holds an earlier run that never finished before the one \
                 cut at {point}: {earlier_started} RunStarted, {earlier_finished} RunFinished"
            ));
        }
        let count = |want: fn(&R) -> bool| records.iter().filter(|r| want(r)).count();
        let started = count(|r| matches!(r, R::RunStart(_)));
        let steps = count(|r| matches!(r, R::StepEnd(_)));
        let intents = count(|r| matches!(r, R::ToolIntent(_)));
        let results = count(|r| matches!(r, R::ToolOutcome(_)));
        let finished = count(|r| matches!(r, R::RunEnd(_)));
        let ok = match point {
            points::ADMITTED | points::SEND_RECORDED => started == 1 && steps == 0,
            points::STEP_RECORDED => steps == 1 && intents == 0,
            points::DISPATCH_INTENT => intents == 1 && results == 0,
            points::TOOL_RESULT_RECORDED => results == 1 && steps == 1,
            points::TERMINAL => finished == 1,
            _ => false,
        };
        if ok {
            Ok(())
        } else {
            Err(format!(
                "session {session}'s log does not hold the state {point} names in its last run \
                 ({earlier_started} earlier finished runs): {started} RunStarted, {steps} steps, \
                 {intents} intents, {results} results, {finished} RunFinished"
            ))
        }
    }
}

#[async_trait]
impl Harness for BrocaSubject {
    type Handle = Live;
    type Route = BrocaRoute;

    fn declared_points(&self) -> Vec<PointDeclaration> {
        if skip_crash_kills() || self.process_kills_disabled {
            return Vec::new();
        }
        // StepRecorded is not declared: the run loop writes a step's dispatch intents right after
        // the step, with nothing between them the rig can hold, so no process kill lands
        // there. The store's in-process crash-cut tests cut its log at that boundary.
        let points: Vec<_> = [
            points::ADMITTED,
            points::SEND_RECORDED,
            points::DISPATCH_INTENT,
            points::TOOL_RESULT_RECORDED,
            points::TERMINAL,
        ]
        .into_iter()
        .map(|point| PointDeclaration {
            point: KillPoint::new(point),
            mechanisms: vec![KillMechanism::ProcessSignal],
            note: "SIGKILL of the ck-broca process while the rig holds the next thing it \
                       waits on, after the session's log is checked to hold exactly the point's \
                       state. broca's run start holds the send with its send_id, so Admitted \
                       and SendRecorded cut at the same place."
                .into(),
        })
        .collect();
        #[cfg(feature = "test-clock")]
        let points = {
            let mut points = points;
            points.push(PointDeclaration {
                point: KillPoint::new(points::RETENTION_TOMBSTONED),
                mechanisms: vec![KillMechanism::ProcessSignal],
                note: "A session head folder the daemon can't write stops a retention deletion \
                       after its expiry marker is durable and before the session is removed. \
                       SIGKILL follows observation of that marker with the session whole; the \
                       folder is made writable again only after the process dies, before \
                       restart."
                    .into(),
            });
            points
        };
        points
    }

    async fn spawn(&self, state_root: &Path) -> Result<Live, HarnessError> {
        self.start(state_root).await
    }

    async fn route(&self, handle: &Live, stamp: &RouteStamp) -> Result<BrocaRoute, HarnessError> {
        self.session_route(handle, "conformance-default", stamp)
            .await
    }

    async fn kill_at(
        &self,
        mut handle: Live,
        point: &KillPoint,
        trigger: Trigger<'_>,
    ) -> Result<KillReport, HarnessError> {
        #[cfg(feature = "test-clock")]
        if point.as_str() == points::RETENTION_TOMBSTONED {
            return self.kill_retention(handle, point, trigger).await;
        }
        let name: &'static str = points::ALL
            .iter()
            .copied()
            .find(|known| *known == point.as_str())
            .ok_or_else(|| HarnessError::new(format!("{point} is not a declared point")))?;
        let reached = Arc::new(Notify::new());
        let session = Arc::new(Mutex::new(None));
        let mut trigger = trigger;
        if name == points::TERMINAL {
            // A run's terminal is the last thing it writes, and the suite's trigger returns once
            // it reads the run as ended, which it does from that durable record.
            trigger.await;
            let session = self
                .shared
                .last_session
                .lock()
                .unwrap()
                .clone()
                .ok_or_else(|| HarnessError::new("the model answered no request"))?;
            if let Err(problem) = self.holds_point(&handle, &session, name).await {
                handle.kill_all();
                return Err(HarnessError::new(problem));
            }
        } else {
            *self.shared.armed.lock().unwrap() = Some(Armed {
                point: name,
                reached: reached.clone(),
                session: session.clone(),
            });
            tokio::select! {
                () = &mut trigger => {
                    *self.shared.armed.lock().unwrap() = None;
                    return Err(HarnessError::new(format!(
                        "the trigger finished without the runner reaching {point}"
                    )));
                }
                () = reached.notified() => {}
            }
            let session = session.lock().unwrap().clone().expect("set when reached");
            if let Err(problem) = self.holds_point(&handle, &session, name).await {
                handle.kill_all();
                self.shared.released.notify_waiters();
                return Err(HarnessError::new(problem));
            }
        }
        let killed = handle.kill_all();
        self.shared.released.notify_waiters();
        drop(handle);
        if !killed {
            return Err(HarnessError::new("no ck-broca process was found to kill"));
        }
        Ok(KillReport {
            point: point.clone(),
            mechanism: KillMechanism::ProcessSignal,
        })
    }

    async fn restart(&self, state_root: &Path) -> Result<Live, HarnessError> {
        // The routed-account runners of this rig's sessions restart with it, so a case that
        // restarts its rig restarts the runner serving its routed head too.
        routed_accounts::stop_under(self, state_root).await;
        self.start(state_root).await
    }
}

#[tokio::test]
#[cfg(feature = "test-clock")]
async fn no_kill_mode_changes_only_points_not_capabilities() {
    let root = broca_test_support::scratch_root().join("fold-recorded-declaration");
    std::fs::create_dir_all(&root).unwrap();
    let mut subject = BrocaSubject::new(&root).await;
    let capabilities = subject.capabilities();
    subject.process_kills_disabled = true;
    assert_eq!(
        subject.capabilities(),
        capabilities,
        "no-kill mode changed declared groups"
    );
    assert!(
        subject.declared_points().is_empty(),
        "the no-kill kit subset declared a kill"
    );
}

/// Numbers come from captured model-request prefixes, never from entry ordinals or keys.
/// Matching against the observed read body rejects changed or ambiguous fixture content.
fn captured_part_numbers(message: &Value) -> (Value, Vec<Option<u64>>) {
    fn strip(value: &mut Value) -> Option<u64> {
        let text = value.as_str()?;
        let (number, rest) = text.strip_prefix('§')?.split_once("§ ")?;
        let number: u64 = number.parse().ok()?;
        if number == 0 {
            return None;
        }
        *value = rest.to_owned().into();
        Some(number)
    }
    let mut cleaned = message.clone();
    let numbers = cleaned["content"]
        .as_array_mut()
        .into_iter()
        .flatten()
        .map(|part| {
            if part["type"] == "tool_result" {
                strip(&mut part["output"]["text"])
            } else {
                strip(&mut part["text"])
            }
        })
        .collect();
    (cleaned, numbers)
}

/// A session-bound management route to the real `ck-broca`, through the real daemon.
struct BrocaRoute {
    subject: BrocaSubject,
    connection: Arc<SubcConnection>,
    route: RouteHandle,
    session: broca_protocol::BindIdentity,
    describe_calls: std::sync::atomic::AtomicUsize,
    logs_dir: PathBuf,
    _routed_live: Option<Arc<Live>>,
}

fn scripted_usage_chunk(id: &str, usage: &ScriptedUsage, output_cap: u64) -> Value {
    let prompt = usage
        .input_tokens
        .saturating_add(usage.cache_read_tokens)
        .saturating_add(usage.cache_write_tokens);
    let output = usage.output_tokens.min(output_cap);
    let mut event = json!({"id":id,"object":"chat.completion.chunk","choices":[],"usage":{
        "prompt_tokens":prompt,"completion_tokens":output,"total_tokens":prompt.saturating_add(output),
        "prompt_tokens_details":{"cached_tokens":usage.cache_read_tokens,"cache_write_tokens":usage.cache_write_tokens}
    }});
    if let Some(reasoning) = usage.reasoning_tokens {
        event["usage"]["completion_tokens_details"] =
            json!({"reasoning_tokens":reasoning.min(output)});
    }
    event
}

#[test]
fn scripted_usage_chunk_preserves_reasoning_as_an_output_breakdown() {
    let usage = ScriptedUsage::new(7, 2, 1, 10).with_reasoning_tokens(3);
    let event = scripted_usage_chunk("id", &usage, u64::MAX);
    assert_eq!(event["usage"]["completion_tokens"], 10);
    assert_eq!(
        event["usage"]["completion_tokens_details"]["reasoning_tokens"],
        3
    );
    let capped = scripted_usage_chunk("id", &usage, 2);
    assert_eq!(capped["usage"]["completion_tokens"], 2);
    assert_eq!(
        capped["usage"]["completion_tokens_details"]["reasoning_tokens"],
        2
    );
    let event = scripted_usage_chunk("id", &ScriptedUsage::new(7, 2, 1, 10), u64::MAX);
    assert!(event["usage"].get("completion_tokens_details").is_none());
}

// Decode the kit's error type from wire bytes, independently of the connection's protocol version.
fn error_body<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> T {
    serde_json::from_slice(bytes).unwrap_or_else(|_| {
        serde_json::from_value(
            json!({"code":"undecodable_error", "message":String::from_utf8_lossy(bytes)}),
        )
        .expect("the fallback refusal has a valid protocol error shape")
    })
}

#[test]
fn role_kit_error_body_keeps_served_refusal_fields() {
    let wire = json!({"code":"invalid_params","message":"bad selector","detail":{"field":"preset","class":"permanent"}});
    let Reply::Error(body) = Reply::Error(error_body(&serde_json::to_vec(&wire).unwrap())) else {
        unreachable!()
    };
    assert_eq!(serde_json::to_value(body).unwrap(), wire);
    let Reply::Error(body) = Reply::Error(error_body(b"not-json")) else {
        unreachable!()
    };
    assert_eq!(body.code, "undecodable_error");
    assert_eq!(body.message, "not-json");
}

impl BrocaRoute {
    /// Capture the actual served JSON without reordering its members. Only the build identity
    /// is normalized, because the golden intentionally survives package-version bumps.
    async fn describe_golden_bytes(&self) -> Vec<u8> {
        #[derive(serde::Deserialize)]
        struct RawReply<'a> {
            #[serde(borrow)]
            result: &'a serde_json::value::RawValue,
        }
        let response = self
            .connection
            .route_request_terminal(
                &self.route,
                serde_json::to_vec(&json!({"method":"role.describe","params":{}})).unwrap(),
            )
            .await
            .unwrap()
            .unwrap();
        let reply: RawReply<'_> = serde_json::from_slice(&response).unwrap();
        let wire = reply.result.get();
        let value: Value = serde_json::from_str(wire).unwrap();
        let version =
            serde_json::to_string(value["implementation_version"].as_str().unwrap()).unwrap();
        let old = format!("\"implementation_version\":{version}");
        assert_eq!(wire.matches(&old).count(), 1);
        wire.replacen(
            &old,
            "\"implementation_version\":\"0.0.0-writer-golden\"",
            1,
        )
        .into_bytes()
    }
}

#[async_trait]
impl RunnerRoute for BrocaRoute {
    async fn observe_message_started(
        &self,
        mut operation: std::pin::Pin<
            &mut (dyn std::future::Future<Output = Result<Reply, RouteFailure>> + Send),
        >,
    ) -> Result<Option<(Reply, Vec<(Value, Reply)>)>, RouteFailure> {
        let route = self
            .connection
            .route_open_management("broca", &self.session)
            .await
            .map_err(|error| RouteFailure::new(error.to_string()))?;
        let mut stream = self
            .connection
            .subscribe(
                &route,
                serde_json::to_vec(&json!({"method":"session.subscribe","params":{"from":"live"}}))
                    .unwrap(),
            )
            .await
            .map_err(|error| RouteFailure::new(error.to_string()))?;
        // The initial reset confirms registration of the live receiver. Starting the send
        // earlier could miss the message_started event whose sender binding is being checked.
        let first = stream
            .next()
            .await
            .map_err(|error| RouteFailure::new(error.to_string()))?;
        if first.is_none() {
            return Err(RouteFailure::new(
                "live observation ended before attachment",
            ));
        }
        let mut reply = None;
        let mut rows = Vec::new();
        loop {
            let event = if reply.is_none() {
                tokio::select! {
                    biased;
                    event = stream.next() => event,
                    result = operation.as_mut() => { reply = Some(result?); continue; }
                }
            } else {
                match tokio::time::timeout(Duration::from_millis(750), stream.next()).await {
                    Ok(event) => event,
                    Err(_) => break,
                }
            }
            .map_err(|error| RouteFailure::new(error.to_string()))?;
            let Some(event) = event else { break };
            let event: Value = serde_json::from_slice(&event)
                .map_err(|error| RouteFailure::new(error.to_string()))?;
            if event["event"]["type"] == "message_started" {
                let mid = event["event"]["mid"]
                    .as_str()
                    .ok_or_else(|| RouteFailure::new("message_started has no mid"))?;
                let row = self
                    .request(
                        "session.history",
                        json!({"from_mid":mid,"limit":1,"form":"display"}),
                    )
                    .await?;
                rows.push((event, row));
            }
        }
        drop(stream);
        let _ = self.connection.route_close(&route).await;
        Ok(Some((
            reply.ok_or_else(|| {
                RouteFailure::new("observation stream ended before the operation")
            })?,
            rows,
        )))
    }
    async fn request(&self, method: &str, mut params: Value) -> Result<Reply, RouteFailure> {
        if method == "session.send" {
            if let Some(key) = self
                .subject
                .shared
                .fork_head_keys
                .lock()
                .unwrap()
                .get(&self.session.session)
            {
                params["conversation_key"] = json!(key);
            }
        }
        let _fork_capture = fork_answer::AskCapture::new(
            &self.subject.shared,
            &self.session.session,
            method,
            &params,
        );
        let polled_run = (method == "run.result")
            .then(|| params["run_id"].as_str().map(str::to_owned))
            .flatten();
        let body = serde_json::to_vec(&json!({"method": method, "params": params})).unwrap();
        let started = Instant::now();
        let describe_call = (method == "role.describe").then(|| {
            self.describe_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1
        });
        let terminal = self
            .connection
            .route_request_terminal(&self.route, body)
            .await;
        if let Some(call) = describe_call {
            eprintln!(
                "role.describe session={} call={} channel={} epoch={} elapsed_us={} transport={} workers={}",
                self.session.session,
                call,
                self.route.channel,
                self.route.epoch,
                started.elapsed().as_micros(),
                if terminal.is_ok() { "ok" } else { "failed" },
                tokio::runtime::Handle::current().metrics().num_workers(),
            );
            if terminal.is_err() {
                let (stale, unmatched) = broca_subc::FRAME_DROPS.counts();
                eprintln!("client process frame drops: stale_epoch={stale} unmatched_terminal={unmatched}");
                for dir in [&self.logs_dir, &client_log_dir()] {
                    match diagnostic_logs(dir) {
                        Ok(logs) if !logs.is_empty() => eprintln!("{logs}"),
                        Ok(_) => eprintln!("diagnostics absent: no segments in {}", dir.display()),
                        Err(error) => {
                            eprintln!("diagnostics unavailable: {}: {error}", dir.display())
                        }
                    }
                }
            }
        }
        match terminal.map_err(|e| RouteFailure::new(e.to_string()))? {
            Ok(response) => {
                let value: Value = serde_json::from_slice(&response)
                    .map_err(|e| RouteFailure::new(format!("an undecodable response: {e}")))?;
                if let Some(run_id) = &polled_run {
                    self.observe_run_result(run_id, &value["result"]).await;
                }
                if method == "session.read" {
                    let mut mids = self.subject.shared.read_mids.lock().unwrap();
                    for entry in value["result"]["messages"].as_array().into_iter().flatten() {
                        if let Some(mid) = entry["mid"].as_str() {
                            mids.insert(entry["message"].to_string(), mid.to_owned());
                            let keys = entry["part_keys"]
                                .as_array()
                                .map(|keys| {
                                    keys.iter()
                                        .filter_map(Value::as_str)
                                        .map(str::to_owned)
                                        .collect::<Vec<_>>()
                                })
                                .or_else(|| {
                                    entry["part_digests"].as_array().map(|parts| {
                                        parts
                                            .iter()
                                            .filter_map(|part| part["part"].as_str())
                                            .map(str::to_owned)
                                            .collect()
                                    })
                                });
                            if let Some(keys) = keys {
                                self.subject
                                    .shared
                                    .read_part_keys
                                    .lock()
                                    .unwrap()
                                    .insert(entry["message"].to_string(), keys);
                            }
                            self.subject.shared.read_entries.lock().unwrap().insert(
                                (self.session.session.clone(), mid.to_owned()),
                                entry.clone(),
                            );
                        }
                    }
                }
                Ok(Reply::Response(value["result"].clone()))
            }
            Err(error) => Ok(Reply::Error(error_body(&error))),
        }
    }

    async fn subscribe(&self, params: Value) -> Result<SubscribeOutcome, RouteFailure> {
        // A subscription holds its route's channel, so it gets a route of its own.
        let route = self
            .connection
            .route_open_management("broca", &self.session)
            .await
            .map_err(|e| RouteFailure::new(e.to_string()))?;
        let body =
            serde_json::to_vec(&json!({"method": "session.subscribe", "params": params})).unwrap();
        let mut stream = self
            .connection
            .subscribe(&route, body)
            .await
            .map_err(|e| RouteFailure::new(e.to_string()))?;
        let mut events = Vec::new();
        let outcome = loop {
            match tokio::time::timeout(Duration::from_millis(750), stream.next()).await {
                Err(_) | Ok(Ok(None)) => break SubscribeOutcome::Events(events),
                Ok(Ok(Some(event))) => events.push(
                    serde_json::from_slice(&event)
                        .map_err(|e| RouteFailure::new(format!("an undecodable event: {e}")))?,
                ),
                Ok(Err(broca_subc::ConnError::Rejected(rejection))) => {
                    break SubscribeOutcome::Refused(
                        serde_json::from_value(json!({"code":rejection.code(),"message":rejection.message(),"detail":rejection.provider_detail()})).map_err(
                            |error| RouteFailure::new(format!("an undecodable refusal: {error}")),
                        )?,
                    );
                }
                Ok(Err(error)) => return Err(RouteFailure::new(error.to_string())),
            }
        };
        drop(stream);
        let _ = self.connection.route_close(&route).await;
        Ok(outcome)
    }
}

#[async_trait]
impl LlmRunnerSubject for BrocaSubject {
    async fn step_event_observation(
        &self,
        handle: &Live,
        session: &str,
    ) -> Result<
        cortexkit_role_llm_runner_conformance::step_events::StepEventObservation,
        HarnessError,
    > {
        step_events::observe(self, handle, session).await
    }

    async fn call_yield_admission_state(
        &self,
        session: &str,
    ) -> Result<Option<Vec<u8>>, HarnessError> {
        let root = self
            .shared
            .session_roots
            .lock()
            .unwrap()
            .get(session)
            .cloned()
            .ok_or_else(|| HarnessError::new(format!("no state root for session {session}")))?;
        // Refusal must leave the session's log (including its baseline, reservations and
        // queued sends) untouched, not merely return an error reply.
        let records = stored_log::stored_records(&root, &self.bind_identity(session)).await;
        // Catalog requests are observed at the provider, independently of runner replies.
        let requests = self
            .shared
            .catalog_requests
            .lock()
            .unwrap()
            .get(session)
            .cloned()
            .unwrap_or_default();
        serde_json::to_vec(&(format!("{records:?}"), requests))
            .map(Some)
            .map_err(|error| HarnessError::new(error.to_string()))
    }

    async fn notices_stored_output(
        &self,
        handle: &Self::Handle,
        session: &str,
        arguments: &Value,
    ) -> Result<(String, u32, String, String), HarnessError> {
        let records = self.records(handle, session).await;
        // Stored arguments are canonical JSON text, so equal values have equal text.
        let wanted = broca_protocol::json::Json::from_value(arguments);
        // An intent keeps its arguments only when a hook or approval changed them; otherwise
        // they are the call's own input, on the assistant message that made the call.
        let calls_with_arguments: Vec<broca_protocol::ToolCallId> = records
            .iter()
            .filter_map(|record| match record {
                Record::Assistant(assistant) => Some(assistant.message.content.iter()),
                _ => None,
            })
            .flatten()
            .filter_map(|block| match block {
                broca_protocol::content::ContentBlock::ToolCall(call) if call.input == wanted => {
                    Some(call.tool_call_id.clone())
                }
                _ => None,
            })
            .collect();
        let (call, key) = records
            .iter()
            .find_map(|record| match record {
                Record::ToolIntent(intent)
                    if match &intent.args {
                        Some(args) => *args == wanted,
                        None => calls_with_arguments.contains(&intent.tool_call_id),
                    } =>
                {
                    Some((intent.tool_call_id.clone(), intent.call_key.clone()))
                }
                _ => None,
            })
            .ok_or_else(|| HarnessError::new("notice carrying intent missing"))?;
        let key = key.ok_or_else(|| HarnessError::new("native carrying key missing"))?;
        let output = records
            .iter()
            .find_map(|record| match record {
                Record::ToolReply(reply) if reply.result.tool_call_id == call => {
                    match &reply.result.output {
                        broca_protocol::ToolOutput::Text { text } => Some(text.clone()),
                        broca_protocol::ToolOutput::Json { value } => {
                            value.parse().as_str().map(str::to_owned)
                        }
                        _ => None,
                    }
                }
                _ => None,
            })
            .ok_or_else(|| HarnessError::new("native notice result text missing"))?;
        // The call's part is its place among its batch's planned calls; its message is the tool
        // message the batch's replies were written in, named by the entry facts that follow.
        let part = records
            .iter()
            .find_map(|record| match record {
                Record::BatchStart(batch) => batch
                    .planned
                    .iter()
                    .position(|id| *id == call)
                    .and_then(|part| u32::try_from(part).ok()),
                _ => None,
            })
            .ok_or_else(|| HarnessError::new("native carrying part missing"))?;
        let mut replied = false;
        let mut mid = None;
        for record in &records {
            match record {
                Record::ToolReply(reply) if reply.result.tool_call_id == call => replied = true,
                Record::EntryFacts(facts) if replied => {
                    mid = Some(facts.mid.clone());
                    break;
                }
                _ => {}
            }
        }
        let mid = mid.ok_or_else(|| HarnessError::new("native carrying message missing"))?;
        Ok((mid, part, key, output))
    }
    async fn routing_runner_declaration(&self) -> Result<Value, HarnessError> {
        Ok(routed_accounts::runner_declaration())
    }

    async fn routed_account_fixture(&self, method: &str) -> Result<Value, HarnessError> {
        routed_accounts::fixture(method)
    }

    async fn install_routing_peer(
        &self,
        session: &str,
        peer: cortexkit_role_llm_runner_conformance::RecordingRoutingPeer,
    ) -> Result<(), HarnessError> {
        let live = self
            .routed_instances
            .lock()
            .await
            .get(session)
            .and_then(std::sync::Weak::upgrade);
        if live.is_none() {
            return Err(HarnessError::new(
                "routed-account session has no vault-backed runner",
            ));
        }
        self.shared
            .routed_account_peers
            .lock()
            .unwrap()
            .insert(session.into(), peer);
        Ok(())
    }

    async fn configure_fork_head_key(
        &self,
        session: &str,
        key: &str,
    ) -> Result<bool, HarnessError> {
        self.shared
            .fork_head_keys
            .lock()
            .unwrap()
            .insert(session.into(), key.into());
        Ok(true)
    }

    async fn head_selection_models(
        &self,
        session: &str,
    ) -> Result<Option<[cortexkit_role_llm_runner::head_selection::PlanModel; 2]>, HarnessError>
    {
        use cortexkit_role_llm_runner::{head_selection::PlanModel, service_tier::ServiceTier};
        self.shared
            .head_selection_sessions
            .lock()
            .unwrap()
            .insert(session.into());
        self.shared.service_tier_keys.lock().unwrap().insert(
            broca_provider::hash::clamp_prompt_cache_key(&format!("tier-fixture-{session}")),
            session.into(),
        );
        let mut selected = PlanModel::new("xai", "conformance-service-tier");
        if self.service_tier_enabled {
            selected.service_tier = Some(ServiceTier::Fast);
        }
        Ok(Some([
            selected,
            PlanModel::new("xai", "head-selection-unavailable"),
        ]))
    }

    fn model_finish_reason_source(&self) -> Option<&'static str> {
        Some("openai.chat_completions")
    }

    async fn configure_service_tier(
        &self,
        session: &str,
        _case: &str,
    ) -> Result<bool, HarnessError> {
        if !self.service_tier_enabled {
            return Ok(false);
        }
        self.shared
            .service_tier_sessions
            .lock()
            .unwrap()
            .insert(session.to_owned());
        self.shared.service_tier_keys.lock().unwrap().insert(
            broca_provider::hash::clamp_prompt_cache_key(&format!("tier-fixture-{session}")),
            session.to_owned(),
        );
        self.shared
            .failure_turns
            .lock()
            .unwrap()
            .insert(session.to_owned(), 0);
        Ok(true)
    }
    fn attachment_model_fields(&self, session: &str, accepts: bool) -> Option<Map<String, Value>> {
        self.shared
            .attachment_models
            .lock()
            .unwrap()
            .insert(session.into(), accepts);
        Some(
            json!({"model":{"provider":"xai","model":format!("conformance-media-{accepts}")}})
                .as_object()
                .unwrap()
                .clone(),
        )
    }

    fn attachment_tool_result(&self, png: &[u8]) -> Option<Value> {
        use base64::Engine;
        let image = broca_core::media_validation::validate_media(png, "image/png").unwrap();
        Some(json!({OWN_CONTENT:[{"type":"text","text":"before-media"},
            {"type":"image","mimeType":"image/png","data":base64::engine::general_purpose::STANDARD.encode(png),"width":image.width,"height":image.height},
            {"type":"text","text":"after-media"}]}))
    }

    async fn restart_attachments(
        &self,
        handle: &Live,
        session: &str,
    ) -> Result<BrocaRoute, HarnessError> {
        // Model the permitted loss of unfinished staging, retaining committed uploads and
        // captured objects. Process-kill recovery of captured media has its own native test.
        let staging = handle.state_root.join("media-uploads");
        for entry in
            std::fs::read_dir(staging.join("refs")).map_err(|e| HarnessError::new(e.to_string()))?
        {
            let entry = entry.map_err(|e| HarnessError::new(e.to_string()))?;
            let bytes =
                std::fs::read(entry.path()).map_err(|e| HarnessError::new(e.to_string()))?;
            let mut meta: Value =
                serde_json::from_slice(&bytes).map_err(|e| HarnessError::new(e.to_string()))?;
            if meta["committed"] == false {
                meta["gone"] = json!(true);
                std::fs::write(entry.path(), serde_json::to_vec(&meta).unwrap())
                    .map_err(|e| HarnessError::new(e.to_string()))?;
            }
        }
        self.session_route(handle, session, &self.owner_stamp())
            .await
    }
    fn supports_model_failure_fixture(&self) -> bool {
        true
    }
    fn runner_id(&self) -> &str {
        "broca"
    }

    async fn hold_next_model_stream(
        &self,
        session: &str,
        parts: usize,
    ) -> Result<(), HarnessError> {
        self.shared
            .model_holds
            .lock()
            .unwrap()
            .insert(session.into(), Arc::default());
        self.shared
            .stream_holds
            .lock()
            .unwrap()
            .insert(session.into(), parts);
        Ok(())
    }
    async fn hold_next_model_step(&self, session: &str) -> Result<(), HarnessError> {
        self.shared
            .model_holds
            .lock()
            .unwrap()
            .insert(session.into(), Arc::default());
        Ok(())
    }

    async fn await_model_step(&self, session: &str) -> Result<(), HarnessError> {
        let gate = self
            .shared
            .model_holds
            .lock()
            .unwrap()
            .get(session)
            .cloned()
            .ok_or_else(|| HarnessError::new("no model hold installed"))?;
        tokio::time::timeout(
            Duration::from_secs(30),
            HoldGate::wait(&gate.arrived, &gate.arrived_wake),
        )
        .await
        .map_err(|_| HarnessError::new("model did not reach its hold"))
    }

    async fn release_model_step(&self, session: &str) -> Result<(), HarnessError> {
        let gate = self
            .shared
            .model_holds
            .lock()
            .unwrap()
            .remove(session)
            .ok_or_else(|| HarnessError::new("no model hold installed"))?;
        HoldGate::set(&gate.released, &gate.released_wake);
        Ok(())
    }

    async fn model_invocations(&self, session: &str) -> Result<usize, HarnessError> {
        Ok(self
            .shared
            .raw_bodies
            .lock()
            .unwrap()
            .get(session)
            .map_or(0, Vec::len))
    }
    fn capabilities(&self) -> BTreeSet<Capability> {
        let mut capabilities: BTreeSet<_> = [
            Capability::TranscriptReads,
            Capability::HostHistory,
            Capability::HistoryDisplay,
            Capability::RunOps,
            Capability::RunToolCalls,
            Capability::StructuredOutput,
            Capability::SendStatus,
            Capability::MaxSteps,
            Capability::Temperature,
            Capability::NoTools,
            Capability::RunStatus,
            Capability::FinishReason,
            Capability::RunUsage,
            Capability::SessionUsage,
            Capability::HeadSelection,
            Capability::RoutedAccounts,
            Capability::AccountRoute,
            Capability::AdmissionReports,
            Capability::RunCancel,
            Capability::Retract,
            Capability::DispatchAttribution,
            Capability::Streaming,
            Capability::LiveEvents,
            Capability::Steer,
            Capability::Queue,
            Capability::Interrupt,
            Capability::InterruptReason,
            Capability::TurnDirectives,
            Capability::HoldToolCalls,
            Capability::Plans,
            Capability::RenderedModelInput,
            Capability::DeliveryOrder,
            Capability::SenderSuccession,
            Capability::ReadDisplayFacts,
            Capability::SendIds,
            Capability::StepEvents,
            Capability::ClientMessageId,
            // The adapter implements the three session_change subject hooks (a tool-free
            // refresh plan, model request observations and holdable tool calls), so the kit
            // runs its cases. The host fixes no tool set, so the outside-host-set case
            // reports inapplicable by name.
            Capability::SessionChange,
            // role.describe declares system_text_default (tool-provider 0.12), and the
            // adapter implements both of its hooks, so the kit runs those cases.
            Capability::SystemTextDefault,
            // role.describe declares session_class. Its cases drive ordinary sends and need no
            // subject hook.
            Capability::SessionClass,
            // role.describe declares plan_view. Its cases read session.plan_view through
            // ordinary routes and need nothing more from the subject.
            Capability::PlanView,
        ]
        .into_iter()
        .collect();
        if broca_core::delivery_order::RIDES_V2_WRITER_ENABLED {
            capabilities.insert(Capability::Notices);
        }
        // Decoding retained logs does not mean session.send accepts retention. Advertise
        // retention and its daemon clock only when the production writer switch enables
        // both admission and the retention group in role.describe.
        if broca_module_serve::RETENTION_WRITER_ENABLED {
            capabilities.extend([Capability::Retention, Capability::RetentionClock]);
        }
        if self.service_tier_enabled {
            capabilities.insert(Capability::ServiceTier);
        }
        // role.describe declares the group from the same switch.
        if broca_protocol::FORK_ANSWER_SERVED {
            capabilities.insert(Capability::ForkAnswer);
        }
        // Admission and role.describe read the same switch for combined and named marks.
        if broca_core::scope_admission::NOTICE_WITH_SPEAKS_FOR {
            capabilities.insert(Capability::NoticeWithSpeaksFor);
        }
        capabilities.extend([
            Capability::Attachments,
            Capability::MediaOnlySend,
            Capability::MessageMarks,
            Capability::MediaRefs,
        ]);
        capabilities
    }

    fn reports_read_completed_at(&self) -> bool {
        true
    }

    async fn seed_host_created_at(
        &self,
        _session: &str,
        _mid: &str,
        _created_at_ms: u64,
    ) -> Result<bool, HarnessError> {
        // Broca has no operation to rewrite an existing host acceptance/request stamp.
        // Editing the rebuildable read cache would fabricate a fact not backed by WAL.
        eprintln!("seed_host_created_at unavailable: recorded host facts are immutable; no independent host-creation write path exists, and cache-only edits are not host facts");
        Ok(false)
    }

    fn scoped_session_start_stamps(&self) -> Option<(RouteStamp, RouteStamp)> {
        Some(scope_fixture_stamps(PLAIN_SCOPE_REF))
    }

    fn flow_session_start_stamps(&self) -> Option<(RouteStamp, RouteStamp)> {
        Some(scope_fixture_stamps(FLOW_SCOPE_REF))
    }

    fn session_usage_fixtures(&self) -> bool {
        true
    }

    fn head_sender_identities(
        &self,
    ) -> Option<[cortexkit_role_llm_runner_conformance::PlanSenderIdentity; 2]> {
        let identity = |module: &str| {
            cortexkit_role_llm_runner_conformance::PlanSenderIdentity::new(
                RouteStamp {
                    principal: format!("reserved:{module}"),
                    scope: None,
                },
                module,
            )
        };
        Some([identity(SENDER_MODULE), identity(HEAD_SENDER_MODULE)])
    }

    fn owner_stamp(&self) -> RouteStamp {
        // A direct caller does not freeze an attested owner, so using one here would make
        // stranger cancellation legitimately admissible on an otherwise unowned session.
        RouteStamp {
            principal: format!("reserved:{OWNER_MODULE}"),
            scope: None,
        }
    }

    fn stranger_stamp(&self) -> RouteStamp {
        RouteStamp {
            principal: format!("reserved:{STRANGER_MODULE}"),
            scope: None,
        }
    }

    async fn session_route(
        &self,
        handle: &Live,
        session: &str,
        stamp: &RouteStamp,
    ) -> Result<BrocaRoute, HarnessError> {
        let stamp = message_marks_route_stamp(session, stamp);
        let routed_live = routed_accounts::session_live(self, handle, session).await?;
        let handle = routed_live.as_deref().unwrap_or(handle);
        let connection = self.connection(handle, &stamp).await?;
        self.shared
            .session_roots
            .lock()
            .unwrap()
            .insert(session.to_owned(), handle.state_root.clone());
        let identity = self.raw_bind_identity(session);
        let started = Instant::now();
        let route = match &stamp.scope {
            Some(scope) => self.open_scope(&connection, &identity, scope).await?,
            None => self.open(&connection, &identity).await?,
        };
        if stamp
            .scope
            .as_ref()
            .is_some_and(|scope| scope.scope_ref == FLOW_SCOPE_REF)
        {
            self.shared
                .flow_scope_sessions
                .lock()
                .unwrap()
                .insert(session.into());
        }
        self.shared
            .session_bindings
            .lock()
            .unwrap()
            .insert(session.into(), self.bind_identity(session));
        eprintln!(
            "route.open session={} channel={} epoch={} elapsed_us={}",
            session,
            route.channel,
            route.epoch,
            started.elapsed().as_micros(),
        );
        Ok(BrocaRoute {
            subject: self.clone(),
            connection,
            route,
            session: identity,
            describe_calls: std::sync::atomic::AtomicUsize::new(0),
            logs_dir: handle.logs_dir.clone(),
            _routed_live: routed_live,
        })
    }

    fn send_fields(&self, session: &str, first: bool) -> Map<String, Value> {
        if let Some(peer) = self
            .shared
            .routed_account_peers
            .lock()
            .unwrap()
            .get(session)
        {
            let mut fields = routed_accounts::send_fields(session, first, peer);
            if let Some(key) = self.shared.fork_head_keys.lock().unwrap().get(session) {
                fields.insert("conversation_key".into(), json!(key));
            }
            if first && step_events::fixture_session(session) {
                // The new-head path records durable attempt identities. The fixture's daemon
                // uses plain storage because its vault has no deployment key.
                fields.insert("session_class".into(), json!("head"));
            }
            return fields;
        }
        if self
            .shared
            .head_selection_sessions
            .lock()
            .unwrap()
            .contains(session)
        {
            let mut fields = json!({"cache":{"key":format!("tier-fixture-{session}")}});
            if first {
                fields["model"] =
                    json!({"provider":"mock","model":format!("{PLAN_MODEL_PREFIX}{session}")});
                let composition = json!({"providers":[],"step_transform_items":[]});
                fields["plan"] = json!({"tool_items":[],"system_text_items":[],"step_transform_items":[],
                    "composition":composition,"composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap()});
            }
            return fields.as_object().unwrap().clone();
        }
        if self
            .shared
            .service_tier_sessions
            .lock()
            .unwrap()
            .contains(session)
        {
            let mut fields = Map::new();
            fields.insert(
                "model".into(),
                json!({"provider":"xai","model":"conformance-service-tier"}),
            );
            // Cache scope carries fixture identity without adding system text beside a plan.
            fields.insert(
                "cache".into(),
                json!({"key":format!("tier-fixture-{session}")}),
            );
            return fields;
        }
        if session.contains("tool_result_too_large")
            && !self.shared.plan_sessions.lock().unwrap().contains(session)
        {
            return self
                .send_fields_with_plan(session, first, &Map::new())
                .unwrap();
        }
        // A plan_view case needs a plan in its params; the case that sends none removes it.
        if (session_change_kit_session(session) || plan_view_session(session))
            && !self.shared.plan_sessions.lock().unwrap().contains(session)
        {
            return self
                .send_fields_with_plan(session, first, &Map::new())
                .unwrap();
        }
        if message_marks_kit_session(session)
            && !self.shared.plan_sessions.lock().unwrap().contains(session)
        {
            return self
                .send_fields_with_plan(session, first, &Map::new())
                .unwrap();
        }
        if held_late_results::needs_plan(&self.shared, session)
            && !self.shared.plan_sessions.lock().unwrap().contains(session)
        {
            return self
                .send_fields_with_plan(session, first, &Map::new())
                .unwrap();
        }
        if let Some(accepts) = self
            .shared
            .attachment_models
            .lock()
            .unwrap()
            .get(session)
            .copied()
            .or_else(|| session.contains("attachments").then_some(true))
        {
            let mut fields =
                json!({"model":{"provider":"xai","model":format!("conformance-media-{accepts}")}});
            if !self.shared.plan_sessions.lock().unwrap().contains(session) {
                fields["system"] = json!(format!("{SESSION_MARKER}{session}"));
                if first {
                    fields["tools"] = json!([{"name":SCRIPTED_TOOL,"description":"Answers each call with its scripted result.","input_schema":{"type":"object"},"module":TOOL_MODULE}]);
                }
            }
            return fields.as_object().unwrap().clone();
        }
        // The pin fixture uses an admitted provider catalog. Caller-declared
        // legacy tools intentionally have no provider semantics to pin.
        if session.starts_with("cf-call-identity-pin-")
            && !self.shared.plan_sessions.lock().unwrap().contains(session)
        {
            return self
                .send_fields_with_plan(session, first, &Map::new())
                .unwrap();
        }
        if self
            .shared
            .output_refs_sessions
            .lock()
            .unwrap()
            .contains(session)
        {
            return json!({"model":{"provider":output_refs::PROVIDER,"model":output_refs::MODEL}})
                .as_object()
                .unwrap()
                .clone();
        }
        if self.shared.plan_sessions.lock().unwrap().contains(session) {
            return json!({"model":{"provider":"mock","model":format!("{PLAN_MODEL_PREFIX}{session}")}})
                .as_object().unwrap().clone();
        }
        // Legacy cases name tools directly; compaction cases use the plan-aware catalogue.
        let markerless = self
            .shared
            .model_id_marker_sessions
            .lock()
            .unwrap()
            .contains(session);
        // The kit's media fixtures script an answer as several text parts and check how reads
        // describe each part. Chat-completions text deltas concatenate into one part, so those
        // sessions use the Messages stream, whose content blocks keep each scripted part.
        let model = if media_fixture_session(session) {
            json!({"provider": output_refs::PROVIDER, "model": MEDIA_FIXTURE_MODEL})
        } else if session.starts_with("cf-temperature-") {
            json!({"provider": STRUCTURED_OUTPUT_PROVIDER, "model": "conformance-temperature"})
        } else if markerless {
            json!({"provider": "mock", "model": format!("{PLAN_MODEL_PREFIX}{session}")})
        } else {
            json!({"provider": "mock", "model": "mock-model"})
        };
        let mut fields = json!({
            "model": model,
            "tools": [{
                "name": SCRIPTED_TOOL,
                "description": "Answers each call with its scripted result.",
                "input_schema": {"type": "object"},
                "module": TOOL_MODULE,
            }],
        });
        if !first {
            fields.as_object_mut().unwrap().remove("tools");
        }
        // Native durability, reader-identity and retention fixtures depend on this original
        // system marker, including error sessions that never install a model script.
        if !markerless {
            fields["system"] = json!(format!("{SESSION_MARKER}{session}"));
        }
        fields.as_object().cloned().expect("an object")
    }

    fn send_fields_with_plan(
        &self,
        session: &str,
        first: bool,
        members: &Map<String, Value>,
    ) -> Option<Map<String, Value>> {
        let mut members = members.clone();
        // Harness principals include the reserved: label; plan sender modules use only
        // the daemon-attested module id, including the flow fixture's opener.
        if let Some(senders) = members.get_mut("senders").and_then(Value::as_array_mut) {
            // The kit names each call_yield case's session after the case, and arms its call
            // only after the plan is sent. The provider must declare late results and
            // `call.yield/v1` to the session from its first run, under a preset of its own:
            // broca keeps a provider's declarations for a minute by provider and preset, and
            // the preset is fixed by this plan. The case's sender may be unmarked, so the
            // session's name is the one signal every case carries.
            if session.contains("call_yield_") {
                held_late_results::mark_call_yield_session(&self.shared, session);
            }
            for sender in senders {
                let module = sender
                    .get("module")
                    .and_then(Value::as_str)
                    .and_then(|module| module.strip_prefix("reserved:"))
                    .map(str::to_owned);
                if let Some(module) = module {
                    sender["module"] = module.into();
                }
            }
        }
        self.shared
            .plan_sessions
            .lock()
            .unwrap()
            .insert(session.into());
        let mut fields = self.send_fields(session, first);
        if first {
            // The kit can promote params already carrying legacy tools into a plan send.
            // Null explicitly clears those legacy defaults; the plan owns this surface.
            fields.insert("tools".into(), Value::Null);
            fields.insert("system".into(), Value::Null);
            let flow = self
                .shared
                .flow_scope_sessions
                .lock()
                .unwrap()
                .contains(session);
            let providers = if flow {
                json!([])
            } else {
                json!([{"provider":TOOL_MODULE,"tools":[{"name":SCRIPTED_TOOL,"capabilities":if held_late_results::yields_session(&self.shared, session) { vec!["call.yield/v1"] } else { vec![] }}]}])
            };
            let composition = json!({"providers":providers,"step_transform_items":[]});
            let mut plan = json!({
                "tool_items":if flow {json!([])} else {json!([{"provider":TOOL_MODULE,"preset":held_late_results::preset(&self.shared, session),"params":{}}])},
                "system_text_items":[],
                "step_transform_items":[],
                "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
                "composition":composition,
            });
            if plan_view_session(session)
                || fork_answer::NEW_STORAGE_PLAN_MEMBERS
                    .iter()
                    .any(|member| members.contains_key(*member))
            {
                fields.insert("session_class".into(), json!("head"));
            }
            plan.as_object_mut().unwrap().extend(members.clone());
            fields.insert("plan".into(), plan);
        }
        Some(fields)
    }

    fn plan_sender(&self) -> Option<cortexkit_role_llm_runner_conformance::PlanSenderIdentity> {
        Some(
            cortexkit_role_llm_runner_conformance::PlanSenderIdentity::new(
                RouteStamp {
                    principal: format!("reserved:{SENDER_MODULE}"),
                    scope: None,
                },
                SENDER_MODULE,
            ),
        )
    }

    fn turn_system_supported(&self) -> bool {
        // The kit's scripted mock route speaks OpenAI Chat, not Responses. Its template
        // cases therefore exercise the required user-authority fall-through, without skips.
        false
    }

    /// A plan for `session.refresh` of `session`. `ChangedTools` is a tool-free plan: the
    /// session's first plan offers the scripted tool, so the next run offering no tools
    /// differs and the cases can tell whether it applied. The host fixes no tool set at
    /// start (broca never answers `tool_outside_host_set`), so both host-set variants are
    /// inapplicable by name.
    fn refresh_plan(
        &self,
        _session: &str,
        variant: cortexkit_role_llm_runner_conformance::RefreshVariant,
    ) -> Option<cortexkit_role_llm_runner_conformance::RefreshPlan> {
        use cortexkit_role_llm_runner_conformance::{RefreshPlan, RefreshVariant};
        match variant {
            RefreshVariant::ChangedTools => {
                let composition = serde_json::json!({"providers": [], "step_transform_items": []});
                let composition_digest =
                    cortexkit_role_tool_provider::catalog::composition_digest(&composition)
                        .expect("empty composition digests");
                let plan = serde_json::json!({
                    "tool_items": [],
                    "system_text_items": [],
                    "step_transform_items": [],
                    "composition_digest": composition_digest,
                    "composition": composition,
                });
                Some(RefreshPlan::new(
                    plan.as_object().unwrap().clone(),
                    Vec::new(),
                ))
            }
            RefreshVariant::OutsideHostSet | RefreshVariant::ChangedDefinition => None,
            _ => None,
        }
    }

    async fn rich_ui_plan_fields(
        &self,
        session: &str,
        stamps: &[Option<cortexkit_role_llm_runner::rich_ui::RichUiStamp>],
    ) -> Result<Option<Map<String, Value>>, HarnessError> {
        let connection = self
            .shared
            .fixture_connections
            .current()
            .ok_or_else(|| HarnessError::new("no private daemon is running"))?;
        let mut fields = self
            .send_fields_with_plan(session, true, &Map::new())
            .unwrap();
        let plan = fields.get_mut("plan").unwrap();
        for stamp in stamps {
            let module = format!(
                "conformance-rich-ui-{}",
                self.shared.default_text_tasks.lock().unwrap().len()
            );
            let mut manifest = tool_manifest();
            manifest.module_id = module.clone();
            let (_, serve) = subc_client_rs::serve_with_handle(
                &connection,
                manifest,
                DefaultTextProvider {
                    shared: self.shared.clone(),
                    session: session.into(),
                    rich_ui: stamp.clone(),
                },
            )
            .await
            .map_err(|error| HarnessError::new(error.to_string()))?;
            self.shared
                .default_text_tasks
                .lock()
                .unwrap()
                .push(tokio::spawn(async move {
                    let _ = serve.await;
                }));
            plan["system_text_items"]
                .as_array_mut()
                .unwrap()
                .push(json!({
                    "provider":module,"preset":"teaching","params":{}
                }));
            plan["composition"]["providers"]
                .as_array_mut()
                .unwrap()
                .push(json!({
                    "provider":module,"tools":[]
                }));
        }
        plan["composition"]["providers"]
            .as_array_mut()
            .unwrap()
            .sort_by(|a, b| {
                a["provider"]
                    .as_str()
                    .unwrap()
                    .cmp(b["provider"].as_str().unwrap())
            });
        plan["composition_digest"] = json!(
            cortexkit_role_tool_provider::catalog::composition_digest(&plan["composition"])
                .unwrap()
        );
        Ok(Some(fields))
    }

    fn tool_provider_module(&self) -> String {
        self.shared
            .default_text_module
            .lock()
            .unwrap()
            .clone()
            .unwrap_or_else(|| TOOL_MODULE.into())
    }

    async fn prepare_system_text_default(
        &self,
        session: &str,
        provides: &[String],
        item: &Value,
    ) -> Result<Map<String, Value>, HarnessError> {
        let connection = self
            .shared
            .fixture_connections
            .current()
            .ok_or_else(|| HarnessError::new("no private daemon is running"))?;
        let module = format!(
            "conformance-default-text-{}",
            self.shared.default_text_tasks.lock().unwrap().len()
        );
        let mut manifest = tool_manifest();
        manifest.module_id = module.clone();
        manifest.capabilities = Some(subc_protocol::manifest::CapabilityDeclarations {
            provides: provides.to_vec(),
            requires: vec![],
            must_never_reach: vec![],
        });
        let (_, serve) = subc_client_rs::serve_with_handle(
            &connection,
            manifest,
            DefaultTextProvider {
                shared: self.shared.clone(),
                session: session.into(),
                rich_ui: None,
            },
        )
        .await
        .map_err(|error| HarnessError::new(error.to_string()))?;
        self.shared
            .default_text_tasks
            .lock()
            .unwrap()
            .push(tokio::spawn(async move {
                let _ = serve.await;
            }));
        *self.shared.default_text_module.lock().unwrap() = Some(module.clone());
        self.shared
            .default_text_seen
            .lock()
            .unwrap()
            .insert(session.into(), Default::default());
        self.shared
            .plan_sessions
            .lock()
            .unwrap()
            .insert(session.into());
        let composition =
            json!({"providers":[{"provider":module,"tools":[]}],"step_transform_items":[]});
        let mut plan_item = item.clone();
        plan_item["provider"] = json!(module);
        Ok(json!({"model":{"provider":"mock","model":format!("{PLAN_MODEL_PREFIX}{session}")},"tools":null,"system":null,
            "plan":{"tool_items":[],"system_text_items":[plan_item],"step_transform_items":[],"composition":composition,
                "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap()}}).as_object().unwrap().clone())
    }

    async fn system_text_default_observation(
        &self,
        handle: &Live,
        session: &str,
    ) -> Result<cortexkit_role_llm_runner_conformance::SystemTextDefaultObservation, HarnessError>
    {
        let mut observation = self
            .shared
            .default_text_seen
            .lock()
            .unwrap()
            .get(session)
            .cloned()
            .unwrap_or_default();
        // Observe the session's log; unrelated sessions may still be draining their own
        // background writes.
        let records = self.records(handle, session).await;
        observation.durable_state = format!("{records:?}").into_bytes();
        Ok(observation)
    }

    fn output_refs_fields(&self, session: &str) -> Result<Map<String, Value>, HarnessError> {
        // llm-runner 0.8 numbers parts only under a compaction item (§9.6), so the fixture no
        // longer asks for the retired `render_tags`.
        let mut fields = self
            .send_fields_with_plan(session, true, &Map::new())
            .ok_or_else(|| HarnessError::new("output references need a scripted tool plan"))?;
        fields["plan"]["tool_items"][0]["output_refs"] = json!(true);
        fields["model"] = json!({"provider":output_refs::PROVIDER,"model":output_refs::MODEL});
        self.shared
            .output_refs_sessions
            .lock()
            .unwrap()
            .insert(session.into());
        fields["plan"]["system_text_items"] =
            json!([{"provider":TOOL_MODULE,"preset":"conformance","params":{"session":session}}]);
        Ok(fields)
    }

    fn send_fields_with_tools(
        &self,
        session: &str,
        first: bool,
        tools: &[&str],
    ) -> Option<Map<String, Value>> {
        let mut fields = self.send_fields(session, first);
        if first && tools.is_empty() {
            // A tool-free session runs under a real plan that offers no tools. The plan
            // replaces `system`, so its system-text item carries the session marker the
            // scripted model identifies the session by.
            let composition = json!({"providers":[{"provider":TOOL_MODULE,"tools":[]}],
                "step_transform_items":[]});
            fields.insert("tools".into(), Value::Null);
            fields.insert("system".into(), Value::Null);
            fields.insert(
                "plan".into(),
                json!({
                    "tool_items":[],
                    "system_text_items":[{"provider":TOOL_MODULE,"preset":"conformance",
                        "params":{"session":session}}],
                    "step_transform_items":[],
                    "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
                    "composition":composition,
                }),
            );
        } else if first {
            fields.insert(
                "tools".into(),
                Value::Array(
                    tools
                        .iter()
                        .map(|name| {
                            json!({
                                "name": name,
                                "description": "Answers each call with its scripted result.",
                                "input_schema": {"type": "object"},
                                "module": TOOL_MODULE,
                            })
                        })
                        .collect(),
                ),
            );
        }
        // The session's route support picks the catalog row its model resolves against: the
        // rows `seed_structured_output_catalog` writes under the `xai` provider, using explicit
        // per-model Chat pins because Chat is the protocol the scripted model speaks.
        if let Some(support) = self
            .shared
            .structured_output_support
            .lock()
            .unwrap()
            .get(session)
        {
            // These fixed catalog rows cannot carry a session-specific model id.
            if fields.get("plan").is_none_or(Value::is_null) {
                fields.insert("system".into(), json!(format!("{SESSION_MARKER}{session}")));
            } else {
                fields["plan"]["system_text_items"] = json!([{"provider":TOOL_MODULE,"preset":"conformance","params":{"session":session}}]);
            }
            fields.insert(
                "model".into(),
                json!({"provider": STRUCTURED_OUTPUT_PROVIDER, "model": structured_output_model(*support)}),
            );
        }
        Some(fields)
    }

    async fn set_structured_output_support(
        &self,
        session: &str,
        supported: Option<bool>,
    ) -> Result<(), HarnessError> {
        self.shared
            .structured_output_support
            .lock()
            .unwrap()
            .insert(session.to_owned(), supported);
        Ok(())
    }

    async fn raw_model_requests(
        &self,
        session: &str,
    ) -> Result<Vec<cortexkit_role_llm_runner_conformance::RawModelRequestObservation>, HarnessError>
    {
        Ok(self
            .shared
            .fork_raw_requests
            .lock()
            .unwrap()
            .get(session)
            .into_iter()
            .flatten()
            .map(|(fork, body)| {
                cortexkit_role_llm_runner_conformance::RawModelRequestObservation::new(
                    fork.clone(),
                    body.clone(),
                )
            })
            .collect())
    }

    async fn model_requests(
        &self,
        session: &str,
    ) -> Result<Vec<cortexkit_role_llm_runner_conformance::ModelRequestObservation>, HarnessError>
    {
        let bodies = self
            .shared
            .raw_bodies
            .lock()
            .unwrap()
            .get(session)
            .cloned()
            .unwrap_or_default();
        bodies
            .iter()
            .map(|body| {
                let request: Value = serde_json::from_slice(body)
                    .map_err(|e| HarnessError::new(format!("a model request is not JSON: {e}")))?;
                let mut observation = observe_model_request(&request);
                observation.service_tier_applied = request
                    .get("service_tier")
                    .and_then(Value::as_str)
                    .filter(|tier| *tier != "default")
                    .map(|tier| if tier == "priority" { "fast" } else { tier }.to_owned());
                Ok(observation)
            })
            .collect()
    }

    async fn install_held_reply(
        &self,
        session: &str,
        arguments: &Value,
        reason: &str,
    ) -> Result<(), HarnessError> {
        held_late_results::install_held_reply(&self.shared, session, arguments, reason);
        Ok(())
    }

    async fn await_held_reply(&self, arguments: &Value) -> Result<String, HarnessError> {
        held_late_results::await_held_reply(&self.shared, arguments).await
    }

    /// The provider keeps the call open until broca asks it to hand back. A provider that
    /// withholds its answer to that request isn't scripted here, so that case doesn't apply.
    async fn install_call_yield(
        &self,
        session: &str,
        arguments: &Value,
        block_answer: bool,
    ) -> Result<bool, HarnessError> {
        if block_answer {
            return Ok(false);
        }
        held_late_results::install_call_yield(&self.shared, session, arguments);
        Ok(true)
    }

    /// The yields and withdrawals the provider received for the call, and each notice the
    /// session's store holds durably, with its sender and its text: one placed in the
    /// conversation, or one queued for the session's next run.
    async fn observe_call_yield(
        &self,
        session: &str,
        arguments: &Value,
        _send_id: &str,
    ) -> Result<cortexkit_role_llm_runner_conformance::CallYieldObservation, HarnessError> {
        let (call_key, yield_requests, withdraw_requests) =
            held_late_results::yields_seen(&self.shared, arguments);
        let root = self
            .shared
            .session_roots
            .lock()
            .unwrap()
            .get(session)
            .cloned()
            .ok_or_else(|| HarnessError::new(format!("no state root for session {session}")))?;
        // A notice is durable once it is in the conversation: an input message (a steer, or
        // the input of a run it started on an idle session) followed by the facts that name
        // its sender and marks. Read those, as `session.read` serves them with display facts.
        let mut notices = Vec::new();
        let mut placed: Option<String> = None;
        let text_of = |message: &broca_protocol::Message| -> String {
            message
                .content
                .iter()
                .filter_map(|block| match block {
                    broca_protocol::content::ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect()
        };
        // A queued send names its module sender on the `DeliveryOrdered` record written just
        // before it, as a principal; take the module id out of it.
        let mut ordered_by: std::collections::HashMap<String, String> = Default::default();
        for record in stored_log::stored_records(&root, &self.bind_identity(session)).await {
            match record {
                Record::DeliveryOrdered(ordered) => {
                    let sender = match ordered
                        .sent_by
                        .clone()
                        .map(serde_json::from_value::<broca_protocol::BindPrincipal>)
                    {
                        Some(Ok(broca_protocol::BindPrincipal::Reserved { module_id })) => {
                            module_id
                        }
                        _ => String::new(),
                    };
                    ordered_by.insert(ordered.submission_id.clone(), sender);
                }
                Record::QueuedSend(queued)
                    if queued
                        .message_marks
                        .as_ref()
                        .is_some_and(|marks| marks["notice"] == true) =>
                {
                    notices.push(json!({
                        "sender": ordered_by.get(&queued.submission_id).cloned().unwrap_or_default(),
                        "notice": true,
                        "text": queued.input.iter().map(text_of).collect::<String>(),
                    }));
                }
                Record::Input(input) => {
                    placed = Some(
                        input
                            .message
                            .content
                            .iter()
                            .filter_map(|block| match block {
                                broca_protocol::content::ContentBlock::Text { text } => {
                                    Some(text.as_str())
                                }
                                _ => None,
                            })
                            .collect(),
                    );
                }
                Record::EntryFacts(facts) => {
                    let text = placed.take();
                    if facts
                        .marks
                        .as_ref()
                        .is_some_and(|marks| marks["notice"] == true)
                    {
                        notices.push(json!({
                            "sender": facts.sender.as_deref().unwrap_or_default(),
                            "notice": true,
                            "text": text.unwrap_or_default(),
                        }));
                    }
                }
                _ => {}
            }
        }
        let mut observation =
            cortexkit_role_llm_runner_conformance::CallYieldObservation::default();
        observation.call_key = call_key;
        observation.yield_requests = yield_requests;
        observation.withdraw_requests = withdraw_requests;
        observation.notices = notices;
        Ok(observation)
    }

    async fn publish_late_entry(
        &self,
        arguments: &Value,
        entry: ScriptedLateEntry,
    ) -> Result<(), HarnessError> {
        held_late_results::publish_late_entry(&self.shared, arguments, entry)
    }

    async fn install_script(&self, session: &str, script: Script) -> Result<(), HarnessError> {
        *self.shared.default_text_module.lock().unwrap() = None;
        // Turns are otherwise matched by the assistant messages a request carries. A fork's
        // asks all extend the head's last request, so they are matched by requests served.
        if script.turns.iter().any(|turn| turn.model_failure.is_some())
            || fork_answer::fixture_session(session)
        {
            self.shared
                .failure_turns
                .lock()
                .unwrap()
                .insert(session.to_owned(), 0);
        }
        if history_display_kit_session(session) {
            self.shared
                .model_id_marker_sessions
                .lock()
                .unwrap()
                .insert(session.into());
        }
        let mut results = self.shared.results.lock().unwrap();
        let mut holds = self.shared.holds.lock().unwrap();
        for call in script.tool_calls() {
            results.insert(call.arguments.to_string(), call.result.clone());
            if call.hold {
                holds.entry(call.arguments.to_string()).or_default();
            }
        }
        drop(holds);
        drop(results);
        self.shared
            .scripts
            .lock()
            .unwrap()
            .insert(session.to_owned(), script);
        Ok(())
    }

    fn oversized_tool_result(
        &self,
        message: &Value,
        tool_call_id: &str,
    ) -> Option<cortexkit_role_llm_runner_conformance::ToolResultLimitObservation> {
        let block = message["content"]
            .as_array()?
            .iter()
            .find(|block| block["tool_call_id"].as_str() == Some(tool_call_id))?;
        let output = &block["output"];
        let detail: Value = serde_json::from_str(output["detail"].as_str()?).ok()?;
        Some(
            cortexkit_role_llm_runner_conformance::ToolResultLimitObservation::new(
                output["code"].as_str()?,
                detail["bytes"].as_u64()?,
                detail["limit"].as_u64()?,
                output["message"].as_str()?,
                block["is_error"].as_bool()?,
            ),
        )
    }

    fn shown_part_number(&self, session: &str, mid: &str, part: &str) -> Option<u64> {
        let entry = self
            .shared
            .read_entries
            .lock()
            .unwrap()
            .get(&(session.to_owned(), mid.to_owned()))
            .cloned()?;
        let keys = self
            .shared
            .read_part_keys
            .lock()
            .unwrap()
            .get(&entry["message"].to_string())
            .cloned()?;
        let index = keys.iter().position(|key| key == part)?;
        let requests = self.shared.requests.lock().unwrap();
        let mut numbers = BTreeSet::new();
        for message in requests.get(session)?.iter().flatten() {
            let (cleaned, shown) = captured_part_numbers(message);
            if cleaned["role"] == entry["message"]["role"]
                && cleaned["content"] == entry["message"]["content"]
            {
                if let Some(Some(number)) = shown.get(index) {
                    numbers.insert(*number);
                }
            }
        }
        (numbers.len() == 1).then(|| *numbers.iter().next().unwrap())
    }

    async fn tool_invocations(&self, arguments: &Value) -> usize {
        self.shared
            .invocations
            .lock()
            .unwrap()
            .get(&arguments.to_string())
            .copied()
            .unwrap_or(0)
    }

    async fn dispatched_schema_pin(
        &self,
        arguments: &Value,
    ) -> Result<Option<String>, HarnessError> {
        self.shared
            .dispatched_pins
            .lock()
            .unwrap()
            .get(&arguments.to_string())
            .cloned()
            .ok_or_else(|| HarnessError::new("the tool provider did not observe these arguments"))
    }

    fn read_parts(&self, message: &Value) -> Vec<(String, Value)> {
        let keys = self
            .shared
            .read_part_keys
            .lock()
            .unwrap()
            .get(&message.to_string())
            .cloned()
            .unwrap_or_default();
        keys.into_iter()
            .zip(message["content"].as_array().into_iter().flatten().cloned())
            .collect()
    }

    fn media_refs_tool_output_pointer(&self, message: &Value, part_key: &str) -> Option<String> {
        self.read_parts(message)
            .iter()
            .position(|(key, part)| {
                key == part_key && part["type"] == "tool_result" && part.get("output").is_some()
            })
            .map(|index| format!("/content/{index}/output"))
    }

    /// Resolve identity from the listed call and the keys of the actual read entry.
    fn tool_result_part(
        &self,
        call: &cortexkit_role_llm_runner::tool_calls::RunToolCall,
        result: &cortexkit_role_llm_runner::read::ReadMessage,
    ) -> Option<String> {
        if call.result_ordinal != Some(result.ordinal) {
            return None;
        }
        let mut matching = result
            .message
            .get("content")?
            .as_array()?
            .iter()
            .enumerate()
            .filter(|(_, block)| {
                block["type"] == "tool_result"
                    && block["tool_call_id"].as_str() == Some(call.tool_call_id.as_str())
            });
        let (index, _) = matching.next()?;
        if matching.next().is_some() {
            return None;
        }
        result
            .served_part_keys()?
            .get(index)
            .map(|key| (*key).to_owned())
    }

    /// Legacy image fixture: invalid bytes keep the marker in a stored media-failure notice.
    fn image_tool_result(&self, marker: &str) -> Option<Value> {
        Some(json!({ OWN_CONTENT: [{
            "type": "image",
            "mimeType": format!("image/png; name={marker}"),
            "data": "aGk=",
        }]}))
    }

    /// The media fixture cases' byte-backed image: the scripted tool answers with exactly
    /// these PNG bytes as its own image block, which broca stores inside that call's
    /// tool_result. broca keeps a tool's image only inside its result, never as a bare image
    /// block, so it cannot supply the un-nested form media_descriptors_image_bytes asks for,
    /// and the kit reports that case inapplicable.
    fn media_tool_result(&self, bytes: &[u8], nested: bool, _scenario: &str) -> Option<Value> {
        use base64::Engine;
        if !nested {
            return None;
        }
        let image = broca_core::media_validation::validate_media(bytes, "image/png")
            .expect("the kit's media fixture is a valid PNG");
        Some(json!({OWN_CONTENT:[{
            "type": "image",
            "mimeType": "image/png",
            "data": base64::engine::general_purpose::STANDARD.encode(bytes),
            "width": image.width,
            "height": image.height,
        }]}))
    }

    async fn revert_session(
        &self,
        _handle: &Live,
        _session: &str,
        _mid: &str,
    ) -> Result<(), HarnessError> {
        Err(HarnessError::new("broca serves no transcript revert"))
    }

    /// The legacy whole-session clone fixture is not the short-lived fork_answer surface.
    async fn fork_session(
        &self,
        _handle: &Self::Handle,
        _parent: &str,
        _fork: &str,
    ) -> Result<(), HarnessError> {
        Err(HarnessError::new(
            "broca has no fork or clone operation: a session cannot be forked, so there is no \
             fork of a managed session for the runner to refuse",
        ))
    }

    fn not_executed_tool_result(
        &self,
        message: &Value,
        tool_call_id: &str,
    ) -> Option<cortexkit_role_llm_runner_conformance::ToolResultObservation> {
        if message["role"] != "tool" {
            return None;
        }
        message["content"].as_array()?.iter().find_map(|part| {
            if part["type"] != "tool_result" || part["tool_call_id"] != tool_call_id {
                return None;
            }
            let result: broca_protocol::ToolResult = serde_json::from_value(part.clone()).ok()?;
            match result.output {
                broca_protocol::ToolOutput::Error { message, .. }
                    if result.is_error
                        && message.starts_with("budget_exhausted: tool not executed;") =>
                {
                    Some(
                        cortexkit_role_llm_runner_conformance::ToolResultObservation::new(
                            false,
                            cortexkit_role_llm_runner::run::states::BUDGET_EXHAUSTED,
                        ),
                    )
                }
                broca_protocol::ToolOutput::Error { message, .. }
                    if result.is_error
                        && message
                            == broca_protocol::structured_output::SKIPPED_AFTER_SUBMIT_MESSAGE =>
                {
                    Some(
                        cortexkit_role_llm_runner_conformance::ToolResultObservation::new(
                            false,
                            broca_protocol::structured_output::SKIPPED_AFTER_SUBMIT,
                        ),
                    )
                }
                broca_protocol::ToolOutput::Error { message, .. }
                    if result.is_error
                        && message == "tool not run: max steps reached before dispatch" =>
                {
                    Some(
                        cortexkit_role_llm_runner_conformance::ToolResultObservation::new(
                            false,
                            "max_steps",
                        ),
                    )
                }
                _ => None,
            }
        })
    }

    async fn await_tool_call(&self, arguments: &Value) -> Result<(), HarnessError> {
        let gate = self
            .shared
            .holds
            .lock()
            .unwrap()
            .get(&arguments.to_string())
            .cloned();
        let Some(gate) = gate else {
            return Err(HarnessError::new("no installed script holds this call"));
        };
        tokio::time::timeout(
            Duration::from_secs(30),
            HoldGate::wait(&gate.arrived, &gate.arrived_wake),
        )
        .await
        .map_err(|_| HarnessError::new("the tool provider did not receive the call within 30 s"))
    }

    async fn release_tool_call(&self, arguments: &Value) -> Result<(), HarnessError> {
        let gate = self
            .shared
            .holds
            .lock()
            .unwrap()
            .get(&arguments.to_string())
            .cloned();
        match gate {
            Some(gate) => {
                // Release is a permit for an installed script, not a claim that it arrived.
                // A later held turn must observe this permit when it reaches the provider.
                HoldGate::set(&gate.released, &gate.released_wake);
                Ok(())
            }
            _ => Err(HarnessError::new("the call is not held")),
        }
    }

    async fn pause(&self) {
        sleep(Duration::from_millis(50)).await;
    }

    /// Every message the new storage keeps belongs to a run: a send's to the run it started or
    /// joined, and an import's to the import's own run, which stands for the history it brought.
    async fn install_attribution_fixture(
        &self,
        _handle: &Live,
        _session: &str,
    ) -> Result<cortexkit_role_llm_runner_conformance::AttributionFixture, HarnessError> {
        Ok(cortexkit_role_llm_runner_conformance::AttributionFixture::CannotHoldUnattributable)
    }
    #[cfg(feature = "test-clock")]
    async fn advance_retention_clock(
        &self,
        session: &str,
        milliseconds: u64,
    ) -> Result<(), HarnessError> {
        self.advance_retention(session, milliseconds).await
    }
    #[cfg(feature = "test-clock")]
    async fn advance_fork_clock(
        &self,
        session: &str,
        milliseconds: u64,
    ) -> Result<(), HarnessError> {
        self.advance_fork_time(session, milliseconds).await
    }
    #[cfg(feature = "test-clock")]
    async fn retention_deletion_finished(
        &self,
        handle: &Live,
        session: &str,
    ) -> Result<bool, HarnessError> {
        retention_clock::deletion_finished(&handle.state_root, &self.bind_identity(session))
    }
    fn retention_probe_params(
        &self,
        _session: &str,
        method: &str,
    ) -> Result<Option<Vec<Value>>, HarnessError> {
        match method {
            "session.baseline" | "compaction.ready" | "run.status" => Ok(Some(vec![json!({})])),
            // A tag the expired session's first part held: resolve reads through the same
            // transcript path as session.read, so an expired session serves none of it.
            "session.resolve" => Ok(Some(vec![json!({"tag": "§1§"})])),
            // llm-runner 0.8's name for `send.status`, served by the same handler. The kit
            // treats `send.status` as a role op that serves no stored content, and this name
            // answers the same receipt, so it is classified the same way.
            "session.send_status" => Ok(None),
            // A run's admission report: its storage, fetches and scope, never message content.
            "run.admission" => Ok(None),
            _ => Err(HarnessError::new(format!(
                "unclassified retention probe op: {method}"
            ))),
        }
    }
    fn serializer_profile(&self, _session: &str) -> Option<String> {
        Some(cortexkit_role_llm_runner::serializer_profile::OWNED_BROCA.into())
    }
}

/// Open two sessions' routes on one connection before sending any requests, then describe the
/// first twice and the second once on a single test-runtime worker. Check the logs on disk:
/// timing alone cannot distinguish an unreceived request from a reply blocked before the socket.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn describe_diagnostics_follow_each_call_across_two_routes_on_one_worker() {
    for (name, env_key) in [
        ("ck-subc", "SUBC_CORE_BIN"),
        ("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
    ] {
        let bin = sibling_bin(name, env_key);
        if !bin.exists() {
            assert!(!require_subc(), "the subc rig binary {bin:?} is missing");
            println!("SKIP describe_diagnostics: {bin:?} is missing");
            return;
        }
    }
    std::env::remove_var("SUBC_MODULE_ID");
    std::env::remove_var("SUBC_LAUNCH_NONCE");
    let scratch = broca_test_support::scratch_root()
        .join(format!("broca-describe-diagnostics-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let subject = BrocaSubject::new(&scratch).await;
    let live = subject.spawn(&scratch.join("root")).await.unwrap();
    let owner = subject.owner_stamp();
    let one = subject
        .session_route(&live, "describe-diag-a", &owner)
        .await
        .unwrap();
    let two = subject
        .session_route(&live, "describe-diag-b", &owner)
        .await
        .unwrap();
    let response = |reply| match reply {
        Reply::Response(value) => value,
        Reply::Error(error) => panic!("role.describe refused: {error:?}"),
    };
    let first = response(one.request("role.describe", json!({})).await.unwrap());
    let again = response(one.request("role.describe", json!({})).await.unwrap());
    let other = response(two.request("role.describe", json!({})).await.unwrap());
    assert_eq!(first, again);
    assert_eq!(first, other);

    // A writer on another worker can deliver a response just before the dispatch task logs
    // its successful enqueue. Wait for that observation, not for an arbitrary scheduling delay.
    let deadline = Instant::now() + Duration::from_secs(5);
    let logs = loop {
        let logs = diagnostic_logs(&live.logs_dir).expect("read the module's actual log segments");
        let queued = logs
            .lines()
            .filter(|line| {
                line.contains("broca.wire.describe") && line.contains("stage=response_queued ")
            })
            .count();
        if queued == 3 || Instant::now() >= deadline {
            break logs;
        }
        sleep(Duration::from_millis(10)).await;
    };
    let describes: Vec<_> = logs
        .lines()
        .filter(|line| line.contains("broca.wire.describe"))
        .collect();
    let received: Vec<_> = describes
        .iter()
        .filter(|line| line.contains("stage=received "))
        .collect();
    assert_eq!(
        received.len(),
        3,
        "every request must reach the diagnostic trace: {logs}"
    );
    assert_eq!(
        received
            .iter()
            .filter(|line| line.contains("session=describe-diag-a "))
            .count(),
        2
    );
    assert_eq!(
        received
            .iter()
            .filter(|line| line.contains("session=describe-diag-b "))
            .count(),
        1
    );
    for request in received {
        let key: Vec<_> = request
            .split_whitespace()
            .map(|field| field.trim_matches(['[', ']']))
            .filter(|field| {
                field.starts_with("channel=")
                    || field.starts_with("epoch=")
                    || field.starts_with("corr=")
            })
            .collect();
        let matches_key = |line: &str| {
            key.iter().all(|field| {
                line.split_whitespace()
                    .any(|token| token.trim_matches(['[', ']']) == *field)
            })
        };
        assert_eq!(
            key.len(),
            3,
            "the trace must name channel, epoch and corr: {request}"
        );
        for stage in [
            "admitted",
            "reserved",
            "permit_acquired",
            "task_started",
            "turn_acquired",
            "authorized",
            "response_ready",
            "response_queued",
        ] {
            let stage = format!("stage={stage} ");
            assert!(
                describes
                    .iter()
                    .any(|line| line.contains(&stage) && matches_key(line)),
                "missing {stage} for {key:?}: {logs}"
            );
        }
        assert!(
            logs.lines().any(|line| line.contains("broca.wire.egress")
                && line.contains("stage=buffered ")
                && matches_key(line)),
            "the actual writer must buffer the correlated response: {key:?}: {logs}"
        );
    }
    println!("describe diagnostics: 3 correlated requests, 9 milestones each, one runtime worker");
    drop(one);
    drop(two);
    drop(live);
    broca_test_support::remove_rig_dir(&scratch);
}

const UNNUMBERED_PLAN_INAPPLICABLE_CASES: &[&str] = &[
    "output_refs_scanning",
    "output_refs_shared_samples",
    "output_refs_writing_request",
    "output_refs_target_reasons",
    "output_refs_held_placeholder",
    "output_refs_snapshot_size",
    "output_refs_snapshot_budget",
    "output_refs_descriptor_digest",
    "output_refs_rewind",
    "output_refs_compaction",
    "output_refs_session_isolation",
    "output_refs_slots",
    "output_refs_reference_limit",
    "output_refs_read_limits",
    "delivery_order_template_part_gone_refused",
];

/// Cases that need the input-token cap, which broca doesn't declare (it keeps no input-token
/// budget): each must be skipped for that capability, never run. `finish_reason_no_model_step`
/// is one: the kit stops its run before the first model request by exhausting that cap.
const UNDECLARED_INPUT_CAP_CASES: &[&str] = &[
    "finish_reason_no_model_step",
    "input_token_cap_at_limit",
    "input_token_cap_crossing_tools",
    "input_token_cap_restart_between_episodes",
    "input_token_cap_raise_refused",
    "input_token_cap_closed_members",
    "input_token_cap_usage_unreported",
];

const REQUIRED_STRUCTURED_OUTPUT_CASES: &[&str] = &[
    "structured_output_final_text_enforced_passed",
    "structured_output_final_text_with_tools_refused",
    "structured_output_submit_tool_ends_run",
    "structured_output_reserved_name_refused",
    "structured_output_absent_without_submit",
    "structured_output_failed_validation_completes",
    "structured_output_frozen_across_restart",
    "structured_output_not_inherited",
    "structured_output_external_ref_refused",
    "structured_output_sibling_never_dispatched",
];

const REQUIRED_INTERRUPT_CANCEL_CASES: &[&str] = &[
    "run_cancel_by_interrupt_now_cancelled",
    "run_cancel_by_interrupt_without_grant_refused",
    "run_cancel_by_interrupt_non_now_refused",
    "run_cancel_by_interrupt_other_session_refused",
];

const REQUIRED_RETRACT_OWN_SEND_CASES: &[&str] = &[
    "retract_own_send_sender_retracted",
    "retract_own_send_other_sender_refused",
    "retract_own_send_shared_send_id_isolated",
];

const STREAMED_SIBLINGS_INAPPLICABLE: &str = "submit_streamed_siblings is not declared; the strict rule is checked by structured_output_sibling_never_dispatched";

const REQUIRED_SCOPE_DISPLAY_MEDIA_CASES: &[&str] = &[
    "scoped_session_start_owner_only",
    "frozen_plan_senders_send_refused",
    "frozen_plan_senders_refresh_refused",
    "read_display_facts_completed_at",
    "media_only_send",
];

fn fixture_outcome_matches(name: &str, outcome: &CaseOutcome) -> bool {
    let reason = match name {
        // Both groups are declared, so these undeclared-feature questions do not apply. Kit
        // 0.17.6 names the declared group in its reason.
        "sender_succession_undeclared_refused" => {
            Some("the subject declares every one of sender_succession, so none is left to refuse")
        }
        "read_display_facts_undeclared_refused" => {
            Some("the subject declares every one of read_display_facts, so none is left to refuse")
        }
        // The native fixture has no controlled delivery clock; this is not a retry failure.
        "delivery_deadline_retry_keeps_original_expiry" => {
            Some("adapter supplies no controlled delivery clock")
        }
        // Existing host creation stamps are immutable; the fixture has no independent
        // host-fact writer, and changing a read cache would not seed recorded host data.
        "read_display_facts_created_at" => {
            Some("adapter could not seed a host creation time via seed_host_created_at")
        }
        // Native receipts have real clocks; the adapter cannot author the kit's synthetic
        // per-call stored times. Production read bytes and times have separate serve tests.
        "read_display_facts_default_absent" | "read_display_facts_opt_in_envelope" => {
            Some("adapter cannot seed stored read tool times")
        }
        _ => None,
    };
    matches!(outcome, CaseOutcome::Passed)
        || matches!(outcome, CaseOutcome::Inapplicable { reason: actual } if reason == Some(actual.as_str()))
}

/// A process-kill-disabled fixture lacks only the case's declared kill points.
/// Missing unrelated prerequisites must not disguise an unchecked contract case.
fn disabled_kill_case(
    case: &cortexkit_role_llm_runner_conformance::CaseReport,
    disabled: bool,
) -> bool {
    let expected: Vec<_> = case
        .requires
        .iter()
        .filter(|cap| matches!(cap, Capability::KillAt(_)))
        .cloned()
        .collect();
    disabled
        && !expected.is_empty()
        && matches!(&case.outcome, CaseOutcome::Skipped { missing } if missing == &expected)
}

fn check_routed_account_cases(
    cases: &[cortexkit_role_llm_runner_conformance::CaseReport],
) -> Result<(), String> {
    for name in [
        "routed_accounts_follows_account_choice",
        "routed_accounts_account_for",
        "account_route_names_the_route",
        "account_route_fixed_per_run",
    ] {
        let case = cases
            .iter()
            .find(|case| case.case == name)
            .ok_or_else(|| format!("the suite has no case {name}"))?;
        if case.outcome != CaseOutcome::Passed {
            return Err(format!("{name}: {:?}", case.outcome));
        }
    }
    Ok(())
}

#[test]
fn routed_accounts_cases_cannot_skip_or_claim_inapplicability() {
    use cortexkit_role_llm_runner_conformance::{CaseReport, CASES};
    let cases: Vec<_> = CASES
        .iter()
        .filter(|case| {
            case.name.starts_with("routed_accounts_") || case.name.starts_with("account_route_")
        })
        .map(|case| CaseReport {
            case: case.name,
            requires: case.requires.to_vec(),
            outcome: CaseOutcome::Passed,
        })
        .collect();
    assert_eq!(cases.len(), 4);
    assert!(check_routed_account_cases(&cases).is_ok());
    for index in 0..cases.len() {
        for outcome in [
            CaseOutcome::Skipped { missing: vec![] },
            CaseOutcome::Inapplicable {
                reason: "routed_accounts is not declared on the wire".into(),
            },
        ] {
            let mut broken = cases.clone();
            broken[index].outcome = outcome;
            assert!(check_routed_account_cases(&broken).is_err());
        }
        let mut missing = cases.clone();
        missing.remove(index);
        assert!(check_routed_account_cases(&missing).is_err());
    }
}

fn check_head_selection_cases(
    cases: &[cortexkit_role_llm_runner_conformance::CaseReport],
    tiers: bool,
) -> Result<(), String> {
    for name in [
        "head_selection_plan_tier",
        "head_selection_send_overrides",
        "head_selection_unsupported_tier",
        "head_selection_unsupported_model",
        "head_selection_immediate_head",
        "head_selection_absent_preserves_choice",
        "head_selection_unchanged_after_host_switch",
    ] {
        let case = cases
            .iter()
            .find(|case| case.case == name)
            .ok_or_else(|| format!("the suite has no case {name}"))?;
        let expected_inapplicable = if name == "head_selection_unchanged_after_host_switch" {
            Some("adapter cannot record a host-side model switch")
        } else if !tiers
            && matches!(
                name,
                "head_selection_plan_tier"
                    | "head_selection_send_overrides"
                    | "head_selection_unsupported_tier"
                    | "head_selection_absent_preserves_choice"
            )
        {
            Some("service_tier is not declared")
        } else {
            None
        };
        let valid = match expected_inapplicable {
            Some(expected) => {
                matches!(&case.outcome, CaseOutcome::Inapplicable { reason } if reason == expected)
            }
            None => case.outcome == CaseOutcome::Passed,
        };
        if !valid {
            return Err(format!("{name}: {:?}", case.outcome));
        }
    }
    Ok(())
}

#[test]
fn head_selection_cases_require_real_fixtures_and_explicit_inapplicability() {
    use cortexkit_role_llm_runner_conformance::{CaseReport, CASES};
    let cases: Vec<_> = CASES
        .iter()
        .filter(|case| case.name.starts_with("head_selection_"))
        .map(|case| CaseReport {
            case: case.name,
            requires: case.requires.to_vec(),
            outcome: if case.name == "head_selection_unchanged_after_host_switch" {
                CaseOutcome::Inapplicable {
                    reason: "adapter cannot record a host-side model switch".into(),
                }
            } else {
                CaseOutcome::Passed
            },
        })
        .collect();
    assert!(check_head_selection_cases(&cases, true).is_ok());
    for index in 0..cases.len() {
        let mut broken = cases.clone();
        broken[index].outcome = CaseOutcome::Skipped { missing: vec![] };
        assert!(check_head_selection_cases(&broken, true).is_err());
        broken.remove(index);
        assert!(check_head_selection_cases(&broken, true).is_err());
    }
}

/// Additive kit cases must not turn an undeclared capability into a required pass.
/// Check the contracts this adapter implements by name, not by a shared name prefix's size.
fn check_0811_cases(
    cases: &[cortexkit_role_llm_runner_conformance::CaseReport],
) -> Result<(), String> {
    check_0811_cases_for_writer(cases, broca_core::delivery_order::RIDES_V2_WRITER_ENABLED)
}

/// Every message-mark case and the outcome it must have for a runner that declares
/// `notice_with_speaks_for` or not. Declaring the group turns the two undeclared-group refusal
/// cases inapplicable, by the kit's own reason, and its own cases into required passes.
fn message_mark_cases(notice_with_speaks_for: bool) -> Vec<(&'static str, CaseOutcome)> {
    let declared_by_subject = || CaseOutcome::Inapplicable {
        reason:
            "the subject declares every one of notice_with_speaks_for, so none is left to refuse"
                .into(),
    };
    let group_case = || {
        if notice_with_speaks_for {
            CaseOutcome::Passed
        } else {
            CaseOutcome::Inapplicable {
                reason: "notice_with_speaks_for is not declared".into(),
            }
        }
    };
    let undeclared_case = || {
        if notice_with_speaks_for {
            declared_by_subject()
        } else {
            CaseOutcome::Passed
        }
    };
    vec![
        ("message_marks_served", CaseOutcome::Passed),
        ("message_marks_both_refused", undeclared_case()),
        ("message_marks_speaks_for_owner_only", CaseOutcome::Passed),
        ("message_marks_retry_changed", CaseOutcome::Passed),
        ("notice_with_speaks_for_served", group_case()),
        ("notice_with_speaks_for_retry", group_case()),
        ("notice_with_speaks_for_name_undeclared", undeclared_case()),
    ]
}

fn check_message_marks_cases_declaring(
    cases: &[cortexkit_role_llm_runner_conformance::CaseReport],
    notice_with_speaks_for: bool,
) -> Result<(), String> {
    for (name, expected) in message_mark_cases(notice_with_speaks_for) {
        let case = cases
            .iter()
            .find(|case| case.case == name)
            .ok_or_else(|| format!("the suite has no case {name}"))?;
        if case.outcome != expected {
            return Err(format!("{name}: {:?}, expected {expected:?}", case.outcome));
        }
    }
    Ok(())
}

fn check_message_marks_cases(
    cases: &[cortexkit_role_llm_runner_conformance::CaseReport],
) -> Result<(), String> {
    check_message_marks_cases_declaring(cases, broca_core::scope_admission::NOTICE_WITH_SPEAKS_FOR)
}

#[test]
fn message_marks_cases_require_exact_outcomes_and_reject_missing_cases() {
    use cortexkit_role_llm_runner_conformance::{CaseReport, CASES};
    for declared in [false, true] {
        let cases: Vec<_> = message_mark_cases(declared)
            .into_iter()
            .map(|(name, outcome)| {
                let spec = CASES.iter().find(|spec| spec.name == name).unwrap();
                CaseReport {
                    case: spec.name,
                    requires: spec.requires.to_vec(),
                    outcome,
                }
            })
            .collect();
        check_message_marks_cases_declaring(&cases, declared).unwrap();
        for index in 0..cases.len() {
            for outcome in [
                CaseOutcome::Passed,
                CaseOutcome::Failed {
                    reason: "failed".into(),
                },
                CaseOutcome::Skipped {
                    missing: vec![Capability::MessageMarks],
                },
                CaseOutcome::Inapplicable {
                    reason: "message_marks is not declared".into(),
                },
            ] {
                if outcome == cases[index].outcome {
                    continue;
                }
                let mut changed = cases.clone();
                changed[index].outcome = outcome;
                assert!(check_message_marks_cases_declaring(&changed, declared).is_err());
            }
            let mut missing = cases.clone();
            missing.remove(index);
            assert!(check_message_marks_cases_declaring(&missing, declared).is_err());
        }
    }
}

fn check_0814_undeclared_cases(
    cases: &[cortexkit_role_llm_runner_conformance::CaseReport],
) -> Result<(), String> {
    for line in include_str!("golden/llm_runner_0814.undeclared_applicability.txt").lines() {
        let (name, reason) = line.strip_prefix("N/A ").unwrap().split_once(": ").unwrap();
        let case = cases
            .iter()
            .find(|case| case.case == name)
            .ok_or_else(|| format!("the suite has no case {name}"))?;
        if case.outcome
            != (CaseOutcome::Inapplicable {
                reason: reason.into(),
            })
        {
            return Err(format!("{name}: {:?}", case.outcome));
        }
    }
    Ok(())
}

fn expected_0821_cases() -> Vec<(&'static str, CaseOutcome)> {
    // The Passed send-id pin depends on the product fix that omits internal submission IDs
    // from unkeyed direct starts, rather than exposing them as caller send IDs.
    include_str!("golden/llm_runner_0821.applicability.txt")
        .lines()
        .chain(include_str!("golden/llm_runner_0828.applicability.txt").lines())
        .map(|line| {
            let (status, row) = line.split_once(' ').unwrap();
            let (name, reason) = row.split_once(": ").unwrap_or((row, ""));
            let outcome = match status {
                "N/A" => CaseOutcome::Inapplicable {
                    reason: reason.into(),
                },
                // cortexkit-role-llm-runner-conformance skips missing fork_answer
                // in runner::gate. Preserve that decision; an upstream fix must update the pin.
                "SKIP" => {
                    assert_eq!(reason, "fork_answer");
                    CaseOutcome::Skipped {
                        missing: vec![Capability::ForkAnswer],
                    }
                }
                "PASS" => {
                    assert!(reason.is_empty());
                    CaseOutcome::Passed
                }
                // An admitted unkeyed send need not return a submission id. The kit verifies
                // absence instead; pin that exact rule rather than requiring an optional id.
                "PASS_WITH_INAPPLICABLE_RULE" => CaseOutcome::PassedWithInapplicableRule {
                    reason: reason.into(),
                },
                _ => panic!("unknown applicability status {status}"),
            };
            (name, outcome)
        })
        .collect()
}

fn check_0821_cases(
    cases: &[cortexkit_role_llm_runner_conformance::CaseReport],
) -> Result<(), String> {
    let mut mismatches = Vec::new();
    for (name, expected) in expected_0821_cases() {
        match cases.iter().find(|case| case.case == name) {
            None => mismatches.push(format!("the suite has no case {name}")),
            Some(case) if case.outcome != expected => {
                mismatches.push(format!("{name}: {:?}, expected {expected:?}", case.outcome))
            }
            Some(_) => {}
        }
    }
    if mismatches.is_empty() {
        Ok(())
    } else {
        Err(mismatches.join("\n"))
    }
}

#[test]
fn runner_0821_send_identity_pins_the_exact_replacement_rule() {
    use cortexkit_role_llm_runner_conformance::{CaseReport, CASES};
    const CASE: &str = "read_display_facts_send_id_when_present";
    const REASON: &str = "send_ids guarantee is checked by read_display_facts_send_id";
    // The case's unkeyed send reaches an idle session and starts its run at once, and a
    // send that starts a run is answered with its run id, never a submission id (serve.rs
    // answers a submission id only to a queued, `Pending`, send). The contract lets an admitted
    // unkeyed send return none, and the kit then checks the id is absent from the read instead.
    const SEND_ID: &str = "read_display_facts_send_id";
    const UNKEYED: &str =
        "read_display_facts_send_id_unkeyed: runner returned no submission_id; verified absence instead";
    assert_eq!(
        expected_0821_cases()
            .into_iter()
            .find(|(name, _)| *name == SEND_ID)
            .unwrap()
            .1,
        CaseOutcome::PassedWithInapplicableRule {
            reason: UNKEYED.into()
        }
    );
    let mut cases: Vec<_> = expected_0821_cases()
        .into_iter()
        .map(|(name, outcome)| {
            let spec = CASES.iter().find(|spec| spec.name == name).unwrap();
            CaseReport {
                case: spec.name,
                requires: spec.requires.to_vec(),
                outcome,
            }
        })
        .collect();
    let index = cases.iter().position(|case| case.case == CASE).unwrap();
    cases[index].outcome = CaseOutcome::Inapplicable {
        reason: REASON.into(),
    };
    check_0821_cases(&cases).unwrap();
    for reason in [
        "",
        "runner returned no submission_id",
        "read_display_facts_send_id_unkeyed: runner returned no submission_id; verified absence instead",
    ] {
        let mut changed = cases.clone();
        changed[index].outcome = CaseOutcome::Inapplicable {
            reason: reason.into(),
        };
        assert!(check_0821_cases(&changed).unwrap_err().contains(CASE));
    }
    for outcome in [
        CaseOutcome::Passed,
        CaseOutcome::PassedWithInapplicableRule {
            reason: REASON.into(),
        },
    ] {
        let mut changed = cases.clone();
        changed[index].outcome = outcome;
        assert!(check_0821_cases(&changed).unwrap_err().contains(CASE));
    }
    // The send-id case itself: any other outcome, or the same outcome for another reason,
    // is a drift.
    let send_id = cases.iter().position(|case| case.case == SEND_ID).unwrap();
    for outcome in [
        CaseOutcome::Passed,
        CaseOutcome::PassedWithInapplicableRule {
            reason:
                "read_display_facts_send_id_unkeyed: runner does not admit unkeyed non-owner sends"
                    .into(),
        },
        CaseOutcome::Inapplicable {
            reason: UNKEYED.into(),
        },
    ] {
        let mut changed = cases.clone();
        changed[send_id].outcome = outcome;
        assert!(check_0821_cases(&changed).unwrap_err().contains(SEND_ID));
    }
    cases.remove(index);
    assert_eq!(
        check_0821_cases(&cases).unwrap_err(),
        format!("the suite has no case {CASE}")
    );
}

#[test]
fn runner_0821_cases_pin_exact_native_outcomes() {
    use cortexkit_role_llm_runner_conformance::{CaseReport, CASES};
    let cases: Vec<_> = expected_0821_cases()
        .into_iter()
        .map(|(name, outcome)| {
            let spec = CASES.iter().find(|spec| spec.name == name).unwrap();
            CaseReport {
                case: spec.name,
                requires: spec.requires.to_vec(),
                outcome,
            }
        })
        .collect();
    assert_eq!(cases.len(), 109);
    check_0821_cases(&cases).unwrap();
    for index in 0..cases.len() {
        for outcome in [
            CaseOutcome::Passed,
            CaseOutcome::PassedWithInapplicableRule {
                reason: "a different optional rule".into(),
            },
            CaseOutcome::Failed {
                reason: "failed".into(),
            },
            CaseOutcome::Skipped { missing: vec![] },
            CaseOutcome::Skipped {
                missing: vec![Capability::ForkAnswer],
            },
            CaseOutcome::Inapplicable {
                reason: "fixture unavailable".into(),
            },
            CaseOutcome::Inapplicable {
                reason: "missing groups: fork_answer".into(),
            },
        ] {
            if outcome == cases[index].outcome {
                continue;
            }
            let mut changed = cases.clone();
            changed[index].outcome = outcome;
            assert!(
                check_0821_cases(&changed).is_err(),
                "{}",
                changed[index].case
            );
        }
        let mut missing = cases.clone();
        missing.remove(index);
        assert!(check_0821_cases(&missing).is_err());
    }
}

#[test]
fn runner_0821_pin_check_reports_every_mismatch_and_missing_case() {
    use cortexkit_role_llm_runner_conformance::CaseReport;
    let mut cases: Vec<_> = expected_0821_cases()
        .into_iter()
        .map(|(case, outcome)| CaseReport {
            case,
            requires: vec![],
            outcome,
        })
        .collect();
    let missing = cases.remove(0).case;
    let first = cases[0].case;
    let second = cases[1].case;
    for case in &mut cases[..2] {
        case.outcome = CaseOutcome::Failed {
            reason: "pin regression fixture".into(),
        };
    }
    let mismatches = check_0821_cases(&cases).unwrap_err();
    let lines: Vec<_> = mismatches.lines().collect();
    assert_eq!(lines.len(), 3, "{mismatches}");
    assert_eq!(lines[0], format!("the suite has no case {missing}"));
    assert!(lines[1].starts_with(&format!("{first}: Failed")));
    assert!(lines[2].starts_with(&format!("{second}: Failed")));
    assert!(lines[1..].iter().all(|line| line.contains(", expected ")));
}

#[test]
fn account_route_undeclared_groups_report_exact_reasons_never_skips() {
    use cortexkit_role_llm_runner_conformance::{CaseReport, CASES};
    let cases: Vec<_> = include_str!("golden/llm_runner_0814.undeclared_applicability.txt")
        .lines()
        .map(|line| {
            let (name, reason) = line.strip_prefix("N/A ").unwrap().split_once(": ").unwrap();
            let spec = CASES
                .iter()
                .find(|case| case.name == name)
                .expect("pinned case exists");
            CaseReport {
                case: spec.name,
                requires: spec.requires.to_vec(),
                outcome: CaseOutcome::Inapplicable {
                    reason: reason.into(),
                },
            }
        })
        .collect();
    assert_eq!(cases.len(), 4);
    check_0814_undeclared_cases(&cases).unwrap();
    for index in 0..cases.len() {
        for outcome in [
            CaseOutcome::Passed,
            CaseOutcome::Skipped { missing: vec![] },
            CaseOutcome::Inapplicable {
                reason: "fixture unavailable".into(),
            },
        ] {
            let mut changed = cases.clone();
            changed[index].outcome = outcome;
            assert!(check_0814_undeclared_cases(&changed).is_err());
        }
        let mut missing = cases.clone();
        missing.remove(index);
        assert!(check_0814_undeclared_cases(&missing).is_err());
    }
}

fn check_0811_cases_for_writer(
    cases: &[cortexkit_role_llm_runner_conformance::CaseReport],
    notices_writer: bool,
) -> Result<(), String> {
    for name in [
        "notices_no_transcript_entry",
        "notices_stored_output_unchanged",
        "notices_display_fact",
        "notices_live_event",
        "notices_append_bytes",
        "notices_tool_result_only_never_system",
        "subscribe_resume_refusals",
        "message_senders_rows_name_their_sender",
        "sender_attribution_undeclared_refused",
    ] {
        let case = cases
            .iter()
            .find(|case| case.case == name)
            .ok_or_else(|| format!("the suite has no case {name}"))?;
        let expected = if name.starts_with("notices_") && !notices_writer {
            CaseOutcome::Inapplicable {
                reason: "missing groups: notices".into(),
            }
        } else {
            CaseOutcome::Passed
        };
        if case.outcome != expected {
            return Err(format!("{name}: {:?}", case.outcome));
        }
    }
    for name in [
        "sender_attribution_stored_header",
        "sender_attribution_fact",
        "sender_attribution_photo_only",
        "sender_attribution_template",
        "sender_attribution_other_sender",
        "sender_attribution_tampered_prefix",
        "sender_attribution_display",
    ] {
        let case = cases
            .iter()
            .find(|case| case.case == name)
            .ok_or_else(|| format!("the suite has no case {name}"))?;
        if case.outcome
            != (CaseOutcome::Inapplicable {
                reason: "missing groups: sender_attribution".into(),
            })
        {
            return Err(format!("{}: {:?}", case.case, case.outcome));
        }
    }
    Ok(())
}

#[test]
fn new_contract_cases_require_exact_declared_writer_prerequisites() {
    use cortexkit_role_llm_runner_conformance::{CaseReport, CASES};
    // The kit's runner::gate is private. Pin its native SuiteReport::render rows
    // instead of inferring applicability from CaseSpec::requires in this adapter.
    let reader: Vec<_> = include_str!("golden/llm_runner_0811.reader_applicability.txt")
        .lines()
        .map(|line| {
            let (status, row) = line.split_once(' ').expect("native report status");
            let (name, reason) = row
                .trim_start()
                .split_once(": ")
                .unwrap_or((row.trim_start(), ""));
            let spec = CASES
                .iter()
                .find(|spec| spec.name == name)
                .expect("the pinned kit still names the native case");
            CaseReport {
                case: spec.name,
                requires: spec.requires.to_vec(),
                outcome: match status {
                    "N/A" => CaseOutcome::Inapplicable {
                        reason: reason.into(),
                    },
                    "PASS" => {
                        assert!(reason.is_empty());
                        CaseOutcome::Passed
                    }
                    _ => panic!("unexpected native report status {status}"),
                },
            }
        })
        .collect();
    assert_eq!(reader.len(), 16);
    assert_eq!(
        reader
            .iter()
            .filter(|case| case.case.starts_with("notices_"))
            .count(),
        6
    );
    assert_eq!(
        reader
            .iter()
            .filter(|case| case.case.starts_with("sender_attribution_")
                && case.case != "sender_attribution_undeclared_refused")
            .count(),
        7
    );
    for notices_writer in [false, true] {
        let mut cases = reader.clone();
        if notices_writer {
            // Declaring notices changes the obligation from the pinned missing-group outcome to Passed.
            for case in &mut cases {
                if case.case.starts_with("notices_") {
                    case.outcome = CaseOutcome::Passed;
                }
            }
        }
        check_0811_cases_for_writer(&cases, notices_writer)
            .expect("the native kit's applicability is accepted exactly");
        if notices_writer == broca_core::delivery_order::RIDES_V2_WRITER_ENABLED {
            check_0811_cases(&cases).unwrap();
        }
        // This kit has no provider-executed capability or case family.
        assert!(!CASES
            .iter()
            .any(|case| case.name.starts_with("provider_executed")));
        for index in 0..cases.len() {
            for outcome in [
                CaseOutcome::Passed,
                CaseOutcome::Failed {
                    reason: "case failed".into(),
                },
                CaseOutcome::Inapplicable {
                    reason: "fixture unavailable".into(),
                },
                CaseOutcome::Inapplicable {
                    reason: "missing groups: notices".into(),
                },
                CaseOutcome::Inapplicable {
                    reason: "missing groups: sender_attribution".into(),
                },
                CaseOutcome::Inapplicable {
                    reason: "missing groups: notices, sender_attribution".into(),
                },
                CaseOutcome::Skipped {
                    missing: vec![Capability::Notices],
                },
                CaseOutcome::Skipped {
                    missing: vec![Capability::SenderAttribution],
                },
                CaseOutcome::Skipped { missing: vec![] },
            ] {
                if outcome == cases[index].outcome {
                    continue;
                }
                let mut broken = cases.clone();
                broken[index].outcome = outcome;
                assert!(
                    check_0811_cases_for_writer(&broken, notices_writer).is_err(),
                    "{} must reject {:?}",
                    broken[index].case,
                    broken[index].outcome
                );
            }
        }
    }
}

fn check_named_contract_cases(
    cases: &[cortexkit_role_llm_runner_conformance::CaseReport],
    crash_kills_disabled: bool,
) -> Result<(), String> {
    for name in UNDECLARED_INPUT_CAP_CASES {
        let case = cases
            .iter()
            .find(|case| case.case == *name)
            .ok_or_else(|| format!("the suite has no case {name}"))?;
        if !matches!(&case.outcome, CaseOutcome::Skipped { missing } if missing.contains(&Capability::InputTokenCap))
        {
            return Err(format!(
                "{name} must be skipped for the undeclared input_token_cap: {:?}",
                case.outcome
            ));
        }
    }
    for name in REQUIRED_STRUCTURED_OUTPUT_CASES
        .iter()
        .chain(REQUIRED_RETRACT_OWN_SEND_CASES)
        .chain(REQUIRED_INTERRUPT_CANCEL_CASES)
        .chain(REQUIRED_SCOPE_DISPLAY_MEDIA_CASES)
    {
        let case = cases
            .iter()
            .find(|case| case.case == *name)
            .ok_or_else(|| format!("the suite has no case {name}"))?;
        let skipped_kill = disabled_kill_case(case, crash_kills_disabled);
        if case.outcome != CaseOutcome::Passed && !skipped_kill {
            return Err(format!("{name}: {:?}", case.outcome));
        }
    }
    let name = "structured_output_streamed_siblings";
    let case = cases
        .iter()
        .find(|case| case.case == name)
        .ok_or_else(|| format!("the suite has no case {name}"))?;
    if !matches!(&case.outcome, CaseOutcome::Inapplicable { reason } if reason == STREAMED_SIBLINGS_INAPPLICABLE)
    {
        return Err(format!("{name}: {:?}", case.outcome));
    }
    Ok(())
}

#[cfg(test)]
mod named_contract_case_tests {
    use super::*;
    use cortexkit_role_llm_runner_conformance::{CaseReport, CASES};

    fn outcomes() -> Vec<CaseReport> {
        CASES
            .iter()
            .filter(|case| {
                UNDECLARED_INPUT_CAP_CASES.contains(&case.name)
                    || case.name.starts_with("structured_output_")
                    || case.name.starts_with("retract_own_send_")
                    || case.name.starts_with("run_cancel_by_interrupt_")
                    || REQUIRED_SCOPE_DISPLAY_MEDIA_CASES.contains(&case.name)
            })
            .map(|case| CaseReport {
                case: case.name,
                requires: case.requires.to_vec(),
                outcome: if case.name == "structured_output_streamed_siblings" {
                    CaseOutcome::Inapplicable {
                        reason: STREAMED_SIBLINGS_INAPPLICABLE.into(),
                    }
                } else if UNDECLARED_INPUT_CAP_CASES.contains(&case.name) {
                    CaseOutcome::Skipped {
                        missing: vec![Capability::InputTokenCap],
                    }
                } else {
                    CaseOutcome::Passed
                },
            })
            .collect()
    }

    #[test]
    fn new_runner_cases_cannot_be_missing_skipped_or_partial() {
        let new_names = [
            "scoped_session_start_owner_only",
            "frozen_plan_senders_send_refused",
            "frozen_plan_senders_refresh_refused",
            "read_display_facts_completed_at",
            "media_only_send",
        ];
        for name in new_names {
            let mut cases = outcomes();
            if !cases.iter().any(|case| case.case == name) {
                let spec = CASES.iter().find(|spec| spec.name == name).unwrap();
                cases.push(CaseReport {
                    case: spec.name,
                    requires: spec.requires.to_vec(),
                    outcome: CaseOutcome::Passed,
                });
            }
            cases
                .iter_mut()
                .find(|case| case.case == name)
                .unwrap()
                .outcome = CaseOutcome::Skipped {
                missing: Vec::new(),
            };
            assert!(
                check_named_contract_cases(&cases, false).is_err(),
                "{name} skipped without detection"
            );
            cases
                .iter_mut()
                .find(|case| case.case == name)
                .unwrap()
                .outcome = CaseOutcome::PassedWithInapplicableRule {
                reason: "not checked".into(),
            };
            assert!(
                check_named_contract_cases(&cases, false).is_err(),
                "{name} partially checked without detection"
            );
            cases.retain(|case| case.case != name);
            assert!(
                check_named_contract_cases(&cases, false).is_err(),
                "{name} missing without detection"
            );
        }
    }

    #[test]
    fn named_contract_cases_accept_the_undeclared_streamed_sibling_case() {
        assert_eq!(check_named_contract_cases(&outcomes(), false), Ok(()));
    }

    #[test]
    fn named_contract_cases_require_each_supported_case_and_the_explicit_inapplicable_case() {
        for name in REQUIRED_STRUCTURED_OUTPUT_CASES
            .iter()
            .chain(REQUIRED_RETRACT_OWN_SEND_CASES)
            .chain(REQUIRED_INTERRUPT_CANCEL_CASES)
            .chain(REQUIRED_SCOPE_DISPLAY_MEDIA_CASES)
            .copied()
            .chain(std::iter::once("structured_output_streamed_siblings"))
        {
            let mut cases = outcomes();
            cases.retain(|case| case.case != name);
            assert!(check_named_contract_cases(&cases, false)
                .unwrap_err()
                .contains(name));
            let mut cases = outcomes();
            cases
                .iter_mut()
                .find(|case| case.case == name)
                .unwrap()
                .outcome = CaseOutcome::Skipped {
                missing: Vec::new(),
            };
            assert!(check_named_contract_cases(&cases, false)
                .unwrap_err()
                .contains(name));
        }
    }

    #[test]
    fn named_contract_cases_skip_only_declared_kill_cases_when_kills_are_disabled() {
        let mut cases = outcomes();
        let index = cases
            .iter()
            .position(|case| case.case == "structured_output_frozen_across_restart")
            .unwrap();
        let missing: Vec<_> = cases[index]
            .requires
            .iter()
            .filter(|cap| matches!(cap, Capability::KillAt(_)))
            .cloned()
            .collect();
        assert!(!missing.is_empty());
        cases[index].outcome = CaseOutcome::Skipped {
            missing: missing.clone(),
        };
        assert!(check_named_contract_cases(&cases, false).is_err());
        assert_eq!(check_named_contract_cases(&cases, true), Ok(()));
        cases[index].outcome = CaseOutcome::Skipped {
            missing: Vec::new(),
        };
        assert!(
            check_named_contract_cases(&cases, true).is_err(),
            "an empty missing-capability list is not a disabled-kill outcome"
        );
        cases[index].outcome = CaseOutcome::Skipped { missing };
        cases
            .iter_mut()
            .find(|case| case.case == "structured_output_submit_tool_ends_run")
            .unwrap()
            .outcome = CaseOutcome::Skipped {
            missing: Vec::new(),
        };
        assert!(check_named_contract_cases(&cases, true).is_err());
    }

    /// An input-cap case broca would run, or one missing from the suite, is caught: broca keeps
    /// no input-token budget, so running one means the capability came back undeclared.
    #[test]
    fn named_contract_cases_require_the_input_cap_cases_skipped() {
        for name in UNDECLARED_INPUT_CAP_CASES {
            let mut cases = outcomes();
            cases
                .iter_mut()
                .find(|case| case.case == *name)
                .unwrap()
                .outcome = CaseOutcome::Passed;
            assert!(check_named_contract_cases(&cases, false)
                .unwrap_err()
                .contains(name));
            let mut cases = outcomes();
            cases.retain(|case| case.case != *name);
            assert!(check_named_contract_cases(&cases, false)
                .unwrap_err()
                .contains(name));
        }
    }
}

/// Every case required by this subject's advertised capabilities must run and pass.
/// Other cases report their missing capability explicitly, and the kill ledger must
/// include a real process kill rather than only simulated recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ck_broca_passes_the_llm_runner_conformance_suite() {
    let test_clock_enabled = cfg!(feature = "test-clock");
    assert!(
        test_clock_enabled,
        "native conformance requires the debug-only test-clock feature"
    );
    for (name, env_key) in [
        ("ck-subc", "SUBC_CORE_BIN"),
        ("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
    ] {
        let bin = sibling_bin(name, env_key);
        if !bin.exists() {
            assert!(
                !require_subc(),
                "llm_runner_conformance: {bin:?} is missing, but this run is REQUIRED by \
                 BROCA_REQUIRE_SUBC (build it: cd ../subconscious && cargo build -p subc-core --bins)"
            );
            println!(
                "SKIP llm_runner_conformance: {bin:?} is missing. This reports `ok` -- set \
                 BROCA_REQUIRE_SUBC=1 to make it a failure instead."
            );
            return;
        }
    }
    // A test process launched by a daemon inherits that daemon's module id and launch nonce in
    // its environment. The caller connections here would present them, and this rig's own
    // daemon refuses them, so they are cleared.
    std::env::remove_var("SUBC_MODULE_ID");
    std::env::remove_var("SUBC_LAUNCH_NONCE");
    let scratch = broca_test_support::scratch_root()
        .join(format!("broca-llm-conformance-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    // Only the test-clock build attaches the notice bus, the one change made to the subject.
    #[cfg_attr(not(feature = "test-clock"), allow(unused_mut))]
    let mut subject = BrocaSubject::new(&scratch).await;
    #[cfg(feature = "test-clock")]
    attach_notice_bus(&mut subject, &scratch).await;
    let work_dir = scratch.join("suite");
    std::fs::create_dir_all(&work_dir).unwrap();
    let report = run_suite(&subject, &work_dir)
        .await
        .expect("the suite set up");
    println!("{}", report.render());
    let failed: Vec<_> = report
        .cases
        .iter()
        .filter(|case| matches!(case.outcome, CaseOutcome::Failed { .. }))
        .collect();
    if !failed.is_empty() {
        // Print before asserting: the panic message keeps only the report, and CI keeps only
        // test output, not the scratch directories this evidence is read from.
        eprintln!("{}", subject.failure_evidence());
    }
    // Report every pin drift even when a failed case would stop the later contract checks.
    let pinned = check_0821_cases(&report.cases);
    if let Err(mismatches) = &pinned {
        eprintln!("Conformance pin mismatches:\n{mismatches}");
    }
    assert!(
        failed.is_empty() && pinned.is_ok(),
        "{}\n{}",
        report.render(),
        pinned.err().unwrap_or_default()
    );
    check_routed_account_cases(&report.cases).unwrap();
    check_head_selection_cases(&report.cases, subject.service_tier_enabled).unwrap();
    // Broca does not promise caller-selected output caps on every provider. Only that
    // prerequisite is skipped; provider-limit finish normalization must still pass below.
    assert_eq!(
        report
            .cases
            .iter()
            .find(|case| case.case == "finish_reason_output_cap")
            .expect("finish_reason_output_cap")
            .outcome,
        CaseOutcome::Skipped {
            missing: vec![Capability::MaxOutputTokens]
        },
        "{}",
        report.render()
    );
    for name in [
        "sender_succession_same_payload_receipt",
        "sender_succession_changed_payload_refused",
        "sender_succession_third_sender_fresh",
        "sender_succession_predecessor_status",
        "sender_succession_undeclared_refused",
        "delivery_deadline_retry_keeps_original_expiry",
        "read_display_facts_default_absent",
        "read_display_facts_opt_in_envelope",
        "read_display_facts_created_at",
        "read_display_facts_undeclared_refused",
    ] {
        let case = report
            .cases
            .iter()
            .find(|case| case.case == name)
            .expect(name);
        assert!(
            fixture_outcome_matches(name, &case.outcome),
            "{}",
            report.render()
        );
    }
    // Sender facts must already be readable when their live message_started event arrives.
    let message_senders = report
        .cases
        .iter()
        .find(|case| case.case == "message_senders_rows_name_their_sender")
        .expect("message_senders_rows_name_their_sender");
    assert_eq!(
        message_senders.outcome,
        CaseOutcome::Passed,
        "{}",
        report.render()
    );
    check_0811_cases(&report.cases).unwrap();
    check_0814_undeclared_cases(&report.cases).unwrap();
    check_message_marks_cases(&report.cases).unwrap();
    let attachment_gaps = attachment_contract_gaps(&report);
    assert!(
        attachment_gaps.is_empty(),
        "unchecked attachment cases: {attachment_gaps:?}\n{}",
        report.render()
    );
    // Broca declares these operations, so a case that drifts to skipped or inapplicable
    // must not silently pass. Input-cap and structured-output outcomes are checked by name
    // below, including the undeclared streamed-sibling behavior.
    for name in [
        "history_pages_back_to_the_first_message",
        "history_mid_matches_read",
        "history_from_ordinal_and_before_ordinal",
        "history_unknown_ordinal_refused",
        "rich_ui_stamp_served_on_stamped_run",
        "rich_ui_stamp_absent_on_unstamped_run",
        "rich_ui_stamp_read_needs_opt_in",
        "rich_ui_stamp_conflict_refused",
        "display_limits_cut_on_a_utf8_boundary",
        "display_without_limits_is_whole",
        "display_pages_carry_finished_messages_only",
        "display_own_run_records_model",
        "client_message_id_on_display_rows_never_in_read",
        "display_row_names_the_sender_never_in_read",
        "finish_reason_provider_limit",
        "finish_reason_last_model_step",
        "output_refs_without_compaction_unknown_tag",
        "run_cancel_step_boundary_finishes_tool_batch",
        "run_cancel_unlisted_at_refused",
        "run_cancel_step_boundary_escalates_to_now",
        "run_usage_counts_reported_steps",
        "send_status_answers_as_a_resend_after_restart",
        "send_status_unknown_send_refused",
        "max_steps_seals_pending_calls",
        "held_calls_lists_waiting_call",
        "held_calls_applies_late_result",
        "held_calls_not_started_becomes_tool_result",
        "held_calls_expired_becomes_tool_result",
        "held_calls_status_event_matches_run_status",
        "no_tools_offered_to_model",
        "no_tools_later_plan_send_refused",
        "send_id_retry_same_answer",
        "session_refresh_applies_from_next_run",
        "session_refresh_waits_for_its_rung",
        "session_refresh_repeat_same_answer",
        "session_refresh_generation_stale_refused",
        "session_refresh_never_changes_a_running_run",
        "flush_prefix_applies_pending",
        "send_id_reuse_refused_naming_field",
        "delivery_change_refused",
        "send_id_retry_written_once",
        "send_id_reuse_writes_nothing",
        "delivery_change_writes_nothing",
        "guaranteed_steer_never_pending_or_unknown",
        "resend_steer_delivered_stable",
        "retention_honoured",
        "retention_run_status_expired",
        "retention_no_content_served",
        "retention_shorten_continues_history_version",
        "retention_equal_noop",
        "retention_lengthen_refused",
        "retention_late_opt_in_refused",
        "retention_zero_refused",
        "retention_above_max_refused",
        "retention_active_run_never_expires",
        "crash_at_RetentionTombstoned",
        // broca declares `client_message_id` (llm-runner 0.5.1 §9.11).
        "client_message_id_resend_with_another_id_refused",
    ] {
        let case = report
            .cases
            .iter()
            .find(|case| case.case == name)
            .unwrap_or_else(|| panic!("the suite has no case {name}"));
        if skip_crash_kills()
            && case
                .requires
                .iter()
                .any(|cap| matches!(cap, Capability::KillAt(_)))
        {
            assert!(disabled_kill_case(case, true), "{name}: {:?}", case.outcome);
            continue;
        }
        assert_eq!(
            case.outcome,
            CaseOutcome::Passed,
            "{name}\n{}",
            report.render()
        );
    }
    // The new storage keeps no message without the run that wrote it, so the harness can't
    // plant one for the kit's unknown-origin rule (`install_attribution_fixture`). That one
    // rule goes unchecked, by declaration; every other attribution rule must pass.
    let attribution = report
        .cases
        .iter()
        .find(|case| case.case == "run_attribution_per_message")
        .expect("the suite has run_attribution_per_message");
    assert_eq!(
        attribution.outcome,
        CaseOutcome::PassedWithInapplicableRule {
            reason: "unknown-origin rule: adapter declares every native write path names a run (CannotHoldUnattributable)".into(),
        },
        "{}",
        report.render()
    );
    // These fixture plans deliberately omit compaction_item. The kit grades numbered
    // scenarios by the opening plan, not by whether a runner happened to render numbers.
    for name in UNNUMBERED_PLAN_INAPPLICABLE_CASES {
        let case = report
            .cases
            .iter()
            .find(|case| case.case == *name)
            .unwrap_or_else(|| panic!("the suite has no case {name}"));
        assert_eq!(case.outcome, CaseOutcome::Inapplicable {
            reason: "the kit's opening plan has no plan.compaction_item, so no part numbers are shown".into(),
        }, "{name}\n{}", report.render());
    }
    let host_set = report
        .cases
        .iter()
        .find(|case| case.case == "session_refresh_tool_outside_host_set_refused")
        .unwrap();
    assert_eq!(
        host_set.outcome,
        CaseOutcome::Inapplicable {
            reason: "the adapter's host does not fix its tool set at start".into(),
        }
    );
    // Rich-UI stamps identify the UI dialect and catalog digest frozen on a run. The
    // fixture cannot publish a replacement stamp through replan_rich_ui; that missing
    // test setup is not evidence that production refresh failed.
    let follows = report
        .cases
        .iter()
        .find(|case| case.case == "rich_ui_stamp_head_follows_last_run")
        .unwrap();
    assert!(
        matches!(&follows.outcome, CaseOutcome::Inapplicable { reason } if reason == "the adapter cannot replan a session's rich UI stamp (replan_rich_ui)")
    );
    // broca does not declare archive_prunes_reasoning, so every archive-pruning case the kit
    // carries must report exactly that, whichever cases a kit version adds or retires.
    let archive_pruning: Vec<_> = report
        .cases
        .iter()
        .filter(|case| case.case.starts_with("archive_pruning_"))
        .collect();
    assert!(
        archive_pruning.len() >= 5,
        "the kit's archive-pruning cases went missing: {}",
        archive_pruning.len()
    );
    for case in archive_pruning {
        let name = &case.case;
        assert_eq!(
            case.outcome,
            CaseOutcome::Inapplicable {
                reason: "archive_prunes_reasoning is not declared".into(),
            },
            "{name}: {:?}",
            case.outcome
        );
        println!("New non-stamp kit case {name}: {:?}", case.outcome);
    }
    // Broca supports output_refs; only a runner without the group can exercise this
    // negative question. Numbered-plan cases were checked against their exact reason above.
    let unsupported = report
        .cases
        .iter()
        .find(|case| case.case == "output_refs_unsupported_plan")
        .expect("output_refs_unsupported_plan");
    assert_eq!(
        unsupported.outcome,
        CaseOutcome::Inapplicable {
            reason: "the runner declares output_refs".into(),
        }
    );
    check_named_contract_cases(&report.cases, skip_crash_kills()).unwrap();
    for name in [
        "service_tier_unsupported_before_model",
        "service_tier_run_fixed",
        "service_tier_unknown_never_echoed",
    ] {
        let case = report.cases.iter().find(|case| case.case == name).unwrap();
        let expected = if subject.service_tier_enabled {
            CaseOutcome::Passed
        } else {
            CaseOutcome::Inapplicable {
                reason: "role.describe does not declare service_tier".into(),
            }
        };
        assert_eq!(case.outcome, expected, "{name}: {:?}", case.outcome);
    }
    if skip_crash_kills() {
        assert!(report.kills.is_empty());
        println!("Crash kills disabled: the kit's overall verdict remains non-conforming without a real kill; run natively without BROCA_CONFORMANCE_SKIP_CRASH_KILLS to verify durability.");
        broca_test_support::remove_rig_dir(&scratch);
        return;
    }
    assert!(
        report
            .kills
            .iter()
            .any(|kill| kill.mechanism.is_real_process_kill()),
        "{}",
        report.render()
    );
    broca_test_support::remove_rig_dir(&scratch);
}

/// Attach one private NATS bus to every daemon the kit starts, as production runs with a bus.
/// The kit reads compaction advance notices captured at the bus, never rebuilt from the WAL,
/// so without a bus its notice cases could only fail. A missing `nats-server` therefore fails
/// the suite here, naming the cause, instead of failing those cases with a misleading reason.
#[cfg(feature = "test-clock")]
async fn attach_notice_bus(subject: &mut BrocaSubject, root: &Path) {
    let bus = bus::BusRig::new(root).await.expect(
        "the llm-runner conformance suite needs a private NATS bus (nats-server, through \
         cortexkit-bus-testkit) to capture the compaction advance notices the kit checks",
    );
    subject.bus = Some(Arc::new(bus));
}

/// Sandboxed runs can exercise every non-kill case without claiming crash conformance.
fn skip_crash_kills() -> bool {
    std::env::var("BROCA_CONFORMANCE_SKIP_CRASH_KILLS").as_deref() == Ok("1")
}

/// An interrupt into an existing session that names only the role's members (prompt, send_id,
/// delivery) takes every runner parameter it omits from the session's frozen config. Its model
/// request is therefore the frozen one's: the same model, generation settings, tool choice,
/// tools and caller cache key, with the earlier request's messages as its prefix. Defaults in
/// their place would change those bytes and lose the provider's cached prefix.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_interrupt_naming_only_role_members_renders_the_frozen_request() {
    use cortexkit_role_llm_runner_conformance::ScriptedTurn;
    if !sibling_bin("ck-subc", "SUBC_CORE_BIN").exists()
        || !sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN").exists()
    {
        assert!(!require_subc(), "the subc rig binaries are missing");
        println!("SKIP: the subc rig binaries are missing");
        return;
    }
    std::env::remove_var("SUBC_MODULE_ID");
    std::env::remove_var("SUBC_LAUNCH_NONCE");
    let scratch = broca_test_support::scratch_root()
        .join(format!("broca-inherit-frozen-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let session = "inherit-frozen";
    let turn = |text: &str| {
        ScriptedTurn::new(vec![ScriptedPart::Text(text.into())]).with_usage(
            cortexkit_role_llm_runner_conformance::ScriptedUsage::new(1, 0, 0, 1),
        )
    };
    subject
        .install_script(
            session,
            Script {
                turns: vec![turn("one"), turn("two")],
            },
        )
        .await
        .unwrap();
    let live = subject.spawn(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let ok = |reply: Reply| match reply {
        Reply::Response(value) => value,
        Reply::Error(error) => panic!("refused {} {}", error.code, error.message),
    };

    // The first turn sets non-default values for the parameters the interrupt omits, so a
    // default taken in place of the frozen value shows in the request comparison below.
    let mut first = subject.send_fields(session, true);
    first.insert("prompt".into(), json!("first"));
    first.insert("send_id".into(), json!("inherit-1"));
    first.insert(
        "generation".into(),
        json!({"temperature": 0.25, "top_p": 0.5, "max_output_tokens": 321}),
    );
    first.insert("tool_choice".into(), json!({"type": "required"}));
    first.insert("cache".into(), json!({"key": "inherit-cache-key"}));
    ok(route
        .request("session.send", Value::Object(first))
        .await
        .unwrap());
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let head = ok(route.request("session.head", json!({})).await.unwrap());
        if head["last_run_state"]["state"] == "completed" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the first turn never ended: {head}"
        );
        sleep(Duration::from_millis(50)).await;
    }

    let interrupt = json!({"prompt": "second", "send_id": "inherit-2", "delivery": "interrupt"});
    ok(route.request("session.send", interrupt).await.unwrap());
    let bodies = loop {
        let bodies = subject
            .shared
            .bodies
            .lock()
            .unwrap()
            .get(session)
            .cloned()
            .unwrap_or_default();
        if bodies.len() >= 2 {
            break bodies;
        }
        assert!(
            Instant::now() < deadline,
            "the interrupt made no model request"
        );
        sleep(Duration::from_millis(50)).await;
    };
    let (frozen, inherited) = (&bodies[0], &bodies[1]);
    println!("frozen request: {frozen}\ninterrupt's request: {inherited}");
    // The comparison below is only worth something if the frozen request carries the values a
    // default would not.
    assert_eq!(frozen["temperature"], json!(0.25), "{frozen}");
    assert_eq!(frozen["tool_choice"], json!("required"), "{frozen}");
    assert_eq!(
        frozen["prompt_cache_key"],
        json!("inherit-cache-key"),
        "{frozen}"
    );
    let without_messages = |body: &Value| {
        let mut body = body.as_object().unwrap().clone();
        body.remove("messages");
        body
    };
    assert_eq!(
        without_messages(inherited),
        without_messages(frozen),
        "everything but the messages is the frozen request's"
    );
    let frozen_messages = frozen["messages"].as_array().unwrap();
    let messages = inherited["messages"].as_array().unwrap();
    assert!(
        messages.len() > frozen_messages.len()
            && messages[..frozen_messages.len()] == frozen_messages[..],
        "the frozen request's messages are the interrupt's prefix"
    );
    drop(route);
    drop(live);
    broca_test_support::remove_rig_dir(&scratch);
}

/// A read exposes the provider's held call without making an incomplete tool turn model input.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn held_provider_call_is_indeterminate_until_its_result() {
    use cortexkit_role_llm_runner_conformance::{ScriptedToolCall, ScriptedTurn};
    if !sibling_bin("ck-subc", "SUBC_CORE_BIN").exists()
        || !sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN").exists()
    {
        assert!(!require_subc(), "the subc rig binaries are missing");
        println!("SKIP: the subc rig binaries are missing");
        return;
    }
    std::env::remove_var("SUBC_MODULE_ID");
    std::env::remove_var("SUBC_LAUNCH_NONCE");
    let scratch =
        broca_test_support::scratch_root().join(format!("broca-held-read-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let session = "held-read";
    let call = ScriptedToolCall {
        tool_call_id: "held_call".into(),
        tool: SCRIPTED_TOOL.into(),
        arguments: json!({"marker": "held-read-args"}),
        result: json!({"marker": "held-read-result"}),
        hold: true,
    };
    subject
        .install_script(
            session,
            Script {
                turns: vec![
                    ScriptedTurn::new(vec![ScriptedPart::ToolCall(call.clone())]).with_usage(
                        cortexkit_role_llm_runner_conformance::ScriptedUsage::new(1, 0, 0, 1),
                    ),
                    ScriptedTurn::new(vec![ScriptedPart::Text("done".into())]).with_usage(
                        cortexkit_role_llm_runner_conformance::ScriptedUsage::new(1, 0, 0, 1),
                    ),
                ],
            },
        )
        .await
        .unwrap();
    let live = subject.spawn(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let response = |reply: Reply| match reply {
        Reply::Response(value) => value,
        Reply::Error(error) => panic!(
            "refused {} {}: {:?}",
            error.code, error.message, error.detail
        ),
    };
    let mut fields = subject.send_fields(session, true);
    fields.insert("prompt".into(), json!("start"));
    fields.insert("send_id".into(), json!("held-start"));
    let admitted = response(
        route
            .request("session.send", Value::Object(fields))
            .await
            .unwrap(),
    );
    subject.await_tool_call(&call.arguments).await.unwrap();
    let read = json!({"from_ordinal": 0});
    let held = response(route.request("session.read", read.clone()).await.unwrap());
    println!("held read: {held}");
    let attribution = |page: &Value| -> Value {
        page["messages"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|message| message["tool_calls"].as_array().into_iter().flatten())
            .find(|entry| entry["tool_call_id"] == "held_call")
            .unwrap_or_else(|| panic!("held call missing: {page}"))
            .clone()
    };
    let before = attribution(&held);
    assert_eq!(before["indeterminate"], true);
    assert_eq!(before["dispatched_to"], TOOL_MODULE);
    assert!(before["call_key"]
        .as_str()
        .is_some_and(|key| !key.is_empty()));

    // A steer has no final receipt while the provider holds this step: only a later
    // step boundary can distinguish delivery in this turn from delivery in the next.
    let mut steer = subject.send_fields(session, false);
    steer.insert("prompt".into(), json!("steer while held"));
    steer.insert("send_id".into(), json!("held-steer"));
    steer.insert("delivery".into(), json!("steer"));
    let first = response(
        route
            .request("session.send", Value::Object(steer.clone()))
            .await
            .unwrap(),
    );
    let retry = response(
        route
            .request("session.send", Value::Object(steer))
            .await
            .unwrap(),
    );
    println!("held steer first: {first}\nheld steer re-send: {retry}");
    assert_eq!(first["run_id"], admitted["run_id"]);
    assert_eq!(retry["run_id"], admitted["run_id"]);
    assert!(first.get("delivered").is_none(), "{first}");
    assert!(retry.get("delivered").is_none(), "{retry}");
    subject.release_tool_call(&call.arguments).await.unwrap();
    let resolved = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let page = response(route.request("session.read", read.clone()).await.unwrap());
            if page.to_string().contains("held-read-result") {
                break page;
            }
            subject.pause().await;
        }
    })
    .await
    .expect("the held call's result becomes readable");
    let after = attribution(&resolved);
    assert_eq!(after["indeterminate"], false);
    assert_eq!(after["call_key"], before["call_key"]);
    assert_eq!(after["dispatched_to"], before["dispatched_to"]);
    assert_eq!(subject.tool_invocations(&call.arguments).await, 1);
    drop(route);
    drop(live);
    broca_test_support::remove_rig_dir(&scratch);
}

/// The production executor must expose the adapter to both the actor and the model loop.
/// A test-only executor with a hook plane cannot prove this wiring.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn production_executor_installs_hooks_and_describes_ordered_phases() {
    use cortexkit_role_llm_runner_conformance::ScriptedTurn;
    use step_transform_rig::*;
    assert!(
        sibling_bin("ck-subc", "SUBC_CORE_BIN").exists(),
        "real ck-subc required"
    );
    assert!(
        sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN").exists(),
        "real stranger module required"
    );
    let scratch =
        broca_test_support::scratch_root().join(format!("production-hooks-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let state = State::new(vec![
        subscription("pre_user", None, "pass", 1000),
        subscription("post_assistant", None, "pass", 1000),
    ]);
    state.answer_with(|_| append("-hooked"));
    let mut live = subject.start(&root).await.unwrap();
    let serving = register(&live.connection_file, state.clone()).await;
    let session = "production-hooks";
    subject
        .install_script(
            session,
            Script {
                turns: vec![ScriptedTurn::new(vec![ScriptedPart::Text("answer".into())])
                    .with_usage(cortexkit_role_llm_runner_conformance::ScriptedUsage::new(
                        1, 0, 0, 1,
                    ))],
            },
        )
        .await
        .unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let response = |reply: Reply| match reply {
        Reply::Response(value) => value,
        other => panic!("expected response, got {other:?}"),
    };
    let describe = response(route.request("role.describe", json!({})).await.unwrap());
    let served = route.describe_golden_bytes().await;
    let golden = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden")
        .join(writer_describe_golden_name(subject.service_tier_enabled));
    if std::env::var_os("BROCA_REGENERATE_ROLE_DESCRIBE_GOLDEN").is_some() {
        std::fs::write(&golden, &served).unwrap();
        println!("role_describe.json: {}", String::from_utf8_lossy(&served));
    }
    assert_eq!(
        served,
        std::fs::read(&golden).unwrap(),
        "role.describe must match the bytes served by the daemon"
    );
    assert!(describe["capabilities"]
        .as_array()
        .unwrap()
        .contains(&json!(
            cortexkit_role_llm_runner::capabilities::ORDERED_HOOK_PHASES
        )));
    // llm-runner 0.8 retired these groups: compaction moved to the compaction-runner role,
    // which the module manifest declares, so role.describe must no longer name them.
    for group in ["compaction", "model_view"] {
        assert!(
            !describe["capabilities"]
                .as_array()
                .unwrap()
                .contains(&json!(group)),
            "the 0.8 runner must not declare the retired {group} group"
        );
    }
    assert!(!describe["majors"][0]["ops"]
        .as_array()
        .unwrap()
        .contains(&json!("compaction.ready")));
    assert_eq!(
        describe.get("retention").is_some(),
        broca_module_serve::RETENTION_WRITER_ENABLED
    );
    assert!(
        !describe["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!("hooks")),
        "the pinned role has no hooks capability group"
    );
    let mut params = subject.send_fields(session, true);
    params.remove("tools");
    // Plans own system text, so identify this script in the user message instead.
    params.remove("system");
    params.insert("plan".into(), plan(&state.subscriptions, false));
    params.insert(
        "prompt".into(),
        json!(format!("{SESSION_MARKER}{session} input")),
    );
    params.insert("send_id".into(), json!("first"));
    let params = Value::Object(params);
    let admitted = response(route.request("session.send", params.clone()).await.unwrap());
    let run_id = admitted["run_id"].as_str().unwrap();
    broca_test_support::event_timeout(
        "production_executor_installs_hooks_and_describes_ordered_phases",
        async {
            loop {
                let reply = response(
                    route
                        .request("run.result", json!({"run_id":run_id}))
                        .await
                        .unwrap(),
                );
                if reply["state"] == "completed" {
                    assert_eq!(reply["final_message"]["text"], "answer-hooked");
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        },
        || "the run never completed".to_owned(),
    )
    .await
    .unwrap();
    let records = subject.records(&live, session).await;
    // The run's input is stored as the pre_user hook rewrote it, and its start records the
    // hooks it ran under.
    let input = records
        .iter()
        .rev()
        .find_map(|r| match r {
            Record::Input(input) => Some(input.message.clone()),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        input.content[0],
        broca_protocol::content::ContentBlock::Text {
            text: format!("{SESSION_MARKER}{session} input-hooked")
        }
    );
    assert!(records
        .iter()
        .any(|r| matches!(r, Record::RunStart(start) if start.hooks.is_some())));
    assert_eq!(
        state.counts(),
        [("pre_user".into(), 1), ("post_assistant".into(), 1)]
            .into_iter()
            .collect()
    );
    // Step-transform v2 hooks name no session in the body; the per-session route does.
    for call in state.hooks() {
        assert!(
            call.get("harness").is_none() && call.get("session").is_none(),
            "{call}"
        );
    }
    state.clear();
    response(route.request("session.send", params).await.unwrap());
    assert!(
        state.hooks().is_empty(),
        "a keyed retry must be answered before pre_user"
    );
    serving.abort();
    live.kill_all();
    std::fs::remove_dir_all(scratch).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orderly_handle_drop_seals_a_held_tool_with_shutdown_reason() {
    use cortexkit_role_llm_runner_conformance::{ScriptedToolCall, ScriptedTurn};
    for (name, key) in [
        ("ck-subc", "SUBC_CORE_BIN"),
        ("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
    ] {
        if !sibling_bin(name, key).exists() {
            assert!(!require_subc(), "{name} is required");
            println!("SKIP: {name} is missing");
            return;
        }
    }
    std::env::remove_var("SUBC_MODULE_ID");
    std::env::remove_var("SUBC_LAUNCH_NONCE");
    let scratch = broca_test_support::scratch_root()
        .join(format!("broca-orderly-drop-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let session = "orderly-drop";
    let held = ScriptedToolCall {
        tool_call_id: "held".into(),
        tool: SCRIPTED_TOOL.into(),
        arguments: json!({"marker":"orderly-drop"}),
        result: json!({"done":true}),
        hold: true,
    };
    subject
        .install_script(
            session,
            Script {
                turns: vec![ScriptedTurn::new(vec![ScriptedPart::ToolCall(
                    held.clone(),
                )])],
            },
        )
        .await
        .unwrap();
    let live = subject.spawn(&root).await.unwrap();
    let state_root = live.state_root.clone();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let mut fields = subject.send_fields(session, true);
    fields.insert("prompt".into(), json!("start"));
    fields.insert("send_id".into(), json!("orderly-drop"));
    let Reply::Response(admitted) = route
        .request("session.send", Value::Object(fields))
        .await
        .unwrap()
    else {
        panic!("send refused");
    };
    subject.await_tool_call(&held.arguments).await.unwrap();
    drop(route);
    // Dropping Live requests an orderly shutdown; verify its recorded cause before restarting.
    drop(live);
    let records = stored_log::stored_records(&state_root, &subject.bind_identity(session)).await;
    let terminal = records.iter().rev().find_map(|record| match record {
        Record::RunEnd(end) => Some(end.interrupt_reason.as_deref()),
        _ => None,
    });
    assert_eq!(
        terminal.flatten(),
        Some("runner_shutdown"),
        "log after orderly stop: {records:?}"
    );
    let live = subject.restart(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    for method in ["run.status", "run.result"] {
        let Reply::Response(reply) = route
            .request(method, json!({"run_id":admitted["run_id"]}))
            .await
            .unwrap()
        else {
            panic!("{method} refused");
        };
        assert_eq!(reply["state"], "interrupted", "{method}: {reply}");
        assert_eq!(reply["reason"], "runner_shutdown", "{method}: {reply}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_delivery_order_send_is_retracted_before_tool_result() {
    use cortexkit_role_llm_runner_conformance::{ScriptedToolCall, ScriptedTurn};
    for (name, key) in [
        ("ck-subc", "SUBC_CORE_BIN"),
        ("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
    ] {
        if !sibling_bin(name, key).exists() {
            assert!(!require_subc(), "{name} is required");
            println!("SKIP: {name} is missing");
            return;
        }
    }
    std::env::remove_var("SUBC_MODULE_ID");
    std::env::remove_var("SUBC_LAUNCH_NONCE");
    let scratch = broca_test_support::scratch_root()
        .join(format!("broca-ordered-retract-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let session = "ordered-retract";
    let held = ScriptedToolCall {
        tool_call_id: "held".into(),
        tool: SCRIPTED_TOOL.into(),
        arguments: json!({"marker":"ordered-retract-held"}),
        result: json!({"done":true}),
        hold: true,
    };
    subject
        .install_script(
            session,
            Script {
                turns: vec![
                    ScriptedTurn::new(vec![ScriptedPart::ToolCall(held.clone())]),
                    ScriptedTurn::new(vec![ScriptedPart::Text("done".into())]),
                ],
            },
        )
        .await
        .unwrap();
    let live = subject.spawn(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let response = |reply| match reply {
        Reply::Response(value) => value,
        other => panic!("expected response, got {other:?}"),
    };
    let mut fields = subject.send_fields(session, true);
    fields.insert("prompt".into(), json!("start"));
    fields.insert("send_id".into(), json!("ordered-run"));
    let admitted = response(
        route
            .request("session.send", Value::Object(fields))
            .await
            .unwrap(),
    );
    subject.await_tool_call(&held.arguments).await.unwrap();
    let marker = "ordered-send-retracted-before-delivery";
    let mut fields = subject.send_fields(session, false);
    fields.insert("prompt".into(), json!(marker));
    fields.insert("send_id".into(), json!("ordered-withdrawn"));
    fields.insert("delivery".into(), json!(["tool_result"]));
    let pending = response(
        route
            .request("session.send", Value::Object(fields))
            .await
            .unwrap(),
    );
    assert_eq!(pending["state"], "pending");
    let retract = json!({"submission_id":pending["submission_id"]});
    assert_eq!(
        response(
            route
                .request("session.retract", retract.clone())
                .await
                .unwrap()
        ),
        "retracted"
    );
    assert_eq!(
        response(route.request("session.retract", retract).await.unwrap()),
        "not_pending"
    );
    subject.release_tool_call(&held.arguments).await.unwrap();
    broca_test_support::wait_until("ordered retract run completes", || async {
        let result = response(
            route
                .request("run.result", json!({"run_id":admitted["run_id"]}))
                .await
                .unwrap(),
        );
        if result["state"] == "completed" {
            Ok(())
        } else {
            Err(format!("{result}"))
        }
    })
    .await;
    let page = response(
        route
            .request("session.read", json!({"from_ordinal":0}))
            .await
            .unwrap(),
    );
    assert!(
        !serde_json::to_string(&page).unwrap().contains(marker),
        "retracted send reached the transcript: {page}"
    );
}

#[cfg(test)]
mod held_call_tests {
    use super::*;

    #[tokio::test]
    async fn service_tier_fixture_uses_cache_scope_identity_for_plan_sends() {
        let shared = Arc::new(Shared::default());
        let mut subject = subject(shared.clone());
        subject.service_tier_enabled = true;
        let session = "tier-compaction-fixture-with-a-long-session-name-that-needs-clamping";
        assert!(subject
            .configure_service_tier(session, "service_tier_ride_before_changed_request")
            .await
            .unwrap());
        let fields = subject.send_fields(session, true);
        // Tier sessions carry no plan: the v1 compaction ride they once fed is retired.
        assert!(fields.get("plan").is_none());
        assert!(
            fields.get("system").is_none(),
            "fixture identity must not alter system text"
        );
        let key =
            broca_provider::hash::clamp_prompt_cache_key(fields["cache"]["key"].as_str().unwrap());
        assert_eq!(key.len(), 32, "exercise the renderer's long-key spelling");
        assert_eq!(
            shared
                .service_tier_keys
                .lock()
                .unwrap()
                .get(&key)
                .map(String::as_str),
            Some(session)
        );
    }
    use std::sync::atomic::Ordering;

    /// A subject that starts no daemon, for checks of what it builds.
    pub(super) fn subject(shared: Arc<Shared>) -> BrocaSubject {
        BrocaSubject {
            shared,
            routed_instances: Default::default(),
            model_url: String::new(),
            project_root: PathBuf::new(),
            dump_dir: None,
            binary: None,
            backup_classes: None,
            bus_events_enabled: false,
            process_kills_disabled: true,
            service_tier_enabled: false,
            reserved_reader: false,
            #[cfg(feature = "test-clock")]
            bus: None,
        }
    }

    #[test]
    fn retract_own_send_kit_plan_names_both_attested_senders() {
        use cortexkit_role_llm_runner::delivery_order::{admit, Mode, PlanSender};
        let subject = subject(Arc::new(Shared::default()));
        let a = subject.plan_sender().unwrap();
        let b = subject.stranger_stamp();
        let owner_nonce = Path::new("fixture/owner.nonce");
        let (module_b, nonce_b) = caller_module_nonce(&b.principal, owner_nonce)
            .unwrap()
            .expect("sender B must present a reserved consumer identity");
        assert_eq!(module_b, STRANGER_MODULE);
        assert_eq!(nonce_b, PathBuf::from("fixture/stranger.nonce"));
        assert_ne!(b.principal, subject.owner_stamp().principal);

        // Match the published shared-ID case: B is named by its RouteStamp label,
        // not by a second PlanSenderIdentity fixture supplied by the adapter.
        let members = Map::from_iter([(
            "senders".into(),
            json!([PlanSender::new(&a.module), PlanSender::new(&b.principal),]),
        )]);
        let fields = subject
            .send_fields_with_plan("shared-id", true, &members)
            .unwrap();
        let senders: Vec<PlanSender> =
            serde_json::from_value(fields["plan"]["senders"].clone()).unwrap();
        assert_eq!(
            senders
                .iter()
                .map(|s| s.module.as_str())
                .collect::<Vec<_>>(),
            [SENDER_MODULE, STRANGER_MODULE]
        );
        for module in [a.module.as_str(), module_b] {
            assert_eq!(
                admit(module, Some(OWNER_MODULE), &senders, &[Mode::Steer]),
                Ok(())
            );
        }
    }

    #[test]
    fn retract_own_send_stranger_stays_unnamed_elsewhere_and_direct_stays_unidentified() {
        use cortexkit_role_llm_runner::delivery_order::{admit, Mode, PlanSender};
        let subject = subject(Arc::new(Shared::default()));
        let stamp = subject.stranger_stamp();
        let owner_nonce = Path::new("fixture/owner.nonce");
        assert!(caller_module_nonce("direct", owner_nonce)
            .unwrap()
            .is_none());
        let (module, _) = caller_module_nonce(&stamp.principal, owner_nonce)
            .unwrap()
            .unwrap();
        for senders in [
            vec![PlanSender::new(SENDER_MODULE)],
            subject
                .head_sender_identities()
                .unwrap()
                .iter()
                .map(|id| PlanSender::new(&id.module))
                .collect(),
        ] {
            assert!(!senders.iter().any(|sender| sender.module == module));
            assert_eq!(
                admit(module, Some(OWNER_MODULE), &senders, &[Mode::Steer]),
                Err((
                    cortexkit_role_llm_runner::errors::SCOPE_OWNER_MISMATCH,
                    None
                ))
            );
        }
        let ownership = broca_core::SessionOwnershipV1 {
            episodes: 1,
            first_principal: Some(broca_protocol::BindPrincipal::Reserved {
                module_id: OWNER_MODULE.into(),
            }),
            ..Default::default()
        };
        assert!(broca_core::scope_admission::decide_owner_access(
            &ownership,
            Some(&broca_protocol::BindPrincipal::Reserved {
                module_id: module.into()
            }),
            &broca_protocol::SendScope::Unstamped,
        )
        .is_err());
    }

    #[test]
    fn session_refresh_fields_are_plan_driven_without_a_model_switch() {
        let subject = subject(Arc::new(Shared::default()));
        for label in [
            "next-run", "rung", "held", "flush", "repeat", "stale", "host-set",
        ] {
            let session = format!("cf-change-{label}-1");
            let first = subject.send_fields(&session, true);
            assert!(first.get("plan").is_some(), "{session}: {first:?}");
            assert!(first["tools"].is_null());
            assert!(first["system"].is_null());
            let next = subject.send_fields(&session, false);
            assert_eq!(next["model"], first["model"], "{session}");
            for field in ["tools", "system", "plan"] {
                assert!(!next.contains_key(field), "{session}: {field} in {next:?}");
            }
        }
        for session in [
            "ordinary-change-next-run-1",
            "cf-change-next-run-0",
            "cf-change-next-run-01",
            "cf-change-next-run-1-other",
        ] {
            let fields = subject.send_fields(session, true);
            assert!(fields.get("plan").is_none(), "{session}");
            assert!(fields["tools"].is_array(), "{session}");
            assert_eq!(fields["system"], format!("{SESSION_MARKER}{session}"));
        }
    }

    struct RefreshPlanAdmitter;

    #[async_trait]
    impl broca_fetch_plan::PlanAdmitter for RefreshPlanAdmitter {
        async fn admit(
            &self,
            plan: &broca_fetch_plan::Plan,
            _: &broca_protocol::BindIdentity,
            _: Option<&broca_protocol::SessionScope>,
        ) -> Result<broca_fetch_plan::Admitted, broca_fetch_plan::Refusal> {
            let tools = plan.composition["providers"]
                .as_array()
                .unwrap()
                .iter()
                .flat_map(|provider| provider["tools"].as_array().unwrap())
                .map(|tool| {
                    serde_json::from_value(json!({
                        "name":tool["name"], "description":"scripted fixture",
                        "input_schema":{"type":"object"}, "module":TOOL_MODULE
                    }))
                    .unwrap()
                })
                .collect();
            Ok(broca_fetch_plan::Admitted {
                manifest: broca_wal::PlanManifest {
                    rich_ui: None,
                    plan: broca_wal::OpaqueJson::from_json_text(plan.text.clone()).unwrap(),
                    plan_identity: plan.identity.clone(),
                    composition_digest: plan.composition_digest.clone(),
                    fetches: Vec::new(),
                    tools: Arc::new(tools),
                    system_text: String::new(),
                    system_digest: String::new(),
                    join_version: 1,
                    role_versions: Default::default(),
                    declarations: Vec::new(),
                },
            })
        }
    }

    /// Build sends from the adapter's real hooks, just as the kit's send_params helper does.
    async fn send_refresh_fixture(
        subject: &BrocaSubject,
        rig: &mut delivery_rig::Rig,
        session: &str,
        first: bool,
    ) -> subc_protocol::Frame {
        let mut params = subject.send_fields(session, first);
        params.insert("prompt".into(), json!("scripted refresh prompt"));
        params.insert(
            "send_id".into(),
            json!(if first { "first" } else { "next" }),
        );
        rig.request(7, json!({"method":"session.send","params":params}))
            .await
    }

    #[tokio::test]
    async fn session_refresh_adapter_sends_apply_at_the_real_serve_boundary() {
        use cortexkit_role_llm_runner_conformance::RefreshVariant;
        use delivery_rig::{result, Rig, Step};
        use subc_protocol::{FrameType, Principal};

        for (label, policy, active, flush) in [
            ("next-run", "immediately", false, false),
            ("held", "immediately", true, false),
            ("flush", "on_prefix_rebuild", true, true),
            ("rung", "on_prefix_rebuild", false, false),
        ] {
            let subject = subject(Arc::new(Shared::default()));
            let session = format!("cf-change-{label}-1");
            let mut rig = Rig::start(&format!("refresh-adapter-{label}")).await;
            rig.bind_as(
                7,
                Some(Principal::Reserved {
                    module_id: "owner".into(),
                }),
            )
            .await;
            rig.registry()
                .set_plan_admitter(Arc::new(RefreshPlanAdmitter));
            rig.script.push(&[Step::Text("first run")]);
            rig.script.hold_model(active);
            let first = send_refresh_fixture(&subject, &mut rig, &session, true).await;
            assert_eq!(first.header.ty, FrameType::Response, "{first:?}");
            if active {
                rig.wait_entered().await;
            } else {
                rig.wait_idle().await;
            }
            let plan = subject
                .refresh_plan(&session, RefreshVariant::ChangedTools)
                .unwrap();
            let refreshed = rig
                .request(
                    7,
                    json!({"method":"session.refresh","params":{
                        "plan":plan.plan,"generation":1,"policy":policy
                    }}),
                )
                .await;
            assert_eq!(refreshed.header.ty, FrameType::Response, "{refreshed:?}");
            assert_eq!(result(&refreshed)["run_in_progress"], active);
            assert_eq!(
                result(&refreshed)["applies_from_next_run"],
                policy == "immediately"
            );
            if flush {
                let reply = rig
                    .request(
                        7,
                        json!({"method":"session.flush_prefix","params":{"send_id":"flush"}}),
                    )
                    .await;
                assert_eq!(result(&reply)["pending_generation"], 1);
            }
            rig.script.hold_model(false);
            rig.wait_idle().await;
            rig.script.push(&[Step::Text("next run")]);
            let next = send_refresh_fixture(&subject, &mut rig, &session, false).await;
            assert_eq!(next.header.ty, FrameType::Response, "{label}: {next:?}");
            rig.wait_idle().await;
            let applied = policy == "immediately" || flush;
            let offered = rig.script.offered_tools.lock().unwrap().clone();
            assert_eq!(offered[0], vec![SCRIPTED_TOOL.to_string()]);
            assert_eq!(
                offered[1],
                if applied {
                    Vec::new()
                } else {
                    vec![SCRIPTED_TOOL.to_string()]
                }
            );
            let baseline = rig
                .request(7, json!({"method":"session.baseline","params":{}}))
                .await;
            assert_eq!(
                result(&baseline)["baseline"]["applied_generation"],
                u64::from(applied)
            );
            let models: Vec<_> = rig
                .run_configs()
                .await
                .into_iter()
                .map(|config| config.model)
                .collect();
            assert_eq!(models[0], models[1], "the adapter must not fake a rebuild");
            rig.finish().await;
        }
    }

    #[tokio::test]
    async fn held_catalog_declarations_do_not_leak_to_other_bound_sessions() {
        let shared = Arc::new(Shared::default());
        let provider = ToolProvider {
            shared: shared.clone(),
            routes: Mutex::new(HashMap::from([
                ((11, 1), "held-catalog".into()),
                ((12, 1), "structured-catalog".into()),
                ((13, 1), "media-catalog".into()),
            ])),
        };
        let request = json!({"name":"tool.catalog", "arguments":{"composition":{
            "providers":[{"provider":TOOL_MODULE,"tools":[{"name":SCRIPTED_TOOL,"capabilities":[]}]}],
            "step_transform_items":[]
        }}});
        let before = serde_json::to_vec(&provider.catalog_for_route((12, 1), &request)).unwrap();
        assert!(serde_json::from_slice::<Value>(&before)
            .unwrap()
            .get("capabilities")
            .is_none());
        subject(shared.clone())
            .install_held_reply("held-catalog", &json!({"nonce":"held-catalog"}), "consent")
            .await
            .unwrap();
        let held = provider.catalog_for_route((11, 1), &request);
        assert_eq!(held["capabilities"], json!({"late_results":true}));
        assert_eq!(held["tools"].as_array().unwrap().len(), 1);
        assert!(held["tools"]
            .as_array()
            .unwrap()
            .iter()
            .all(|tool| tool["result_format"] == "tool-result/v1"));
        for route in [(12, 1), (13, 1), (11, 2), (14, 1)] {
            let catalog = provider.catalog_for_route(route, &request);
            // Every catalog declares tool-result/v1 results; only the held session's adds
            // late-result capabilities.
            assert!(catalog.get("capabilities").is_none(), "{catalog}");
            assert_eq!(
                serde_json::to_vec(&catalog).unwrap(),
                before,
                "route {route:?} stays legacy after a held reply is installed"
            );
        }
    }

    #[tokio::test]
    async fn registered_held_reply_uses_a_plan_and_publishes_each_late_kind() {
        let shared = Arc::new(Shared::default());
        let subject = subject(shared.clone());
        let arguments = json!({"nonce":"kit-held-reply"});
        subject
            .install_held_reply("held-kit", &arguments, "consent")
            .await
            .unwrap();
        let fields = subject.send_fields("held-kit", true);
        assert!(fields.contains_key("plan"));
        assert!(fields["tools"].is_null());
        assert_eq!(
            subject.send_fields("ordinary", true)["system"],
            format!("{SESSION_MARKER}ordinary")
        );
        held_late_results::note_bind(
            &shared,
            (4, 1),
            Some(&["tool-result/v1".into(), "held-reply/v1".into()]),
        );
        let answer = held_late_results::answer_held(
            &shared,
            (4, 1),
            &arguments.to_string(),
            &json!({"call_key":"lineage:6"}),
        )
        .unwrap();
        let subc_client_rs::HandlerOutcome::Response(body) = answer else {
            panic!("held response expected")
        };
        let answer: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(answer["held"]["reason"], "consent");
        assert_eq!(
            subject.await_held_reply(&arguments).await.unwrap(),
            "lineage:6"
        );
        for entry in [
            ScriptedLateEntry::result(json!({"nonce":"late-result"})),
            ScriptedLateEntry::not_started("restart_before_dispatch"),
            ScriptedLateEntry::expired(),
        ] {
            subject.publish_late_entry(&arguments, entry).await.unwrap();
        }
        let feed = held_late_results::serve_feed(
            &shared,
            &json!({"name":"late_results", "arguments":{"since":null}}),
        )
        .await
        .unwrap();
        let subc_client_rs::HandlerOutcome::Response(body) = feed else {
            panic!("feed response expected")
        };
        let feed: Value = serde_json::from_slice(&body).unwrap();
        let entries = feed["entries"].as_array().unwrap();
        assert_eq!(
            entries
                .iter()
                .map(|entry| entry["kind"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["result", "not_started", "expired"]
        );
        assert_eq!(entries[1]["reason"], "restart_before_dispatch");
        assert!(entries[0]["result"].to_string().contains("late-result"));
        assert_eq!(subject.tool_invocations(&arguments).await, 1);
    }

    #[tokio::test]
    async fn ordinary_fixture_fields_keep_legacy_system_marker() {
        let shared = Arc::new(Shared::default());
        let subject = subject(shared.clone());
        for session in ["step-cut", "mc-e2e"] {
            subject
                .install_script(session, Script::default())
                .await
                .unwrap();
        }
        for session in [
            "step-cut",
            "mc-e2e",
            "provider-error-retained",
            "provider-error-forever",
        ] {
            let fields = subject.send_fields(session, true);
            assert_eq!(
                fields["system"],
                format!("{SESSION_MARKER}{session}"),
                "{session}"
            );
            assert_eq!(fields["model"]["model"], "mock-model", "{session}");
        }
        assert!(
            !shared
                .scripts
                .lock()
                .unwrap()
                .contains_key("provider-error-forever"),
            "provider-error fixtures must route by their system marker even without a script"
        );
    }

    #[test]
    fn temperature_kit_fixture_declares_full_range_and_observes_the_actual_request() {
        let subject = subject(Arc::new(Shared::default()));
        assert!(subject.capabilities().contains(&Capability::Temperature));
        let fields = subject.send_fields("cf-temperature-1", true);
        assert_eq!(
            fields["model"],
            json!({"provider": STRUCTURED_OUTPUT_PROVIDER, "model": "conformance-temperature"})
        );
        let root = broca_test_support::scratch_root()
            .join(format!("temperature-kit-catalog-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        seed_structured_output_catalog(&root).unwrap();
        let response: Value =
            serde_json::from_slice(&std::fs::read(root.join("catalog/fusiform-lkg.json")).unwrap())
                .unwrap();
        let row =
            &response["models"][format!("{STRUCTURED_OUTPUT_PROVIDER}/conformance-temperature")];
        assert_eq!(row["capability.temperature"], true);
        assert_eq!(row["limit.temperature_min"], 0.0);
        assert_eq!(row["limit.temperature_max"], 2.0);
        let observation = observe_model_request(&json!({"temperature": 2.0, "max_tokens": 16}));
        assert_eq!(observation.temperature, Some(2.0));
        assert_eq!(observation.max_output_tokens, Some(16));
        std::fs::remove_dir_all(root).unwrap();
    }

    #[tokio::test]
    async fn only_registered_history_and_display_fixture_names_use_model_routing() {
        let subject = subject(Arc::new(Shared::default()));
        for session in [
            "cf-cf-history-3-4",
            "cf-display-5",
            "cf-display-sender-6",
            "cf-display-model-7",
        ] {
            assert!(
                subject.send_fields(session, true).contains_key("system"),
                "unregistered {session}"
            );
            subject
                .install_script(session, Script::default())
                .await
                .unwrap();
            let fields = subject.send_fields(session, true);
            assert!(!fields.contains_key("system"), "{session}");
            assert_eq!(
                fields["model"]["model"],
                format!("{PLAN_MODEL_PREFIX}{session}")
            );
        }
        for session in [
            "cf-cf-history-3-4-extra",
            "cf-display-ish-5",
            "prefix-cf-display-5",
            "cf-display-0",
            "cf-display-05",
            "cf-cf-history-0-7",
        ] {
            assert!(!history_display_kit_session(session), "{session}");
            subject
                .install_script(session, Script::default())
                .await
                .unwrap();
            let fields = subject.send_fields(session, true);
            assert_eq!(
                fields["system"],
                format!("{SESSION_MARKER}{session}"),
                "{session}"
            );
            assert_eq!(fields["model"]["model"], "mock-model", "{session}");
        }
    }

    #[tokio::test]
    async fn release_registered_future_call_keeps_release_until_arrival() {
        let shared = Arc::new(Shared::default());
        let arguments = json!({"nonce":"future-held-call"});
        let gate = Arc::new(HoldGate::default());
        shared
            .holds
            .lock()
            .unwrap()
            .insert(arguments.to_string(), gate.clone());
        let subject = subject(shared);
        subject
            .release_tool_call(&arguments)
            .await
            .expect("a registered future hold can be released");
        assert!(
            !gate.arrived.load(Ordering::SeqCst),
            "release must not fabricate provider arrival"
        );
        assert!(gate.released.load(Ordering::SeqCst));
        HoldGate::set(&gate.arrived, &gate.arrived_wake);
        tokio::time::timeout(
            Duration::from_secs(1),
            HoldGate::wait(&gate.released, &gate.released_wake),
        )
        .await
        .expect("a later arriving provider must see the earlier release");
    }

    #[tokio::test]
    async fn unregistered_call_release_is_rejected() {
        let subject = subject(Arc::new(Shared::default()));
        let error = subject
            .release_tool_call(&json!({"nonce":"unknown-call"}))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("the call is not held"));
    }
}

#[tokio::test]
async fn scripted_model_failure_ends_one_turn_and_preserves_fractional_retry_hint() {
    use cortexkit_role_llm_runner_conformance::ScriptedTurn;
    let shared = Arc::new(Shared::default());
    let session = "scripted-failure-fixture";
    shared
        .failure_turns
        .lock()
        .unwrap()
        .insert(session.into(), 0);
    shared.scripts.lock().unwrap().insert(
        session.into(),
        Script {
            turns: vec![
                ScriptedTurn::new(vec![]).with_model_failure(
                    cortexkit_role_llm_runner::run::RunError::new(
                        "transient",
                        "synthetic failure",
                        "rate_limited",
                    )
                    .with_retry_after_secs(1.5),
                ),
                ScriptedTurn::new(vec![ScriptedPart::Text("recovered".into())]),
            ],
        },
    );
    let base = start_model(shared.clone()).await;
    async fn post(base: &str, session: &str) -> String {
        let address = base
            .strip_prefix("http://")
            .unwrap()
            .strip_suffix("/v1")
            .unwrap();
        let mut stream = tokio::net::TcpStream::connect(address).await.unwrap();
        let body = json!({"model":"mock-model","messages":[{"role":"system","content":format!("{SESSION_MARKER}{session}")},{"role":"user","content":"synthetic prompt"}]}).to_string();
        stream.write_all(format!("POST /v1/chat/completions HTTP/1.1\r\nhost: localhost\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        let mut reply = Vec::new();
        stream.read_to_end(&mut reply).await.unwrap();
        String::from_utf8(reply).unwrap()
    }
    let failure = post(&base, session).await;
    assert!(failure.starts_with("HTTP/1.1 429"), "{failure}");
    assert!(failure.contains("retry-after: 1.5\r\n"));
    assert!(failure.contains("x-should-retry: false\r\n"));
    let recovered = post(&base, session).await;
    assert!(recovered.starts_with("HTTP/1.1 200"), "{recovered}");
    assert!(recovered.contains("recovered"));
    assert_eq!(shared.failure_turns.lock().unwrap()[session], 2);
}

/// The adapter claims `notice_with_speaks_for` exactly when broca's role.describe declares it,
/// so the kit grades that group's cases by what broca serves.
#[tokio::test]
async fn conformance_subject_declares_notice_with_speaks_for_as_described() {
    let root = broca_test_support::scratch_root().join("subject-notice-with-speaks-for");
    let subject = BrocaSubject::new(&root).await;
    let described = broca_wire::broca_role_describe_with_writers("test", true, true, true)
        .capabilities
        .iter()
        .any(|name| name == cortexkit_role_llm_runner::capabilities::NOTICE_WITH_SPEAKS_FOR);
    assert!(described);
    assert_eq!(
        subject
            .capabilities()
            .contains(&Capability::NoticeWithSpeaksFor),
        described
    );
}

#[tokio::test]
async fn conformance_subject_always_declares_attachments() {
    let root = broca_test_support::scratch_root().join("subject-attachments");
    let subject = BrocaSubject::new(&root).await;
    assert!(subject.capabilities().contains(&Capability::Attachments));
    assert!(subject.capabilities().contains(&Capability::MediaOnlySend));
    assert!(subject.image_tool_result("old-image-rule").is_some());
    assert!(subject.capabilities().contains(&Capability::MessageMarks));
    assert!(subject.capabilities().contains(&Capability::MediaRefs));
}

#[test]
fn message_marks_fixture_grants_only_the_required_plan_fetch_target() {
    let scopes = scope_fixture_records();
    let scopes = scopes.as_array().unwrap();
    let plain = scopes
        .iter()
        .find(|scope| scope["ref"] == PLAIN_SCOPE_REF)
        .unwrap();
    let flow = scopes
        .iter()
        .find(|scope| scope["ref"] == FLOW_SCOPE_REF)
        .unwrap();
    assert_eq!(
        plain["carriers"],
        json!([
            {"principal":{"kind":"reserved","module_id":SENDER_MODULE},"targets":["broca"]},
            {"principal":{"kind":"reserved","module_id":"broca"},"targets":[TOOL_MODULE]}
        ])
    );
    assert_eq!(
        flow["carriers"],
        json!([
            {"principal":{"kind":"reserved","module_id":SENDER_MODULE},"targets":["broca"]}
        ])
    );
}

#[tokio::test]
async fn message_marks_fixture_scope_and_plan_are_limited_to_exact_kit_sessions() {
    let root = broca_test_support::scratch_root().join("subject-message-marks-plan");
    let subject = BrocaSubject::new(&root).await;
    let owner = subject.owner_stamp();
    for session in ["cf-message-marks-1", "cf-marks-retry-2"] {
        let stamp = message_marks_route_stamp(session, &owner);
        assert_eq!(stamp, scope_fixture_stamps(PLAIN_SCOPE_REF).1);
        let first = subject.send_fields(session, true);
        assert!(first["plan"].is_object(), "{first:?}");
        let later = subject.send_fields(session, false);
        for field in ["tools", "system", "plan"] {
            assert!(!later.contains_key(field), "{later:?}");
        }
        let stranger = subject.stranger_stamp();
        assert_eq!(message_marks_route_stamp(session, &stranger), stranger);
    }
    for session in [
        "ordinary-message-marks-1",
        "cf-message-marks-0",
        "cf-message-marks-01",
        "cf-message-marks-1-other",
    ] {
        assert!(!message_marks_kit_session(session));
        assert_eq!(message_marks_route_stamp(session, &owner), owner);
        assert!(subject.send_fields(session, true).get("plan").is_none());
    }
}

#[tokio::test]
async fn media_refs_output_pointer_identifies_only_the_named_tool_output() {
    let root = broca_test_support::scratch_root().join("subject-media-refs-output-pointer");
    let subject = BrocaSubject::new(&root).await;
    for output in [
        json!({"kind":"text","text":"before [image omitted] after"}),
        json!({"kind":"parts","parts":[{"media":{"type":"media_ref","sha256":"a".repeat(64)}}]}),
    ] {
        let message = json!({"role":"tool","content":[
            {"type":"text","text":"before"},
            {"type":"tool_result","tool_call_id":"one","output":output,"metadata":{"type":"media_ref"}},
            {"type":"tool_call","tool_call_id":"two","output":"not a tool result"},
            {"type":"tool_result","tool_call_id":"three","output":{"kind":"text","text":"second result"}},
            {"type":"tool_result","tool_call_id":"missing-output"}
        ]});
        subject.shared.read_part_keys.lock().unwrap().insert(
            message.to_string(),
            ["text", "opaque-first", "call", "opaque-second", "missing"]
                .map(str::to_owned)
                .to_vec(),
        );
        for (key, expected) in [
            ("opaque-first", "/content/1/output"),
            ("opaque-second", "/content/3/output"),
        ] {
            let pointer = subject
                .media_refs_tool_output_pointer(&message, key)
                .unwrap();
            assert_eq!(pointer, expected);
            assert!(message.pointer(&pointer).is_some());
        }
        for key in ["text", "call", "missing", "unknown"] {
            assert_eq!(subject.media_refs_tool_output_pointer(&message, key), None);
        }
    }
}

#[tokio::test]
async fn conformance_later_send_fields_inherit_the_frozen_tools() {
    let root = broca_test_support::scratch_root().join("subject-frozen-tool-fields");
    let subject = BrocaSubject::new(&root).await;
    for session in ["ordinary", "cf-attachments-1"] {
        let first = subject.send_fields(session, true);
        assert!(first["tools"].is_array());
        let later = subject.send_fields(session, false);
        assert!(!later.contains_key("tools"), "{session}: {later:?}");
        assert_eq!(first["model"], later["model"]);
        let explicit = subject
            .send_fields_with_tools(session, false, &[SCRIPTED_TOOL])
            .unwrap();
        assert!(!explicit.contains_key("tools"), "{explicit:?}");
    }
}

fn attachment_contract_gaps(
    report: &cortexkit_role_llm_runner_conformance::SuiteReport,
) -> Vec<&'static str> {
    let specs = cortexkit_role_llm_runner_conformance::CASES
        .iter()
        .filter(|spec| spec.name.starts_with("attachments_") || spec.name == "media_only_send")
        .collect::<Vec<_>>();
    assert!(
        !specs.is_empty(),
        "the shared suite must contain attachment cases"
    );
    specs
        .into_iter()
        .filter(|spec| !matches!(report.outcome(spec.name), Some(CaseOutcome::Passed)))
        .map(|spec| spec.name)
        .collect()
}

#[test]
fn attachment_report_guard_rejects_missing_or_skipped_cases() {
    use cortexkit_role_llm_runner_conformance::{CaseReport, SuiteReport, SuiteVerdict, CASES};
    let mut report = SuiteReport {
        cases: CASES
            .iter()
            .filter(|spec| spec.name.starts_with("attachments_") || spec.name == "media_only_send")
            .map(|spec| CaseReport {
                case: spec.name,
                requires: spec.requires.to_vec(),
                outcome: CaseOutcome::Passed,
            })
            .collect(),
        kills: vec![],
        verdict: SuiteVerdict::Passed,
    };
    assert!(attachment_contract_gaps(&report).is_empty());
    for index in 0..report.cases.len() {
        let name = report.cases[index].case;
        report.cases[index].outcome = CaseOutcome::Skipped {
            missing: vec![Capability::Attachments],
        };
        assert_eq!(attachment_contract_gaps(&report), vec![name]);
        report.cases[index].outcome = CaseOutcome::Passed;
    }
    let missing = report.cases.pop().unwrap();
    assert_eq!(attachment_contract_gaps(&report), vec![missing.case]);
    report.cases.push(missing);
    let name = report.cases[0].case;
    report.cases[0].outcome = CaseOutcome::PassedWithInapplicableRule {
        reason: "not checked".into(),
    };
    assert_eq!(attachment_contract_gaps(&report), vec![name]);
}

#[tokio::test]
async fn conformance_subject_declares_the_served_admission_reports_group() {
    let scratch = broca_test_support::scratch_root().join("admission-reports-declaration");
    let subject = BrocaSubject::new(&scratch).await;
    for writer in [false, true] {
        let describe = broca_wire::broca_role_describe_with_writers("test", writer, writer, writer);
        assert!(describe
            .capabilities
            .iter()
            .any(|group| group == cortexkit_role_llm_runner::capabilities::ADMISSION_REPORTS));
        assert!(subject
            .capabilities()
            .contains(&Capability::AdmissionReports));
    }
}

#[tokio::test]
async fn conformance_subject_declares_the_served_step_events_group() {
    let scratch = broca_test_support::scratch_root().join("step-events-declaration");
    let subject = BrocaSubject::new(&scratch).await;
    let describe = broca_wire::broca_role_describe_with_writers("test", false, false, false);
    assert!(describe
        .capabilities
        .iter()
        .any(|group| group == cortexkit_role_llm_runner::capabilities::STEP_EVENTS));
    assert!(subject.capabilities().contains(&Capability::StepEvents));
}

#[tokio::test]
async fn conformance_subject_declares_the_served_send_ids_group() {
    let scratch = broca_test_support::scratch_root().join("send-ids-declaration");
    let subject = BrocaSubject::new(&scratch).await;
    let describe = broca_wire::broca_role_describe_with_writers("test", false, false, false);
    assert!(describe
        .capabilities
        .iter()
        .any(|group| group == cortexkit_role_llm_runner::capabilities::SEND_IDS));
    assert!(subject.capabilities().contains(&Capability::SendIds));
}

#[tokio::test]
async fn conformance_subject_declares_the_served_finish_reason_group() {
    let scratch = broca_test_support::scratch_root().join("finish-reason-declaration");
    let subject = BrocaSubject::new(&scratch).await;
    assert!(subject.capabilities().contains(&Capability::FinishReason));
    assert_eq!(
        subject.model_finish_reason_source(),
        Some("openai.chat_completions")
    );
}

#[test]
fn assertion_address_lookup_does_not_contend_with_a_held_writer() {
    {
        let root = broca_test_support::scratch_root().join("assertion-address-held-writer-legacy");
        std::fs::create_dir_all(&root).unwrap();
        let seed = broca_protocol::BindIdentity::new(root.join("legacy"), "harness", "session");
        broca_test_support::register_legacy_session(&root, &seed).unwrap();
        let database = root.join("run-index.db");
        let blocker = rusqlite::Connection::open(database).unwrap();
        blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
        let claim = broca_protocol::BindIdentity::new(root.join("route"), "harness", "session");
        assert_eq!(
            broca_test_support::resolve_session_address(&root, &claim).unwrap(),
            seed,
            "the committed identity must remain readable while another connection owns the writer"
        );
        blocker.execute_batch("ROLLBACK").unwrap();
    }
}

#[tokio::test]
async fn conformance_subject_reports_recorded_completion_facts() {
    let root = broca_test_support::scratch_root().join("subject-completion-facts");
    let subject = BrocaSubject::new(&root).await;
    assert!(subject.reports_read_completed_at());
}

#[tokio::test]
async fn conformance_subject_exposes_plain_and_flow_scope_fixtures() {
    let root = broca_test_support::scratch_root().join("subject-scope-fixtures");
    let subject = BrocaSubject::new(&root).await;
    for fixture in [
        subject.scoped_session_start_stamps(),
        subject.flow_session_start_stamps(),
    ] {
        let (carrier, owner) = fixture.expect("scope fixture must be available");
        assert_ne!(carrier.principal, owner.principal);
        assert_eq!(carrier.scope, owner.scope);
        assert_eq!(owner.scope.as_ref().unwrap().owner, owner.principal);
    }
}

#[test]
fn conformance_scope_records_match_the_daemon_schema() {
    let records: Vec<subc_protocol::scope::ScopeRecord> =
        serde_json::from_value(scope_fixture_records()).unwrap();
    assert_eq!(records.len(), 2);
    let value = serde_json::to_value(records).unwrap();
    assert!(value[0]["attributes"].get("flow_id").is_none());
    assert!(value[0]["attributes"].get("run_id").is_none());
    assert_eq!(value[1]["attributes"]["flow_id"], "conformance-flow");
    assert_eq!(value[1]["attributes"]["agent_id"], "conformance-agent");
}

#[tokio::test]
async fn conformance_flow_plan_is_model_only_and_names_the_attested_opener() {
    let root = broca_test_support::scratch_root().join("subject-flow-plan");
    let subject = BrocaSubject::new(&root).await;
    let session = "synthetic-flow-plan";
    subject
        .shared
        .flow_scope_sessions
        .lock()
        .unwrap()
        .insert(session.into());
    let (opener, _) = subject.flow_session_start_stamps().unwrap();
    let members = json!({"senders":[{"module":opener.principal,"idle_start":true}]})
        .as_object()
        .unwrap()
        .clone();
    let fields = subject
        .send_fields_with_plan(session, true, &members)
        .unwrap();
    let plan = broca_fetch_plan::parse_plan(
        fields.get("plan").unwrap(),
        broca_fetch_plan::CompactionSupport::Supported,
    )
    .unwrap();
    assert!(plan.tool_items.is_empty());
    assert!(fields["tools"].is_null());
    assert!(fields["system"].is_null());
    assert_eq!(plan.senders[0].module, SENDER_MODULE);
}

#[test]
fn fixture_exclusions_reject_unrelated_inapplicability_and_any_failure() {
    for (name, reason) in [
        (
            "read_display_facts_created_at",
            "adapter could not seed a host creation time via seed_host_created_at",
        ),
        (
            "sender_succession_undeclared_refused",
            "the subject declares every one of sender_succession, so none is left to refuse",
        ),
        (
            "read_display_facts_undeclared_refused",
            "the subject declares every one of read_display_facts, so none is left to refuse",
        ),
        (
            "delivery_deadline_retry_keeps_original_expiry",
            "adapter supplies no controlled delivery clock",
        ),
        (
            "read_display_facts_default_absent",
            "adapter cannot seed stored read tool times",
        ),
        (
            "read_display_facts_opt_in_envelope",
            "adapter cannot seed stored read tool times",
        ),
    ] {
        assert!(fixture_outcome_matches(
            name,
            &CaseOutcome::Inapplicable {
                reason: reason.into()
            }
        ));
        assert!(!fixture_outcome_matches(
            name,
            &CaseOutcome::Inapplicable {
                reason: format!("{reason}; another unchecked rule")
            }
        ));
        assert!(!fixture_outcome_matches(
            name,
            &CaseOutcome::Failed {
                reason: reason.into()
            }
        ));
    }
    for name in [
        "sender_succession_same_payload_receipt",
        "display_without_limits_is_whole",
        "retention_honoured",
    ] {
        assert!(fixture_outcome_matches(name, &CaseOutcome::Passed));
        assert!(!fixture_outcome_matches(
            name,
            &CaseOutcome::Inapplicable {
                reason: "an unrelated gap".into()
            }
        ));
        assert!(!fixture_outcome_matches(
            name,
            &CaseOutcome::Failed {
                reason: "a real failure".into()
            }
        ));
    }
}

#[tokio::test]
async fn conformance_creation_seed_refuses_to_fabricate_a_host_fact() {
    let root = broca_test_support::scratch_root().join("subject-creation-seed");
    let subject = BrocaSubject::new(&root).await;
    assert!(!subject
        .seed_host_created_at("synthetic-session", "m0", 600)
        .await
        .unwrap());
}

#[tokio::test]
async fn conformance_subject_declares_service_tier_exactly_when_enabled() {
    let root = broca_test_support::scratch_root().join("subject-service-tier-declaration");
    let mut subject = BrocaSubject::new(&root).await;
    for enabled in [false, true] {
        subject.service_tier_enabled = enabled;
        assert_eq!(
            subject.capabilities().contains(&Capability::ServiceTier),
            enabled
        );
    }
}

#[tokio::test]
async fn conformance_subject_notices_follow_the_served_writer_declaration() {
    let root = broca_test_support::scratch_root().join("subject-notice-writer-declaration");
    let subject = BrocaSubject::new(&root).await;
    let declared = broca_wire::broca_role_describe_with_writers("test", true, true, true)
        .capabilities
        .iter()
        .any(|name| name == cortexkit_role_llm_runner::capabilities::NOTICES);
    assert_eq!(
        subject.capabilities().contains(&Capability::Notices),
        declared
    );
    assert_eq!(
        declared,
        broca_core::delivery_order::RIDES_V2_WRITER_ENABLED
    );
    assert!(subject.capabilities().contains(&Capability::DeliveryOrder));
}

fn writer_describe_golden_name(service_tier_enabled: bool) -> &'static str {
    if service_tier_enabled {
        "role_describe_service_tier.json"
    } else {
        "role_describe.json"
    }
}

#[test]
fn conformance_describe_golden_matches_enabled_boot_switch() {
    const CHILD: &str = "BROCA_DESCRIBE_GOLDEN_CHILD";
    if let Ok(state) = std::env::var(CHILD) {
        let enabled = match state.as_str() {
            "on" => true,
            "off" => false,
            _ => panic!("invalid child switch"),
        };
        broca_protocol::configure_service_tier(enabled).unwrap();
        let actual =
            broca_wire::broca_role_describe_with_writers("0.0.0-writer-golden", true, true, true);
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../broca-wire/tests/golden")
            .join(writer_describe_golden_name(enabled));
        if std::env::var_os("BROCA_REGENERATE_ROLE_REPLIES").is_some() {
            std::fs::write(&path, serde_json::to_vec(&actual).unwrap()).unwrap();
        }
        let expected = std::fs::read(path).unwrap();
        assert_eq!(
            serde_json::to_vec(&actual).unwrap(),
            expected,
            "the exact discovery golden must represent the same boot switch as its daemon"
        );
        let value: Value = serde_json::from_slice(&expected).unwrap();
        let describe = cortexkit_role_llm_runner::describe::check_describe(&value).unwrap();
        assert_eq!(
            describe.declares(cortexkit_role_llm_runner::capabilities::SERVICE_TIER),
            enabled
        );
        return;
    }
    for state in ["off", "on"] {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "conformance_describe_golden_matches_enabled_boot_switch",
                "--nocapture",
            ])
            .env(CHILD, state)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{state}: {}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
}
