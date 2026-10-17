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
    ScriptedPart, SubscribeOutcome, SCRIPTED_TOOL,
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
/// The system-prompt marker naming the session a model request belongs to.
const SESSION_MARKER: &str = "conformance-session:";
/// The harness every session the suite opens is bound under.
const HARNESS: &str = "conformance";
#[path = "llm_runner_conformance/compaction.rs"]
mod compaction;

fn require_compaction_test_clock() {
    #[cfg(not(feature = "test-clock"))]
    panic!("real compaction conformance requires the debug-only test-clock feature");
}

#[path = "llm_runner_conformance/attribution.rs"]
mod attribution;
#[path = "llm_runner_conformance/boundary_cancel.rs"]
mod boundary_cancel;
#[path = "llm_runner_conformance/evidence.rs"]
mod evidence;
#[path = "llm_runner_conformance/optional_tools.rs"]
mod optional_tools;
/// Retention deletion scenarios on the same real-daemon rig. They move the daemon's clock
/// through its `test-clock` control file, which only a `test-clock` build reads.
#[cfg(feature = "test-clock")]
#[path = "llm_runner_conformance/retention.rs"]
mod retention;
#[path = "llm_runner_conformance/session_class.rs"]
mod session_class;
/// The `it` target's real-daemon diagnostics (log tails, watchdog stall files, decoded session
/// WALs), included by path so this rig prints the same evidence when a suite case fails.
#[allow(dead_code)]
#[path = "it/session_client.rs"]
mod session_client;

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
    model_holds: Mutex<HashMap<String, Arc<HoldGate>>>,
    raw_bodies: Mutex<HashMap<String, Vec<Vec<u8>>>>,
    compaction: Mutex<HashMap<String, compaction::Session>>,
    session_roots: Mutex<HashMap<String, PathBuf>>,
    compaction_clocks: Mutex<HashMap<PathBuf, u64>>,
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
    /// Per session, how many durable model steps the rig cut out of the transcript. broca
    /// leaves a step whose calls were never dispatched out of the transcript when it seals the
    /// run, so a later request holds one assistant message fewer than the script has served;
    /// the next turn the script serves is past them.
    dropped_steps: Mutex<HashMap<String, usize>>,
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
}

/// One held call's two edges: the tool provider has received it, and the suite has released it.
/// Each edge is a flag plus a `Notify`, so a waiter that arrives after the edge sees the flag
/// rather than waiting for a wakeup that already happened.
#[derive(Default)]
struct HoldGate {
    arrived: std::sync::atomic::AtomicBool,
    arrived_wake: Notify,
    released: std::sync::atomic::AtomicBool,
    released_wake: Notify,
}

impl HoldGate {
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

/// Serve OpenAI-compatible chat completions on a loopback port; returns the base URL.
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
                let _ = serve_model_request(stream, shared).await;
            });
        }
    });
    format!("http://{address}/v1")
}

async fn serve_model_request(
    mut stream: tokio::net::TcpStream,
    shared: Arc<Shared>,
) -> std::io::Result<()> {
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
    let session = if let Some(session) = request["model"].as_str().and_then(|model| {
        model
            .strip_prefix("mock/")
            .unwrap_or(model)
            .strip_prefix(compaction::MODEL_PREFIX)
    }) {
        self::compaction::known_session(&shared, session).then(|| session.to_owned())
    } else {
        messages
            .iter()
            .filter(|message| message["role"] == "system" || message["role"] == "user")
            .find_map(|message| {
                let text = message["content"].to_string();
                let at = text.find(SESSION_MARKER)? + SESSION_MARKER.len();
                let name: String = text[at..]
                    .chars()
                    .take_while(|c| c.is_ascii_alphanumeric() || *c == '-' || *c == '_')
                    .collect();
                Some(name)
            })
            .filter(|session| !compaction::known_session(&shared, session))
    };
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
    let dropped = session.as_ref().map_or(0, |session| {
        shared
            .dropped_steps
            .lock()
            .unwrap()
            .get(session)
            .copied()
            .unwrap_or(0)
    });
    let turn_index = session
        .as_ref()
        .and_then(|session| {
            shared
                .compaction
                .lock()
                .unwrap()
                .get(session)
                .map(|state| state.emitted)
        })
        .unwrap_or_else(|| {
            messages
                .iter()
                .filter(|message| message["role"] == "assistant")
                .count()
                + dropped
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
        shared
            .raw_bodies
            .lock()
            .unwrap()
            .entry(session.clone())
            .or_default()
            .push(buffer[body_start..body_start + length].to_vec());
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
    let model_gate = shared.model_holds.lock().unwrap().get(&session).cloned();
    if let Some(gate) = model_gate {
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

    if let Some(gate) = compaction::capture(&shared, &session, &messages) {
        tokio::select! {
            () = HoldGate::wait(&gate.released, &gate.released_wake) => {},
            () = shared.released.notified() => return Ok(()),
        }
    }

    *shared.last_session.lock().unwrap() = Some(session.clone());
    let id = format!("chatcmpl-{session}-{turn_index}");
    let chunk = |delta: Value, finish: Option<&str>| {
        json!({"id": id, "object": "chat.completion.chunk", "model": "mock-model",
            "choices": [{"index": 0, "delta": delta, "finish_reason": finish}]})
    };
    let mut events = vec![chunk(json!({"role": "assistant"}), None)];
    let mut calls = 0;
    for part in &turn.parts {
        events.push(match part {
            ScriptedPart::Text(text) => chunk(json!({"content": text}), None),
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
        Some(if calls > 0 { "tool_calls" } else { "stop" }),
    ));
    if let Some(usage) = &turn.usage {
        let prompt = usage
            .input_tokens
            .saturating_add(usage.cache_read_tokens)
            .saturating_add(usage.cache_write_tokens);
        events.push(json!({"id": id, "object": "chat.completion.chunk", "choices": [],
            "usage": {"prompt_tokens": prompt, "completion_tokens": usage.output_tokens,
                "total_tokens": prompt.saturating_add(usage.output_tokens),
                "prompt_tokens_details": {"cached_tokens": usage.cache_read_tokens, "cache_write_tokens": usage.cache_write_tokens}}}));
    }
    let mut body = String::new();
    for event in events {
        body.push_str(&format!("data: {event}\n\n"));
    }
    body.push_str("data: [DONE]\n\n");
    let reply = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(reply.as_bytes()).await?;
    if let Some(state) = shared.compaction.lock().unwrap().get_mut(&session) {
        state.emitted += 1;
    }
    stream.shutdown().await
}

// ---- the scripted tool provider -----------------------------------------------------------------

struct ToolProvider {
    shared: Arc<Shared>,
    /// The session each bound route channel serves, from its bind.
    routes: Mutex<HashMap<(u16, u32), String>>,
}

#[async_trait]
impl subc_client_rs::ModuleHandler for ToolProvider {
    async fn handle(
        &self,
        ctx: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap_or(Value::Null);
        if request["name"] == "tool.catalog" {
            return subc_client_rs::HandlerOutcome::Response(
                serde_json::to_vec(&compaction::catalog(&request)).unwrap(),
            );
        }
        let arguments = request["arguments"].to_string();
        let handle = ctx.route_handle();
        let session = self
            .routes
            .lock()
            .unwrap()
            .get(&(handle.channel, handle.epoch))
            .cloned()
            .unwrap_or_default();
        // For a cut at the step record the call is held before it runs or is counted: it never
        // executes, and the harness then cuts the WAL back to before its dispatch intent.
        if self.shared.reach(points::STEP_RECORDED, &session) {
            self.shared.released.notified().await;
            return subc_client_rs::HandlerOutcome::Error {
                code: "conformance_killed".into(),
                message: "the runner was killed while this call was held".into(),
            };
        }
        *self
            .shared
            .invocations
            .lock()
            .unwrap()
            .entry(arguments.clone())
            .or_default() += 1;
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
        let text = result.map_or_else(
            || format!("no scripted result for arguments {arguments}"),
            |result| result.to_string(),
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
        subc_client_rs::BindDecision::accept()
    }
}

fn tool_manifest() -> subc_protocol::manifest::ModuleManifest {
    use subc_protocol::manifest::{Concurrency, ExecutionMode, IdentityScope, ProviderRole, Tool};
    subc_protocol::manifest::ModuleManifest::builder(TOOL_MODULE, env!("CARGO_PKG_VERSION"))
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

// ---- the harness ----------------------------------------------------------------------------------

/// One live rig on one state root: the daemon (supervising `ck-broca` and the stand-in), the
/// in-process tool provider, and the caller connections opened on it.
struct Live {
    daemon: Option<broca_test_support::RigChild>,
    connection_file: PathBuf,
    owner_nonce: PathBuf,
    state_root: PathBuf,
    logs_dir: PathBuf,
    tools: tokio::task::JoinHandle<()>,
    compactions: tokio::task::JoinHandle<()>,
    connections: tokio::sync::Mutex<HashMap<String, Arc<SubcConnection>>>,
}

impl Live {
    /// Every process the daemon supervises, by pid and command name.
    fn children(&self) -> Vec<(i32, String)> {
        let Some(pid) = self.daemon.as_ref().and_then(|child| child.id()) else {
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
        if let Some(mut daemon) = self.daemon.take() {
            // The daemon may already have respawned a module killed above. The tree kill stops
            // the daemon before listing and killing its remaining processes, so no module
            // outlives the rig and writes into the scratch directory while it is removed.
            broca_test_support::kill_and_reap_tree(&mut daemon);
        }
        self.tools.abort();
        self.compactions.abort();
        killed_broca
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
        self.kill_all();
    }
}

#[derive(Clone)]
struct BrocaSubject {
    shared: Arc<Shared>,
    model_url: String,
    /// The project root every session is bound under, the same across restarts.
    project_root: PathBuf,
    /// Where `ck-broca` writes rendered request dumps (`BROCA_DUMP_REQUEST_DIR`), when a test
    /// turns dumps on. The retention scenarios scan it for content that must never be dumped.
    dump_dir: Option<PathBuf>,
    binary: Option<PathBuf>,
    backup_classes: Option<Vec<String>>,
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
            model_url,
            project_root: project_root.canonicalize().unwrap(),
            dump_dir: None,
            binary: None,
            backup_classes: None,
        }
    }

    fn bind_identity(&self, session: &str) -> broca_protocol::BindIdentity {
        broca_protocol::BindIdentity::new(self.project_root.clone(), HARNESS, session)
    }

    async fn start(&self, root: &Path) -> Result<Live, HarnessError> {
        let fail = |what: String| HarnessError::new(what);
        let config_home = root.join("config");
        let runtime_dir = root.join("runtime");
        let data_home = root.join("data-home");
        let home = root.join("home");
        std::fs::create_dir_all(&home).map_err(|e| fail(e.to_string()))?;
        let state_root = root.join("broca-state");
        let config_path = config_home.join("cortexkit").join("subc.jsonc");
        std::fs::create_dir_all(config_path.parent().unwrap()).map_err(|e| fail(e.to_string()))?;
        std::fs::create_dir_all(&runtime_dir).map_err(|e| fail(e.to_string()))?;
        let owner_nonce = root.join("owner.nonce");
        let _ = std::fs::remove_file(&owner_nonce);
        let connection_file = runtime_dir.join("subc-connection.json");
        let _ = std::fs::remove_file(&connection_file);
        let config = json!({
            "version": 1,
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
                        "CK_LOG": "info,broca.wire.describe=debug,broca.wire.ingress=debug,broca.wire.egress=debug",
                    },
                    "enabled": true
                },
                OWNER_MODULE: {
                    "program": sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
                    "args": [],
                    "env": {
                        "FAKE_AFT_MODULE_ID": OWNER_MODULE,
                        "FAKE_AFT_LAUNCH_NONCE_PATH": owner_nonce,
                    },
                    "enabled": true,
                    "reserved": true,
                },
            }
        });
        let mut config = config;
        let broca_config = config_home.join("cortexkit/broca.jsonc");
        std::fs::write(
            &broca_config,
            serde_json::to_vec(
                &self
                    .backup_classes
                    .as_ref()
                    .map(|classes| json!({"backup_classes": classes}))
                    .unwrap_or_else(|| json!({})),
            )
            .unwrap(),
        )
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
        let (_, serve) = subc_client_rs::serve_with_handle(
            &connection_file,
            compaction::manifest(),
            compaction::Provider {
                shared: self.shared.clone(),
            },
        )
        .await
        .map_err(|e| fail(format!("registering compaction provider: {e}")))?;
        let compactions = tokio::spawn(async move {
            let _ = serve.await;
        });
        Ok(Live {
            daemon: Some(daemon),
            connection_file,
            owner_nonce,
            state_root,
            logs_dir: data_home.join("cortexkit/broca/logs"),
            tools,
            compactions,
            connections: tokio::sync::Mutex::new(HashMap::new()),
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
        let identity = if stamp.principal == "direct" {
            None
        } else {
            let deadline = Instant::now() + Duration::from_secs(20);
            let nonce = loop {
                if let Ok(text) = std::fs::read_to_string(&live.owner_nonce) {
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
                module_id: OWNER_MODULE.into(),
                launch_nonce: nonce,
            })
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

    /// The records of `session`'s WAL on `live`'s state root.
    fn records(&self, live: &Live, session: &str) -> Vec<broca_wal::WalRecord> {
        use broca_core::WalStore;
        broca_core::FileWalStore::new(live.state_root.join("wal"))
            .read_records(&self.bind_identity(session))
            .unwrap_or_default()
            .into_iter()
            .map(|framed| framed.record)
            .collect()
    }

    /// Cut `session`'s WAL back to end at its last model step record, dropping every frame
    /// written after it (the batch start and the dispatch intents) but keeping the lineage
    /// frame that follows each record. The process that wrote it is already dead.
    ///
    /// Frames are walked by their own length fields: a frame is a 53-byte header (payload
    /// length as u32 LE, version byte, sequence, fence, digest) and its payload. A version-2
    /// payload is the lineage identity; a version-3 payload is `{"requires", "record"}` with the
    /// record's JSON under `record`; any other version's payload is the record's JSON itself.
    fn cut_back_to_last_step(&self, live: &Live, session: &str) -> Result<(), HarnessError> {
        let path = broca_core::FileWalStore::new(live.state_root.join("wal"))
            .wal_path(&self.bind_identity(session));
        let bytes = std::fs::read(&path)
            .map_err(|e| HarnessError::new(format!("reading {}: {e}", path.display())))?;
        let mut at = 0usize;
        let mut cut = None;
        while at + 53 <= bytes.len() {
            let length = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
            let version = bytes[at + 4];
            let end = at + 53 + length;
            if end > bytes.len() {
                break;
            }
            if version == 2 {
                // A lineage frame right after the step record stays with it.
                if cut == Some(at) {
                    cut = Some(end);
                }
            } else {
                let payload: Value = serde_json::from_slice(&bytes[at + 53..end])
                    .map_err(|e| HarnessError::new(format!("an undecodable frame: {e}")))?;
                let record = if version == 3 {
                    &payload["record"]
                } else {
                    &payload
                };
                if record["type"] == "model_step_finished" {
                    cut = Some(end);
                }
            }
            at = end;
        }
        let cut =
            cut.ok_or_else(|| HarnessError::new("the WAL holds no step record to cut back to"))?;
        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(&path)
            .map_err(|e| HarnessError::new(e.to_string()))?;
        file.set_len(cut as u64)
            .and_then(|()| file.sync_all())
            .map_err(|e| HarnessError::new(format!("cutting {} back: {e}", path.display())))
    }

    /// Whether `session`'s WAL holds exactly the durable state `point` names: what must be
    /// there, and nothing the next step would have written.
    fn holds_point(&self, live: &Live, session: &str, point: &str) -> Result<(), String> {
        use broca_wal::WalRecord as R;
        let records = self.records(live, session);
        let count = |want: fn(&R) -> bool| records.iter().filter(|r| want(r)).count();
        let started = count(|r| matches!(r, R::RunStarted { .. }));
        let steps = count(|r| matches!(r, R::ModelStepFinished { .. }));
        let intents = count(|r| matches!(r, R::ToolDispatchIntent { .. }));
        let results = count(|r| matches!(r, R::ToolResult { .. }));
        let finished = count(|r| matches!(r, R::RunFinished { .. }));
        let ok = match point {
            points::ADMITTED | points::SEND_RECORDED => started == 1 && steps == 0,
            points::STEP_RECORDED => steps == 1 && intents == 0,
            points::DISPATCH_INTENT => intents == 1 && results == 0,
            points::TOOL_RESULT_RECORDED => results == 1 && steps == 1,
            points::TERMINAL => finished == 1,
            points::FOLD_RECORDED => {
                count(|r| {
                    matches!(
                        r,
                        R::CompactionMessage {
                            kind: broca_wal::CompactionMessageKind::Setup,
                            ..
                        }
                    )
                }) == 1
                    && steps == 0
            }
            points::COMPACTION_APPLIED => {
                count(|r| {
                    matches!(
                        r,
                        R::CompactionMessage {
                            kind: broca_wal::CompactionMessageKind::Step,
                            version: 2,
                            cursor: Some(_),
                            ..
                        }
                    )
                }) == 1
                    && steps == 1
            }
            _ => false,
        };
        if ok {
            Ok(())
        } else {
            Err(format!(
                "session {session}'s WAL does not hold the state {point} names: {started} RunStarted, \
                 {steps} steps, {intents} intents, {results} results, {finished} RunFinished"
            ))
        }
    }
}

#[async_trait]
impl Harness for BrocaSubject {
    type Handle = Live;
    type Route = BrocaRoute;

    fn declared_points(&self) -> Vec<PointDeclaration> {
        let points: Vec<_> = [
            points::ADMITTED,
            points::SEND_RECORDED,
            points::STEP_RECORDED,
            points::DISPATCH_INTENT,
            points::TOOL_RESULT_RECORDED,
            points::TERMINAL,
            points::COMPACTION_APPLIED,
            points::FOLD_RECORDED,
        ]
        .into_iter()
        .map(|point| {
            if point == points::STEP_RECORDED {
                return PointDeclaration {
                    point: KillPoint::new(point),
                    mechanisms: vec![KillMechanism::LogTruncation],
                    note: "broca writes a step's dispatch intents right after the step, with \
                           nothing between them the rig can hold, so this cut kills ck-broca \
                           (SIGKILL) while the call is held unrun at the tool provider, then \
                           cuts the WAL back to the step record. A process was killed, but the \
                           durable state was reached by truncation, so it is reported as that."
                        .into(),
                };
            }
            PointDeclaration {
                point: KillPoint::new(point),
                mechanisms: vec![KillMechanism::ProcessSignal],
                note: "SIGKILL of the ck-broca process while the rig holds the next thing it \
                       waits on, after the root's WAL is checked to hold exactly the point's \
                       state. broca's start record holds the send with its send_id, so Admitted \
                       and SendRecorded cut at the same place."
                    .into(),
            }
        })
        .collect();
        #[cfg(feature = "test-clock")]
        let points = {
            let mut points = points;
            points.push(PointDeclaration {
                point: KillPoint::new(points::RETENTION_TOMBSTONED),
                mechanisms: vec![KillMechanism::ProcessSignal],
                note: "A directory at the deletion rename destination prevents content removal. \
                   SIGKILL follows observation of a durable incomplete tombstone and live WAL; \
                   the obstacle is removed only after the process dies, before restart."
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
        if [points::COMPACTION_APPLIED, points::FOLD_RECORDED].contains(&point.as_str()) {
            return self.kill_compaction(handle, point, trigger).await;
        }
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
            if let Err(problem) = self.holds_point(&handle, &session, name) {
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
            if name == points::STEP_RECORDED {
                if !handle.kill_all() {
                    return Err(HarnessError::new("no ck-broca process was found to kill"));
                }
                self.shared.released.notify_waiters();
                self.cut_back_to_last_step(&handle, &session)?;
                *self
                    .shared
                    .dropped_steps
                    .lock()
                    .unwrap()
                    .entry(session.clone())
                    .or_default() += 1;
                self.holds_point(&handle, &session, name)
                    .map_err(HarnessError::new)?;
                drop(handle);
                return Ok(KillReport {
                    point: point.clone(),
                    mechanism: KillMechanism::LogTruncation,
                });
            }
            if let Err(problem) = self.holds_point(&handle, &session, name) {
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
        self.start(state_root).await
    }
}

/// A session-bound management route to the real `ck-broca`, through the real daemon.
struct BrocaRoute {
    subject: BrocaSubject,
    connection: Arc<SubcConnection>,
    route: RouteHandle,
    session: broca_protocol::BindIdentity,
    describe_calls: std::sync::atomic::AtomicUsize,
    logs_dir: PathBuf,
}

fn error_body(bytes: &[u8]) -> subc_protocol::ErrorBody {
    serde_json::from_slice(bytes).unwrap_or_else(|_| {
        subc_protocol::ErrorBody::new("undecodable_error", String::from_utf8_lossy(bytes))
    })
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
    async fn request(&self, method: &str, params: Value) -> Result<Reply, RouteFailure> {
        let send_id = params["send_id"].as_str().map(str::to_owned);
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
                if method == "session.send"
                    && self
                        .subject
                        .shared
                        .compaction
                        .lock()
                        .unwrap()
                        .contains_key(&self.session.session)
                {
                    if let Some(send_id) = &send_id {
                        self.subject
                            .wait_compaction_send_record(&self.session.session, send_id)
                            .await
                            .map_err(|e| RouteFailure::new(e.to_string()))?;
                    }
                    self.subject
                        .settle_compaction(&self.session.session)
                        .await
                        .map_err(|e| RouteFailure::new(e.to_string()))?;
                }
                if let Some(run_id) = &polled_run {
                    self.observe_run_result(run_id, &value["result"]).await;
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
                    // This path keeps only the refusal's code and message.
                    let (code, message) = rejection
                        .split_once(" — ")
                        .unwrap_or((rejection.as_str(), ""));
                    break SubscribeOutcome::Refused(subc_protocol::ErrorBody::new(code, message));
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
            Capability::RunOps,
            Capability::InputTokenCap,
            Capability::RunStatus,
            Capability::RunCancel,
            Capability::Retract,
            Capability::DispatchAttribution,
            Capability::Streaming,
            Capability::Steer,
            Capability::Queue,
            Capability::Interrupt,
            Capability::HoldToolCalls,
            Capability::Compaction,
            Capability::ModelView,
        ]
        .into_iter()
        .collect();
        // Decoding retained logs does not mean session.send accepts retention. Advertise
        // retention and its daemon clock only when the production writer switch enables
        // both admission and the retention group in role.describe.
        if broca_module_serve::RETENTION_WRITER_ENABLED {
            capabilities.extend([Capability::Retention, Capability::RetentionClock]);
        }
        capabilities
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
            principal: "direct".into(),
            scope: None,
        }
    }

    async fn session_route(
        &self,
        handle: &Live,
        session: &str,
        stamp: &RouteStamp,
    ) -> Result<BrocaRoute, HarnessError> {
        let connection = self.connection(handle, stamp).await?;
        self.shared
            .session_roots
            .lock()
            .unwrap()
            .insert(session.to_owned(), handle.state_root.clone());
        let identity = self.bind_identity(session);
        let started = Instant::now();
        let route = self.open(&connection, &identity).await?;
        if self.shared.compaction.lock().unwrap().contains_key(session) {
            self.settle_compaction(session).await?;
        }
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
        })
    }

    fn send_fields(&self, session: &str, first: bool) -> Map<String, Value> {
        if let Some(fields) = self.compaction_fields(session, first) {
            return fields;
        }
        // Legacy cases name tools directly; compaction cases use the plan-aware catalogue.
        let fields = json!({
            "model": {"provider": "mock", "model": "mock-model"},
            "system": format!("{SESSION_MARKER}{session}"),
            "tools": [{
                "name": SCRIPTED_TOOL,
                "description": "Answers each call with its scripted result.",
                "input_schema": {"type": "object"},
                "module": TOOL_MODULE,
            }],
        });
        fields.as_object().cloned().expect("an object")
    }

    fn tool_provider_module(&self) -> String {
        TOOL_MODULE.into()
    }

    async fn install_script(&self, session: &str, script: Script) -> Result<(), HarnessError> {
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

    async fn tool_invocations(&self, arguments: &Value) -> usize {
        self.shared
            .invocations
            .lock()
            .unwrap()
            .get(&arguments.to_string())
            .copied()
            .unwrap_or(0)
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
                broca_protocol::ToolOutput::Error { message }
                    if result.is_error
                        && message.starts_with("budget_exhausted: tool not executed;") =>
                {
                    Some(
                        cortexkit_role_llm_runner_conformance::ToolResultObservation::new(
                            false,
                            cortexkit_role_llm_runner::run::RUN_STATE_BUDGET_EXHAUSTED,
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
            Some(gate) if gate.arrived.load(std::sync::atomic::Ordering::SeqCst) => {
                HoldGate::set(&gate.released, &gate.released_wake);
                Ok(())
            }
            _ => Err(HarnessError::new("the call is not held")),
        }
    }

    async fn pause(&self) {
        sleep(Duration::from_millis(50)).await;
    }
    async fn install_attribution_fixture(
        &self,
        handle: &Live,
        session: &str,
    ) -> Result<cortexkit_role_llm_runner_conformance::AttributionFixture, HarnessError> {
        use broca_core::{FileLeaseStore, FileWalStore, LeaseStore, WalHandle, WalStore};
        let identity = self.bind_identity(session);
        let marker = format!("unattributable-import-{session}");
        let seed_root = handle.state_root.join("attribution-import-fixture");
        let seed_store = FileWalStore::new(&seed_root);
        let writer = WalHandle::spawn(
            seed_store
                .open(&identity)
                .map_err(|e| HarnessError::new(e.to_string()))?,
            1,
        )
        .map_err(|e| HarnessError::new(e.to_string()))?;
        // Use the real importer to construct the native historical episode. Its synthetic
        // import id describes the ingest operation, not the original message's producer.
        broca_core::run_import(
            broca_core::ImportRequest {
                project_id: None,
                principal: None,
                run_id: broca_protocol::RunId(format!("fixture-import-{session}")),
                session: identity.clone(),
                messages: vec![broca_protocol::Message::new(
                    broca_protocol::Role::User,
                    vec![broca_protocol::ContentBlock::Text {
                        text: marker.clone(),
                    }],
                )],
                origin: broca_core::ImportOrigin {
                    source_ref: "fixture:unknown-harness".into(),
                },
            },
            &writer,
            &seed_store,
        )
        .await
        .map_err(|e| HarnessError::new(e.to_string()))?;
        let _ = writer.shutdown().await;
        let import = seed_store
            .read_records(&identity)
            .map_err(|e| HarnessError::new(e.to_string()))?;
        let leases = FileLeaseStore::new(handle.state_root.join("leases"));
        let deadline = Instant::now() + Duration::from_secs(45);
        let lease = loop {
            match leases.acquire(&broca_core::lease_key_for(&identity)) {
                Ok(lease) => break lease,
                Err(broca_core::LeaseError::Held { .. }) if Instant::now() < deadline => {
                    sleep(Duration::from_millis(25)).await
                }
                Err(error) => {
                    return Err(HarnessError::new(format!(
                        "fixture could not obtain the idle session lease: {error}"
                    )))
                }
            }
        };
        let wal_store = FileWalStore::new(handle.state_root.join("wal"));
        let previous = wal_store
            .read_records(&identity)
            .map_err(|e| HarnessError::new(e.to_string()))?;
        let ordinal = broca_core::replay(
            &previous
                .iter()
                .map(|frame| frame.record.clone())
                .collect::<Vec<_>>(),
        )
        .map_err(|e| HarnessError::new(e.to_string()))?
        .prompt
        .len() as u64;
        let mut wal = wal_store
            .open(&identity)
            .map_err(|e| HarnessError::new(e.to_string()))?;
        for frame in import {
            wal.append(lease.epoch(), &frame.record, false)
                .map_err(|e| HarnessError::new(e.to_string()))?;
        }
        wal.sync().map_err(|e| HarnessError::new(e.to_string()))?;
        drop(wal);
        drop(lease);
        std::fs::remove_dir_all(seed_root).map_err(|e| HarnessError::new(e.to_string()))?;
        Ok(
            cortexkit_role_llm_runner_conformance::AttributionFixture::Installed {
                ordinal,
                marker,
            },
        )
    }
    async fn install_compaction_script(
        &self,
        session: &str,
        script: cortexkit_role_llm_runner_conformance::CompactionScript,
    ) -> Result<(), HarnessError> {
        self.install_compaction(session, script).await
    }
    async fn compaction_observation(
        &self,
        session: &str,
    ) -> Result<cortexkit_role_llm_runner_conformance::CompactionObservation, HarnessError> {
        self.compaction_observe(session).await
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
    async fn retention_deletion_finished(
        &self,
        handle: &Live,
        session: &str,
    ) -> Result<bool, HarnessError> {
        retention::deletion_finished(&handle.state_root, &self.bind_identity(session))
    }
    fn retention_probe_params(
        &self,
        _session: &str,
        method: &str,
    ) -> Result<Option<Vec<Value>>, HarnessError> {
        match method {
            "session.baseline" | "compaction.ready" | "run.status" => Ok(Some(vec![json!({})])),
            _ => Err(HarnessError::new(format!(
                "unclassified retention probe op: {method}"
            ))),
        }
    }
    async fn advance_compaction_clock(
        &self,
        session: &str,
        milliseconds: u64,
    ) -> Result<(), HarnessError> {
        self.advance_compaction(session, milliseconds).await
    }
    async fn answer_compaction(
        &self,
        session: &str,
        index: usize,
        id: Option<String>,
        answer: cortexkit_role_llm_runner_conformance::CompactionAnswer,
    ) -> Result<(), HarnessError> {
        self.deliver_compaction(session, index, id, answer).await
    }
    async fn release_compaction_model(&self, session: &str) -> Result<(), HarnessError> {
        self.release_model_compaction(session).await
    }
    async fn durable_compaction_setup(
        &self,
        handle: &Live,
        session: &str,
    ) -> Result<Option<cortexkit_role_llm_runner_conformance::CompactionMessage>, HarnessError>
    {
        self.durable_setup(handle, session)
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

/// Every case required by this subject's advertised capabilities must run and pass.
/// Other cases report their missing capability explicitly, and the kill ledger must
/// include a real process kill rather than only simulated recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ck_broca_passes_the_llm_runner_conformance_suite() {
    require_compaction_test_clock();
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
    let subject = BrocaSubject::new(&scratch).await;
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
    assert!(failed.is_empty(), "{}", report.render());
    // The cases a `session.send` retry is held to. broca declares every capability they need,
    // including hold_tool_calls for the two steer cases, so each one must have run and passed:
    // a case that drifts to skipped or inapplicable would otherwise pass this test without
    // checking anything.
    for name in [
        "run_cancel_step_boundary_finishes_tool_batch",
        "run_cancel_unlisted_at_refused",
        "run_cancel_step_boundary_escalates_to_now",
        "send_id_retry_same_answer",
        "run_attribution_per_message",
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
        "retention_shorten_continues_lineage",
        "retention_equal_noop",
        "retention_lengthen_refused",
        "retention_late_opt_in_refused",
        "retention_zero_refused",
        "retention_above_max_refused",
        "retention_active_run_never_expires",
        "crash_at_RetentionTombstoned",
        "compaction_setup_durable_once",
        "compaction_fence_crash_replays_model_view",
        "model_view_half_open_ranges_and_travel",
        "compaction_unavailable_distinct_from_refuse",
        "compaction_wait_cap",
        "compaction_late_or_stale_answers_discarded",
        "compaction_step_timeout_uses_last_view",
        "compaction_one_view_per_step",
        "input_token_cap_at_limit",
        "input_token_cap_crossing_tools",
        "input_token_cap_restart_between_episodes",
        "input_token_cap_raise_refused",
        "input_token_cap_closed_members",
        "input_token_cap_usage_unreported",
    ] {
        let case = report
            .cases
            .iter()
            .find(|case| case.case == name)
            .unwrap_or_else(|| panic!("the suite has no case {name}"));
        assert_eq!(
            case.outcome,
            CaseOutcome::Passed,
            "{name}\n{}",
            report.render()
        );
    }
    let cap_cases: Vec<_> = report
        .cases
        .iter()
        .filter(|case| case.case.starts_with("input_token_cap_"))
        .collect();
    assert_eq!(
        cap_cases.len(),
        6,
        "all shared input-cap cases must be present"
    );
    assert!(cap_cases
        .iter()
        .all(|case| case.outcome == CaseOutcome::Passed));
    println!(
        "Shared input-token-cap cases: {} ran, {} passed",
        cap_cases.len(),
        cap_cases.len()
    );
    assert!(
        report
            .kills
            .iter()
            .any(|kill| kill.mechanism.is_real_process_kill()),
        "{}",
        report.render()
    );
    for point in [points::COMPACTION_APPLIED, points::FOLD_RECORDED] {
        assert!(
            report
                .kills
                .iter()
                .any(|kill| kill.point.as_str() == point && kill.mechanism.is_real_process_kill()),
            "missing real compaction cut at {point}"
        );
    }
    broca_test_support::remove_rig_dir(&scratch);
}

/// After a cut between a step's record and its calls' dispatch intents, broca does not resume
/// the run: the restarted module seals it `interrupted` without sending the call. So the tool
/// never ran, the run reads `interrupted`, the cut step is left out of the transcript (no call
/// of it is read, so none is indeterminate), and the session's next model request carries no
/// tool call without its result.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_step_cut_before_its_intent_is_sealed_without_sending_the_call() {
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
        broca_test_support::scratch_root().join(format!("broca-step-cut-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("root");
    std::fs::create_dir_all(&root).unwrap();

    let session = "step-cut";
    let call = ScriptedToolCall {
        tool_call_id: "call_cut".into(),
        tool: SCRIPTED_TOOL.into(),
        arguments: json!({"marker": "step-cut-args"}),
        result: json!({"marker": "step-cut-result"}),
        hold: false,
    };
    let turn = |parts| {
        ScriptedTurn::new(parts).with_usage(
            cortexkit_role_llm_runner_conformance::ScriptedUsage::new(1, 0, 0, 1),
        )
    };
    subject
        .install_script(
            session,
            Script {
                turns: vec![
                    turn(vec![
                        ScriptedPart::Text("before the call".into()),
                        ScriptedPart::ToolCall(call.clone()),
                    ]),
                    turn(vec![ScriptedPart::Text("after the cut".into())]),
                ],
            },
        )
        .await
        .unwrap();
    let owner = subject.owner_stamp();
    let send = |prompt: &str, send_id: &str| {
        let mut params = subject.send_fields(session, true);
        params.insert("prompt".into(), json!(prompt));
        params.insert("send_id".into(), json!(send_id));
        Value::Object(params)
    };
    let response = |reply: Reply| match reply {
        Reply::Response(value) => value,
        Reply::Error(error) => panic!(
            "refused {} {}: {:?}",
            error.code, error.message, error.detail
        ),
    };

    let live = subject.spawn(&root).await.unwrap();
    let route = subject.session_route(&live, session, &owner).await.unwrap();
    let first = send("first", "send-1");
    let trigger: Trigger<'_> = Box::pin(async {
        let _ = route.request("session.send", first.clone()).await;
        sleep(Duration::from_secs(60)).await;
    });
    let report = subject
        .kill_at(live, &KillPoint::new(points::STEP_RECORDED), trigger)
        .await
        .unwrap();
    assert_eq!(report.mechanism, KillMechanism::LogTruncation);
    drop(route);
    assert_eq!(subject.tool_invocations(&call.arguments).await, 0);

    let live = subject.restart(&root).await.unwrap();
    let route = subject.session_route(&live, session, &owner).await.unwrap();
    // The owner's retry of the cut send is answered with the run the restart sealed.
    let retry = response(route.request("session.send", first.clone()).await.unwrap());
    let run_id = retry["run_id"]
        .as_str()
        .expect("the retry names its run")
        .to_owned();
    let result = response(
        route
            .request("run.result", json!({"run_id": run_id}))
            .await
            .unwrap(),
    );
    assert_eq!(result["state"], "interrupted", "{result}");
    assert_eq!(
        subject.tool_invocations(&call.arguments).await,
        0,
        "the call never ran"
    );

    // The sealed run's transcript does not hold the cut step at all: an assistant message whose
    // calls were never dispatched is left out, so no call of it is on a page, indeterminate or
    // otherwise, and no result is recorded for it.
    let page = response(route.request("session.read", json!({})).await.unwrap());
    let roles: Vec<&str> = page["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|m| m["message"]["role"].as_str())
        .collect();
    assert_eq!(roles, ["system", "user"], "{page}");
    assert!(!page.to_string().contains("call_cut"), "{page}");

    // The next send renders the cut step: every tool call it carries is answered.
    let next = response(
        route
            .request("session.send", send("next", "send-2"))
            .await
            .unwrap(),
    );
    assert_eq!(next["state"], "active", "{next}");
    let deadline = Instant::now() + Duration::from_secs(60);
    let request = loop {
        let requests = subject
            .shared
            .requests
            .lock()
            .unwrap()
            .get(session)
            .cloned();
        if let Some(request) = requests.and_then(|all| all.into_iter().nth(1)) {
            break request;
        }
        assert!(
            Instant::now() < deadline,
            "the next send made no model request"
        );
        sleep(Duration::from_millis(50)).await;
    };
    println!(
        "the next model request's messages: {}",
        Value::Array(request.clone())
    );
    let called: Vec<String> = request
        .iter()
        .filter(|m| m["role"] == "assistant")
        .flat_map(|m| m["tool_calls"].as_array().cloned().unwrap_or_default())
        .filter_map(|c| c["id"].as_str().map(String::from))
        .collect();
    let answered: Vec<String> = request
        .iter()
        .filter(|m| m["role"] == "tool")
        .filter_map(|m| m["tool_call_id"].as_str().map(String::from))
        .collect();
    // The cut step is not rendered at all, like any step whose calls never left the process.
    assert!(called.is_empty(), "{}", Value::Array(request.clone()));
    for id in &called {
        assert!(
            answered.contains(id),
            "the next request carries tool call {id} with no result: {}",
            Value::Array(request.clone())
        );
    }
    drop(route);
    drop(live);
    broca_test_support::remove_rig_dir(&scratch);
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

/// A daemon restarted without retention admission must still discover and delete
/// expired WALs left by a build that accepted retention.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ck_broca_boot_enrolls_and_deletes_expired_retention_before_serving_content() {
    for (name, env_key) in [
        ("ck-subc", "SUBC_CORE_BIN"),
        ("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
    ] {
        if !sibling_bin(name, env_key).exists() {
            assert!(
                !require_subc(),
                "required real-daemon binary is missing: {name}"
            );
            println!("SKIP retention boot: missing {name}");
            return;
        }
    }
    let scratch =
        broca_test_support::scratch_root().join(format!("retention-boot-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("daemon");
    let state = root.join("broca-state");
    let key = subject.bind_identity("rolled-back-retention");
    let wal_store = broca_core::FileWalStore::new(state.join("wal"));
    let live_path = wal_store.ephemeral_wal_path(&key);
    let lineage;
    {
        let leases = broca_core::FileLeaseStore::new(state.join("leases"));
        use broca_core::LeaseStore;
        let _lease = leases.acquire(&broca_core::lease_key_for(&key)).unwrap();
        let mut wal = wal_store.open_with_retention(&key, Some(1)).unwrap();
        let mut start: broca_wal::WalRecord = serde_json::from_str(include_str!(
            "../../broca-wal/tests/golden/pre_step_transforms/run_started.json"
        ))
        .unwrap();
        if let broca_wal::WalRecord::RunStarted {
            retention,
            ts_ms,
            session,
            title,
            ..
        } = &mut start
        {
            *retention = Some(1);
            *ts_ms = Some(100);
            *session = key.clone();
            *title = Some("RETENTION-REAL-BOOT-MARKER-8031".into());
        }
        wal.append(1, &start, true).unwrap();
        wal.append(
            1,
            &broca_wal::WalRecord::RunFinished {
                ts_ms: Some(200),
                reason: broca_protocol::RunFinishReason::Completed,
                usage: Default::default(),
                error: None,
                retries_used: 0,
            },
            true,
        )
        .unwrap();
        lineage = wal.lineage_id().unwrap().to_string();
    }
    // No retention deadline row or identity sidecar is seeded. Require completed deletion
    // before any session request, not only a read-time expiry refusal.
    assert!(!live_path.with_extension("session").exists());
    let live = subject.start(&root).await.unwrap();
    let stone = wal_store.tombstone_path(&key, &lineage);
    broca_test_support::event_timeout(
        "ck_broca_boot_enrolls_and_deletes_expired_retention_before_serving_content",
        async {
            loop {
                if !live_path.exists() && stone.exists() {
                    let value: Value =
                        serde_json::from_slice(&std::fs::read(&stone).unwrap()).unwrap();
                    if value.get("completed_at_ms").is_some() {
                        assert_eq!(value["expired_at_ms"], 1200);
                        break;
                    }
                }
                sleep(Duration::from_millis(25)).await;
            }
        },
        || {
            format!(
                "live_exists={}, stone_exists={}",
                live_path.exists(),
                stone.exists()
            )
        },
    )
    .await
    .expect("startup enrollment and the first sweep must delete the WAL without a session read");
    let route = subject
        .session_route(&live, "rolled-back-retention", &subject.owner_stamp())
        .await
        .unwrap();
    match route.request("session.read", json!({})).await.unwrap() {
        Reply::Error(body) => {
            assert_eq!(body.code, "expired");
            assert_eq!(body.detail.unwrap()["expired_at_ms"], 1200);
        }
        other => panic!("expired content was served: {other:?}"),
    }
    drop(route);
    drop(live);
    broca_test_support::remove_rig_dir(scratch);
}

/// Recovery must finish incomplete deletion steps before the module begins serving requests.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ck_broca_boot_completes_incomplete_tombstones_before_serving() {
    for (name, env_key) in [
        ("ck-subc", "SUBC_CORE_BIN"),
        ("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
    ] {
        if !sibling_bin(name, env_key).exists() {
            assert!(
                !require_subc(),
                "required real-daemon binary is missing: {name}"
            );
            println!("SKIP retention tombstone recovery: missing {name}");
            return;
        }
    }
    let scratch = broca_test_support::scratch_root()
        .join(format!("retention-tombstone-boot-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("daemon");
    let state = root.join("broca-state");
    let store = broca_core::FileWalStore::new(state.join("wal"));
    let mut cuts = Vec::new();
    for cut in ["live", "moved", "deleted"] {
        let key = subject.bind_identity(cut);
        let mut wal = store.open_with_retention(&key, Some(1)).unwrap();
        let mut start: broca_wal::WalRecord = serde_json::from_str(include_str!(
            "../../broca-wal/tests/golden/pre_step_transforms/run_started.json"
        ))
        .unwrap();
        if let broca_wal::WalRecord::RunStarted {
            retention,
            ts_ms,
            session,
            run_id,
            ..
        } = &mut start
        {
            *retention = Some(1);
            *ts_ms = Some(100);
            *session = key.clone();
            *run_id = broca_protocol::RunId(format!("boot-{cut}"));
        }
        wal.append(1, &start, true).unwrap();
        wal.append(
            1,
            &broca_wal::WalRecord::RunFinished {
                ts_ms: Some(200),
                reason: broca_protocol::RunFinishReason::Completed,
                usage: Default::default(),
                error: None,
                retries_used: 0,
            },
            true,
        )
        .unwrap();
        let lineage = wal.lineage_id().unwrap();
        drop(wal);
        let live_path = store.ephemeral_wal_path(&key);
        let deleting_path = store.deleting_path(&key, lineage);
        let stone_path = store.tombstone_path(&key, &lineage.to_string());
        std::fs::create_dir_all(stone_path.parent().unwrap()).unwrap();
        std::fs::write(
            &stone_path,
            serde_json::to_vec(&json!({
                "lineage_id": lineage.to_string(), "expired_at_ms": 1200, "seq": 1
            }))
            .unwrap(),
        )
        .unwrap();
        std::fs::write(
            live_path.with_extension("session"),
            serde_json::to_vec(&key).unwrap(),
        )
        .unwrap();
        match cut {
            "moved" => std::fs::rename(&live_path, &deleting_path).unwrap(),
            "deleted" => {
                // File deletion follows the committed expiry transaction; recovery needs
                // that content-free row when the WAL is no longer available to replay.
                broca_store::SqliteStore::open(state.join("run-index.db"))
                    .unwrap()
                    .upsert_retention_lineage(
                        &broca_store::retention::RetentionLineageRow {
                            lineage_id: lineage.to_string(),
                            session: key.clone(),
                            retention_s: 1,
                            last_activity_ms: 200,
                            expires_at_ms: None,
                            wal_bytes: std::fs::metadata(&live_path).unwrap().len(),
                            expired_at_ms: Some(1200),
                        },
                        &[],
                    )
                    .unwrap();
                std::fs::remove_file(&live_path).unwrap();
            }
            _ => {}
        }
        cuts.push((key, live_path, deleting_path, stone_path));
    }
    let live = subject.start(&root).await.unwrap();
    for (key, live_path, deleting_path, stone_path) in cuts {
        let route = subject
            .session_route(&live, &key.session, &subject.owner_stamp())
            .await
            .unwrap();
        // Opening a management route proves the module is registered. Check deletion now,
        // without polling for completion after the module has started accepting requests.
        let stone: Value = serde_json::from_slice(&std::fs::read(stone_path).unwrap()).unwrap();
        assert!(stone["completed_at_ms"].is_u64(), "{}", key.session);
        assert_eq!(stone["expired_at_ms"], 1200);
        assert!(!live_path.exists());
        assert!(!deleting_path.exists());
        match route.request("session.read", json!({})).await.unwrap() {
            Reply::Error(body) => {
                assert_eq!(body.code, "expired");
                assert_eq!(body.detail.unwrap()["expired_at_ms"], 1200);
            }
            other => panic!("tombstoned content was served: {other:?}"),
        }
    }
    drop(live);
    broca_test_support::remove_rig_dir(scratch);
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
    let golden =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../broca-wire/tests/golden/role_describe.json");
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
    for group in ["compaction", "model_view"] {
        assert!(
            describe["capabilities"]
                .as_array()
                .unwrap()
                .contains(&json!(group)),
            "the installed compaction writer must declare {group}"
        );
    }
    assert!(describe["majors"][0]["ops"]
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
        || broca_test_support::record_kinds(subject.records(&live, session)),
    )
    .await
    .unwrap();
    let records = subject.records(&live, session);
    let started = records
        .iter()
        .find_map(|r| match r {
            broca_wal::WalRecord::RunStarted { input, hooks, .. } => Some((input, hooks)),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        started.0.last().unwrap().content[0],
        broca_protocol::content::ContentBlock::Text {
            text: format!("{SESSION_MARKER}{session} input-hooked")
        }
    );
    assert!(started.1.is_some());
    assert_eq!(
        state.counts(),
        [("pre_user".into(), 1), ("post_assistant".into(), 1)]
            .into_iter()
            .collect()
    );
    for call in state.hooks() {
        assert_eq!(call["harness"], HARNESS);
        assert_eq!(call["session"], session);
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

/// The external binary is the independently built reader cut, never the writer under another
/// name. Release-order verification sets both variables; ordinary test runs report its absence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reader_release_reads_writer_records_and_refuses_hooked_advancement() {
    use cortexkit_role_llm_runner_conformance::{ScriptedToolCall, ScriptedTurn};
    use cortexkit_role_step_transform_provider::answer::HookAnswer;
    use step_transform_rig::*;
    let Some(reader) = std::env::var_os("BROCA_STEP_TRANSFORM_READER_BIN").map(PathBuf::from)
    else {
        assert!(
            std::env::var_os("BROCA_REQUIRE_STEP_READER").is_none(),
            "the separately built step-transform reader is required"
        );
        println!(
            "SKIP reader_release_reads_writer_records: BROCA_STEP_TRANSFORM_READER_BIN is not set"
        );
        return;
    };
    assert!(reader.exists(), "reader binary missing: {reader:?}");
    // `--version` touches no state, but this is still a broca binary: clear its environment so
    // it cannot resolve the operator's state root, as every broca spawn in the tests must.
    let version = std::process::Command::new(&reader)
        .arg("--version")
        .env_clear()
        .output()
        .unwrap();
    assert!(version.status.success(), "reader version command failed");
    assert!(
        String::from_utf8_lossy(&version.stdout).contains("0.3.191"),
        "rollback must use released reader v0.3.191, not a renamed writer: {version:?}"
    );
    let scratch = broca_test_support::scratch_root()
        .join(format!("step-transform-rollback-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let mut subject = BrocaSubject::new(&scratch).await;
    let writer_root = scratch.join("writer");
    std::fs::create_dir_all(&writer_root).unwrap();
    let state = State::new(vec![
        subscription("pre_user", None, "pass", 1000),
        subscription("post_assistant", None, "pass", 1000),
        subscription("pre_tool", Some("mutate"), "pass", 1000),
        subscription("pre_tool", Some("validate"), "refuse", 1000),
        subscription("post_tool", None, "pass", 1000),
    ]);
    state.answer_with(|c| {
        let response = match key(c).as_str() {
            "pre_tool:mutate" => HookAnswer::Mutate {
                input: if c["tool_call_id"] == "invalid" {
                    json!({"value":"x".repeat(1024*1024+1)})
                } else {
                    json!({"value":"mutated"})
                },
                note: None,
            },
            "pre_tool:validate" => {
                if c["tool_call_id"] == "denied" {
                    HookAnswer::Deny {
                        text: "denied by provider".into(),
                    }
                } else {
                    HookAnswer::Pass
                }
            }
            _ => return append("-hooked"),
        };
        serde_json::to_value(response).unwrap()
    });
    state.held.lock().unwrap().insert("work".into());
    let mut writer = subject.start(&writer_root).await.unwrap();
    let serving = register(&writer.connection_file, state.clone()).await;
    let session = "rollback-writer";
    let call = |id: &str| {
        ScriptedPart::ToolCall(ScriptedToolCall {
            tool_call_id: id.into(),
            tool: "work".into(),
            arguments: json!({"value":"original"}),
            result: json!("worked"),
            hold: false,
        })
    };
    subject
        .install_script(
            session,
            Script {
                turns: vec![
                    ScriptedTurn::new(vec![
                        ScriptedPart::Text("assistant".into()),
                        call("good"),
                        call("denied"),
                        call("invalid"),
                    ])
                    .with_usage(
                        cortexkit_role_llm_runner_conformance::ScriptedUsage::new(1, 0, 0, 1),
                    ),
                    ScriptedTurn::new(vec![ScriptedPart::Text("done".into())]).with_usage(
                        cortexkit_role_llm_runner_conformance::ScriptedUsage::new(1, 0, 0, 1),
                    ),
                    ScriptedTurn::new(vec![ScriptedPart::Text("queued done".into())]).with_usage(
                        cortexkit_role_llm_runner_conformance::ScriptedUsage::new(1, 0, 0, 1),
                    ),
                ],
            },
        )
        .await
        .unwrap();
    let route = subject
        .session_route(&writer, session, &subject.owner_stamp())
        .await
        .unwrap();
    let response = |reply: Reply| match reply {
        Reply::Response(value) => value,
        other => panic!("expected response, got {other:?}"),
    };
    let frozen_plan = plan(&state.subscriptions, false);
    let mut params = subject.send_fields(session, true);
    params.remove("tools");
    params.remove("system");
    params.insert("plan".into(), frozen_plan.clone());
    params.insert(
        "prompt".into(),
        json!(format!("{SESSION_MARKER}{session} input")),
    );
    params.insert("send_id".into(), json!("first"));
    response(
        route
            .request("session.send", Value::Object(params))
            .await
            .unwrap(),
    );
    broca_test_support::event_timeout(
        "reader_release_reads_writer_records_and_refuses_hooked_advancement",
        async {
            while state.tool_calls.lock().unwrap().is_empty() {
                sleep(Duration::from_millis(10)).await;
            }
        },
        || broca_test_support::record_kinds(subject.records(&writer, session)),
    )
    .await
    .unwrap();
    for (id, delivery) in [("steer", "steer"), ("queued", "queue")] {
        response(route.request("session.send",json!({"prompt":id,"send_id":id,"delivery":delivery,"model":{"provider":"mock","model":"mock-model"},"plan":frozen_plan})).await.unwrap());
    }
    state.held.lock().unwrap().clear();
    state.release.notify_waiters();
    broca_test_support::event_timeout(
        "reader_release_reads_writer_records_and_refuses_hooked_advancement",
        async {
            loop {
                let records = subject.records(&writer, session);
                if records
                    .iter()
                    .filter(|r| matches!(r, broca_wal::WalRecord::RunFinished { .. }))
                    .count()
                    >= 2
                {
                    break;
                }
                sleep(Duration::from_millis(10)).await;
            }
        },
        || broca_test_support::record_kinds(subject.records(&writer, session)),
    )
    .await
    .unwrap();
    let records = subject.records(&writer, session);
    for kind in [
        "run_started",
        "queued",
        "steered",
        "model_step_finished",
        "tool_dispatch_intent",
        "tool_no_dispatch",
        "tool_result",
    ] {
        assert!(
            records
                .iter()
                .map(|r| serde_json::to_value(r).unwrap())
                .any(|r| r["type"] == kind && r.get("hooks").is_some()),
            "writer must actually produce {kind} with hooks"
        );
    }
    let written_page = response(
        route
            .request("session.read", json!({"limit":500}))
            .await
            .unwrap(),
    );
    let store = broca_core::FileWalStore::new(writer.state_root.join("wal"));
    let bytes = std::fs::read(store.wal_path(&subject.bind_identity(session))).unwrap();
    let mut cut_end = 0;
    let mut at = 0;
    let mut saw_intent = false;
    while at < bytes.len() {
        match broca_wal::framing::decode_next(&bytes[at..]) {
            broca_wal::framing::DecodeStep::Record { framed, consumed } => {
                if saw_intent {
                    break;
                }
                at += consumed;
                if matches!(
                    framed.record,
                    broca_wal::WalRecord::ToolDispatchIntent { .. }
                ) {
                    saw_intent = true;
                    cut_end = at;
                }
            }
            broca_wal::framing::DecodeStep::Lineage { consumed, .. } => {
                at += consumed;
                if saw_intent {
                    cut_end = at;
                }
            }
            _ => panic!("writer WAL is not fully decodable"),
        }
    }
    assert!(saw_intent);
    serving.abort();
    writer.kill_all();
    let reader_root = scratch.join("reader");
    let state_root = reader_root.join("broca-state");
    let store = broca_core::FileWalStore::new(state_root.join("wal"));
    let complete_path = store.wal_path(&subject.bind_identity(session));
    std::fs::create_dir_all(complete_path.parent().unwrap()).unwrap();
    std::fs::write(&complete_path, &bytes).unwrap();
    let inflight = "rollback-inflight";
    let inflight_path = store.wal_path(&subject.bind_identity(inflight));
    std::fs::write(&inflight_path, &bytes[..cut_end]).unwrap();
    subject.binary = Some(reader);
    let mut reader_live = subject.start(&reader_root).await.unwrap();
    state.clear();
    state.tool_calls.lock().unwrap().clear();
    let serving = register(&reader_live.connection_file, state.clone()).await;
    let complete = subject
        .session_route(&reader_live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let describe = response(complete.request("role.describe", json!({})).await.unwrap());
    assert!(
        !describe["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!(
                cortexkit_role_llm_runner::capabilities::ORDERED_HOOK_PHASES
            )),
        "the reader must not be the writer binary"
    );
    let read = response(
        complete
            .request("session.read", json!({"limit":500}))
            .await
            .unwrap(),
    );
    assert_eq!(
        read["messages"], written_page["messages"],
        "the reader must decode and project every writer record"
    );
    for target in [session, inflight, "rollback-fresh"] {
        let route = subject
            .session_route(&reader_live, target, &subject.owner_stamp())
            .await
            .unwrap();
        let path = store.wal_path(&subject.bind_identity(target));
        let before = std::fs::read(&path).unwrap_or_default();
        for include_plan in [true, false] {
            if target == "rollback-fresh" && !include_plan {
                continue;
            }
            let mut params = json!({"prompt":"input","send_id":"first","model":{"provider":"fake","model":"fake"}});
            if include_plan {
                params["plan"] = frozen_plan.clone();
            }
            let reply = route.request("session.send", params).await.unwrap();
            let Reply::Error(error) = reply else {
                panic!("reader accepted a hooked send: {reply:?}")
            };
            assert_eq!(error.code, "invalid_params");
            assert_eq!(
                error.detail,
                Some(json!({"field":"plan.step_transform_items"}))
            );
            assert_eq!(
                std::fs::read(&path).unwrap_or_default(),
                before,
                "reader resume/send writes nothing"
            );
        }
        if target == "rollback-fresh" {
            assert!(!path.exists(), "refusal creates no lineage header");
        }
    }
    assert!(state.hooks().is_empty());
    assert_eq!(state.declares(), 0);
    assert!(state.tool_calls.lock().unwrap().is_empty());
    assert_eq!(std::fs::read(complete_path).unwrap(), bytes);
    assert_eq!(std::fs::read(inflight_path).unwrap(), bytes[..cut_end]);
    serving.abort();
    reader_live.kill_all();
    std::fs::remove_dir_all(scratch).unwrap();
}
