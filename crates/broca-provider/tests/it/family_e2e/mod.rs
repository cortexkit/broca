//! Shared harness for the provider-family + error-mode e2e suite.
//!
//! It exercises the FULL provider stack — render → SSE/normalize → error classify —
//! driving the REAL `run_loop` against a mock LLM server (aimock), with a
//! deterministic in-process `StubToolPlane` for tool results. NO subc, NO AFT: the
//! variable under test is the PROVIDER response shape, and subc/AFT are
//! provider-agnostic, so booting them would add cost+flake with zero value for this
//! axis. (The whole-spine proof lives in the cortexkit-e2e M1/M2 tests.)
//!
//! Each test writes an aimock fixture file (the scripted LLM responses), boots
//! aimock on an ephemeral port, points a `LiveProvider<Family>` at it via base_url,
//! runs the loop, and asserts on the collected events. aimock is the pinned
//! `@copilotkit/aimock` devDep (version-locked so its SSE framing can't drift under
//! the byte-level assertions); the rig skips cleanly if node/aimock are absent.

#![allow(dead_code)]

use std::path::PathBuf;
use std::time::Duration;

use broca_core::{
    run_loop, AllowAll, FileLeaseStore, FileWalStore, OutputEvent, RunRequest, RunSummary,
    StubToolPlane,
};
use broca_protocol::{
    caller::ControlUnit,
    content::{ContentBlock, Message, Role, ToolOutput},
    request::{ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice},
    BindIdentity, GenerationConfig, RunId,
};
use broca_provider::spec::WireFamilyId;
use broca_provider::wire::{AuthHandle, FrozenRenderConfig, WireFamily};
use broca_provider::LiveProvider;
use tokio::sync::mpsc;
use tokio::time::timeout;

/// A booted aimock on an ephemeral port; failed runs keep its logs for diagnosis.
pub struct AiMock {
    child: broca_test_support::RigChild,
    pub base_url: String,
    pub dir: PathBuf,
}

impl Drop for AiMock {
    fn drop(&mut self) {
        broca_test_support::kill_and_reap(&mut self.child);
        if !std::thread::panicking() {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

impl AiMock {
    /// Boot aimock with `fixtures_json` (the full `{"fixtures":[...]}` document) on an
    /// ephemeral port. Returns None when node/aimock aren't available (caller skips).
    ///
    /// AIMOCK_CMD overrides the launcher argv (whitespace-split); the default runs the
    /// repo-pinned devDep directly with node.
    pub async fn start(fixtures_json: &str) -> Option<AiMock> {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-aimock-{}-{}",
            std::process::id(),
            unique_suffix()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).ok()?;
        let fixture_path = dir.join("fixtures.json");
        std::fs::write(&fixture_path, fixtures_json).ok()?;

        let argv = aimock_argv();
        let (program, base_args) = argv.split_first()?;
        let child = match broca_test_support::tokio_rig_command(program, &dir, "aimock")
            .expect("create aimock log files")
            .args(base_args)
            .arg("--fixtures")
            .arg(&fixture_path)
            .arg("--port")
            .arg("0")
            .arg("--host")
            .arg("127.0.0.1")
            .kill_on_drop(true)
            .spawn()
        {
            Ok(c) => c,
            Err(e) => {
                eprintln!(
                    "SKIP family_e2e: cannot spawn aimock ({}): {e}",
                    program.display()
                );
                let _ = std::fs::remove_dir_all(&dir);
                return None;
            }
        };
        let mut child = broca_test_support::RigChild::new(child, &dir, "aimock");

        // Read stdout until the "listening on http://HOST:PORT" line appears — that
        // line IS the readiness signal. Tolerant of both shipped bins (one prefixes
        // "[aimock] ", one doesn't), so we match the address anywhere in the line.
        let base_url = match timeout(Duration::from_secs(20), async {
            loop {
                let logs =
                    std::fs::read_to_string(dir.join("aimock.stdout.log")).unwrap_or_default();
                if let Some(addr) = logs.lines().find_map(parse_listening_addr) {
                    return Some(addr);
                }
                if child.try_wait().ok().flatten().is_some() {
                    return None;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        {
            Ok(Some(addr)) => format!("http://{addr}"),
            _ => {
                eprintln!("SKIP family_e2e: aimock never reported a listening port");
                broca_test_support::kill_and_reap(&mut child);
                eprintln!("{}", broca_test_support::rig_process_logs(&dir, "aimock"));
                let _ = std::fs::remove_dir_all(&dir);
                return None;
            }
        };

        Some(AiMock {
            child,
            base_url,
            dir,
        })
    }
}

/// A process-unique suffix for temp dir + session names. A monotonic atomic counter
/// (NOT a clock) so parallel tests can't collide — macOS clock granularity is coarse
/// enough that two tests starting close together read the same nanosecond, which would
/// give them the same temp dir + lease and one would fail with LeaseHeld.
fn unique_suffix() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// The launcher argv: `$AIMOCK_CMD` (whitespace-split) or the repo-pinned devDep run
/// directly with node.
///
/// The package ships TWO bins with DIFFERENT flags: `aimock` (dist/aimock-cli.js)
/// takes `--config <file>` (a two-file config that references a fixtures file by
/// path), while `llmock` (dist/cli.js) takes `--fixtures <file>` directly. We use
/// `cli.js` because the single-file `--fixtures` path is what this rig writes.
fn aimock_argv() -> Vec<PathBuf> {
    if let Ok(cmd) = std::env::var("AIMOCK_CMD") {
        return cmd.split_whitespace().map(PathBuf::from).collect();
    }
    // .../broca/crates/broca-provider → repo root node_modules.
    let cli = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../node_modules/@copilotkit/aimock/dist/cli.js");
    vec![PathBuf::from("node"), cli]
}

/// Extract `HOST:PORT` from a "listening on http://127.0.0.1:PORT" line, tolerant of
/// any prefix the launcher bin adds.
fn parse_listening_addr(line: &str) -> Option<String> {
    let marker = "http://";
    let start = line.find(marker)? + marker.len();
    let rest = &line[start..];
    let end = rest.find(|c: char| c.is_whitespace()).unwrap_or(rest.len());
    let addr = rest[..end].trim_end_matches('/');
    // Sanity: it must look like host:port.
    if addr.contains(':') {
        Some(addr.to_string())
    } else {
        None
    }
}

/// Inputs for a family e2e run.
pub struct RunSpec {
    /// The scripted aimock fixtures document (`{"fixtures":[...]}`).
    pub fixtures_json: String,
    /// The user prompt (aimock usually matches on hasToolResult, not prompt text).
    pub prompt: String,
    /// Tools to offer the model; each canned in the StubToolPlane via `tool_results`.
    pub tools: Vec<Tool>,
    /// Canned tool results by tool name (the deterministic StubToolPlane).
    pub tool_results: Vec<(String, String)>,
    /// Max model steps (a structural stop so a misbehaving script can't loop).
    pub max_steps: u64,
}

impl RunSpec {
    /// A simple two-step spec: offer one tool, canned result, bounded steps.
    pub fn new(fixtures_json: impl Into<String>) -> Self {
        RunSpec {
            fixtures_json: fixtures_json.into(),
            prompt: "Do the task.".to_string(),
            tools: Vec::new(),
            tool_results: Vec::new(),
            max_steps: 4,
        }
    }

    pub fn prompt(mut self, p: impl Into<String>) -> Self {
        self.prompt = p.into();
        self
    }

    /// Offer a tool AND register its canned StubToolPlane result in one call.
    pub fn tool(mut self, name: &str, result: &str) -> Self {
        self.tools.push(Tool {
            name: name.to_string(),
            description: format!("The {name} tool."),
            input_schema: serde_json::json!({"type": "object"}),
            module: None,
            execution_mode: ExecutionMode::Parallel,
            reply: None,
        });
        self.tool_results
            .push((name.to_string(), result.to_string()));
        self
    }
}

/// The result of a family e2e run: the run summary + the full ordered event stream.
pub struct RunOutcome {
    pub summary: RunSummary,
    pub events: Vec<OutputEvent>,
}

impl RunOutcome {
    pub fn tool_call_names(&self) -> Vec<String> {
        self.events
            .iter()
            .filter_map(|e| match e {
                OutputEvent::Control(ControlUnit::ToolCall { call }) => {
                    Some(call.tool_name.clone())
                }
                _ => None,
            })
            .collect()
    }

    pub fn tool_result_texts(&self) -> Vec<String> {
        self.events
            .iter()
            .filter_map(|e| match e {
                OutputEvent::Control(ControlUnit::ToolResult { result }) => {
                    Some(match &result.output {
                        ToolOutput::Text { text } => text.clone(),
                        ToolOutput::Error { message } => message.clone(),
                        _ => String::new(),
                    })
                }
                _ => None,
            })
            .collect()
    }

    pub fn final_text(&self) -> String {
        self.events
            .iter()
            .rev()
            .find_map(|e| match e {
                OutputEvent::Control(ControlUnit::AssistantMessage { message }) => Some(
                    message
                        .content
                        .iter()
                        .filter_map(|b| match b {
                            ContentBlock::Text { text } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect::<String>(),
                ),
                _ => None,
            })
            .unwrap_or_default()
    }

    pub fn first_error(&self) -> Option<broca_protocol::ProviderError> {
        self.events.iter().find_map(|e| match e {
            OutputEvent::Control(ControlUnit::Error { error }) => Some(error.clone().into_inner()),
            _ => None,
        })
    }

    /// The pause reason + error, if the run emitted a `Paused` control unit (a mid-run
    /// auth failure pauses the run rather than terminating it).
    pub fn first_pause(
        &self,
    ) -> Option<(
        broca_protocol::PauseReason,
        Option<broca_protocol::ProviderError>,
    )> {
        self.events.iter().find_map(|e| match e {
            OutputEvent::Control(ControlUnit::Paused { reason, error, .. }) => {
                Some((*reason, error.clone().map(Into::into)))
            }
            _ => None,
        })
    }
}

/// Run the loop for `family` + `frozen` against aimock per `spec`. Returns None when
/// aimock can't boot (caller skips cleanly). `frozen.base_url` is overwritten with the
/// booted aimock's URL — the caller supplies the family-shaped frozen config (policies,
/// model id), the rig points it at the mock.
/// Whether a failure to boot aimock must FAIL the test (rather than skip it). True in
/// CI (GitHub Actions sets `CI=true`) or when `BROCA_REQUIRE_AIMOCK` is set. The
/// skip-clean path exists only for local dev convenience (run without `npm ci`); in CI
/// a missing aimock is a real failure, because skip-clean would silently give the whole
/// mock matrix ZERO coverage while showing green.
pub fn aimock_required() -> bool {
    std::env::var_os("BROCA_REQUIRE_AIMOCK").is_some()
        || std::env::var("CI").map(|v| v == "true").unwrap_or(false)
}

/// The one chokepoint that turns "aimock didn't boot" into either a loud failure (when
/// required, i.e. CI) or a clean skip (local dev). Every aimock e2e test routes its boot
/// result through here, so the behavior is uniform: a future aimock-boot regression
/// fails loudly in CI instead of silently skipping. Pure (env read is separate) so the
/// panic-vs-skip decision is unit-testable without an actual aimock.
pub fn decide<T>(outcome: Option<T>, required: bool, context: &str) -> Option<T> {
    match outcome {
        Some(v) => Some(v),
        None if required => panic!(
            "{context}: aimock did not boot, but it is REQUIRED (CI or BROCA_REQUIRE_AIMOCK). \
             Run `npm ci` to install the pinned @copilotkit/aimock devDep. Skipping here \
             would give the mock matrix zero coverage while showing green."
        ),
        None => None,
    }
}

pub async fn run_family_e2e<F: WireFamily + 'static>(
    family: F,
    mut frozen: FrozenRenderConfig,
    spec: RunSpec,
) -> Option<RunOutcome> {
    let mock = decide(
        AiMock::start(&spec.fixtures_json).await,
        aimock_required(),
        "provider-family e2e",
    )?;
    // Each family's endpoint() appends its OWN path (including any version segment),
    // so the base_url it needs is the BARE host for almost everyone — e.g. Anthropic
    // appends "/v1/messages", so a "host/v1" base would double to "/v1/v1/messages".
    // Gemini is the lone exception: it appends "/models/<m>:..." with the version in
    // the base, and aimock only serves the Gemini route under "/v1beta".
    frozen.base_url = Some(match frozen.family_id {
        WireFamilyId::Gemini => format!("{}/v1beta", mock.base_url),
        _ => mock.base_url.clone(),
    });
    let provider = LiveProvider::new(family, frozen, AuthHandle::None);

    let mut plane = StubToolPlane::new();
    for (name, result) in &spec.tool_results {
        plane = plane.with_canned_text(name, result);
    }

    let dir = mock.dir.clone();
    let store = FileLeaseStore::new(dir.join("leases"));
    let wal = FileWalStore::new(dir.join("wal"));

    // A per-run session id (derived from the unique temp dir name) so parallel runs
    // never share a lease key — the lease is keyed on the session, not just the dir.
    let session = dir
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("family-e2e")
        .to_string();
    let policy_allowed = spec.tools.iter().map(|t| t.name.clone()).collect();
    let req = RunRequest {
        directives: None,
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId(format!("family-e2e-{session}")),
        session: BindIdentity::new(&dir, "runner", &session),
        model: ModelHandle::new("mock", "mock-model"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: spec.prompt.clone(),
            }],
        }],
        tools: spec.tools.clone(),
        policy_allowed,
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: spec.max_steps }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    };

    let (tx, mut rx) = mpsc::channel::<OutputEvent>(2048);
    let collector = tokio::spawn(async move {
        let mut v = Vec::new();
        while let Some(e) = rx.recv().await {
            v.push(e);
        }
        v
    });
    let summary = run_loop(req, &provider, &plane, &store, &AllowAll, &wal, &tx)
        .await
        .expect("run_loop should not fail at the lease/WAL level");
    drop(tx);
    let events = collector.await.unwrap();

    Some(RunOutcome { summary, events })
}
