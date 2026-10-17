//! Shared harness for the LIVE-provider e2e suite (feature `live`, local-only).
//!
//! This is REALITY-CONFORMANCE / DRIFT-DETECTION, a different purpose than the mock
//! matrix: the mock suite checks our LOGIC against our ASSUMPTIONS; this checks our
//! ASSUMPTIONS against what the provider's wire ACTUALLY does today. It catches a
//! provider silently changing its SSE shape, adding a field, or changing
//! finish_reason values — the thing a mock structurally can't (a mock emits what we
//! told it to).
//!
//! So assertions are BEHAVIORAL and STRUCTURAL, never byte-exact (model output is
//! nondeterministic): the run completes, the right tool is called with a sane arg, a
//! nonce round-trips, the real wire still carries the STRUCTURE our normalizer
//! depends on (a signed thinking block, index-keyed tool fragments, etc.), usage is
//! populated, finish_reason is a known variant.
//!
//! NO-MASKING DISCIPLINE: provider-side problems (expired token, rate-limit, 5xx,
//! network) SKIP with a clear message — they are not our bug and must never be
//! retried-to-green. OUR-side problems (a normalize break, a malformed request, a
//! failed behavioral assertion) FAIL loudly — that is the drift signal this suite
//! exists to surface.
//!
//! CAPTURE-AND-REVIEW: every run records RESPONSE wire bytes + normalized events to
//! `tests/live/captures/<family>/<scenario>.json` for local drift inspection. Committed
//! artifacts are synthetic shape equivalents. Re-recorded responses must be scrubbed
//! before committing: responses can echo prompts and carry identifying opaque signatures.
//! Only the response side is recorded; request Authorization headers are not captured.

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[cfg(feature = "live")]
use broca_core::{run_loop, AllowAll, FileLeaseStore, FileWalStore, RunRequest, StubToolPlane};
use broca_core::{OutputEvent, RunSummary};
use broca_protocol::NormalizedEvent;
use broca_protocol::{
    caller::{ControlUnit, RunFinishReason},
    content::{ContentBlock, Message, Role, ToolOutput},
    error::ErrorClass,
    request::{ExecutionMode, Tool},
    ProviderError, StepId,
};
#[cfg(feature = "live")]
use broca_protocol::{
    request::{ModelHandle, StopCondition, ToolChoice},
    BindIdentity, GenerationConfig, RunId,
};
#[cfg(feature = "live")]
use broca_provider::transport::LiveProvider;
use broca_provider::wire::{
    AuthHandle, FrozenRenderConfig, HttpRequest, NormalizeError, StreamNormalizer, WireFamily,
};
use serde_json::json;
#[cfg(feature = "live")]
use tokio::sync::mpsc;

// ───────────────────────── capture ─────────────────────────

/// Accumulates the raw response bytes + normalized events of one live run. Shared
/// (via `Arc<Mutex>`) between the capturing family and the test so the test can write
/// the artifact after the run.
#[derive(Default)]
pub struct CaptureInner {
    /// Raw response chunks exactly as the transport handed them to the normalizer.
    pub raw_chunks: Vec<Vec<u8>>,
    /// Each normalized event, debug-rendered in emission order (our normalizer's view
    /// of the wire — the drift tripwire).
    pub events: Vec<String>,
}

#[derive(Clone, Default)]
pub struct Capture(Arc<Mutex<CaptureInner>>);

impl Capture {
    pub fn new() -> Self {
        Capture(Arc::new(Mutex::new(CaptureInner::default())))
    }

    fn record_chunk(&self, chunk: &[u8]) {
        self.0.lock().unwrap().raw_chunks.push(chunk.to_vec());
    }

    fn record_events(&self, events: &[NormalizedEvent]) {
        let mut inner = self.0.lock().unwrap();
        for ev in events {
            inner.events.push(format!("{ev:?}"));
        }
    }

    /// The concatenated raw response bytes.
    pub fn raw_bytes(&self) -> Vec<u8> {
        self.0.lock().unwrap().raw_chunks.concat()
    }

    /// Write the capture artifact under `tests/live/captures/<family>/<scenario>.json`.
    /// The raw response is stored as text when it's valid UTF-8 (SSE grammars) and
    /// additionally as base64 when it isn't (Bedrock's binary event-stream), so the
    /// artifact is both human-inspectable and byte-faithful for drift diffs.
    pub fn write(&self, family: &str, scenario: &str, model: &str) {
        let raw = self.raw_bytes();

        // A CAPTURE WITH NO RESPONSE BYTES MUST NOT OVERWRITE ONE THAT HAS THEM.
        //
        // The caller writes unconditionally, deliberately: a provider-side skip's PARTIAL
        // response is real drift evidence and worth keeping. But a transport failure -- DNS,
        // refused connection, rate-limit before any body -- produces ZERO bytes, and writing that
        // replaces a good fixture with an artifact recording only that the network was down.
        //
        // THESE FILES ARE TRACKED (22 of them) AND ARE NOT IGNORED. `.gitignore` listed this
        // directory until 2026-09-02; the rule never untracked anything, so its only effects were
        // to mislead a reader who checked it and to hide a NEW family's capture from `git status`
        // entirely. Both directions reported from outside.
        //
        // What they are FOR is human review: re-record, diff, read. Nothing on disk reads them --
        // the drift oracle is the structural assertions in the live tests, which fail loudly on
        // real wire changes. A capture is evidence a person inspects, not a fixture a test loads,
        // and describing it as a corpus something reads overstates what it does.
        //
        // Empty means "nothing came back", which is the one case that carries no evidence at all.
        if raw.is_empty() {
            return;
        }

        let inner = self.0.lock().unwrap();
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/live/captures")
            .join(family);
        let _ = std::fs::create_dir_all(&dir);
        let raw_text = String::from_utf8(raw.clone()).ok();
        let artifact = json!({
            "family": family,
            "scenario": scenario,
            "model": model,
            "note": "RESPONSE bytes + normalized events only; no request/auth captured (credentials live on the request Authorization header, never in a response).",
            "raw_response_is_utf8": raw_text.is_some(),
            "raw_response_text": raw_text,
            "raw_response_base64": if raw_text.is_none() { Some(base64_encode(&raw)) } else { None },
            "normalized_events": inner.events,
        });
        let path = dir.join(format!("{scenario}.json"));
        let _ = std::fs::write(
            &path,
            serde_json::to_string_pretty(&artifact).unwrap_or_default(),
        );
        eprintln!("[live capture] wrote {}", path.display());
    }
}

/// A `WireFamily` decorator that captures the raw response bytes + normalized events
/// flowing through its normalizer. Render/endpoint are untouched (delegated), so the
/// request bytes + auth are exactly the real provider's — only the RESPONSE is teed.
pub struct CapturingFamily<F: WireFamily> {
    inner: F,
    capture: Capture,
}

impl<F: WireFamily> CapturingFamily<F> {
    pub fn new(inner: F, capture: Capture) -> Self {
        Self { inner, capture }
    }
}

impl<F: WireFamily> WireFamily for CapturingFamily<F> {
    fn shape(
        &self,
        frozen: &FrozenRenderConfig,
        opts: &broca_protocol::CallOptions,
    ) -> serde_json::Value {
        self.inner.shape(frozen, opts)
    }

    fn render(&self, frozen: &FrozenRenderConfig, opts: &broca_protocol::CallOptions) -> Vec<u8> {
        self.inner.render(frozen, opts)
    }

    fn endpoint(
        &self,
        frozen: &FrozenRenderConfig,
        auth: &AuthHandle,
        body: Vec<u8>,
    ) -> HttpRequest {
        self.inner.endpoint(frozen, auth, body)
    }

    fn normalizer(
        &self,
        frozen: &FrozenRenderConfig,
        step_id: StepId,
    ) -> Box<dyn StreamNormalizer> {
        Box::new(CapturingNormalizer {
            inner: self.inner.normalizer(frozen, step_id),
            capture: self.capture.clone(),
        })
    }
}

struct CapturingNormalizer {
    inner: Box<dyn StreamNormalizer>,
    capture: Capture,
}

impl StreamNormalizer for CapturingNormalizer {
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        self.capture.record_chunk(chunk);
        let events = self.inner.push(chunk)?;
        self.capture.record_events(&events);
        Ok(events)
    }

    fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let events = self.inner.finish()?;
        self.capture.record_events(&events);
        Ok(events)
    }
}

// ───────────────────────── run + classify ─────────────────────────

/// The outcome of a live run after taxonomy classification.
pub enum LiveResult {
    /// A provider-side condition (no token, expired/rejected token, rate-limit, 5xx,
    /// network) — NOT our bug. The caller returns (skips) with this message printed.
    Skip(String),
    /// The run reached our side: assert behaviorally on it.
    Ran(LiveOutcome),
}

pub struct LiveOutcome {
    pub summary: RunSummary,
    pub events: Vec<OutputEvent>,
    pub capture: Capture,
    /// The run's full input (seed history + this run's opening user prompt) — the
    /// authoritative start of the canonical conversation, so `canonical_history` can
    /// reconstruct a WELL-FORMED history that begins with a user turn (the loop's
    /// `prompt` always starts with `req.input`).
    pub input: Vec<Message>,
}

/// What the test wants the model to do.
pub struct LiveSpec {
    pub prompt: String,
    pub tools: Vec<Tool>,
    pub tool_results: Vec<(String, String)>,
    pub max_steps: u64,
    pub max_tokens_note: u32,
    /// Prior canonical history to seed the run with, BEFORE the user `prompt`. Used by
    /// the provider-switch tests: the messages produced under provider A (assistant
    /// turns + tool results) are replayed as the opening context of a run under
    /// provider B, so B continues A's conversation. Empty for a fresh run.
    pub seed_history: Vec<Message>,
}

impl LiveSpec {
    pub fn new(prompt: impl Into<String>) -> Self {
        LiveSpec {
            prompt: prompt.into(),
            tools: Vec::new(),
            tool_results: Vec::new(),
            max_steps: 4,
            max_tokens_note: 1024,
            seed_history: Vec::new(),
        }
    }

    pub fn tool(mut self, name: &str, schema: serde_json::Value, result: &str) -> Self {
        self.tools.push(Tool {
            name: name.to_string(),
            description: format!("The {name} tool."),
            input_schema: schema,
            module: None,
            execution_mode: ExecutionMode::Parallel,
        });
        self.tool_results
            .push((name.to_string(), result.to_string()));
        self
    }

    /// Seed the run with prior canonical history (e.g. another provider's conversation)
    /// placed before the user `prompt`.
    pub fn seed(mut self, history: Vec<Message>) -> Self {
        self.seed_history = history;
        self
    }
}

/// Drive `run_loop` against a REAL provider (family + frozen + auth), capturing the
/// response, and classify the result by the error taxonomy (provider-side → Skip,
/// our-side → return the outcome for loud assertions). The caller resolves auth +
/// frozen config (per-family) and passes them in; the rig owns capture + classify.
#[cfg(feature = "live")]
pub async fn run_live<F: WireFamily + 'static>(
    family: F,
    frozen: FrozenRenderConfig,
    auth: AuthHandle,
    family_name: &str,
    scenario: &str,
    spec: LiveSpec,
) -> LiveResult {
    let model = frozen.wire_model_id.clone();
    let capture = Capture::new();
    let provider = LiveProvider::new(CapturingFamily::new(family, capture.clone()), frozen, auth);

    let mut plane = StubToolPlane::new();
    for (name, result) in &spec.tool_results {
        plane = plane.with_canned_text(name, result);
    }

    let dir = broca_test_support::scratch_root().join(format!(
        "broca-live-{}-{}-{}",
        std::process::id(),
        family_name,
        scenario
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let store = FileLeaseStore::new(dir.join("leases"));
    let wal = FileWalStore::new(dir.join("wal"));

    let policy_allowed = spec.tools.iter().map(|t| t.name.clone()).collect();
    // The run input is the seed history (a prior provider's conversation, empty for a
    // fresh run) followed by this run's user prompt.
    let mut input = spec.seed_history.clone();
    input.push(Message {
        cache_prefix_blocks: None,
        role: Role::User,
        content: vec![ContentBlock::Text {
            text: spec.prompt.clone(),
        }],
        origin: None,
        ordinal: None,
    });
    let req_input = input.clone();
    let req = RunRequest {
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
        run_id: RunId(format!("live-{family_name}-{scenario}")),
        session: BindIdentity::new(&dir, "runner", scenario),
        model: ModelHandle::new(family_name, &model),
        input,
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
    };

    let (tx, mut rx) = mpsc::channel::<OutputEvent>(4096);
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
    let _ = std::fs::remove_dir_all(&dir);

    // Always write the capture — even a provider-side skip's partial response is
    // useful drift evidence.
    capture.write(family_name, scenario, &model);

    // A mid-run auth failure now PAUSES the run (a durable RunPaused + a Paused control
    // unit), rather than surfacing a terminal Error — the resumable-pause model. For a
    // LIVE test that means the credential is bad: a provider-side condition → Skip,
    // exactly like an Auth-class Error was skipped before the pause model existed.
    if let Some(reason) = first_pause_skip(&events) {
        return LiveResult::Skip(reason);
    }

    // Classify by the error taxonomy: provider-side → Skip, our-side → assert.
    if let Some(err) = first_error(&events) {
        if let Some(reason) = provider_side_skip(&err) {
            return LiveResult::Skip(reason);
        }
        // Permanent request failures and transient normalization breaks are OUR side —
        // fall through so the caller's assertions fail loudly on malformed provider bytes.
    }

    LiveResult::Ran(LiveOutcome {
        summary,
        events,
        capture,
        input: req_input,
    })
}

/// Map a provider error to a skip reason when it's a provider-side condition, else
/// `None` (our-side → must fail loudly).
fn provider_side_skip(err: &ProviderError) -> Option<String> {
    match err.class {
        ErrorClass::Auth => Some(format!(
            "SKIP live: token expired/invalid — refresh via opencode. ({})",
            err.message
        )),
        ErrorClass::Transient if err.message.starts_with("normalize response:") => None,
        ErrorClass::Transient | ErrorClass::ProviderUnavailable => Some(format!(
            "SKIP live: provider rate-limited/unavailable — not a code failure. ({})",
            err.message
        )),
        // ContextOverflow shouldn't happen on these tiny prompts; if it does it's a
        // provider-side limit, skip. Permanent is our-side (malformed request /
        // normalize break) → do NOT skip.
        ErrorClass::ContextOverflow => {
            Some(format!("SKIP live: context overflow ({})", err.message))
        }
        // A 403 is Permanent for the engine on purpose: forbidden is a refusal to serve
        // this request, not evidence that the credential is dead, so it must not trigger
        // credential invalidation. For a live test the practical cause is the local key
        // having aged out, which is a condition of this machine rather than a defect in
        // the code under test. Keyed on the status rather than the message so it does not
        // depend on provider wording.
        ErrorClass::Permanent if err.status == Some(403) => Some(format!(
            "SKIP live: forbidden (403) — usually an expired or unentitled local key. ({})",
            err.message
        )),
        ErrorClass::Permanent => None,
    }
}

// ───────────────────────── assertion helpers ─────────────────────────

pub fn first_error(events: &[OutputEvent]) -> Option<ProviderError> {
    events.iter().find_map(|e| match e {
        OutputEvent::Control(ControlUnit::Error { error }) => Some(error.clone().into_inner()),
        _ => None,
    })
}

/// A `Paused` control unit means a mid-run auth failure paused the run. For a LIVE
/// reality-conformance test that's a provider-side credential problem → a clean skip
/// (the same disposition an Auth-class `Error` got before the resumable-pause model).
fn first_pause_skip(events: &[OutputEvent]) -> Option<String> {
    events.iter().find_map(|e| match e {
        OutputEvent::Control(ControlUnit::Paused { reason, error, .. }) => Some(format!(
            "SKIP live: run paused for re-auth ({reason:?}) — credential expired/invalid, \
             refresh via opencode. ({})",
            error.as_ref().map(|x| x.message.as_str()).unwrap_or("")
        )),
        _ => None,
    })
}

/// Drain accumulated tool results into one `Role::Tool` message (the loop emits all of
/// a turn's results as a single tool-role message). No-op when there are none.
fn flush_tool_results(
    history: &mut Vec<Message>,
    pending: &mut Vec<broca_protocol::content::ToolResult>,
) {
    if pending.is_empty() {
        return;
    }
    history.push(Message {
        cache_prefix_blocks: None,
        role: Role::Tool,
        content: std::mem::take(pending)
            .into_iter()
            .map(ContentBlock::ToolResult)
            .collect(),
        origin: None,
        ordinal: None,
    });
}

impl LiveOutcome {
    pub fn tool_calls(&self) -> Vec<&broca_protocol::ToolCall> {
        self.events
            .iter()
            .filter_map(|e| match e {
                OutputEvent::Control(ControlUnit::ToolCall { call }) => Some(call),
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

    /// Reconstruct the run's FULL canonical conversation history as a `Vec<Message>` to
    /// seed a follow-on run (the provider-switch tests replay this under a different
    /// provider). Mirrors EXACTLY how the loop grows its prompt: it STARTS with the
    /// run's input (the opening user turn — a well-formed history must begin with a user
    /// message, which every provider requires), then for each turn appends the assembled
    /// assistant message, then — if that turn dispatched tools — one `Role::Tool`
    /// message carrying the batch's results in order. The assistant content is kept
    /// VERBATIM, including any provider-signed reasoning blocks, so a follow-on provider
    /// genuinely exercises its drop/translate-foreign-state path (a target renderer that
    /// can't use another provider's signed thinking must drop it, not 400).
    pub fn canonical_history(&self) -> Vec<Message> {
        let mut history = self.input.clone();
        let mut pending_results: Vec<broca_protocol::content::ToolResult> = Vec::new();
        for ev in &self.events {
            match ev {
                OutputEvent::Control(ControlUnit::AssistantMessage { message }) => {
                    // A new assistant turn closes any prior turn's tool-result batch.
                    flush_tool_results(&mut history, &mut pending_results);
                    history.push(Message {
                        cache_prefix_blocks: None,
                        role: Role::Assistant,
                        content: message.content.clone(),
                        origin: None,
                        ordinal: None,
                    });
                }
                OutputEvent::Control(ControlUnit::ToolResult { result }) => {
                    pending_results.push(result.clone());
                }
                _ => {}
            }
        }
        flush_tool_results(&mut history, &mut pending_results);
        history
    }

    /// True if any assistant message carried a signed reasoning (thinking) block —
    /// the Anthropic structural conformance signal.
    pub fn produced_signed_thinking(&self) -> bool {
        self.events.iter().any(|e| {
            match e {
            OutputEvent::Control(ControlUnit::AssistantMessage { message }) => {
                message.content.iter().any(|b| {
                    matches!(b, ContentBlock::Reasoning { signature, .. } if signature.is_some())
                })
            }
            _ => false,
        }
        })
    }

    /// The completed status (the behavioral "run finished cleanly" check).
    pub fn completed(&self) -> bool {
        self.summary.reason == RunFinishReason::Completed
    }

    /// Total usage tokens (input + output) — structural "the provider reported usage".
    pub fn total_tokens(&self) -> u64 {
        // This live-test probe needs positive evidence of usage; no measurements
        // must fail the positive-token check rather than count as reported usage.
        broca_protocol::usage::sum_reported(
            self.summary.usage.input_tokens,
            self.summary.usage.output_tokens,
        )
        .unwrap_or(0)
    }
}

// ───────────────────────── auth loading ─────────────────────────

/// The opt-in every live credential source is gated on.
///
/// WHY PRESENCE IS NOT CONSENT. Until this existed, `cargo test --workspace` on a FRESH CLONE
/// made real, billed provider calls against whatever credentials happened to be on the machine --
/// including another application's store at `~/.local/share/opencode/auth.json`. Reported from
/// outside (issue #11) with 8 billed calls across deepseek and anthropic, from a contributor who
/// had configured nothing.
///
/// The `live` feature was NOT the gate it looked like: three workspace members
/// (broca-run, broca-subc, broca-module-serve) enable `broca-provider/live`, and cargo unifies
/// features across a workspace build -- so `--workspace` compiles these tests with `live` on no
/// matter what this crate's own default says. A feature flag cannot express consent when any
/// dependent can turn it on.
///
/// So the gate is an explicit environment variable, checked at the two functions every credential
/// flows through. Absent, they return None and each caller takes its existing clean skip path.
/// Spending someone's money must be something they asked for, not something their filesystem
/// implies.
fn live_calls_opted_in() -> bool {
    std::env::var("BROCA_LIVE_TESTS")
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false)
}

/// Read the opencode auth file fresh (tokens are short-lived — never cache across
/// runs). Returns the parsed JSON, or None if absent.
///
/// Gated on `BROCA_LIVE_TESTS`: this reads ANOTHER APPLICATION'S credential store, so finding
/// the file is not permission to spend against it.
pub fn read_opencode_auth() -> Option<serde_json::Value> {
    if !live_calls_opted_in() {
        return None;
    }
    let path = dirs_home()?.join(".local/share/opencode/auth.json");
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

/// An `api`-type key for `provider` from the opencode auth file (e.g. deepseek,
/// amazon-bedrock). `None` if absent or not an api-type entry.
pub fn opencode_api_key(provider: &str) -> Option<String> {
    let auth = read_opencode_auth()?;
    let entry = auth.get(provider)?;
    if entry.get("type")?.as_str()? != "api" {
        return None;
    }
    entry
        .get("key")?
        .as_str()
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

/// An `oauth` access token for `provider` from the opencode auth file, or `None` if
/// missing OR expired. Tokens are short-lived — read fresh each run; an expired token
/// is a provider-side condition the caller turns into a clean skip, never a failure.
/// `expires == 0` is treated as "no expiry info" (attempt the call; a 401 then
/// classifies as Auth → skip).
pub fn opencode_oauth_token(provider: &str) -> Option<String> {
    let auth = read_opencode_auth()?;
    let entry = auth.get(provider)?;
    if entry.get("type")?.as_str()? != "oauth" {
        return None;
    }
    let access = entry.get("access")?.as_str().filter(|s| !s.is_empty())?;
    if let Some(exp) = entry.get("expires").and_then(serde_json::Value::as_u64) {
        if exp != 0 && exp <= now_ms() + 5_000 {
            return None; // expired (5s skew so a call can't expire mid-flight)
        }
    }
    Some(access.to_owned())
}

/// The Gemini API key: `$GEMINI_API_KEY` if set (CI / another machine), else the
/// ground-truth file `~/.config/programbench/gemini.key`, else `None` (the caller
/// skips cleanly — a machine without the key just can't run gemini-live, not a
/// failure). The key authenticates via the `x-goog-api-key` header.
pub fn gemini_api_key() -> Option<String> {
    key_from_env_or_file("GEMINI_API_KEY", ".config/programbench/gemini.key")
}

/// The OpenAI PLATFORM key (sk-...): `$OPENAI_API_KEY` if set, else the file
/// `~/.config/openai.key`, else `None` (caller skips). This is a platform key carrying
/// the `api.responses.write` scope — distinct from the opencode OAuth token
/// (ChatGPT/Codex), which lacks that scope and 401s on `/v1/responses`. So
/// openai-RESPONSES live sources THIS key specifically; openai-chat can use either.
/// Authenticates as `Bearer <key>`.
pub fn openai_platform_key() -> Option<String> {
    key_from_env_or_file("OPENAI_API_KEY", ".config/openai.key")
}

/// Read a one-line key from `$env_var` if set, else from `~/<rel_path>`, trimmed;
/// `None` when neither yields a non-empty value (the caller skips cleanly).
fn key_from_env_or_file(env_var: &str, rel_path: &str) -> Option<String> {
    // Gated even for the ENV arm. A key exported in a shell for some other purpose is not an
    // instruction to bill it from a test run, and the contributor who reported this had set no
    // broca variables at all.
    if !live_calls_opted_in() {
        return None;
    }
    if let Ok(k) = std::env::var(env_var) {
        let k = k.trim();
        if !k.is_empty() {
            return Some(k.to_owned());
        }
    }
    let path = dirs_home()?.join(rel_path);
    std::fs::read_to_string(path)
        .ok()
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn dirs_home() -> Option<PathBuf> {
    std::env::var_os("HOME").map(PathBuf::from)
}

/// A minimal base64 (no external dep) for the binary-capture fallback.
fn base64_encode(bytes: &[u8]) -> String {
    const T: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let b = [
            chunk[0],
            *chunk.get(1).unwrap_or(&0),
            *chunk.get(2).unwrap_or(&0),
        ];
        let n = (b[0] as u32) << 16 | (b[1] as u32) << 8 | b[2] as u32;
        out.push(T[(n >> 18 & 63) as usize] as char);
        out.push(T[(n >> 12 & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            T[(n >> 6 & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            T[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}
