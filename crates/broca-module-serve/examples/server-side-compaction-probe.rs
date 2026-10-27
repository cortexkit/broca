//! Subscription compaction experiment; no network or vault access without `--live`.
//!
//! Background: `.cortexkit/internal-docs/research/server-side-compaction.md`.
//! Request shapes rechecked against the cited provider docs on 2026-10-22:
//! - https://developers.openai.com/api/reference/resources/responses/methods/compact
//! - https://developers.openai.com/api/docs/guides/compaction#standalone-compact-endpoint
//! - https://platform.claude.com/docs/en/build-with-claude/compaction-threshold
//!
//! The public OpenAI endpoint is NOT the subscription experiment: Codex's ChatGPT route
//! uses streamed `/responses` with a final `compaction_trigger` (see the research note).
//! Replay here isolates that opaque item, not the public `/responses/compact` output
//! window, which the public API requires callers to retain in full.
//!
//! Run in order on the Mac with broca's dev-tools vault handles installed:
//! ```text
//! cargo run --locked -p broca-module-serve --example server-side-compaction-probe
//! cargo run --locked -p broca-module-serve --example server-side-compaction-probe -- --live --only openai
//! cargo run --locked -p broca-module-serve --example server-side-compaction-probe -- --live --only anthropic
//! ```
//! The two live commands together send at most eight provider requests, with no retries.
//! `--only cross-account` or `--only cross-model` includes A's preflight, compaction,
//! and same-model control; do not run those as well if keeping a ten-request budget.
//! Models can be overridden with `--openai-model`, `--replay-model`, `--anthropic-model`.
//!
//! Interpretation: A's preflight must complete before any request on B. A refusal stops
//! the OpenAI lane, leaving B untouched. A completed compaction plus successful same-model
//! recall establishes a usable artifact. Cross-account/model acceptance AND recall, with
//! no recall in the corresponding no-context control, demonstrate use across that boundary.
//! A refusal establishes only that route/model's refusal; acceptance without recall is
//! inconclusive (possibly ignored/lossy), not proof of portability. Anthropic must return a
//! nonempty compaction block AND stop_reason=compaction to demonstrate standalone support.
//! Timings include network and stream completion, not just time to first byte. Provider
//! usage is authoritative: the deterministic transcript targets ~150k tokens, not an exact
//! tokenizer count. No live call is made to count tokens. No result establishes API-key
//! support or future subscription availability.

use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::time::Instant;

use broca_core::Provider;
use broca_module_serve::{vault_config::load_devtools_handle, CredentialId};
use broca_protocol::{
    CallOptions, ContentBlock, Message, NormalizedEvent, ProviderControl, Role, StepId,
};
use broca_provider::{
    families::{anthropic_frozen_config, openai_responses_chatgpt_frozen_config},
    wire::{AuthHandle, FrozenRenderConfig, HttpRequest, StreamNormalizer, WireFamily},
};
use tokio_util::sync::CancellationToken;

use clap::{Parser, ValueEnum};
use serde_json::{json, Value};

const MAX_REQUESTS: usize = 10;
const FACTS: [&str; 3] = ["violet-otter-731", "copper-lark-492", "silver-moth-856"];
const QUESTION: &str = "What are the project's launch key, archive key, and recovery key? Reply with those three values only. If unavailable, reply UNKNOWN. Do not invent values.";
const SUMMARY: &str = "Summarize the synthetic project log for continuation. Preserve the launch key, archive key, and recovery key verbatim; they are essential project state. Observations are disposable.";

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
enum Case {
    #[default]
    All,
    Openai,
    CrossAccount,
    CrossModel,
    Anthropic,
}

#[derive(Parser)]
#[command(
    about = "Dry by default: preview subscription compaction probes; --live sends at most 8 requests"
)]
struct Args {
    #[arg(long)]
    live: bool,
    #[arg(long, conflicts_with = "live")]
    dry_run: bool,
    #[arg(long, value_enum, default_value = "all")]
    only: Case,
    #[arg(long, default_value = "gpt-6-luna")]
    openai_model: String,
    #[arg(long, default_value = "gpt-6.1-sol")]
    replay_model: String,
    #[arg(long, default_value = "claude-opus-5-5")]
    anthropic_model: String,
    #[arg(long)]
    subc: Option<PathBuf>,
    #[arg(long, default_value = "claustrum")]
    credentials_module: String,
    #[arg(long, default_value_t = 600, value_parser = clap::value_parser!(u64).range(1..=1800))]
    timeout_secs: u64,
}

fn transcript() -> Vec<Value> {
    // Common space-prefixed words are approximately one token each in both families.
    // A fixed generator spreads the three facts through the log without reading session data.
    let words = ["map", "oak", "sea", "sun", "red", "sky", "hill", "gold"];
    let mut state = 19_u64;
    (0..150)
        .map(|page| {
            let mut text = format!("Synthetic observation log page {page}. ");
            match page {
                0 => text.push_str(&format!(
                    "Project launch key: {}. Preserve project keys. ",
                    FACTS[0]
                )),
                74 => text.push_str(&format!("Project archive key: {}. ", FACTS[1])),
                148 => text.push_str(&format!("Project recovery key: {}. ", FACTS[2])),
                _ => {}
            }
            for _ in 0..1000 {
                state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
                text.push(' ');
                text.push_str(words[(state >> 32) as usize % words.len()]);
            }
            json!({"role": if page % 2 == 0 { "user" } else { "assistant" }, "content": text})
        })
        .collect()
}

fn openai_body(model: &str, mut input: Vec<Value>, compact: bool) -> Value {
    if compact {
        input.push(json!({"type": "compaction_trigger"}));
    }
    let frozen = openai_responses_chatgpt_frozen_config(model);
    let mut body = shaped(&frozen, &input);
    body["instructions"] = json!(if compact {
        SUMMARY
    } else {
        "Answer the user's question concisely."
    });
    // Only the experimental control/opaque items bypass the canonical message renderer.
    for (index, item) in input.iter().enumerate() {
        if item.get("type").is_some() {
            body["input"]
                .as_array_mut()
                .unwrap()
                .insert(index, item.clone());
        }
    }
    body
}

fn replay_body(model: &str, compact: Option<&Value>) -> Value {
    let mut input = compact.cloned().into_iter().collect::<Vec<_>>();
    input.push(json!({"role": "user", "content": QUESTION}));
    openai_body(model, input, false)
}

fn anthropic_body(model: &str) -> Value {
    let mut messages = transcript();
    messages.push(json!({"role": "user", "content": "Compact the project log now."}));
    let mut frozen = anthropic_frozen_config(model);
    frozen.max_output_tokens = 4096;
    let mut body = shaped(&frozen, &messages);
    body["context_management"] = json!({"edits": [{"type": "compact_20260112",
        "trigger": {"type": "input_tokens", "value": 100000},
        "pause_after_compaction": true, "instructions": SUMMARY}]});
    body
}

fn shaped(frozen: &FrozenRenderConfig, input: &[Value]) -> Value {
    let messages = input
        .iter()
        .filter(|item| item.get("type").is_none())
        .map(|item| {
            Message::new(
                if item["role"] == "assistant" {
                    Role::Assistant
                } else {
                    Role::User
                },
                vec![ContentBlock::Text {
                    text: item["content"].as_str().unwrap().to_owned(),
                }],
            )
        })
        .collect();
    family(frozen).shape(frozen, &CallOptions::new(messages))
}

fn family(frozen: &FrozenRenderConfig) -> Box<dyn WireFamily> {
    broca_provider::registry::wire_family(frozen.family_id).expect("built-in wire family")
}

#[derive(Default)]
struct Counter(usize);

impl Counter {
    fn reserve(&mut self) -> Result<usize, &'static str> {
        if self.0 >= MAX_REQUESTS {
            return Err("provider request limit (10) reached; refusing to send");
        }
        self.0 += 1;
        Ok(self.0)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Credential {
    A,
    B,
    Claude,
}

impl Credential {
    fn key(self) -> &'static str {
        match self {
            Self::A => "chatgpt:openai",
            Self::B => "chatgpt:openai:gmail",
            Self::Claude => "oauth:anthropic",
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::A => "A",
            Self::B => "B",
            Self::Claude => "Claude subscription",
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Stage {
    Preflight,
    Compact,
    SameModel,
    Baseline,
    Replay,
    Anthropic,
}

struct Step {
    name: &'static str,
    credential: Credential,
    stage: Stage,
    model: String,
}

impl Step {
    fn body(&self, compact: Option<&Value>) -> Value {
        match self.stage {
            Stage::Preflight => openai_body(
                &self.model,
                vec![json!({"role": "user", "content": "Reply OK."})],
                false,
            ),
            Stage::Compact => openai_body(&self.model, transcript(), true),
            Stage::Anthropic => anthropic_body(&self.model),
            Stage::Baseline => replay_body(&self.model, None),
            Stage::SameModel | Stage::Replay => replay_body(&self.model, compact),
        }
    }
}

fn plan(args: &Args) -> Vec<Step> {
    let mut steps = Vec::new();
    let mut add = |name, credential, stage, model: &str| {
        steps.push(Step {
            name,
            credential,
            stage,
            model: model.to_owned(),
        })
    };
    if args.only != Case::Anthropic {
        add(
            "A-preflight",
            Credential::A,
            Stage::Preflight,
            &args.openai_model,
        );
        add(
            "A-150k-compaction",
            Credential::A,
            Stage::Compact,
            &args.openai_model,
        );
        add(
            "A-same-model-control",
            Credential::A,
            Stage::SameModel,
            &args.openai_model,
        );
        if matches!(args.only, Case::All | Case::Openai | Case::CrossAccount) {
            add(
                "B-no-context-control",
                Credential::B,
                Stage::Baseline,
                &args.openai_model,
            );
            add(
                "B-cross-account-replay",
                Credential::B,
                Stage::Replay,
                &args.openai_model,
            );
        }
        if matches!(args.only, Case::All | Case::Openai | Case::CrossModel) {
            add(
                "A-other-model-no-context-control",
                Credential::A,
                Stage::Baseline,
                &args.replay_model,
            );
            add(
                "A-cross-model-replay",
                Credential::A,
                Stage::Replay,
                &args.replay_model,
            );
        }
    }
    if matches!(args.only, Case::All | Case::Anthropic) {
        add(
            "Claude-150k-compaction",
            Credential::Claude,
            Stage::Anthropic,
            &args.anthropic_model,
        );
    }
    steps
}

fn preview(step: &Step, request: &HttpRequest, body: &Value) -> Value {
    let mut body = body.clone();
    // The opaque item can contain provider metadata. Never render any part of it.
    if let Some(input) = body["input"].as_array_mut() {
        for item in input {
            if matches!(
                item["type"].as_str(),
                Some("compaction" | "compaction_summary")
            ) {
                *item = json!({"type": "compaction", "encrypted_content": "[REDACTED: response-dependent opaque item]"});
            }
        }
    }
    let headers: serde_json::Map<String, Value> = request
        .headers
        .iter()
        .map(|(name, value)| {
            let value = if matches!(
                name.to_ascii_lowercase().as_str(),
                "authorization" | "chatgpt-account-id" | "cookie"
            ) {
                json!("[REDACTED]")
            } else {
                json!(value)
            };
            (name.clone(), value)
        })
        .collect();
    json!({"attempt": step.name, "credential": step.credential.label(), "method": "POST", "url": request.url, "headers": headers, "body": body})
}

struct Secret {
    token: String,
    account: Option<String>,
}

async fn fetch_secret(args: &Args, credential: Credential) -> Result<Secret, &'static str> {
    let id = CredentialId::parse(credential.key()).ok_or("invalid built-in credential key")?;
    // Vault errors can contain a bearer handle or account identity; report only the failing phase.
    let handle = load_devtools_handle(&id).map_err(|_| "dev-tools handle unavailable")?;
    let path = args.subc.clone().unwrap_or_else(|| {
        PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
            .join(".local/share/cortexkit/run/subc-connection.json")
    });
    let conn = broca_subc::SubcConnection::connect(&path)
        .await
        .map_err(|_| "subc connection unavailable")?;
    let cwd = std::env::current_dir().map_err(|_| "current directory unavailable")?;
    let identity = broca_protocol::BindIdentity::new(cwd, "compaction-probe", "compaction-probe");
    let client = broca_subc::VaultClient::new(
        Arc::new(conn),
        &args.credentials_module,
        &identity,
        Duration::from_secs(10),
    );
    let token = client
        .get(&handle, None, false)
        .await
        .result
        .map_err(|_| "vault credential unavailable")?;
    Ok(Secret {
        token: token.token,
        account: token.account_id,
    })
}

fn frozen(step: &Step) -> FrozenRenderConfig {
    if step.credential == Credential::Claude {
        let mut config = anthropic_frozen_config(&step.model);
        config.max_output_tokens = 4096;
        config
    } else {
        openai_responses_chatgpt_frozen_config(&step.model)
    }
}

fn request(step: &Step, body: &Value, secret: &Secret) -> Result<HttpRequest, &'static str> {
    if secret.token.is_empty() {
        return Err("vault token absent");
    }
    let auth = if step.credential == Credential::Claude {
        AuthHandle::Bearer(secret.token.clone())
    } else {
        let account = secret
            .account
            .clone()
            .filter(|a| !a.trim().is_empty())
            .ok_or("vault account id absent")?;
        AuthHandle::Headers(vec![
            ("authorization".into(), format!("Bearer {}", secret.token)),
            ("chatgpt-account-id".into(), account),
            ("originator".into(), "codex_cli_rs".into()),
            (
                "user-agent".into(),
                format!(
                    "codex_cli_rs/0.144.0 ({}; {})",
                    std::env::consts::OS,
                    std::env::consts::ARCH
                ),
            ),
            (
                "session-id".into(),
                format!("synthetic-compaction-{}", step.name),
            ),
            (
                "thread-id".into(),
                format!("synthetic-compaction-{}", step.name),
            ),
            (
                "x-client-request-id".into(),
                format!("synthetic-compaction-{}", step.name),
            ),
        ])
    };
    let config = frozen(step);
    let mut req = family(&config).endpoint(&config, &auth, serde_json::to_vec(body).unwrap());
    if step.credential == Credential::Claude {
        if let Some((_, value)) = req
            .headers
            .iter_mut()
            .find(|(key, _)| key.eq_ignore_ascii_case("anthropic-beta"))
        {
            value.push_str(",compact-2026-01-12");
        } else {
            req.headers
                .push(("anthropic-beta".into(), "compact-2026-01-12".into()));
        }
    }
    Ok(req)
}

// The normal renderers cannot yet express compaction items. Replace only the request bytes;
// endpoints, HTTP policy, cancellation, SSE decoding and error classification stay in broca.
struct ProbeFamily {
    req: HttpRequest,
    raw: Arc<Mutex<Vec<u8>>>,
    initialized: AtomicBool,
    cancel: CancellationToken,
}

impl WireFamily for ProbeFamily {
    fn shape(&self, _: &FrozenRenderConfig, _: &CallOptions) -> Value {
        serde_json::from_slice(&self.req.body).unwrap()
    }
    fn render(&self, _: &FrozenRenderConfig, _: &CallOptions) -> Vec<u8> {
        self.req.body.clone()
    }
    fn endpoint(&self, _: &FrozenRenderConfig, _: &AuthHandle, _: Vec<u8>) -> HttpRequest {
        self.req.clone()
    }
    fn normalizer(&self, config: &FrozenRenderConfig, step: StepId) -> Box<dyn StreamNormalizer> {
        // LiveProvider normally retries. Its factory runs before each send, and the transport
        // checks cancellation before sending. Cancel here as well as on AttemptFinished so even
        // an immediate retry cannot spend another request or bypass the global request counter.
        if self.initialized.swap(true, Ordering::SeqCst) {
            self.cancel.cancel();
        }
        Box::new(Capture {
            inner: family(config).normalizer(config, step),
            raw: self.raw.clone(),
        })
    }
}

struct Capture {
    inner: Box<dyn StreamNormalizer>,
    raw: Arc<Mutex<Vec<u8>>>,
}

impl StreamNormalizer for Capture {
    fn push(
        &mut self,
        bytes: &[u8],
    ) -> Result<Vec<NormalizedEvent>, broca_provider::wire::NormalizeError> {
        self.raw.lock().unwrap().extend_from_slice(bytes);
        self.inner.push(bytes)
    }
    fn finish(&mut self) -> Result<Vec<NormalizedEvent>, broca_provider::wire::NormalizeError> {
        self.inner.finish()
    }
    fn progress_marker(&self) -> u64 {
        self.inner.progress_marker()
    }
    fn liveness_marker(&self) -> Option<u64> {
        self.inner.liveness_marker()
    }
    fn private_error_diagnostics(&mut self) {
        self.inner.private_error_diagnostics();
    }
    fn observe_response_headers(&mut self, headers: &broca_provider::wire::ResponseHeaders) {
        self.inner.observe_response_headers(headers);
    }
}

#[derive(Default)]
struct Answer {
    complete: bool,
    compact: Option<Value>,
    recall: bool,
    paused: bool,
    summary_present: bool,
    input_tokens: Option<u64>,
}

fn parse_answer(raw: &[u8], anthropic: bool) -> Answer {
    let mut answer = Answer::default();
    let mut text = String::new();
    let mut summary = String::new();
    // Parse only after the production transport has bounded and consumed the stream. Never
    // print provider payloads: even a refusal or opaque metadata can contain credential data.
    for line in String::from_utf8_lossy(raw).lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let Ok(event) = serde_json::from_str::<Value>(data.trim()) else {
            continue;
        };
        match event["type"].as_str() {
            Some("response.completed") if !anthropic => {
                let response = &event["response"];
                answer.complete = response["status"] == "completed";
                answer.input_tokens = response["usage"]["input_tokens"].as_u64();
                if let Some(output) = response["output"].as_array() {
                    let compact: Vec<_> = output
                        .iter()
                        .filter(|item| {
                            matches!(
                                item["type"].as_str(),
                                Some("compaction" | "compaction_summary")
                            )
                        })
                        .collect();
                    if compact.len() == 1
                        && compact[0]["encrypted_content"]
                            .as_str()
                            .is_some_and(|s| !s.is_empty())
                    {
                        answer.compact = Some(compact[0].clone());
                    }
                    for item in output {
                        if item["type"] == "message" {
                            if let Some(content) = item["content"].as_array() {
                                for block in content {
                                    if block["type"] == "output_text" {
                                        text.push_str(block["text"].as_str().unwrap_or_default());
                                    }
                                }
                            }
                        }
                    }
                }
            }
            Some("message_start") if anthropic => {
                answer.input_tokens = event["message"]["usage"]["input_tokens"].as_u64();
            }
            Some("content_block_start")
                if anthropic && event["content_block"]["type"] == "compaction" =>
            {
                summary.push_str(
                    event["content_block"]["content"]
                        .as_str()
                        .unwrap_or_default(),
                );
            }
            Some("content_block_delta")
                if anthropic && event["delta"]["type"] == "compaction_delta" =>
            {
                summary.push_str(event["delta"]["content"].as_str().unwrap_or_default());
            }
            Some("message_delta") if anthropic => {
                answer.paused = event["delta"]["stop_reason"] == "compaction";
            }
            Some("message_stop") if anthropic => {
                answer.complete = true;
            }
            _ => {}
        }
    }
    answer.recall = FACTS.iter().all(|fact| text.contains(fact));
    answer.summary_present = !summary.trim().is_empty();
    answer
}

async fn send(args: &Args, step: &Step, req: HttpRequest, number: usize) -> Answer {
    let raw = Arc::new(Mutex::new(Vec::new()));
    let cancel = CancellationToken::new();
    let provider = broca_provider::LiveProvider::new(
        ProbeFamily {
            req,
            raw: raw.clone(),
            initialized: AtomicBool::new(false),
            cancel: cancel.clone(),
        },
        frozen(step),
        AuthHandle::Bearer(String::new()),
    );
    provider.set_chatgpt_https_policy(Arc::new(|| true));
    let start = Instant::now();
    let mut events = provider
        .step_with_cancel(
            StepId(number as u64),
            &CallOptions::new(vec![]),
            cancel.clone(),
            Some(start + Duration::from_secs(args.timeout_secs)),
        )
        .await;
    let mut status = None;
    let mut failed = false;
    let mut attempts = 0;
    let exchange = async {
        while let Some(event) = events.recv().await {
            match event {
                NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                    attempts += 1;
                    status = attempt.http_status;
                    println!(
                        "attempt {number} {}: HTTP {} elapsed_ms={} (stream completion)",
                        step.name,
                        status.map_or("absent".into(), |s| s.to_string()),
                        start.elapsed().as_millis()
                    );
                    cancel.cancel();
                }
                NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                    failed = true;
                    // Without the provider's own reason a refusal can't be told apart from a
                    // retired model or an exhausted account. The request text is synthetic and
                    // credentials travel only in headers, so the message is safe to print; cap it
                    // so a large echoed body stays readable.
                    let message: String = error.message.chars().take(600).collect();
                    println!(
                        "attempt {number} {}: error class={:?} provider_code={} message={message}",
                        step.name,
                        error.class,
                        error.provider_code.as_deref().unwrap_or("absent"),
                    );
                }
                _ => {}
            }
        }
    };
    if tokio::time::timeout(Duration::from_secs(args.timeout_secs), exchange)
        .await
        .is_err()
    {
        cancel.cancel();
        failed = true;
        println!(
            "attempt {number} {}: timeout elapsed_ms={}",
            step.name,
            start.elapsed().as_millis()
        );
    }
    if attempts == 0 {
        println!(
            "attempt {number} {}: no HTTP result elapsed_ms={}",
            step.name,
            start.elapsed().as_millis()
        );
    }
    let mut answer = parse_answer(&raw.lock().unwrap(), step.credential == Credential::Claude);
    answer.complete &= !failed && status.is_some_and(|s| (200..300).contains(&s));
    println!("{}: complete={} compaction_item={} recall_all_three={} paused={} summary_present={} input_tokens={}", step.name,
        answer.complete, answer.compact.is_some(), answer.recall, answer.paused, answer.summary_present,
        answer.input_tokens.map_or("unreported".into(), |n| n.to_string()));
    answer
}

fn skip_step(live: bool, openai_ready: bool, credential: Credential) -> bool {
    live && credential != Credential::Claude && !openai_ready
}

#[tokio::main]
async fn main() -> Result<(), &'static str> {
    let args = Args::parse();
    if args.openai_model == args.replay_model
        && matches!(args.only, Case::All | Case::Openai | Case::CrossModel)
    {
        return Err("cross-model replay requires two different OpenAI model names");
    }
    let steps = plan(&args);
    let mut count = Counter::default();
    let mut compact = None;
    let mut openai_ready = true;
    let mut secrets: Vec<(Credential, Secret)> = Vec::new();
    let mut baseline_clean = false;
    println!("mode={} planned_requests={} cap={MAX_REQUESTS}; cookies used=false; transcript ~150000 tokens plus framing (estimate)", if args.live { "LIVE" } else { "DRY" }, steps.len());
    for step in steps {
        if skip_step(args.live, openai_ready, step.credential) {
            println!(
                "{}: skipped; OpenAI prerequisite failed; no request sent",
                step.name
            );
            continue;
        }
        let placeholder =
            json!({"type": "compaction", "encrypted_content": "[response-dependent opaque item]"});
        let artifact = if args.live {
            compact.as_ref()
        } else {
            Some(&placeholder)
        };
        let body = step.body(artifact);
        let dummy = Secret {
            token: "[not loaded]".into(),
            account: Some("[not loaded]".into()),
        };
        let req = if args.live {
            if !secrets.iter().any(|(key, _)| *key == step.credential) {
                let loaded = fetch_secret(&args, step.credential).await;
                let secret = match loaded {
                    Ok(secret) => secret,
                    Err(phase) => {
                        println!("{}: {phase}; no request sent", step.credential.label());
                        if step.credential != Credential::Claude {
                            openai_ready = false;
                        }
                        continue;
                    }
                };
                println!(
                    "{}: token_present={} account_id_present={} cookie_present=false",
                    step.credential.label(),
                    !secret.token.is_empty(),
                    secret.account.as_ref().is_some_and(|a| !a.is_empty())
                );
                if step.credential == Credential::B {
                    let a = &secrets
                        .iter()
                        .find(|(key, _)| *key == Credential::A)
                        .ok_or("A credential missing")?
                        .1;
                    if secret.account == a.account {
                        println!(
                            "B: same account as A; cross-account case invalid, no B request sent"
                        );
                        continue;
                    }
                }
                secrets.push((step.credential, secret));
            }
            let secret = &secrets
                .iter()
                .find(|(key, _)| *key == step.credential)
                .ok_or("credential missing")?
                .1;
            match request(&step, &body, secret) {
                Ok(req) => req,
                Err(phase) => {
                    println!("{}: {phase}; no request sent", step.credential.label());
                    if step.credential != Credential::Claude {
                        openai_ready = false;
                    }
                    continue;
                }
            }
        } else {
            request(&step, &body, &dummy)?
        };
        println!(
            "{}",
            serde_json::to_string(&preview(&step, &req, &body)).unwrap()
        );
        let number = count.reserve()?;
        if !args.live {
            println!("planned {number} {}: conditional on prior successes; status=not-sent timing=not-measured token/account/cookie presence=not-read", step.name);
            continue;
        }
        let answer = send(&args, &step, req, number).await;
        match step.stage {
            Stage::Preflight => {
                openai_ready = answer.complete;
            }
            Stage::Compact => {
                openai_ready = answer.complete && answer.compact.is_some();
                compact = answer.compact;
            }
            Stage::SameModel => {
                openai_ready = answer.complete && answer.recall;
            }
            Stage::Baseline => {
                baseline_clean = answer.complete && !answer.recall;
            }
            Stage::Replay => {
                println!(
                    "{}: {}",
                    step.name,
                    if !answer.complete {
                        "REFUSED/FAILED; no portability demonstrated"
                    } else if answer.recall && baseline_clean {
                        "ACCEPTED_AND_USED (all three facts recovered, absent from no-context control)"
                    } else {
                        "ACCEPTED_BUT_INCONCLUSIVE (ignored/lossy or invalid control)"
                    }
                );
            }
            Stage::Anthropic => {
                println!(
                    "Claude subscription compaction: {}",
                    if answer.complete && answer.paused && answer.summary_present {
                        "SUPPORTED: paused with nonempty summary"
                    } else {
                        "NOT DEMONSTRATED: refused, unsupported, no trigger, or incomplete stream"
                    }
                );
            }
        }
    }
    println!(
        "{} provider requests {}; vault {}",
        if args.live { count.0 } else { 0 },
        if args.live {
            "attempted (no retries)"
        } else {
            "sent"
        },
        if args.live {
            "used only for selected credentials"
        } else {
            "not contacted"
        }
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dry_default_and_case_budgets() {
        let args = Args::parse_from(["probe"]);
        assert!(!args.live);
        assert_eq!(plan(&args).len(), 8);
        for (case, expected) in [
            ("openai", 7),
            ("cross-account", 5),
            ("cross-model", 5),
            ("anthropic", 1),
        ] {
            let args = Args::parse_from(["probe", "--only", case]);
            let steps = plan(&args);
            assert_eq!(steps.len(), expected);
            if case != "anthropic" {
                assert!(steps[0].stage == Stage::Preflight && steps[0].credential == Credential::A);
                assert!(steps[1].stage == Stage::Compact && steps[2].stage == Stage::SameModel);
            }
        }
        assert!(Args::try_parse_from(["probe", "--live", "--dry-run"]).is_err());
    }

    #[test]
    fn refused_a_blocks_b_and_later_openai_requests() {
        assert!(skip_step(true, false, Credential::A));
        assert!(skip_step(true, false, Credential::B));
        assert!(!skip_step(true, true, Credential::B));
        assert!(!skip_step(false, false, Credential::B));
        assert!(!skip_step(true, false, Credential::Claude));
    }

    #[test]
    fn retry_factory_cancels_before_a_second_send() {
        let step = plan(&Args::parse_from(["probe"])).remove(0);
        let req = request(
            &step,
            &step.body(None),
            &Secret {
                token: "synthetic-token".into(),
                account: Some("synthetic-account".into()),
            },
        )
        .unwrap();
        let cancel = CancellationToken::new();
        let wrapper = ProbeFamily {
            req,
            raw: Arc::new(Mutex::new(Vec::new())),
            initialized: AtomicBool::new(false),
            cancel: cancel.clone(),
        };
        drop(wrapper.normalizer(&frozen(&step), StepId(1)));
        assert!(!cancel.is_cancelled());
        drop(wrapper.normalizer(&frozen(&step), StepId(1)));
        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    async fn real_transport_refusal_is_not_retried() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut chunk = [0; 4096];
            loop {
                let n = socket.read(&mut chunk).await.unwrap();
                assert_ne!(n, 0);
                request.extend_from_slice(&chunk[..n]);
                if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8_lossy(&request[..end]);
                    let length: usize = headers
                        .lines()
                        .find_map(|line| {
                            line.to_ascii_lowercase()
                                .strip_prefix("content-length:")
                                .map(|n| n.trim().parse().unwrap())
                        })
                        .unwrap_or(0);
                    if request.len() >= end + 4 + length {
                        break;
                    }
                }
            }
            let body =
                r#"{"error":{"message":"synthetic quota refusal","type":"rate_limit_error"}}"#;
            socket.write_all(format!("HTTP/1.1 429 Too Many Requests\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            drop(socket);
            assert!(
                tokio::time::timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err(),
                "transport reissued a refused request"
            );
        });
        let args = Args::parse_from(["probe", "--timeout-secs", "5"]);
        let step = plan(&args).remove(0);
        let mut req = request(
            &step,
            &step.body(None),
            &Secret {
                token: "synthetic-token".into(),
                account: Some("synthetic-account".into()),
            },
        )
        .unwrap();
        req.url = format!("http://{address}/responses");
        let answer = send(&args, &step, req, 1).await;
        assert!(!answer.complete);
        server.await.unwrap();
    }

    #[test]
    fn counter_refuses_eleventh_request() {
        let mut counter = Counter::default();
        for expected in 1..=10 {
            assert_eq!(counter.reserve(), Ok(expected));
        }
        assert!(counter.reserve().is_err());
        assert_eq!(counter.0, 10);
    }

    #[test]
    fn deterministic_large_input_and_wire_shapes() {
        assert_eq!(transcript(), transcript());
        let input = transcript();
        assert_eq!(input.len(), 150);
        assert!((150000..155000).contains(
            &input
                .iter()
                .map(|m| m["content"].as_str().unwrap().split_whitespace().count())
                .sum::<usize>()
        ));
        let body = openai_body("source", input, true);
        assert_eq!(
            body["input"].as_array().unwrap().last().unwrap()["type"],
            "compaction_trigger"
        );
        assert_eq!(body["store"], false);
        assert_eq!(body["stream"], true);
        let body = anthropic_body("claude-opus-4-6");
        assert_eq!(
            body["context_management"]["edits"][0]["pause_after_compaction"],
            true
        );
        assert_eq!(
            body["context_management"]["edits"][0]["trigger"]["value"],
            100000
        );
        let step = plan(&Args::parse_from(["probe", "--only", "anthropic"])).remove(0);
        let req = request(
            &step,
            &body,
            &Secret {
                token: "synthetic-token".into(),
                account: None,
            },
        )
        .unwrap();
        assert!(req.headers.iter().any(|(k, v)| k == "anthropic-beta"
            && v.contains("compact-2026-01-12")
            && v.contains("oauth-2025-04-20")));
    }

    #[test]
    fn replay_is_opaque_only_and_preview_hides_secrets() {
        let artifact = json!({"type": "compaction", "encrypted_content": "synthetic-secret", "id": "private-id",
            "internal_chat_message_metadata_passthrough": {"account_id": "private-account"}});
        let body = replay_body("different-model", Some(&artifact));
        assert_eq!(body["input"].as_array().unwrap().len(), 2);
        assert_eq!(body["input"][0], artifact);
        let plain = replay_body("different-model", None).to_string();
        assert!(FACTS.iter().all(|fact| !plain.contains(fact)));
        assert!(FACTS.iter().all(|fact| !body.to_string().contains(fact)));
        let step = plan(&Args::parse_from(["probe"])).remove(2);
        let req = request(
            &step,
            &body,
            &Secret {
                token: "vault-secret".into(),
                account: Some("vault-account".into()),
            },
        )
        .unwrap();
        let shown = preview(&step, &req, &body).to_string();
        for secret in [
            "synthetic-secret",
            "private-id",
            "private-account",
            "vault-secret",
            "vault-account",
        ] {
            assert!(!shown.contains(secret));
        }
    }

    #[test]
    fn parse_completion_requires_canary_and_compaction_not_just_http_success() {
        let event = json!({"type": "response.completed", "response": {"status": "completed", "usage": {"input_tokens": 150123},
            "output": [{"type": "compaction", "encrypted_content": "opaque"},
                {"type": "message", "content": [{"type": "output_text", "text": FACTS.join(" ")}]}]}});
        let answer = parse_answer(format!("data: {event}\n\n").as_bytes(), false);
        assert!(answer.complete && answer.recall && answer.compact.is_some());
        assert_eq!(answer.input_tokens, Some(150123));
        let empty = parse_answer(b"data: {\"type\":\"response.completed\",\"response\":{\"status\":\"completed\",\"output\":[]}}\n\n", false);
        assert!(empty.complete && !empty.recall && empty.compact.is_none());
        let paused = parse_answer(b"data: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"compaction_delta\",\"content\":\"summary\"}}\n\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"compaction\"}}\n\ndata: {\"type\":\"message_stop\"}\n\n", true);
        assert!(paused.complete && paused.paused && paused.summary_present);
        assert!(!parse_answer(b"data: {\"type\":\"error\"}\n\n", false).complete);
    }
}
