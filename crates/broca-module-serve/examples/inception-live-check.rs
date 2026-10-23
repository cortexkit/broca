//! Bounded Mercury 3 compatibility probe; dry-run is the default and never reads a credential.
//!
//! Preview: `cargo run -p broca-module-serve --example inception-live-check`
//! Send: `cargo run -p broca-module-serve --example inception-live-check -- --live`
//! Uses the operator's Fusiform LKG and, only with `--live`, the dev-tools vault handle
//! for `apikey:inception` through subc. All tools and file contents are synthetic.

use std::{path::PathBuf, sync::Arc, time::Duration};

use broca_catalog::{
    live::{
        resolve_catalog_episode_with_client, AuthMethod, AuthMode, EpisodeRenderConfig,
        ResolveOptions,
    },
    CatalogCache,
};
use broca_core::provider::Provider;
use broca_module_serve::{
    state_root::resolve_state_root, vault_config::load_devtools_handle, CredentialId,
};
use broca_protocol::{
    content::{
        AssistantMessage, ContentBlock, FinishReason, Message, Role, ToolCall, ToolOutput,
        ToolResult,
    },
    ids::{StepId, ToolCallId},
    provider::{NormalizedEvent, ProviderControl},
    request::{CallOptions, ExecutionMode, GenerationConfig, ModelHandle, Tool, ToolChoice},
    usage::Usage,
    BindIdentity, ProviderError,
};
use broca_provider::{
    build_provider_http_client,
    registry::wire_family,
    transport::{classify_account_limit_exhausted, classify_http_error},
    wire::{AuthHandle, FrozenRenderConfig, HttpRequest, WireFamily},
    ProviderHttpClient,
};
use clap::Parser;
use serde_json::{json, Value};

const MODEL: &str = "inception/mercury-3";
const CREDENTIAL: &str = "apikey:inception";
const MAX_REQUESTS: usize = 8;
const OUTPUT_CAP: u32 = 512;
const FILES: [&str; 3] = [
    "workspace/README.md",
    "workspace/design.md",
    "workspace/tests.md",
];
const SMALL_TURN: &str = "What is 17 plus 25? Answer in one short sentence.";
const RESPONSE_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Debug, Parser)]
#[command(about = "Preview or send a bounded Inception Mercury 3 compatibility check")]
struct Args {
    /// Send provider requests. Without this flag neither the vault nor the network is used.
    #[arg(long)]
    live: bool,
    /// Override the subc connection file used for the operator dev-tools handle.
    #[arg(long)]
    subc: Option<PathBuf>,
    /// Override the Fusiform last-known-good catalog path (not a models.dev snapshot).
    #[arg(long)]
    catalog: Option<PathBuf>,
    /// Watchdog for a complete step, including retries, in seconds.
    #[arg(long, default_value_t = 180, value_parser = clap::value_parser!(u64).range(1..=600))]
    timeout_secs: u64,
}

#[derive(Default)]
struct RequestBudget {
    sent: usize,
}

impl RequestBudget {
    fn take(&mut self) -> Result<usize, String> {
        if self.sent >= MAX_REQUESTS {
            return Err(format!(
                "hard cap {MAX_REQUESTS}: refusing HTTP request {}",
                self.sent + 1
            ));
        }
        self.sent += 1;
        Ok(self.sent)
    }
}

#[derive(Clone, Copy, Debug)]
enum Step {
    Plain,
    Read(usize),
    Answer,
    FollowUp,
    Low,
    High,
}

impl Step {
    const ALL: [Self; 8] = [
        Self::Plain,
        Self::Read(0),
        Self::Read(1),
        Self::Read(2),
        Self::Answer,
        Self::FollowUp,
        Self::Low,
        Self::High,
    ];

    fn effort(self) -> Option<&'static str> {
        match self {
            Self::Low => Some("low"),
            Self::High => Some("high"),
            _ => None,
        }
    }
}

struct Resolved {
    family_id: broca_provider::spec::WireFamilyId,
    provider: Box<dyn Provider>,
    family: Box<dyn WireFamily>,
    frozen: FrozenRenderConfig,
    format: String,
    base_url: String,
    wire_model: String,
}

fn install_catalog(args: &Args) -> Result<PathBuf, String> {
    let path = match &args.catalog {
        Some(path) => path.clone(),
        None => resolve_state_root()
            .map_err(|error| error.to_string())?
            .path
            .join("catalog/fusiform-lkg.json"),
    };
    let cache = Arc::new(CatalogCache::open(&path));
    cache
        .snapshot()
        .map_err(|error| format!("read Fusiform LKG {}: {error}", path.display()))?;
    broca_catalog::install_global_catalog_cache(cache);
    Ok(path)
}

fn resolve(step: Step, token: &str, client: ProviderHttpClient) -> Result<Resolved, String> {
    let resolution = resolve_catalog_episode_with_client(
        MODEL,
        Some(OUTPUT_CAP),
        &AuthMode::Secret(token.to_owned()),
        &ResolveOptions {
            auth_method: Some(AuthMethod::ApiKey),
            variant: step.effort(),
            ..Default::default()
        },
        client,
        EpisodeRenderConfig::default(),
    )
    .map_err(|error| format!("resolve {MODEL}: {error}"))?;
    let frozen = serde_json::from_value(resolution.provider.render_config_blob())
        .map_err(|error| format!("decode resolved render config: {error}"))?;
    Ok(Resolved {
        family_id: resolution.family_id,
        provider: resolution.provider,
        family: wire_family(resolution.family_id).ok_or("resolved wire family is unavailable")?,
        frozen,
        format: resolution.family,
        base_url: resolution.base_url,
        wire_model: resolution.wire_model,
    })
}

fn text(role: Role, value: impl Into<String>) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: value.into() }])
}

fn mason_prompt() -> Vec<Message> {
    let mut system = String::from("You are a coding assistant examining a synthetic in-memory workspace. No tool accesses the real filesystem. Read workspace/README.md, then workspace/design.md, then workspace/tests.md, in that exact order. Make exactly one read_file call per response, wait for its result, then read the next file. Only after all three results, answer briefly with the project name, design color and test count.\nStable project reference (keep unchanged across turns):\n");
    // A shared prefix precedes the changing transcript so cache-read usage can be observed.
    for row in 0..96 {
        system.push_str(&format!("reference {row:03}: copper amber cedar delta ember fern granite harbor iris juniper.\n"));
    }
    vec![text(Role::System, system), text(Role::User, "Inspect all three named files sequentially with read_file, then summarize what you learned.")]
}

fn tool() -> Tool {
    Tool {
        name: "read_file".into(),
        description: "Read one of three named synthetic files. No real filesystem is accessed."
            .into(),
        input_schema: json!({"type":"object", "properties":{"path":{"type":"string", "enum":FILES}}, "required":["path"], "additionalProperties":false}),
        module: None,
        execution_mode: ExecutionMode::Sequential,
        reply: None,
    }
}

fn execute_tool(call: &ToolCall, expected_index: usize) -> Result<Value, String> {
    let expected = FILES
        .get(expected_index)
        .ok_or("invalid synthetic file index")?;
    if call.tool_name != "read_file" || call.input != json!({"path":expected}) {
        return Err(format!("expected exactly read_file({{\"path\":\"{expected}\"}}); received a different synthetic call"));
    }
    let contents = [
        "Project: Mercury Garden.",
        "Design color: amber.",
        "Test count: 42.",
    ][expected_index];
    Ok(json!({"path":expected, "contents":contents}))
}

fn calls(message: &AssistantMessage) -> Vec<ToolCall> {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(call.clone()),
            _ => None,
        })
        .collect()
}

fn append_response(
    history: &mut Vec<Message>,
    step: Step,
    message: &AssistantMessage,
) -> Result<(), String> {
    let tools = calls(message);
    let result = match step {
        Step::Read(index) => {
            if tools.len() != 1 {
                return Err(format!(
                    "{step:?}: expected one sequential tool call, received {}",
                    tools.len()
                ));
            }
            Some((
                tools[0].tool_call_id.clone(),
                execute_tool(&tools[0], index)?,
            ))
        }
        _ if !tools.is_empty() => {
            return Err(format!(
                "{step:?}: unexpected tool call on an answer-only step"
            ))
        }
        _ => None,
    };
    history.push(Message::assistant_from(
        message.content.clone(),
        ModelHandle::new("inception", "mercury-3"),
    ));
    if let Some((tool_call_id, value)) = result {
        history.push(Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id,
                output: ToolOutput::Json { value },
                is_error: false,
            })],
        ));
    }
    Ok(())
}

fn options(history: &[Message], step: Step) -> CallOptions {
    let mut options = CallOptions::new(history.to_vec());
    options.target_model = Some(ModelHandle::new("inception", "mercury-3"));
    options.generation = Arc::new(GenerationConfig {
        max_output_tokens: Some(u64::from(OUTPUT_CAP)),
        ..Default::default()
    });
    if matches!(step, Step::Read(_)) {
        options.tools = Arc::new(vec![tool()]);
        options.tool_choice = Arc::new(ToolChoice::Auto);
    } else {
        options.tool_choice = Arc::new(ToolChoice::None);
    }
    options
}

fn render(resolved: &Resolved, options: &CallOptions, token: &str) -> HttpRequest {
    resolved.family.endpoint(
        &resolved.frozen,
        &AuthHandle::Bearer(token.to_owned()),
        resolved.provider.render_request(options),
    )
}

fn redact(value: &str, token: &str) -> String {
    if token.is_empty() {
        value.to_owned()
    } else {
        value.replace(token, "<redacted>")
    }
}

fn print_request(
    step: Step,
    resolved: &Resolved,
    request: &HttpRequest,
    token: &str,
    live: bool,
) -> Result<(), String> {
    let body: Value = serde_json::from_slice(&request.body)
        .map_err(|error| format!("rendered body is not JSON: {error}"))?;
    let effort = body
        .get("reasoning_effort")
        .or_else(|| body.pointer("/reasoning/effort"));
    println!("{step:?}: model={MODEL} wire_family={} wire_model={} base_url={} address={} requested_effort={} rendered_effort={}", resolved.format, resolved.wire_model, resolved.base_url, request.url, step.effort().unwrap_or("default"), effort.unwrap_or(&Value::Null));
    if let Some(expected) = step.effort() {
        if effort.and_then(Value::as_str) != Some(expected) {
            return Err(format!(
                "{step:?}: catalog variant did not render reasoning effort {expected}"
            ));
        }
    }
    if !live {
        for (name, value) in &request.headers {
            // Only protocol metadata is shown; unknown authentication headers stay hidden too.
            let shown = if name.eq_ignore_ascii_case("content-type")
                || name.eq_ignore_ascii_case("accept")
            {
                value.as_str()
            } else {
                "<redacted>"
            };
            println!("  header {name}: {}", redact(shown, token));
        }
        println!(
            "  body:\n{}",
            redact(
                &serde_json::to_string_pretty(&body).map_err(|error| error.to_string())?,
                token
            )
        );
    }
    Ok(())
}

#[derive(Default)]
struct Observation {
    assistant: Option<AssistantMessage>,
    usage: Usage,
    finish: Option<FinishReason>,
    error: Option<ProviderError>,
}

fn normalize(
    resolved: &Resolved,
    bytes: &[u8],
    number: usize,
) -> Result<Observation, broca_provider::wire::NormalizeError> {
    let mut normalizer = resolved
        .family
        .normalizer(&resolved.frozen, StepId(number as u64));
    let mut events = normalizer.push(bytes)?;
    // Chat normalizers assemble their authoritative message on end-of-stream, not on push.
    events.extend(normalizer.finish()?);
    let mut observation = Observation::default();
    for event in events {
        match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                observation.assistant = Some(message)
            }
            NormalizedEvent::Control(
                ProviderControl::StepFinished {
                    usage,
                    finish_reason,
                    ..
                }
                | ProviderControl::Finished {
                    usage,
                    finish_reason,
                    ..
                },
            ) => {
                observation.usage = usage;
                observation.finish = Some(finish_reason);
            }
            NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                observation.error = Some(error)
            }
            _ => {}
        }
    }
    Ok(observation)
}

fn retry_after(headers: &reqwest::header::HeaderMap) -> Option<f64> {
    headers
        .get("retry-after-ms")
        .and_then(|value| value.to_str().ok()?.parse::<f64>().ok())
        .map(|value| value / 1000.0)
        .or_else(|| {
            let value = headers.get("retry-after")?.to_str().ok()?;
            value.parse::<f64>().ok().or_else(|| {
                let reset = chrono::DateTime::parse_from_rfc2822(value).ok()?;
                Some(
                    ((reset.timestamp_millis()
                        - chrono::DateTime::<chrono::Utc>::from(std::time::SystemTime::now())
                            .timestamp_millis())
                    .max(0) as f64)
                        / 1000.0,
                )
            })
        })
        .filter(|seconds| seconds.is_finite())
}

fn number(value: Option<u64>) -> String {
    value.map_or_else(|| "unreported".into(), |value| value.to_string())
}

fn print_observation(observation: &Observation, token: &str) {
    println!(
        "  finish={:?} usage(input={},cached_input={},output={},reasoning={})",
        observation.finish,
        number(observation.usage.input_tokens),
        number(observation.usage.cached_input_tokens),
        number(observation.usage.output_tokens),
        number(observation.usage.reasoning_tokens)
    );
    if let Some(message) = &observation.assistant {
        for block in &message.content {
            match block {
                ContentBlock::Text { text } => println!("  answer: {}", redact(text, token)),
                ContentBlock::ToolCall(call) => println!(
                    "  tool={} arguments={}",
                    redact(&call.tool_name, token),
                    redact(&call.input.to_string(), token)
                ),
                _ => {}
            }
        }
    }
}

struct Attempt {
    status: Option<u16>,
    observation: Observation,
    error: Option<String>,
    retryable: bool,
    retry_after: Option<f64>,
    decode_failure: bool,
}

impl Attempt {
    fn failed(message: String, decode_failure: bool) -> Self {
        Self {
            status: None,
            observation: Observation::default(),
            error: Some(message),
            retryable: true,
            retry_after: None,
            decode_failure,
        }
    }
}

async fn send_once(
    client: &ProviderHttpClient,
    resolved: &Resolved,
    request: &HttpRequest,
    number: usize,
) -> Attempt {
    let mut builder = client.without_response_decoding().post(&request.url);
    for (name, value) in &request.headers {
        builder = builder.header(name, value);
    }
    let mut response = match builder.body(request.body.clone()).send().await {
        Ok(response) => response,
        Err(error) => return Attempt::failed(format!("transport: {}", error.without_url()), false),
    };
    let status = response.status().as_u16();
    let headers = broca_provider::wire::ResponseHeaders::new(
        response
            .headers()
            .iter()
            .filter_map(|(name, value)| Some((name.to_string(), value.to_str().ok()?.to_owned()))),
    );
    let veto = response
        .headers()
        .get("x-should-retry")
        .is_some_and(|value| value == "false");
    let retry_after = retry_after(response.headers());
    let mut bytes = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) if bytes.len() + chunk.len() <= RESPONSE_LIMIT => {
                bytes.extend_from_slice(&chunk)
            }
            Ok(Some(_)) => {
                return Attempt {
                    status: Some(status),
                    ..Attempt::failed(
                        "response exceeds the probe's 4 MiB safety limit".into(),
                        false,
                    )
                }
            }
            Ok(None) => break,
            Err(error) => {
                return Attempt {
                    status: Some(status),
                    ..Attempt::failed(format!("read response: {}", error.without_url()), false)
                }
            }
        }
    }
    let observation = if (200..300).contains(&status) {
        match normalize(resolved, &bytes, number) {
            Ok(observation) => observation,
            Err(error) => {
                return Attempt {
                    status: Some(status),
                    retryable: !matches!(
                        error,
                        broca_provider::wire::NormalizeError::OutputCeiling(_)
                    ),
                    ..Attempt::failed(format!("normalize: {error}"), true)
                }
            }
        }
    } else {
        let body = String::from_utf8_lossy(&bytes);
        let mut error = classify_account_limit_exhausted(
            resolved.family_id,
            status,
            &headers,
            &body,
            std::time::SystemTime::now(),
            classify_http_error(status, &body, retry_after, None),
        );
        if error.is_credential_scoped() {
            error.credential_id = Some(CREDENTIAL.into());
        }
        Observation {
            error: Some(error),
            ..Default::default()
        }
    };
    let error = observation.error.as_ref().map(|error| {
        format!(
            "class={:?} provider_code={} credential={} message={}",
            error.class,
            error.provider_code.as_deref().unwrap_or("unclassified"),
            error.credential_id.as_deref().unwrap_or("none"),
            error.message
        )
    });
    let retryable = !veto
        && observation
            .error
            .as_ref()
            .is_some_and(|error| error.class.step_reissuable());
    let retry_after = observation
        .error
        .as_ref()
        .and_then(|error| error.retry_after_secs);
    if error.is_none() && (observation.assistant.is_none() || observation.finish.is_none()) {
        return Attempt {
            status: Some(status),
            ..Attempt::failed(
                "response lacked an assembled message or finish reason".into(),
                true,
            )
        };
    }
    Attempt {
        status: Some(status),
        observation,
        error,
        retryable,
        retry_after,
        decode_failure: false,
    }
}

fn retry_delay(attempt: usize, retry_after: Option<f64>) -> Duration {
    if let Some(seconds) = retry_after.filter(|seconds| seconds.is_finite()) {
        return Duration::from_secs_f64(seconds.clamp(0.0, 120.0));
    }
    let nominal = (2u64.saturating_mul(1u64 << attempt.min(16))).min(30);
    let mut random = [0u8; 1];
    let _ = getrandom::fill(&mut random);
    Duration::from_secs_f64(nominal as f64 * (0.8 + f64::from(random[0]) / 255.0 * 0.4))
}

struct LiveContext<'a> {
    client: &'a ProviderHttpClient,
    token: &'a str,
    budget: &'a mut RequestBudget,
    timeout: Duration,
}

async fn live_step(
    context: LiveContext<'_>,
    resolved: &Resolved,
    request: &HttpRequest,
    step: Step,
) -> Result<AssistantMessage, String> {
    let LiveContext {
        client,
        token,
        budget,
        timeout,
    } = context;
    // Send explicitly rather than hiding retries inside Provider::step: the request budget
    // must be checked BEFORE each send, including failures. Classification and normalization
    // remain broca's; the probe mirrors its six-attempt, one-decode-retry HTTPS schedule.
    let deadline = tokio::time::Instant::now() + timeout;
    let mut decode_retries = 0;
    let mut waiting = Duration::ZERO;
    for attempt_index in 0..6 {
        let number = budget.take()?;
        println!(
            "LIVE request={number}/{MAX_REQUESTS} step={step:?} attempt={} sending",
            attempt_index + 1
        );
        let started = std::time::Instant::now();
        let result =
            tokio::time::timeout_at(deadline, send_once(client, resolved, request, number)).await;
        let attempt = match result {
            Ok(attempt) => attempt,
            Err(_) => {
                println!(
                    "  attempt={} watchdog expired after {} ms",
                    attempt_index + 1,
                    started.elapsed().as_millis()
                );
                return Err(format!("{step:?}: step watchdog expired"));
            }
        };
        println!(
            "  attempt={} status={:?} elapsed_ms={}",
            attempt_index + 1,
            attempt.status,
            started.elapsed().as_millis()
        );
        print_observation(&attempt.observation, token);
        let Some(error) = attempt.error else {
            return attempt
                .observation
                .assistant
                .ok_or_else(|| format!("{step:?}: missing assistant message"));
        };
        println!("  error: {}", redact(&error, token));
        if !attempt.retryable
            || attempt_index == 5
            || (attempt.decode_failure && decode_retries >= 1)
        {
            return Err(format!("{step:?}: {}", redact(&error, token)));
        }
        if attempt.decode_failure {
            decode_retries += 1;
        }
        if budget.sent == MAX_REQUESTS {
            return Err(format!(
                "{step:?}: retry refused by hard cap {MAX_REQUESTS}; remaining cases not sent"
            ));
        }
        let delay = retry_delay(attempt_index, attempt.retry_after);
        if waiting + delay > Duration::from_secs(240)
            || tokio::time::Instant::now() + delay >= deadline
        {
            return Err(format!(
                "{step:?}: retry cannot fit the waiting budget/watchdog"
            ));
        }
        waiting += delay;
        println!(
            "  retry in {} ms (consumes the same global eight-request budget)",
            delay.as_millis()
        );
        tokio::time::sleep(delay).await;
    }
    Err("attempt limit reached".into())
}

fn dry_message(step: Step) -> AssistantMessage {
    let content = match step {
        Step::Read(index) => vec![ContentBlock::ToolCall(ToolCall {
            tool_call_id: ToolCallId(format!("dry-read-{index}")),
            tool_name: "read_file".into(),
            input: json!({"path":FILES[index]}),
            thought_signature: None,
        })],
        _ => vec![ContentBlock::Text {
            text: "Synthetic dry-run answer; not a provider observation.".into(),
        }],
    };
    AssistantMessage {
        message_id: broca_protocol::ids::MessageId("dry-run".into()),
        content,
    }
}

async fn load_api_key(args: &Args) -> Result<String, String> {
    let credential = CredentialId::parse(CREDENTIAL).ok_or("invalid Inception credential id")?;
    let handle = load_devtools_handle(&credential)?;
    let socket = args
        .subc
        .clone()
        .or_else(|| {
            std::env::var_os("BROCA_LIVE_SUBC")
                .or_else(|| std::env::var_os("SUBC_SOCKET"))
                .map(PathBuf::from)
        })
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                .join(".local/share/cortexkit/run/subc-connection.json")
        });
    let connection = broca_subc::SubcConnection::connect(&socket)
        .await
        .map_err(|_| format!("connect subc ({}) failed", socket.display()))?;
    let identity = BindIdentity::new(
        std::env::current_dir().map_err(|error| error.to_string())?,
        "inception-live-check",
        "inception-live-check",
    );
    let vault = broca_subc::VaultClient::new(
        Arc::new(connection),
        "claustrum",
        &identity,
        Duration::from_secs(15),
    );
    let secret = vault.get(&handle, None, false).await.result.map_err(|_| {
        format!("vault credential.get failed for {CREDENTIAL} (details omitted to protect the key)")
    })?;
    if secret.token.is_empty() {
        return Err(format!("vault returned an empty {CREDENTIAL} key"));
    }
    Ok(secret.token)
}

async fn run(args: Args) -> Result<(), String> {
    let catalog = install_catalog(&args)?;
    let client = build_provider_http_client();
    let token = if args.live {
        load_api_key(&args).await?
    } else {
        "dry-run-placeholder".into()
    };
    let mut budget = RequestBudget::default();
    let result = async {
        println!("{} {MODEL}: eight planned steps, hard cap {MAX_REQUESTS} HTTP requests INCLUDING retries. Catalog: {}", if args.live { "LIVE" } else { "DRY RUN (no credential read, no requests sent)" }, catalog.display());
        println!("Retries can consume later cases; a failed step stops the plan. Missing usage is unreported, not zero. Cache hits are observations, not guaranteed.");
        let mut history = Vec::new();
        for (index, step) in Step::ALL.into_iter().enumerate() {
            match step {
                Step::Plain | Step::Low | Step::High => history = vec![text(Role::User, SMALL_TURN)],
                Step::Read(0) => history = mason_prompt(),
                Step::FollowUp => history.push(text(Role::User, "In this same conversation, what is the design color and test count? Answer briefly.")),
                _ => {}
            }
            let resolved = resolve(step, &token, client.clone())?;
            let request = render(&resolved, &options(&history, step), &token);
            println!("\nSTEP {}/{}", index + 1, Step::ALL.len());
            print_request(step, &resolved, &request, &token, args.live)?;
            let message = if args.live {
                live_step(LiveContext { client: &client, token: &token, budget: &mut budget, timeout: Duration::from_secs(args.timeout_secs) }, &resolved, &request, step).await?
            } else {
                dry_message(step)
            };
            append_response(&mut history, step, &message)?;
        }
        Ok(())
    }.await;
    println!(
        "\nHTTP send slots used: {}/{MAX_REQUESTS} (includes failed/timed-out attempts)",
        budget.sent
    );
    result.map_err(|error: String| redact(&error, &token))
}

#[tokio::main]
async fn main() {
    if let Err(error) = run(Args::parse()).await {
        eprintln!("inception-live-check: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn test_resolved(base_url: &str) -> Resolved {
        // Only mock-server tests bypass the catalog; the executable always resolves its LKG.
        let resolution = broca_catalog::live::resolve_provider_with_client(
            "mercury-3",
            Some(base_url),
            Some(OUTPUT_CAP),
            &AuthMode::Secret("test-key".into()),
            &ResolveOptions::default(),
            build_provider_http_client(),
        )
        .unwrap();
        Resolved {
            frozen: serde_json::from_value(resolution.provider.render_config_blob()).unwrap(),
            family: wire_family(resolution.family_id).unwrap(),
            family_id: resolution.family_id,
            provider: resolution.provider,
            format: resolution.family,
            base_url: resolution.base_url,
            wire_model: resolution.wire_model,
        }
    }

    async fn mock_server(
        status: u16,
        body: String,
    ) -> (String, Arc<AtomicUsize>, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}", listener.local_addr().unwrap());
        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let header_end = loop {
                    bytes.push(socket.read_u8().await.unwrap());
                    if bytes.ends_with(b"\r\n\r\n") {
                        break bytes.len();
                    }
                };
                let headers = String::from_utf8_lossy(&bytes).to_ascii_lowercase();
                let length: usize = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .unwrap()
                    .parse()
                    .unwrap();
                bytes.resize(header_end + length, 0);
                socket.read_exact(&mut bytes[header_end..]).await.unwrap();
                observed.fetch_add(1, Ordering::SeqCst);
                let response = format!("HTTP/1.1 {status} mock\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nRetry-After: 0\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (address, count, server)
    }

    fn success_stream() -> String {
        let delta = json!({"id":"mock", "choices":[{"index":0,"delta":{"role":"assistant","content":"42"},"finish_reason":null}]});
        let finish = json!({"id":"mock", "choices":[{"index":0,"delta":{},"finish_reason":"stop"}], "usage":{"prompt_tokens":11,"completion_tokens":5,"prompt_tokens_details":{"cached_tokens":3},"completion_tokens_details":{"reasoning_tokens":2}}});
        format!("data: {delta}\n\ndata: {finish}\n\ndata: [DONE]\n\n")
    }

    #[tokio::test]
    async fn normalized_usage_and_finish_come_from_the_wire_response() {
        let resolved = test_resolved("http://127.0.0.1:1");
        let observation = normalize(&resolved, success_stream().as_bytes(), 1).unwrap();
        assert!(observation.error.is_none());
        assert_eq!(observation.finish, Some(FinishReason::Stop));
        // Canonical input is fresh input; the three cached tokens are counted separately.
        assert_eq!(observation.usage.input_tokens, Some(8));
        assert_eq!(observation.usage.cached_input_tokens, Some(3));
        assert_eq!(observation.usage.output_tokens, Some(5));
        assert_eq!(observation.usage.reasoning_tokens, Some(2));
        assert_eq!(
            observation.assistant.unwrap().content,
            vec![ContentBlock::Text { text: "42".into() }]
        );
    }

    #[tokio::test]
    async fn live_sender_refuses_a_ninth_request_before_http() {
        let (url, count, server) = mock_server(200, success_stream()).await;
        let resolved = test_resolved(&url);
        let client = build_provider_http_client();
        let request = render(
            &resolved,
            &options(&[text(Role::User, SMALL_TURN)], Step::Plain),
            "test-key",
        );
        let mut budget = RequestBudget::default();
        for _ in 0..8 {
            live_step(
                LiveContext {
                    client: &client,
                    token: "test-key",
                    budget: &mut budget,
                    timeout: Duration::from_secs(5),
                },
                &resolved,
                &request,
                Step::Plain,
            )
            .await
            .unwrap();
        }
        let error = live_step(
            LiveContext {
                client: &client,
                token: "test-key",
                budget: &mut budget,
                timeout: Duration::from_secs(5),
            },
            &resolved,
            &request,
            Step::Plain,
        )
        .await
        .unwrap_err();
        server.abort();
        assert!(error.contains("refusing HTTP request 9"));
        assert_eq!(count.load(Ordering::SeqCst), 8);
    }

    #[tokio::test]
    async fn retry_consumes_the_same_global_budget() {
        let (url, count, server) = mock_server(
            503,
            json!({"error":{"type":"server_error", "message":"try again"}}).to_string(),
        )
        .await;
        let resolved = test_resolved(&url);
        let client = build_provider_http_client();
        let request = render(
            &resolved,
            &options(&[text(Role::User, SMALL_TURN)], Step::Plain),
            "test-key",
        );
        let mut budget = RequestBudget { sent: 6 };
        let error = live_step(
            LiveContext {
                client: &client,
                token: "test-key",
                budget: &mut budget,
                timeout: Duration::from_secs(5),
            },
            &resolved,
            &request,
            Step::Plain,
        )
        .await
        .unwrap_err();
        server.abort();
        assert!(error.contains("retry refused by hard cap 8"));
        assert_eq!(count.load(Ordering::SeqCst), 2);
        assert_eq!(budget.sent, 8);
    }

    #[test]
    fn live_is_opt_in() {
        assert!(!Args::try_parse_from(["inception-live-check"]).unwrap().live);
        assert!(
            Args::try_parse_from(["inception-live-check", "--live"])
                .unwrap()
                .live
        );
    }

    #[test]
    fn ninth_send_is_refused_even_after_failures() {
        let mut budget = RequestBudget::default();
        assert_eq!(Step::ALL.len(), 8);
        for expected in 1..=8 {
            assert_eq!(budget.take().unwrap(), expected);
        }
        assert!(budget
            .take()
            .unwrap_err()
            .contains("refusing HTTP request 9"));
        assert_eq!(budget.sent, 8);
    }

    #[test]
    fn three_distinct_tool_results_precede_the_cached_follow_up() {
        let mut history = mason_prompt();
        let prefix = history[0].clone();
        for step in [Step::Read(0), Step::Read(1), Step::Read(2), Step::Answer] {
            append_response(&mut history, step, &dry_message(step)).unwrap();
        }
        assert_eq!(history.len(), 9);
        assert_eq!(history[0], prefix);
        assert_eq!(
            history
                .iter()
                .filter(|message| message.role == Role::Tool)
                .count(),
            3
        );
        let wrong = dry_message(Step::Read(1));
        assert!(append_response(&mut history, Step::Read(0), &wrong).is_err());
        assert!(append_response(&mut history, Step::Answer, &wrong).is_err());
    }

    #[test]
    fn key_echoes_are_redacted_before_display() {
        assert_eq!(
            redact("oops secret-token secret-token", "secret-token"),
            "oops <redacted> <redacted>"
        );
    }

    #[test]
    fn retry_after_is_capped_and_backoff_is_bounded() {
        assert_eq!(retry_delay(0, Some(500.0)), Duration::from_secs(120));
        assert_eq!(retry_delay(0, Some(0.25)), Duration::from_millis(250));
        for index in 0..6 {
            assert!(retry_delay(index, None) <= Duration::from_secs(36));
        }
    }
}
