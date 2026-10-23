//! OpenCode Go smoke check. Dry-run is the default; `--live` opts into provider calls.
//! The live path resolves each model and variant through broca, reads the API key from the
//! operator dev-tools handle through subc, and uses broca's wire-family renderer and HTTP client.
//!
//! Run a harmless preview with:
//! `cargo run -p broca-module-serve --example opencode-go-live-check`
//!
//! Send the bounded plan with:
//! `cargo run -p broca-module-serve --example opencode-go-live-check -- --live`

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use broca_catalog::{
    live::{
        resolve_catalog_episode_with_client, AuthMethod, AuthMode, EpisodeRenderConfig,
        ResolveOptions,
    },
    CatalogCache,
};
use broca_core::provider::Provider;
use broca_module_serve::{
    opencode_go_identity, state_root::resolve_state_root, vault_config::load_devtools_handle,
    CredentialId,
};
use broca_protocol::{
    content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult},
    ids::{StepId, ToolCallId},
    provider::{NormalizedEvent, ProviderControl},
    request::{CallOptions, ExecutionMode, GenerationConfig, ModelHandle, Tool, ToolChoice},
    BindIdentity,
};
use broca_provider::{
    build_provider_http_client,
    registry::wire_family,
    wire::{AuthHandle, FrozenRenderConfig, HttpRequest, WireFamily},
    ProviderHttpClient,
};
use clap::{Parser, ValueEnum};
use serde_json::{json, Value};

const MAX_REQUESTS: usize = 12;
const CHAT_OUTPUT_CAP: u32 = 512;
const DEEPSEEK_OUTPUT_CAP: u32 = 1_024;
const SYNTHETIC_FILE_PATH: &str = "workspace/README.md";
/// Stable fixture text so the same-session DeepSeek follow-up has a cacheable prompt prefix.
const DEEPSEEK_CACHE_LINES: usize = 96;

#[derive(Debug, Parser)]
#[command(name = "opencode-go-live-check")]
#[command(about = "Preview or send a bounded OpenCode Go compatibility check")]
struct Args {
    /// Send requests to OpenCode Go. Without this flag the script is a dry run.
    #[arg(long)]
    live: bool,
    /// Run only one model case; the total request limit remains enforced.
    #[arg(long, value_enum)]
    only: Option<Case>,
    /// Override the subc connection file used for the dev-tools vault lookup.
    #[arg(long)]
    subc: Option<PathBuf>,
    /// Timeout for each provider request, in seconds.
    #[arg(long, default_value_t = 180)]
    timeout_secs: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Case {
    Free,
    Kimi,
    Gpt6,
    Deepseek,
}

impl Case {
    const ALL: [Self; 4] = [Self::Free, Self::Kimi, Self::Gpt6, Self::Deepseek];

    fn label(self) -> &'static str {
        match self {
            Self::Free => "minimax-free",
            Self::Kimi => "kimi",
            Self::Gpt6 => "gpt6",
            Self::Deepseek => "deepseek",
        }
    }

    fn model_id(self) -> &'static str {
        match self {
            Self::Free => "minimax-m3.1-flash-free",
            Self::Kimi => "kimi-k2.7-code",
            Self::Gpt6 => "gpt-6-luna",
            Self::Deepseek => "deepseek-v4.1-flash",
        }
    }

    fn output_cap(self) -> u32 {
        if self == Self::Deepseek {
            DEEPSEEK_OUTPUT_CAP
        } else {
            CHAT_OUTPUT_CAP
        }
    }

    fn planned_requests(self) -> usize {
        match self {
            Self::Deepseek => 5, // three tool calls, their final answer, then a follow-up turn
            Self::Free | Self::Kimi | Self::Gpt6 => 2, // one tool call and its final answer
        }
    }
}

#[derive(Default)]
struct RequestBudget {
    sent: usize,
}

impl RequestBudget {
    fn take(&mut self) -> Result<usize, String> {
        if self.sent >= MAX_REQUESTS {
            return Err(format!(
                "request budget is {MAX_REQUESTS}; refusing HTTP request {}",
                self.sent + 1
            ));
        }
        self.sent += 1;
        Ok(self.sent)
    }

    fn sent(&self) -> usize {
        self.sent
    }
}

struct ResolvedCase {
    case: Case,
    format: String,
    base_url: String,
    wire_model: String,
    provider: Box<dyn Provider>,
    family: Box<dyn WireFamily>,
    frozen: FrozenRenderConfig,
}

#[derive(Default)]
struct SyntheticWorkspace {
    readme: String,
}

impl SyntheticWorkspace {
    fn new(cache_lines: usize) -> Self {
        let mut readme = String::from(
            "# Synthetic workspace\nThis file exists only in the live-check fixture.\n",
        );
        for line in 0..cache_lines {
            readme.push_str(&format!(
                "fixture row {line:03}: copper amber cedar delta ember fern granite harbor iris juniper.\n"
            ));
        }
        Self { readme }
    }

    /// These tools are deterministic fixtures: this function never reads, executes, or writes a
    /// real filesystem or process.
    fn execute(&mut self, call: &ToolCall) -> Result<Value, String> {
        match call.tool_name.as_str() {
            "read_file" => Ok(json!({
                "path": SYNTHETIC_FILE_PATH,
                "contents": self.readme,
            })),
            "run_command" => Ok(json!({
                "command": "cargo test -p broca-provider",
                "exit_code": 0,
                "stdout": "synthetic result: 8 tests passed",
                "stderr": "",
            })),
            "edit_file" => {
                self.readme
                    .push_str("\nSynthetic edit: Go wire compatibility checked.\n");
                Ok(json!({
                    "path": SYNTHETIC_FILE_PATH,
                    "updated": true,
                    "result": "synthetic edit applied",
                }))
            }
            other => Err(format!("unexpected synthetic tool {other}")),
        }
    }
}

fn tool_definition(name: &str) -> Tool {
    let (description, input_schema) = match name {
        "read_file" => (
            "Read a file from the synthetic in-memory workspace.",
            json!({
                "type": "object",
                "properties": {"path": {"type": "string"}},
                "required": ["path"],
                "additionalProperties": false
            }),
        ),
        "run_command" => (
            "Return the deterministic result of a synthetic command; no process is started.",
            json!({
                "type": "object",
                "properties": {"command": {"type": "string"}},
                "required": ["command"],
                "additionalProperties": false
            }),
        ),
        "edit_file" => (
            "Edit a file in the synthetic in-memory workspace; no real file is changed.",
            json!({
                "type": "object",
                "properties": {
                    "path": {"type": "string"},
                    "contents": {"type": "string"}
                },
                "required": ["path", "contents"],
                "additionalProperties": false
            }),
        ),
        other => panic!("unknown synthetic tool {other}"),
    };
    Tool {
        name: name.into(),
        description: description.into(),
        input_schema,
        module: None,
        execution_mode: ExecutionMode::Sequential,
        reply: None,
    }
}

fn install_fusiform_lkg() -> Result<PathBuf, String> {
    let state_root = resolve_state_root().map_err(|error| error.to_string())?;
    let path = state_root.path.join("catalog").join("fusiform-lkg.json");
    let cache = Arc::new(CatalogCache::open(&path));
    cache.snapshot().map_err(|error| {
        format!(
            "OpenCode Go live check needs a readable Fusiform LKG at {}: {error}",
            path.display()
        )
    })?;
    broca_catalog::install_global_catalog_cache(cache);
    Ok(path)
}

fn request_options(
    prompt: &[Message],
    model_id: &str,
    tools: Vec<Tool>,
    choice: ToolChoice,
    output_cap: u32,
) -> CallOptions {
    let mut options = CallOptions::new(prompt.to_vec());
    options.tools = Arc::new(tools);
    options.tool_choice = Arc::new(choice);
    options.generation = Arc::new(GenerationConfig {
        max_output_tokens: Some(u64::from(output_cap)),
        ..Default::default()
    });
    options.target_model = Some(ModelHandle::new("opencode-go", model_id));
    options
}

fn resolve_case(
    case: Case,
    session_id: &str,
    variant: Option<&str>,
    auth_token: &str,
    http_client: ProviderHttpClient,
) -> Result<ResolvedCase, String> {
    let resolve_options = ResolveOptions {
        auth_method: Some(AuthMethod::ApiKey),
        variant,
        ..Default::default()
    };
    let render = EpisodeRenderConfig {
        opencode_go_session_id: Some(session_id),
        ..Default::default()
    };
    let resolution = resolve_catalog_episode_with_client(
        &format!("opencode-go/{}", case.model_id()),
        Some(case.output_cap()),
        &AuthMode::Secret(auth_token.to_owned()),
        &resolve_options,
        http_client,
        render,
    )
    .map_err(|error| format!("resolve opencode-go/{}: {error}", case.model_id()))?;
    let family_id = resolution.family_id;
    let format = resolution.family.clone();
    let base_url = resolution.base_url.clone();
    let wire_model = resolution.wire_model.clone();
    let family =
        wire_family(family_id).ok_or_else(|| format!("no wire family for {family_id:?}"))?;
    let mut frozen: FrozenRenderConfig =
        serde_json::from_value(resolution.provider.render_config_blob())
            .map_err(|error| format!("decode resolved render config: {error}"))?;
    // This field is intentionally transport-only and skipped by serde. The real episode path
    // derives it from BindIdentity on admission and resume; this standalone probe does the same.
    frozen.opencode_go_session_id = Some(session_id.to_owned());
    Ok(ResolvedCase {
        case,
        format,
        base_url,
        wire_model,
        provider: resolution.provider,
        family,
        frozen,
    })
}

#[derive(Clone)]
struct StepPlan {
    label: &'static str,
    tool: Option<&'static str>,
    new_user_message: Option<&'static str>,
}

fn steps_for(case: Case) -> Vec<StepPlan> {
    match case {
        Case::Free | Case::Kimi | Case::Gpt6 => vec![
            StepPlan {
                label: "tool-read-file",
                tool: Some("read_file"),
                new_user_message: None,
            },
            StepPlan {
                label: "turn-answer",
                tool: None,
                new_user_message: None,
            },
        ],
        Case::Deepseek => vec![
            StepPlan {
                label: "tool-read-file",
                tool: Some("read_file"),
                new_user_message: None,
            },
            StepPlan {
                label: "tool-run-command",
                tool: Some("run_command"),
                new_user_message: None,
            },
            StepPlan {
                label: "tool-edit-file",
                tool: Some("edit_file"),
                new_user_message: None,
            },
            StepPlan {
                label: "turn-answer",
                tool: None,
                new_user_message: None,
            },
            StepPlan {
                label: "follow-up-turn",
                tool: None,
                new_user_message: Some(
                    "In this same conversation, give a one-sentence summary of the synthetic edit.",
                ),
            },
        ],
    }
}

fn session_id_for(case: Case) -> Result<String, String> {
    let root = std::env::current_dir().map_err(|error| format!("current directory: {error}"))?;
    let bind = BindIdentity::new(root, "opencode-go-live-check", case.label());
    Ok(opencode_go_identity::for_session(&bind))
}

fn initial_prompt(case: Case) -> Vec<Message> {
    let system = Message::new(
        Role::System,
        vec![ContentBlock::Text {
            text: "You are exercising a synthetic coding-agent tool loop. The only tools are test fixtures: do not claim to access a real file or execute a real command. Use a requested tool exactly once per response and follow the requested order.".into(),
        }],
    );
    let user_text = if case == Case::Deepseek {
        "Please read workspace/README.md with read_file, then run the requested synthetic test command with run_command, then edit the synthetic README with edit_file. Use one tool call in each response and the results from earlier steps. After all three tools, summarize the result briefly."
    } else {
        "Please use read_file to inspect workspace/README.md, then give a concise summary of the synthetic file."
    };
    vec![
        system,
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: user_text.into(),
            }],
        ),
    ]
}

fn options_for_step(history: &[Message], case: Case, plan: &StepPlan) -> CallOptions {
    let tools = plan.tool.map(tool_definition).into_iter().collect();
    // Offer the step's tool with `auto`, as production runs do. Forcing a named tool is not a
    // production shape, and thinking models such as Kimi refuse it while thinking is on
    // ("tool_choice 'specified' is incompatible with thinking enabled").
    let choice = if plan.tool.is_some() {
        ToolChoice::Auto
    } else {
        ToolChoice::None
    };
    request_options(history, case.model_id(), tools, choice, case.output_cap())
}

fn fake_tool_call(name: &str, id: &str) -> ToolCall {
    let input = match name {
        "read_file" => json!({"path": SYNTHETIC_FILE_PATH}),
        "run_command" => json!({"command": "cargo test -p broca-provider"}),
        "edit_file" => json!({
            "path": SYNTHETIC_FILE_PATH,
            "contents": "Synthetic edit: Go wire compatibility checked."
        }),
        other => panic!("unknown synthetic tool {other}"),
    };
    ToolCall {
        tool_call_id: ToolCallId(id.into()),
        tool_name: name.into(),
        input,
        thought_signature: None,
    }
}

fn append_assistant_message(
    history: &mut Vec<Message>,
    message: &broca_protocol::content::AssistantMessage,
    model_id: &str,
) {
    history.push(Message::assistant_from(
        message.content.clone(),
        ModelHandle::new("opencode-go", model_id),
    ));
}

fn append_tool_results(
    history: &mut Vec<Message>,
    calls: &[ToolCall],
    workspace: &mut SyntheticWorkspace,
    expected_tool: &str,
) -> Result<Vec<String>, String> {
    if calls.len() != 1 || calls[0].tool_name != expected_tool {
        let names = calls
            .iter()
            .map(|call| call.tool_name.as_str())
            .collect::<Vec<_>>();
        return Err(format!(
            "expected one `{expected_tool}` call, received {} tool call(s): {names:?}",
            calls.len()
        ));
    }
    let mut names = Vec::with_capacity(calls.len());
    for call in calls {
        let value = workspace.execute(call)?;
        names.push(call.tool_name.clone());
        history.push(Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::Json { value },
                is_error: false,
            })],
        ));
    }
    Ok(names)
}

fn append_dry_run_tool_exchange(
    history: &mut Vec<Message>,
    case: Case,
    expected_tool: &str,
    step_index: usize,
    workspace: &mut SyntheticWorkspace,
) -> Result<Vec<String>, String> {
    let call = fake_tool_call(
        expected_tool,
        &format!("dry-run-{}-{step_index}", case.label()),
    );
    let names = vec![call.tool_name.clone()];
    history.push(Message::assistant_from(
        vec![ContentBlock::ToolCall(call.clone())],
        ModelHandle::new("opencode-go", case.model_id()),
    ));
    let value = workspace.execute(&call)?;
    history.push(Message::new(
        Role::Tool,
        vec![ContentBlock::ToolResult(ToolResult {
            tool_call_id: call.tool_call_id,
            output: ToolOutput::Json { value },
            is_error: false,
        })],
    ));
    Ok(names)
}

fn render_http_request(
    resolved: &ResolvedCase,
    options: &CallOptions,
    auth_token: &str,
) -> HttpRequest {
    let body = resolved.provider.render_request(options);
    resolved.family.endpoint(
        &resolved.frozen,
        &AuthHandle::Bearer(auth_token.to_owned()),
        body,
    )
}

fn display_header_value(name: &str, value: &str) -> String {
    if name.eq_ignore_ascii_case("authorization")
        || name.eq_ignore_ascii_case("x-api-key")
        || name.eq_ignore_ascii_case("x-goog-api-key")
    {
        return "<redacted>".into();
    }
    if name.eq_ignore_ascii_case("x-opencode-session") {
        return format!("{}…", value.chars().take(8).collect::<String>());
    }
    value.to_owned()
}

fn pretty_body(body: &[u8]) -> String {
    serde_json::from_slice::<Value>(body)
        .and_then(|value| serde_json::to_string_pretty(&value))
        .unwrap_or_else(|_| "<non-JSON body omitted>".into())
}

fn print_dry_request(
    index: usize,
    total: usize,
    resolved: &ResolvedCase,
    plan: &StepPlan,
    request: &HttpRequest,
) {
    println!(
        "DRY RUN {index}/{total} case={} model={} format={} wire_model={} base_url={} address={} step={}",
        resolved.case.label(),
        resolved.case.model_id(),
        resolved.format,
        resolved.wire_model,
        resolved.base_url,
        request.url,
        plan.label,
    );
    for (name, value) in &request.headers {
        println!("  header {name}: {}", display_header_value(name, value));
    }
    println!("  body:\n{}", pretty_body(&request.body));
}

fn assistant_tool_calls(message: &broca_protocol::content::AssistantMessage) -> Vec<ToolCall> {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(call.clone()),
            _ => None,
        })
        .collect()
}

fn tool_names(message: Option<&broca_protocol::content::AssistantMessage>) -> Vec<String> {
    message
        .map(assistant_tool_calls)
        .unwrap_or_default()
        .into_iter()
        .map(|call| call.tool_name)
        .collect()
}

fn is_rate_or_quota_header(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name == "retry-after"
        || name.contains("ratelimit")
        || name.contains("rate-limit")
        || name.contains("quota")
        || name.contains("limit")
}

fn usage_number(value: Option<u64>) -> String {
    value.map_or_else(|| "unreported".into(), |value| value.to_string())
}

struct Observation {
    status: Option<u16>,
    elapsed_ms: u128,
    tool_calls: Vec<String>,
    usage: broca_protocol::usage::Usage,
    rate_headers: Vec<(String, String)>,
    error: Option<String>,
    assistant: Option<broca_protocol::content::AssistantMessage>,
}

impl Observation {
    fn print(
        &self,
        request_number: usize,
        resolved: &ResolvedCase,
        plan: &StepPlan,
        address: &str,
    ) {
        let calls = if self.tool_calls.is_empty() {
            "none".to_owned()
        } else {
            self.tool_calls.join(",")
        };
        let limits = if self.rate_headers.is_empty() {
            "none".to_owned()
        } else {
            self.rate_headers
                .iter()
                .map(|(name, value)| format!("{name}={value}"))
                .collect::<Vec<_>>()
                .join(",")
        };
        println!(
            "LIVE {request_number} case={} model={} format={} address={} step={} status={} elapsed_ms={} tool_calls={} usage(input={},cached_input={},output={},reasoning={}) rate_headers=[{}]{}",
            resolved.case.label(),
            resolved.case.model_id(),
            resolved.format,
            address,
            plan.label,
            self.status.map_or_else(|| "unavailable".into(), |status| status.to_string()),
            self.elapsed_ms,
            calls,
            usage_number(self.usage.input_tokens),
            usage_number(self.usage.cached_input_tokens),
            usage_number(self.usage.output_tokens),
            usage_number(self.usage.reasoning_tokens),
            limits,
            self.error
                .as_ref()
                .map_or_else(String::new, |error| format!(" error={error}")),
        );
    }
}

fn safe_provider_error(body: &[u8]) -> Option<String> {
    let value: Value = serde_json::from_slice(body).ok()?;
    let error = value.get("error")?;
    let code = error
        .get("code")
        .or_else(|| error.get("type"))
        .and_then(Value::as_str)?;
    // The provider's own message names the refused field; it is request feedback, not a
    // credential, and is cut to a bounded length before it is printed.
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .map(|message| message.chars().take(300).collect::<String>());
    Some(match message {
        Some(message) => format!("{code}: {message}"),
        None => code.to_owned(),
    })
}

fn normalize_response(
    family: &dyn WireFamily,
    frozen: &FrozenRenderConfig,
    step_id: StepId,
    bytes: &[u8],
) -> Result<
    (
        Option<broca_protocol::content::AssistantMessage>,
        broca_protocol::usage::Usage,
        Option<String>,
    ),
    String,
> {
    let mut normalizer = family.normalizer(frozen, step_id);
    let mut events = normalizer
        .push(bytes)
        .map_err(|error| format!("normalize provider response: {error}"))?;
    // End of stream, as the production transport signals it. Chat-format normalizers emit the
    // assembled assistant message only here; without it every Chat reply reads as empty.
    events.extend(
        normalizer
            .finish()
            .map_err(|error| format!("finish provider response: {error}"))?,
    );
    let mut assistant = None;
    let mut usage = broca_protocol::usage::Usage::default();
    let mut provider_error = None;
    for event in events {
        let NormalizedEvent::Control(control) = event else {
            continue;
        };
        match control {
            ProviderControl::AssistantMessage { message } => assistant = Some(message),
            ProviderControl::StepFinished {
                usage: step_usage, ..
            }
            | ProviderControl::Finished {
                usage: step_usage, ..
            } => usage = step_usage,
            ProviderControl::Error { error, .. } => {
                provider_error = Some(format!(
                    "{:?}/{}",
                    error.class,
                    error.provider_code.as_deref().unwrap_or("unclassified")
                ));
            }
            _ => {}
        }
    }
    Ok((assistant, usage, provider_error))
}

fn append_user_message(history: &mut Vec<Message>, text: &str) {
    history.push(Message::new(
        Role::User,
        vec![ContentBlock::Text { text: text.into() }],
    ));
}

fn append_final_answer(
    history: &mut Vec<Message>,
    message: &broca_protocol::content::AssistantMessage,
    model_id: &str,
) -> Result<(), String> {
    if !assistant_tool_calls(message).is_empty() {
        return Err("the final-answer step unexpectedly called a tool".into());
    }
    append_assistant_message(history, message, model_id);
    Ok(())
}

fn dry_run_case(
    resolved: &ResolvedCase,
    auth_token: &str,
    request_number: &mut usize,
    total_requests: usize,
) -> Result<(), String> {
    let case = resolved.case;
    let plans = steps_for(case);
    let mut history = initial_prompt(case);
    let mut workspace = SyntheticWorkspace::new(if case == Case::Deepseek {
        DEEPSEEK_CACHE_LINES
    } else {
        0
    });

    for (step_index, plan) in plans.iter().enumerate() {
        if let Some(text) = plan.new_user_message {
            append_user_message(&mut history, text);
        }
        let options = options_for_step(&history, case, plan);
        let request = render_http_request(resolved, &options, auth_token);
        *request_number += 1;
        print_dry_request(*request_number, total_requests, resolved, plan, &request);
        if let Some(tool) = plan.tool {
            append_dry_run_tool_exchange(&mut history, case, tool, step_index + 1, &mut workspace)?;
        } else {
            history.push(Message::assistant_from(
                vec![ContentBlock::Text {
                    text: "Synthetic dry-run answer.".into(),
                }],
                ModelHandle::new("opencode-go", case.model_id()),
            ));
        }
    }
    Ok(())
}

async fn send_live_request(
    client: &ProviderHttpClient,
    resolved: &ResolvedCase,
    options: &CallOptions,
    auth_token: &str,
    budget: &mut RequestBudget,
    plan: &StepPlan,
    timeout_duration: Duration,
) -> Result<Observation, String> {
    let request_number = budget.take()?;
    let normalizer_step_id = StepId(
        u64::try_from(request_number)
            .map_err(|_| "request number does not fit the provider step id".to_owned())?,
    );
    let request = render_http_request(resolved, options, auth_token);
    let started = Instant::now();
    let mut builder = client
        .without_response_decoding()
        .post(&request.url)
        .timeout(timeout_duration);
    for (name, value) in &request.headers {
        builder = builder.header(name, value);
    }
    let response = match builder.body(request.body).send().await {
        Ok(response) => response,
        Err(error) => {
            let observation = Observation {
                status: None,
                elapsed_ms: started.elapsed().as_millis(),
                tool_calls: Vec::new(),
                usage: Default::default(),
                rate_headers: Vec::new(),
                error: Some(format!("transport: {}", error.without_url())),
                assistant: None,
            };
            observation.print(request_number, resolved, plan, &request.url);
            return Ok(observation);
        }
    };
    let status = response.status().as_u16();
    let rate_headers = response
        .headers()
        .iter()
        .filter(|(name, _)| is_rate_or_quota_header(name.as_str()))
        .map(|(name, value)| {
            (
                name.to_string(),
                value.to_str().unwrap_or("<non-utf8>").to_owned(),
            )
        })
        .collect::<Vec<_>>();
    let bytes = match response.bytes().await {
        Ok(bytes) => bytes,
        Err(error) => {
            let observation = Observation {
                status: Some(status),
                elapsed_ms: started.elapsed().as_millis(),
                tool_calls: Vec::new(),
                usage: Default::default(),
                rate_headers,
                error: Some(format!("read response: {}", error.without_url())),
                assistant: None,
            };
            observation.print(request_number, resolved, plan, &request.url);
            return Ok(observation);
        }
    };
    let elapsed_ms = started.elapsed().as_millis();
    let (assistant, usage, stream_error) = if (200..300).contains(&status) {
        match normalize_response(
            &*resolved.family,
            &resolved.frozen,
            normalizer_step_id,
            &bytes,
        ) {
            Ok((assistant, usage, error)) => (assistant, usage, error),
            Err(error) => (None, Default::default(), Some(error)),
        }
    } else {
        (None, Default::default(), safe_provider_error(&bytes))
    };
    if (200..300).contains(&status) && assistant.is_none() {
        // A success the normalizer could not read is a wire-shape finding: show the start and
        // end of the stream (synthetic fixture content only) so the shape can be compared.
        let text = String::from_utf8_lossy(&bytes);
        let head: String = text.chars().take(1500).collect();
        let tail: String = text
            .chars()
            .rev()
            .take(800)
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();
        eprintln!(
            "unreadable 2xx body ({} bytes), head:\n{head}\n...tail:\n{tail}",
            bytes.len()
        );
    }
    let tool_calls = tool_names(assistant.as_ref());
    let error = if (200..300).contains(&status) {
        stream_error
    } else {
        Some(format!(
            "HTTP {status} {}",
            stream_error.unwrap_or_else(|| "provider refusal".into())
        ))
    };
    let observation = Observation {
        status: Some(status),
        elapsed_ms,
        tool_calls,
        usage,
        rate_headers,
        error,
        assistant,
    };
    observation.print(request_number, resolved, plan, &request.url);
    Ok(observation)
}

async fn live_case(
    resolved: &ResolvedCase,
    client: &ProviderHttpClient,
    auth_token: &str,
    budget: &mut RequestBudget,
    timeout_duration: Duration,
) -> Result<(), String> {
    let case = resolved.case;
    let plans = steps_for(case);
    let mut history = initial_prompt(case);
    let mut workspace = SyntheticWorkspace::new(if case == Case::Deepseek {
        DEEPSEEK_CACHE_LINES
    } else {
        0
    });

    for plan in &plans {
        if let Some(text) = plan.new_user_message {
            append_user_message(&mut history, text);
        }
        let options = options_for_step(&history, case, plan);
        let observation = send_live_request(
            client,
            resolved,
            &options,
            auth_token,
            budget,
            plan,
            timeout_duration,
        )
        .await?;
        if let Some(error) = observation.error {
            return Err(format!("{} {}: {error}", case.label(), plan.label));
        }
        let assistant = observation.assistant.as_ref().ok_or_else(|| {
            format!(
                "{} {}: response contained no assistant message",
                case.label(),
                plan.label
            )
        })?;
        match plan.tool {
            Some(expected_tool) => {
                let calls = assistant_tool_calls(assistant);
                append_assistant_message(&mut history, assistant, case.model_id());
                append_tool_results(&mut history, &calls, &mut workspace, expected_tool)?;
            }
            None => append_final_answer(&mut history, assistant, case.model_id())?,
        }
    }
    Ok(())
}

fn default_subc_path() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    home.join(".local/share/cortexkit/run/subc-connection.json")
}

fn subc_path(args: &Args) -> PathBuf {
    args.subc
        .clone()
        .or_else(|| {
            std::env::var_os("BROCA_LIVE_SUBC")
                .or_else(|| std::env::var_os("SUBC_SOCKET"))
                .map(PathBuf::from)
        })
        .unwrap_or_else(default_subc_path)
}

async fn load_api_key(args: &Args) -> Result<String, String> {
    let credential = CredentialId::parse("apikey:opencode-go")
        .ok_or_else(|| "invalid OpenCode Go credential id".to_owned())?;
    let handle = load_devtools_handle(&credential)?;
    let socket = subc_path(args);
    let connection = broca_subc::SubcConnection::connect(&socket)
        .await
        .map_err(|error| format!("connect subc ({}): {error}", socket.display()))?;
    let identity = BindIdentity::new(
        std::env::current_dir().map_err(|error| format!("current directory: {error}"))?,
        "opencode-go-live-check",
        "opencode-go-live-check",
    );
    let vault = broca_subc::VaultClient::new(
        Arc::new(connection),
        "claustrum",
        &identity,
        Duration::from_secs(15),
    );
    let secret = vault
        .get(&handle, None, false)
        .await
        .result
        .map_err(|error| format!("vault credential.get: {error}"))?;
    if secret.token.is_empty() {
        return Err("vault returned an empty opencode-go API key".into());
    }
    Ok(secret.token)
}

fn selected_cases(args: &Args) -> Vec<Case> {
    args.only
        .map_or_else(|| Case::ALL.to_vec(), |case| vec![case])
}

fn planned_request_count(cases: &[Case]) -> usize {
    cases.iter().map(|case| case.planned_requests()).sum()
}

fn check_deepseek_variant(resolved: &ResolvedCase) -> Result<(), String> {
    if resolved.case != Case::Deepseek {
        return Ok(());
    }
    let variant = resolved
        .frozen
        .variant
        .as_ref()
        .ok_or_else(|| "DeepSeek V4.1 Flash `high` variant was not resolved".to_owned())?;
    let effort = match &variant.request {
        broca_provider::variant::VariantRequest::OpenAiChat { effort } => effort,
        other => {
            return Err(format!(
                "DeepSeek V4.1 Flash resolved a non-Chat variant: {other:?}"
            ))
        }
    };
    if variant.name != "high" || effort != "high" {
        return Err(format!(
            "DeepSeek V4.1 Flash variant resolved unexpectedly: {:?}",
            variant
        ));
    }
    Ok(())
}

async fn run(args: Args) -> Result<(), String> {
    let cases = selected_cases(&args);
    let planned = planned_request_count(&cases);
    if planned > MAX_REQUESTS {
        return Err(format!(
            "selected plan contains {planned} requests, over the hard budget of {MAX_REQUESTS}"
        ));
    }
    let catalog_path = install_fusiform_lkg()?;
    let http_client = build_provider_http_client();
    let auth_token = if args.live {
        load_api_key(&args).await?
    } else {
        // Resolution needs an auth-shaped request config, but dry-run never sends this placeholder.
        "dry-run-placeholder".to_owned()
    };
    let timeout_duration = Duration::from_secs(args.timeout_secs);
    println!(
        "{} OpenCode Go plan: {} request(s), hard cap {}. Fusiform LKG: {}",
        if args.live {
            "LIVE"
        } else {
            "DRY RUN (no requests sent)"
        },
        planned,
        MAX_REQUESTS,
        catalog_path.display(),
    );
    let mut budget = RequestBudget::default();
    let mut dry_request_number = 0usize;
    let mut failures = Vec::new();

    for case in cases {
        let session_id = session_id_for(case)?;
        let variant = (case == Case::Deepseek).then_some("high");
        let resolved =
            match resolve_case(case, &session_id, variant, &auth_token, http_client.clone()) {
                Ok(resolved) => resolved,
                Err(error) => {
                    failures.push(format!("{}: {error}", case.label()));
                    continue;
                }
            };
        if let Err(error) = check_deepseek_variant(&resolved) {
            failures.push(format!("{}: {error}", case.label()));
            continue;
        }
        println!(
            "case={} model={} format={} wire_model={} base_url={} variant={}",
            case.label(),
            case.model_id(),
            resolved.format,
            resolved.wire_model,
            resolved.base_url,
            variant.unwrap_or("none"),
        );
        let result = if args.live {
            live_case(
                &resolved,
                &http_client,
                &auth_token,
                &mut budget,
                timeout_duration,
            )
            .await
        } else {
            dry_run_case(&resolved, &auth_token, &mut dry_request_number, planned)
        };
        if let Err(error) = result {
            failures.push(format!("{}: {error}", case.label()));
        }
    }

    if args.live {
        println!("HTTP request budget: {}/{}", budget.sent(), MAX_REQUESTS);
    } else {
        println!("No credential was read and no HTTP request was sent; pass --live to opt in.");
    }
    if failures.is_empty() {
        Ok(())
    } else {
        Err(failures.join("; "))
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    if let Err(error) = run(args).await {
        eprintln!("opencode-go-live-check: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn install_test_catalog() {
        let catalog = broca_catalog::Catalog::from_json_str(
            include_str!("../../broca-catalog/data/models.dev.json"),
            include_str!("../../broca-catalog/data/overlay.json"),
        )
        .expect("the bundled catalog fixture parses");
        let cache = Arc::new(CatalogCache::empty());
        cache.install_parsed_for_test(&catalog, 0, broca_catalog::runtime::CatalogSource::Fusiform);
        broca_catalog::install_global_catalog_cache(cache);
    }

    #[test]
    fn request_plan_stays_under_the_hard_budget() {
        let total = planned_request_count(&Case::ALL);
        assert_eq!(total, 11);
        assert!(total <= MAX_REQUESTS);
        assert_eq!(Case::Deepseek.planned_requests(), 5);
    }

    #[test]
    fn request_budget_refuses_the_thirteenth_http_request() {
        let mut budget = RequestBudget::default();
        for expected in 1..=MAX_REQUESTS {
            assert_eq!(budget.take().unwrap(), expected);
        }
        assert!(budget
            .take()
            .unwrap_err()
            .contains("refusing HTTP request 13"));
        assert_eq!(budget.sent(), MAX_REQUESTS);
    }

    #[test]
    fn rate_limit_and_quota_headers_are_selected_by_name() {
        assert!(is_rate_or_quota_header("retry-after"));
        assert!(is_rate_or_quota_header("x-ratelimit-remaining-requests"));
        assert!(is_rate_or_quota_header("x-opencode-usage-limit"));
        assert!(!is_rate_or_quota_header("content-type"));
    }

    #[test]
    fn tools_return_deterministic_in_memory_results_only() {
        let mut workspace = SyntheticWorkspace::new(2);
        let before = workspace.readme.clone();
        let read = workspace
            .execute(&fake_tool_call("read_file", "read-1"))
            .unwrap();
        assert_eq!(read["contents"], before);
        let command = workspace
            .execute(&fake_tool_call("run_command", "command-1"))
            .unwrap();
        assert_eq!(command["exit_code"], 0);
        assert_eq!(command["stdout"], "synthetic result: 8 tests passed");
        let edit = workspace
            .execute(&fake_tool_call("edit_file", "edit-1"))
            .unwrap();
        assert_eq!(edit["updated"], true);
        assert_ne!(workspace.readme, before);
    }

    #[tokio::test]
    async fn deepseek_high_uses_catalog_variant_and_pi_openai_completions_route() {
        install_test_catalog();
        let session = session_id_for(Case::Deepseek).unwrap();
        let resolved = resolve_case(
            Case::Deepseek,
            &session,
            Some("high"),
            "test-only-placeholder",
            build_provider_http_client(),
        )
        .unwrap();
        check_deepseek_variant(&resolved).unwrap();
        assert_eq!(resolved.format, "OpenAiChat");
        assert_eq!(resolved.base_url, "https://opencode.ai/inference/go/v1");
        let options = request_options(
            &initial_prompt(Case::Deepseek),
            Case::Deepseek.model_id(),
            Vec::new(),
            ToolChoice::None,
            DEEPSEEK_OUTPUT_CAP,
        );
        let request = render_http_request(&resolved, &options, "test-only-placeholder");
        assert_eq!(
            request.url,
            "https://opencode.ai/inference/go/v1/chat/completions"
        );
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["reasoning_effort"], "high");
        assert_eq!(body["thinking"]["type"], "enabled");
        assert!(request
            .headers
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case("x-opencode-session")));
    }

    #[test]
    fn live_is_opt_in_and_case_selection_is_single_case() {
        let args = Args::try_parse_from(["opencode-go-live-check"]).unwrap();
        assert!(!args.live);
        assert_eq!(selected_cases(&args), Case::ALL);
        let args = Args::try_parse_from(["opencode-go-live-check", "--only", "deepseek"]).unwrap();
        assert!(!args.live);
        assert_eq!(selected_cases(&args), vec![Case::Deepseek]);
    }
}
