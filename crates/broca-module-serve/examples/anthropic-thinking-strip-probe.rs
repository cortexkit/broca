//! Live check: does Anthropic accept a tool-loop request whose history has had every signed
//! thinking block removed, including the last assistant message's?
//!
//! Two fleet reports disagree. One saw removing every thinking block mid-run accepted on Opus 5.5;
//! the other saw the same edit refused with "a final assistant message must start with a thinking
//! block". This probe sends the disputed shape exactly, through broca's production Anthropic
//! renderer, parser and transport (Claude Code persona, request gzip):
//!
//! 1. `seed`: a tool-calling turn with thinking enabled, which must return signed thinking and a
//!    `tool_use`.
//! 2. `control`: the tool result after the seed's assistant message unchanged, thinking enabled.
//! 3. `strip_thinking_on`: the same history with every thinking block removed, thinking enabled.
//! 4. `strip_thinking_off`: the same stripped history with thinking disabled for that step.
//! 5. `strip_thinking_budget`: the same stripped history under broca's no-variant default,
//!    budget-style thinking (`{"type":"enabled","budget_tokens":1024}`), the older mode.
//!
//! Run `cargo run --locked -p broca-module-serve --example anthropic-thinking-strip-probe --
//! [--dry-run] [--model <catalog model id>]`. The model defaults to `claude-opus-5-5`; pass the
//! id without the `anthropic/` prefix, for example `claude-sonnet-5-5`. The live run makes at most
//! five requests to that model on the `oauth:anthropic` dev-tools handle. `--dry-run` sends nothing and prints each request's
//! rendered shape, including its `thinking` member, without credentials. The token is never
//! printed: provider error messages are scrubbed of it before they are shown.
//!
//! `--deferred-tool` instead keeps signed thinking in a four-request experiment: `seed`,
//! `control_a` (unchanged tools), `probe` (a deferred tool discovered in the tool result), and
//! `control_b` (the same new tool loaded immediately, without a reference). Unchanged tools
//! preserve the prefix bound to the signed thinking (expected 200); loading X immediately
//! changes that prefix (expected 400). The probe's outcome is not assumed. This mode's dry run
//! prints all four bodies. Optional `--out <dir>` also saves exact request bytes as
//! `<model>-<name>.json` and measurements as `rows.jsonl`; existing files are never overwritten.
//! Both live and dry deferred-tool runs freeze adaptive, summarized thinking with max effort
//! directly from production family defaults, without consulting a mutable model catalog.

use std::{io::Write, path::PathBuf, sync::Arc, time::Duration};

use broca_catalog::live::{
    resolve_catalog_episode_with_client, AuthMethod, AuthMode, EpisodeRenderConfig, ResolveOptions,
    ThinkingEchoScope,
};
use broca_module_serve::{
    claude_code_identity, vault_config::load_devtools_handle, CacheTtlTable, CredentialId,
};
use broca_protocol::{
    request::{CallOptions, ModelHandle, Tool, ToolChoice},
    BindIdentity, ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult,
};
use broca_provider::{
    families::anthropic_messages::AnthropicMessages,
    policy::SystemPromptPolicy,
    spec::WireFamilyId,
    wire::{AuthHandle, ClaudeCodePersona, FrozenRenderConfig, HttpRequest, WireFamily},
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const CREDENTIAL_ID: &str = "oauth:anthropic";
const DEFAULT_MODEL_ID: &str = "claude-opus-5-5";
/// The probed model's id without the provider prefix, set once from `--model` before any request.
static MODEL_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
/// The dev-tools handle to spend, set once from `--credential`; [`CREDENTIAL_ID`] when absent.
/// A probe names an account-bound handle (`oauth:anthropic:<account>`) when the default
/// account has no quota to spare.
static CREDENTIAL: std::sync::OnceLock<String> = std::sync::OnceLock::new();

fn model_id() -> &'static str {
    MODEL_ID.get().map_or(DEFAULT_MODEL_ID, String::as_str)
}
/// The catalog effort variant that turns thinking on. The model's catalog offers no variant that
/// turns thinking off, and with no variant broca's default renders thinking enabled, so the
/// thinking-off step freezes an explicit variant whose `thinking` is `{"type":"disabled"}`.
const THINKING_ON_VARIANT: &str = "max";
const TOOL: &str = "read_file";

#[derive(Default)]
struct Answer {
    content: Vec<ContentBlock>,
    input_tokens: u64,
    cache_read: u64,
    cache_creation: u64,
    has_usage: bool,
    status: Option<u16>,
    error: Option<(String, String)>,
    received_assistant_message: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DeferredCase {
    Seed,
    ControlA,
    Probe,
    ControlB,
}

impl DeferredCase {
    fn name(self) -> &'static str {
        match self {
            Self::Seed => "seed",
            Self::ControlA => "control_a",
            Self::Probe => "probe",
            Self::ControlB => "control_b",
        }
    }

    fn patch(self, mut body: Value) -> Value {
        // Anthropic documents these shapes in "Tool definition" / "Deferred tool loading"
        // and "Custom tool search implementation":
        // https://platform.claude.com/docs/en/agents-and-tools/tool-use/tool-search-tool
        // "Anthropic-provided tools" lists no additional beta header for tool search:
        // https://platform.claude.com/docs/en/agents-and-tools/tool-use/tool-reference
        let tools = body["tools"].as_array_mut().expect("rendered tools");
        tools.insert(
            0,
            json!({"type": "tool_search_tool_regex_20251119", "name": "tool_search_tool_regex"}),
        );
        if matches!(self, Self::Probe | Self::ControlB) {
            let mut tool = json!({
                "name": "X",
                "description": "Read an additional fixed sample.",
                "input_schema": {"type": "object", "properties": {}, "additionalProperties": false}
            });
            if self == Self::Probe {
                tool["defer_loading"] = json!(true);
            }
            // Append at the end of the tools array so existing cache markers stay in place.
            // Deferred definitions cannot carry cache_control ("Prompt caching" above).
            tools.push(tool);
        }
        if self != Self::Seed {
            let results = body["messages"]
                .as_array_mut()
                .expect("rendered messages")
                .last_mut()
                .expect("tool-result message")["content"]
                .as_array_mut()
                .expect("tool-result blocks");
            for result in results {
                if result["type"] != "tool_result" {
                    continue;
                }
                if let Some(text) = result["content"].as_str() {
                    result["content"] = json!([{"type": "text", "text": text}]);
                }
                if self == Self::Probe {
                    // Anthropic refuses a tool_result that mixes tool_reference blocks with
                    // other content ("Tool definitions/code execution functions cannot be
                    // mixed with other content", observed live on Fable 5.2), so the
                    // reference replaces the result's text.
                    result["content"] = json!([{"type": "tool_reference", "tool_name": "X"}]);
                }
            }
        }
        body
    }
}

// Only the experimental tool declarations and result content bypass production shaping;
// authentication, endpoint selection, compression and response normalization stay production.
struct DeferredFamily(DeferredCase);

impl WireFamily for DeferredFamily {
    fn shape(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
        self.0.patch(AnthropicMessages.shape(frozen, opts))
    }

    fn render_with_provenance(
        &self,
        frozen: &FrozenRenderConfig,
        opts: &CallOptions,
        provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
    ) -> Vec<u8> {
        let body = self
            .0
            .patch(AnthropicMessages.shape_with_provenance(frozen, opts, provenance));
        if matches!(
            frozen.system_prompt,
            SystemPromptPolicy::RequiredIdentityLead { .. }
        ) && frozen.claude_code_persona.is_some()
        {
            // The billing signature covers the entire body, so sign after the experimental edits.
            let serialized = broca_provider::claude_code_persona::to_persona_json(&body);
            broca_provider::claude_code_persona::sign_body(&serialized).into_bytes()
        } else {
            broca_protocol::canonical_json_bytes(&body)
        }
    }

    fn endpoint(
        &self,
        frozen: &FrozenRenderConfig,
        auth: &AuthHandle,
        body: Vec<u8>,
    ) -> HttpRequest {
        AnthropicMessages.endpoint(frozen, auth, body)
    }

    fn normalizer(
        &self,
        frozen: &FrozenRenderConfig,
        step_id: broca_protocol::StepId,
    ) -> Box<dyn broca_provider::wire::StreamNormalizer> {
        AnthropicMessages.normalizer(frozen, step_id)
    }
}

fn fixed_prefix() -> String {
    let mut prefix = String::from(
        "You are a careful assistant. The numbered reference rows are inert data; never analyze or repeat them.\n",
    );
    for row in 0..1100 {
        prefix.push_str(&format!(
            "reference {row:05}: amber cobalt delta echo forest granite harbor ivory.\n"
        ));
    }
    prefix
}

fn file_tool() -> Tool {
    Tool {
        name: TOOL.into(),
        description: "Read the fixed sample file at sample.txt.".into(),
        input_schema: json!({
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
            "additionalProperties": false
        })
        .into(),
        module: None,
        execution_mode: Default::default(),
        reply: None,
    }
}

/// Set by `--tool-first`: a seed that asks for the tool call before any answer, for models (such
/// as Haiku 5.5) that answer the arithmetic seed directly instead of calling the tool.
static TOOL_FIRST: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

fn seed_text() -> String {
    if TOOL_FIRST.get().copied().unwrap_or(false) {
        format!(
            "You cannot answer without the file. Think briefly about why, then call {TOOL} with path sample.txt. Do not write the answer until the tool result arrives."
        )
    } else {
        format!(
            "Before anything else, think step by step: which of 391, 437 and 493 is prime, and what is 17 * 23 minus that prime? Then write one short sentence with the result and call {TOOL} exactly once with path sample.txt. Do not finish until the tool result arrives."
        )
    }
}

fn seed_prompt() -> Vec<Message> {
    vec![
        Message::new(
            Role::System,
            vec![ContentBlock::Text {
                text: fixed_prefix().into(),
            }],
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: seed_text().into(),
            }],
        ),
    ]
}

fn with_tools(prompt: Vec<Message>, choice: ToolChoice) -> CallOptions {
    let mut opts = CallOptions::new(prompt);
    opts.tools = vec![file_tool()].into();
    opts.tool_choice = choice.into();
    opts
}

fn is_thinking(block: &ContentBlock) -> bool {
    matches!(
        block,
        ContentBlock::Reasoning { .. } | ContentBlock::RedactedReasoning { .. }
    )
}

fn signed_thinking_count(content: &[ContentBlock]) -> usize {
    content
        .iter()
        .filter(|block| {
            matches!(block, ContentBlock::Reasoning { signature: Some(s), .. } if !s.is_empty())
                || matches!(block, ContentBlock::RedactedReasoning { .. })
        })
        .count()
}

fn tool_results(content: &[ContentBlock]) -> Vec<ContentBlock> {
    content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(ContentBlock::ToolResult(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::Text {
                    text: "The sample file contains the number 42.".into(),
                },
                is_error: false,
            })),
            _ => None,
        })
        .collect()
}

/// The tool-loop request after the seed: its assistant message (thinking kept or removed) and
/// the tool result. The assistant message is the last one in the history, which is the case the
/// refusal report names.
fn tool_loop(seed: &Answer, strip_thinking: bool) -> CallOptions {
    let mut prompt = seed_prompt();
    let content: Vec<ContentBlock> = seed
        .content
        .iter()
        .filter(|block| !(strip_thinking && is_thinking(block)))
        .cloned()
        .collect();
    prompt.push(Message::assistant_from(
        content,
        ModelHandle::new("anthropic", model_id()),
    ));
    prompt.push(Message::new(Role::Tool, tool_results(&seed.content)));
    with_tools(prompt, ToolChoice::None)
}

fn cache_policy(
    session: &BindIdentity,
) -> Result<Option<broca_provider::wire::CachePolicy>, Box<dyn std::error::Error>> {
    Ok(broca_module_serve::resolve_cache_policy(
        WireFamilyId::AnthropicMessages,
        "anthropic",
        Some(&{
            let mut cache = cortexkit_role_llm_runner::cache::Cache::default();
            cache.ttl = Some(cortexkit_role_llm_runner::cache::Ttl::Short);
            cache
        }),
        &CacheTtlTable::shipped_defaults(),
        session,
    ))
}

fn deferred_config(
    persona: Option<&ClaudeCodePersona>,
    session: &BindIdentity,
) -> Result<FrozenRenderConfig, Box<dyn std::error::Error>> {
    let mut frozen = broca_provider::families::anthropic_frozen_config(model_id());
    frozen.cache = cache_policy(session)?;
    frozen.claude_code_persona = persona.cloned();
    // Freeze the same experimental settings for dry and live requests so catalog changes
    // cannot silently change the thinking mode between inspecting a body and sending it.
    frozen.variant = Some(broca_provider::variant::FrozenVariant {
        name: THINKING_ON_VARIANT.into(),
        request: broca_provider::variant::VariantRequest::Anthropic {
            thinking: Some(json!({"type": "adaptive", "display": "summarized"})),
            effort: Some("max".into()),
        },
    });
    Ok(frozen)
}

fn resolve_provider(
    token: &str,
    persona: &ClaudeCodePersona,
    session: &BindIdentity,
    variant: Option<&str>,
    frozen_variant: Option<&broca_provider::variant::FrozenVariant>,
) -> Result<broca_catalog::live::ProviderResolution, Box<dyn std::error::Error>> {
    let cache_policy = cache_policy(session)?.ok_or_else(|| {
        std::io::Error::other("Anthropic ephemeral marker cache policy is unavailable")
    })?;
    let options = ResolveOptions {
        auth_method: Some(AuthMethod::Oauth),
        cache_policy: Some(cache_policy),
        variant,
        claude_code_persona: Some(persona),
        ..ResolveOptions::default()
    };
    resolve_catalog_episode_with_client(
        &format!("anthropic/{}", model_id()),
        Some(2048),
        &AuthMode::Secret(token.to_owned()),
        &options,
        broca_provider::build_provider_http_client(),
        EpisodeRenderConfig {
            thinking_echo_scope: Some(ThinkingEchoScope::AllAssistantMessages),
            variant: frozen_variant,
            ..EpisodeRenderConfig::default()
        },
    )
    .map_err(|error| Box::new(std::io::Error::other(error.to_string())) as _)
}

fn rendered_thinking(provider: &dyn broca_core::provider::Provider, opts: &CallOptions) -> Value {
    let body: Value = serde_json::from_slice(&provider.render_request(opts))
        .expect("catalog-resolved renderer emits JSON");
    body["thinking"].clone()
}

fn message_shapes(provider: &dyn broca_core::provider::Provider, opts: &CallOptions) -> String {
    let body: Value = serde_json::from_slice(&provider.render_request(opts))
        .expect("catalog-resolved renderer emits JSON");
    body["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|message| {
            let blocks = message["content"]
                .as_array()
                .into_iter()
                .flatten()
                .map(|block| block["type"].as_str().unwrap_or("?").to_owned())
                .collect::<Vec<_>>()
                .join(",");
            format!("{}:[{blocks}]", message["role"].as_str().unwrap_or("?"))
        })
        .collect::<Vec<_>>()
        .join(";")
}

fn scrub(message: &str, token: &str) -> String {
    let without = if token.is_empty() {
        message.to_owned()
    } else {
        message.replace(token, "[redacted]")
    };
    without
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .take(400)
        .collect()
}

async fn send(
    step: u64,
    provider: &dyn broca_core::provider::Provider,
    token: &str,
    opts: &CallOptions,
) -> Result<Answer, Box<dyn std::error::Error>> {
    use broca_protocol::{NormalizedEvent, ProviderControl, StepId};

    let mut events = provider.step(StepId(step), opts).await;
    tokio::time::timeout(Duration::from_secs(180), async {
        let mut answer = Answer::default();
        let mut finished = false;
        while let Some(event) = events.recv().await {
            match event {
                NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                    answer.status = attempt.http_status;
                }
                NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                    answer.content = message.content;
                    answer.received_assistant_message = true;
                }
                NormalizedEvent::Control(ProviderControl::Finished { usage, .. }) => {
                    answer.input_tokens = usage.input_tokens.unwrap_or(0);
                    answer.cache_read = usage.cached_input_tokens.unwrap_or(0);
                    answer.cache_creation = usage.cache_write_tokens.unwrap_or(0);
                    answer.has_usage = true;
                    finished = true;
                }
                NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                    answer.status = error.status.or(answer.status);
                    answer.error = Some((
                        error
                            .provider_code
                            .unwrap_or_else(|| format!("{:?}", error.class)),
                        scrub(&error.message, token),
                    ));
                }
                _ => {}
            }
        }
        if !finished && answer.error.is_none() {
            answer.error = Some((
                "incomplete_response".into(),
                "stream ended without a terminal success".into(),
            ));
        }
        Ok::<_, Box<dyn std::error::Error>>(answer)
    })
    .await?
}

fn print_result(label: &str, answer: &Answer) {
    print!(
        "{label}: HTTP {} input={} cache_read={} reply_thinking_blocks={} reply_tool_use={}",
        answer
            .status
            .map_or_else(|| "none".into(), |status| status.to_string()),
        answer.input_tokens,
        answer.cache_read,
        signed_thinking_count(&answer.content),
        answer
            .content
            .iter()
            .filter(|block| matches!(block, ContentBlock::ToolCall(_)))
            .count()
    );
    if let Some((kind, message)) = &answer.error {
        print!(" error_type={kind} error_message={message:?}");
    }
    println!();
}

fn accepted(answer: &Answer) -> bool {
    answer.status == Some(200) && answer.error.is_none() && answer.received_assistant_message
}

fn dry_seed() -> Answer {
    Answer {
        content: vec![
            ContentBlock::Reasoning {
                text: "brief private thought".into(),
                signature: Some("dry-run-signature".into()),
            },
            ContentBlock::Text {
                text: "I will read the sample.".into(),
            },
            ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                provider_state: None,
                tool_call_id: ToolCallId("toolu_dry_run".into()),
                tool_name: TOOL.into(),
                input: json!({"path": "sample.txt"}).into(),
            }),
        ],
        received_assistant_message: true,
        ..Answer::default()
    }
}

fn first_sentence(message: &str) -> &str {
    for (index, ch) in message.char_indices() {
        if matches!(ch, '.' | '!' | '?') {
            let end = index + ch.len_utf8();
            if message[end..].starts_with(char::is_whitespace) || end == message.len() {
                return &message[..end];
            }
        }
    }
    message
}

fn deferred_row(case: DeferredCase, body: &[u8], answer: Option<&Answer>) -> Value {
    let usage = answer.filter(|answer| answer.has_usage);
    json!({
        "request_name": case.name(),
        "model": model_id(),
        "http_status": answer.and_then(|answer| answer.status),
        "error_first_sentence": answer.and_then(|answer| answer.error.as_ref())
            .map(|(_, message)| first_sentence(message)),
        "cache_read_input_tokens": usage.map(|answer| answer.cache_read),
        "cache_creation_input_tokens": usage.map(|answer| answer.cache_creation),
        "input_tokens": usage.map(|answer| answer.input_tokens),
        "request_body_sha256": format!("{:x}", Sha256::digest(body)),
    })
}

struct DeferredOutput {
    directory: PathBuf,
    rows: std::fs::File,
}

impl DeferredOutput {
    fn open(directory: PathBuf) -> Result<Self, Box<dyn std::error::Error>> {
        let in_cortexkit = |path: &std::path::Path| {
            path.components()
                .any(|component| component.as_os_str() == ".cortexkit")
        };
        let absolute = if directory.is_absolute() {
            directory.clone()
        } else {
            std::env::current_dir()?.join(&directory)
        };
        let existing = absolute
            .ancestors()
            .find(|ancestor| ancestor.exists())
            .ok_or("--out has no existing ancestor")?
            .canonicalize()?;
        if in_cortexkit(&absolute) || in_cortexkit(&existing) {
            return Err("--out must not be under any .cortexkit/ folder".into());
        }
        if !model_id()
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.'))
        {
            return Err("--out requires a model id usable as a filename".into());
        }
        std::fs::create_dir_all(&directory)?;
        let rows = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(directory.join("rows.jsonl"))?;
        Ok(Self { directory, rows })
    }

    fn body(&self, case: DeferredCase, body: &[u8]) -> std::io::Result<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(
                self.directory
                    .join(format!("{}-{}.json", model_id(), case.name())),
            )?;
        file.write_all(body)
    }

    fn row(&mut self, row: &Value) -> Result<(), Box<dyn std::error::Error>> {
        serde_json::to_writer(&mut self.rows, row)?;
        writeln!(self.rows)?;
        self.rows.flush()?;
        Ok(())
    }
}

async fn deferred_request(
    case: DeferredCase,
    frozen: &FrozenRenderConfig,
    token: &str,
    opts: &CallOptions,
    dry: bool,
    output: &mut Option<DeferredOutput>,
) -> Result<Answer, Box<dyn std::error::Error>> {
    use broca_core::Provider as _;

    let provider = broca_provider::LiveProvider::new(
        DeferredFamily(case),
        frozen.clone(),
        AuthHandle::Bearer(token.to_owned()),
    );
    let body = provider.render_request(opts);
    if let Some(output) = output {
        output.body(case, &body)?;
    }
    println!(
        "{} {}: {}",
        if dry { "dry-run" } else { "request" },
        case.name(),
        std::str::from_utf8(&body)?
    );
    let answer = if dry {
        dry_seed()
    } else {
        let step = match case {
            DeferredCase::Seed => 1,
            DeferredCase::ControlA => 2,
            DeferredCase::Probe => 3,
            DeferredCase::ControlB => 4,
        };
        let answer = send(step, &provider, token, opts).await?;
        print_result(case.name(), &answer);
        answer
    };
    let row = deferred_row(case, &body, (!dry).then_some(&answer));
    println!("{row}");
    if let Some(output) = output {
        output.row(&row)?;
    }
    Ok(answer)
}

async fn deferred_run(
    frozen: &FrozenRenderConfig,
    token: &str,
    dry: bool,
    mut output: Option<DeferredOutput>,
) -> Result<String, Box<dyn std::error::Error>> {
    let seed = deferred_request(
        DeferredCase::Seed,
        frozen,
        token,
        &with_tools(seed_prompt(), ToolChoice::Auto),
        dry,
        &mut output,
    )
    .await?;
    if !dry
        && (!accepted(&seed)
            || signed_thinking_count(&seed.content) == 0
            || tool_results(&seed.content).is_empty())
    {
        return Ok("INCONCLUSIVE: the seed did not return signed thinking with a tool_use; nothing else was sent.".into());
    }
    let opts = tool_loop(&seed, false);
    let mut answers = Vec::new();
    for case in [
        DeferredCase::ControlA,
        DeferredCase::Probe,
        DeferredCase::ControlB,
    ] {
        answers.push(deferred_request(case, frozen, token, &opts, dry, &mut output).await?);
    }
    if dry {
        return Ok(
            "DRY RUN: nothing sent; seed and three deferred-tool follow-ups printed.".into(),
        );
    }
    if !accepted(&answers[0]) || answers[2].status != Some(400) {
        return Ok(format!(
            "INCONCLUSIVE: expected control_a=200 and control_b=400; observed {:?} and {:?}.",
            answers[0].status, answers[2].status
        ));
    }
    Ok(format!(
        "model {}: unchanged tools accepted={}; deferred tool referenced in tool_result accepted={}; immediate tool control HTTP {:?}.",
        model_id(), accepted(&answers[0]), accepted(&answers[1]), answers[2].status
    ))
}

async fn load_auth() -> Result<(String, Option<String>), Box<dyn std::error::Error>> {
    let credential = CredentialId::parse(CREDENTIAL.get().map_or(CREDENTIAL_ID, String::as_str))
        .ok_or("--credential is not a credential id (method:provider[:account])")?;
    let handle = load_devtools_handle(&credential)?;
    let subc_path = std::env::var_os("BROCA_LIVE_SUBC")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                .join(".local/share/cortexkit/run/subc-connection.json")
        });
    let connection = broca_subc::SubcConnection::connect(&subc_path).await?;
    let identity = BindIdentity::new(
        std::env::current_dir()?,
        "anthropic-thinking-strip-probe",
        "anthropic-thinking-strip-probe",
    );
    let vault = broca_subc::VaultClient::new(
        Arc::new(connection),
        "claustrum",
        &identity,
        Duration::from_secs(15),
    );
    let secret = vault.get(&handle, None, false).await.result?;
    Ok((secret.token, secret.account_id))
}

async fn run() -> Result<String, Box<dyn std::error::Error>> {
    let usage = "usage: anthropic-thinking-strip-probe [--dry-run] [--model <id>] [--credential <dev-tools handle id>] [--tool-first] [--deferred-tool [--out <dir>]]";
    let mut dry = false;
    let mut deferred_tool = false;
    let mut output = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dry-run" if !dry => dry = true,
            "--deferred-tool" if !deferred_tool => deferred_tool = true,
            "--out" if output.is_none() => output = Some(PathBuf::from(args.next().ok_or(usage)?)),
            "--tool-first" if TOOL_FIRST.get().is_none() => {
                TOOL_FIRST.set(true).map_err(|_| usage)?;
            }
            "--credential" if CREDENTIAL.get().is_none() => {
                let id = args.next().ok_or(usage)?;
                CREDENTIAL.set(id).map_err(|_| usage)?;
            }
            "--model" if MODEL_ID.get().is_none() => {
                let id = args.next().ok_or(usage)?;
                MODEL_ID.set(id).map_err(|_| usage)?;
            }
            _ => return Err(usage.into()),
        }
    }
    if output.is_some() && !deferred_tool {
        return Err("--out requires --deferred-tool".into());
    }
    let output = output.map(DeferredOutput::open).transpose()?;
    let thinking_disabled = broca_provider::variant::FrozenVariant {
        name: "probe-thinking-disabled".into(),
        request: broca_provider::variant::VariantRequest::Anthropic {
            thinking: Some(json!({"type": "disabled"})),
            effort: None,
        },
    };

    let project_root = std::env::current_dir()?;
    let session = BindIdentity::new(
        project_root,
        "anthropic-thinking-strip-probe",
        format!("anthropic-thinking-strip-probe-{}", std::process::id()),
    );
    // The deferred-tool dry run needs neither a catalog nor the person's state root.
    let state_root = if dry && deferred_tool {
        None
    } else {
        let root = broca_module_serve::state_root::resolve_state_root()?;
        if !deferred_tool {
            broca_catalog::install_global_catalog_cache(Arc::new(
                broca_catalog::CatalogCache::open(root.join("catalog/fusiform-lkg.json")),
            ));
        }
        Some(root)
    };

    let (token, account_id) = if dry {
        ("dry-run-token-never-sent".to_owned(), None)
    } else {
        load_auth().await?
    };
    let install_id = if dry {
        "0".repeat(64)
    } else {
        claude_code_identity::load_or_create_install_id(
            state_root.as_ref().expect("live state root"),
        )?
    };
    let version = ClaudeCodePersona::CURRENT_VERSION;
    let suffix = broca_provider::families::anthropic_messages::cc_version_suffix_for_transcript(
        &seed_prompt(),
        version,
    );
    let persona = claude_code_identity::fresh_persona(
        &install_id,
        account_id.as_deref(),
        CREDENTIAL_ID,
        &session,
        suffix,
        1,
    )?;
    if deferred_tool {
        let frozen = deferred_config(Some(&persona), &session)?;
        return deferred_run(&frozen, &token, dry, output).await;
    }
    let thinking_on =
        resolve_provider(&token, &persona, &session, Some(THINKING_ON_VARIANT), None)?;
    let thinking_off =
        resolve_provider(&token, &persona, &session, None, Some(&thinking_disabled))?;
    let thinking_budget = resolve_provider(&token, &persona, &session, None, None)?;
    println!(
        "budget-mode request thinking={}",
        rendered_thinking(
            thinking_budget.provider.as_ref(),
            &with_tools(seed_prompt(), ToolChoice::Auto)
        )
    );

    let seed_opts = with_tools(seed_prompt(), ToolChoice::Auto);
    let on_thinking = rendered_thinking(thinking_on.provider.as_ref(), &seed_opts);
    let off_thinking = rendered_thinking(thinking_off.provider.as_ref(), &seed_opts);
    println!("thinking-on request thinking={on_thinking}");
    println!("thinking-off request thinking={off_thinking}");
    let off_is_off = off_thinking.is_null() || off_thinking["type"].as_str() == Some("disabled");

    if dry {
        let seed = dry_seed();
        for (label, provider, strip) in [
            ("control", &thinking_on, false),
            ("strip_thinking_on", &thinking_on, true),
            ("strip_thinking_off", &thinking_off, true),
        ] {
            let opts = tool_loop(&seed, strip);
            println!(
                "dry-run {label}: thinking={} messages=[{}]",
                rendered_thinking(provider.provider.as_ref(), &opts),
                message_shapes(provider.provider.as_ref(), &opts)
            );
        }
        return Ok(format!(
            "DRY RUN: nothing sent; thinking-off request {}.",
            if off_is_off {
                "renders thinking disabled"
            } else {
                "STILL RENDERS THINKING"
            }
        ));
    }
    if !off_is_off {
        return Ok(format!(
            "INCONCLUSIVE: the thinking-off provider still renders thinking ({off_thinking}). Nothing was sent."
        ));
    }

    let seed = send(1, thinking_on.provider.as_ref(), &token, &seed_opts).await?;
    print_result("seed", &seed);
    let has_tool_use = seed
        .content
        .iter()
        .any(|block| matches!(block, ContentBlock::ToolCall(_)));
    if !accepted(&seed) || !has_tool_use || signed_thinking_count(&seed.content) == 0 {
        return Ok(
            "INCONCLUSIVE: the seed did not return signed thinking with a tool_use; nothing else was sent."
                .into(),
        );
    }

    let control = send(
        2,
        thinking_on.provider.as_ref(),
        &token,
        &tool_loop(&seed, false),
    )
    .await?;
    print_result("control", &control);
    let strip_on = send(
        3,
        thinking_on.provider.as_ref(),
        &token,
        &tool_loop(&seed, true),
    )
    .await?;
    print_result("strip_thinking_on", &strip_on);
    let strip_off = send(
        4,
        thinking_off.provider.as_ref(),
        &token,
        &tool_loop(&seed, true),
    )
    .await?;
    print_result("strip_thinking_off", &strip_off);
    let strip_budget = send(
        5,
        thinking_budget.provider.as_ref(),
        &token,
        &tool_loop(&seed, true),
    )
    .await?;
    print_result("strip_thinking_budget", &strip_budget);

    Ok(format!(
        "model {}: control accepted={}; every thinking block removed: adaptive thinking accepted={}, thinking disabled accepted={}, budget thinking accepted={}.",
        model_id(),
        accepted(&control),
        accepted(&strip_on),
        accepted(&strip_off),
        accepted(&strip_budget)
    ))
}

#[tokio::main]
async fn main() {
    match run().await {
        Ok(verdict) => {
            println!("VERDICT: {verdict}");
            if verdict.starts_with("INCONCLUSIVE:") {
                std::process::exit(2);
            }
        }
        Err(error) => {
            eprintln!("probe failed: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frozen() -> FrozenRenderConfig {
        let session = BindIdentity::new(PathBuf::from("."), "probe-test", "probe-test");
        deferred_config(None, &session).unwrap()
    }

    fn rendered(case: DeferredCase, opts: &CallOptions) -> Value {
        let family = DeferredFamily(case);
        let frozen = frozen();
        let bytes = family.render(&frozen, opts);
        assert_eq!(bytes, family.render(&frozen, opts), "deterministic body");
        let request = family.endpoint(&frozen, &AuthHandle::None, bytes.clone());
        assert_eq!(request.body, bytes, "endpoint must send the recorded bytes");
        serde_json::from_slice(&bytes).unwrap()
    }

    fn assert_no_prefix_override(value: &Value) {
        match value {
            Value::Object(object) => {
                assert!(!object.contains_key("prefix_mismatch_behavior"));
                for value in object.values() {
                    assert_no_prefix_override(value);
                }
            }
            Value::Array(values) => {
                for value in values {
                    assert_no_prefix_override(value);
                }
            }
            _ => {}
        }
    }

    #[test]
    fn deferred_followups_differ_only_in_x_and_its_reference() {
        let seed = dry_seed();
        let opts = tool_loop(&seed, false);
        let baseline: Value =
            serde_json::from_slice(&AnthropicMessages.render(&frozen(), &opts)).unwrap();
        let seed_body = rendered(
            DeferredCase::Seed,
            &with_tools(seed_prompt(), ToolChoice::Auto),
        );
        let control = rendered(DeferredCase::ControlA, &opts);
        let mut probe = rendered(DeferredCase::Probe, &opts);
        let mut immediate = rendered(DeferredCase::ControlB, &opts);
        for body in [&seed_body, &control, &probe, &immediate] {
            assert_no_prefix_override(body);
            assert_eq!(
                body["tools"][0],
                json!({"type": "tool_search_tool_regex_20251119", "name": "tool_search_tool_regex"})
            );
        }
        assert_eq!(seed_body["tools"], control["tools"]);
        assert_eq!(control["tools"][1], baseline["tools"][0]);
        assert!(control["tools"]
            .as_array()
            .unwrap()
            .iter()
            .all(|tool| tool.get("cache_control").is_none()));
        assert_eq!(
            control["system"].as_array().unwrap().last().unwrap()["cache_control"],
            json!({"type": "ephemeral", "ttl": "5m"})
        );
        assert_eq!(control["system"], baseline["system"]);
        assert_eq!(control["messages"][1], baseline["messages"][1]);
        assert_eq!(control["messages"][1]["content"][0]["type"], "thinking");
        assert_eq!(
            control["messages"][1]["content"][0]["signature"],
            "dry-run-signature"
        );

        let deferred_x = probe["tools"].as_array_mut().unwrap().pop().unwrap();
        let immediate_x = immediate["tools"].as_array_mut().unwrap().pop().unwrap();
        assert_eq!(
            immediate_x,
            json!({
                "name": "X",
                "description": "Read an additional fixed sample.",
                "input_schema": {"type": "object", "properties": {}, "additionalProperties": false}
            })
        );
        let mut expected_deferred = immediate_x;
        expected_deferred["defer_loading"] = json!(true);
        assert_eq!(deferred_x, expected_deferred);
        assert!(deferred_x.get("cache_control").is_none());
        let result = &mut probe["messages"][2]["content"][0];
        assert_eq!(result["type"], "tool_result");
        assert_eq!(result["tool_use_id"], "toolu_dry_run");
        assert_eq!(
            result["content"],
            json!([{"type": "tool_reference", "tool_name": "X"}])
        );
        result["content"] = control["messages"][2]["content"][0]["content"].clone();
        assert_eq!(probe, control, "only X and the result's content may differ");
        assert_eq!(immediate, control, "only X may differ");

        let control_result = &control["messages"][2]["content"][0];
        let original_result = &baseline["messages"][2]["content"][0];
        assert_eq!(
            control_result["cache_control"],
            original_result["cache_control"]
        );
        assert_eq!(
            control_result["content"],
            json!([{"type": "text", "text": original_result["content"]}])
        );
    }

    #[test]
    fn deferred_rows_record_measurements_and_hash_exact_bytes() {
        let answer = Answer {
            status: Some(400),
            error: Some((
                "invalid_request_error".into(),
                "Prefix rejected. More detail.".into(),
            )),
            cache_read: 123,
            cache_creation: 456,
            input_tokens: 789,
            has_usage: true,
            ..Answer::default()
        };
        let row = deferred_row(DeferredCase::Probe, b"abc", Some(&answer));
        assert_eq!(row["request_name"], "probe");
        assert_eq!(row["model"], model_id());
        assert_eq!(row["http_status"], 400);
        assert_eq!(row["error_first_sentence"], "Prefix rejected.");
        assert_eq!(row["cache_read_input_tokens"], 123);
        assert_eq!(row["cache_creation_input_tokens"], 456);
        assert_eq!(row["input_tokens"], 789);
        assert_eq!(
            row["request_body_sha256"],
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        let dry = deferred_row(DeferredCase::Seed, b"abc", None);
        for key in [
            "http_status",
            "error_first_sentence",
            "cache_read_input_tokens",
            "cache_creation_input_tokens",
            "input_tokens",
        ] {
            assert!(dry[key].is_null(), "dry run must not invent {key}");
        }
    }

    #[test]
    fn deferred_persona_bodies_are_signed_after_patching() {
        let session = BindIdentity::new(PathBuf::from("."), "probe-test", "probe-test");
        let version = ClaudeCodePersona::CURRENT_VERSION;
        let suffix = broca_provider::families::anthropic_messages::cc_version_suffix_for_transcript(
            &seed_prompt(),
            version,
        );
        let persona = claude_code_identity::fresh_persona(
            &"0".repeat(64),
            None,
            CREDENTIAL_ID,
            &session,
            suffix,
            1,
        )
        .unwrap();
        let frozen = deferred_config(Some(&persona), &session).unwrap();
        for case in [
            DeferredCase::Seed,
            DeferredCase::ControlA,
            DeferredCase::Probe,
            DeferredCase::ControlB,
        ] {
            let opts = if case == DeferredCase::Seed {
                with_tools(seed_prompt(), ToolChoice::Auto)
            } else {
                tool_loop(&dry_seed(), false)
            };
            let body = String::from_utf8(DeferredFamily(case).render(&frozen, &opts)).unwrap();
            let parsed: Value = serde_json::from_str(&body).unwrap();
            assert!(parsed["system"][0]["text"]
                .as_str()
                .unwrap()
                .contains("cch="));
            assert_eq!(
                broca_provider::claude_code_persona::to_persona_json(&parsed),
                body
            );
            assert_eq!(broca_provider::claude_code_persona::sign_body(&body), body);
            assert_no_prefix_override(&parsed);
        }
    }

    #[test]
    fn deferred_output_refuses_cortexkit_and_preserves_exact_bodies() {
        assert!(DeferredOutput::open(PathBuf::from(".cortexkit/probe-output")).is_err());
        let directory = PathBuf::from(format!(
            "target/scratch/deferred-probe-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let forbidden = directory.join(".cortexkit/output");
        assert!(DeferredOutput::open(forbidden.clone()).is_err());
        assert!(!forbidden.exists());
        let mut output = DeferredOutput::open(directory.clone()).unwrap();
        let body = b"{\"exact\":true}";
        output.body(DeferredCase::Probe, body).unwrap();
        assert!(output.body(DeferredCase::Probe, b"overwrite").is_err());
        let row = deferred_row(DeferredCase::Probe, body, None);
        output.row(&row).unwrap();
        drop(output);
        assert_eq!(
            std::fs::read(directory.join(format!("{}-probe.json", model_id()))).unwrap(),
            body
        );
        let rows = std::fs::read_to_string(directory.join("rows.jsonl")).unwrap();
        assert_eq!(rows.lines().count(), 1);
        assert_eq!(serde_json::from_str::<Value>(&rows).unwrap(), row);
        assert!(DeferredOutput::open(directory.clone()).is_err());
        std::fs::remove_dir_all(directory).unwrap();
    }
}
