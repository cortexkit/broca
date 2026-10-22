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

use std::{sync::Arc, time::Duration};

use broca_catalog::live::{
    resolve_catalog_episode_with_client, AuthMethod, AuthMode, EpisodeRenderConfig, ResolveOptions,
    ThinkingEchoScope,
};
use broca_module_serve::{
    claude_code_identity, vault_config::load_devtools_handle, CacheTtlTable, CredentialId,
};
use broca_protocol::{
    request::{CacheRequest, CacheRole, CallOptions, ModelHandle, Tool, ToolChoice},
    BindIdentity, ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult,
};
use broca_provider::{spec::WireFamilyId, wire::ClaudeCodePersona};
use serde_json::{json, Value};

const CREDENTIAL_ID: &str = "oauth:anthropic";
const DEFAULT_MODEL_ID: &str = "claude-opus-5-5";
/// The probed model's id without the provider prefix, set once from `--model` before any request.
static MODEL_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();

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
    status: Option<u16>,
    error: Option<(String, String)>,
    received_assistant_message: bool,
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
        }),
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
                text: fixed_prefix(),
            }],
        ),
        Message::new(Role::User, vec![ContentBlock::Text { text: seed_text() }]),
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
    broca_module_serve::resolve_cache_policy(
        WireFamilyId::AnthropicMessages,
        "anthropic",
        Some(&CacheRequest {
            role: Some(CacheRole::Ephemeral),
            ttl: None,
            key: None,
            tail: None,
            mode: None,
        }),
        &CacheTtlTable::shipped_defaults(),
        session,
    )
    .map_err(|error| Box::new(std::io::Error::other(error.to_string())) as _)
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

async fn load_auth() -> Result<(String, Option<String>), Box<dyn std::error::Error>> {
    let credential = CredentialId::parse(CREDENTIAL_ID).expect("canonical credential id");
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
    let usage = "usage: anthropic-thinking-strip-probe [--dry-run] [--model <id>] [--tool-first]";
    let mut dry = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dry-run" if !dry => dry = true,
            "--tool-first" if TOOL_FIRST.get().is_none() => {
                TOOL_FIRST.set(true).map_err(|_| usage)?;
            }
            "--model" if MODEL_ID.get().is_none() => {
                let id = args.next().ok_or(usage)?;
                MODEL_ID.set(id).map_err(|_| usage)?;
            }
            _ => return Err(usage.into()),
        }
    }
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
    let state_root = broca_module_serve::state_root::resolve_state_root()?;
    broca_catalog::install_global_catalog_cache(Arc::new(broca_catalog::CatalogCache::open(
        state_root.path.join("catalog/fusiform-lkg.json"),
    )));

    let (token, account_id) = if dry {
        ("dry-run-token-never-sent".to_owned(), None)
    } else {
        load_auth().await?
    };
    let install_id = if dry {
        "0".repeat(64)
    } else {
        claude_code_identity::load_or_create_install_id(&state_root.path)?
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
        let seed = Answer {
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
                    tool_call_id: ToolCallId("toolu_dry_run".into()),
                    tool_name: TOOL.into(),
                    input: json!({"path": "sample.txt"}),
                }),
            ],
            received_assistant_message: true,
            ..Answer::default()
        };
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
