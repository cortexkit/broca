//! Live measurement of text-only tags around Anthropic signed thinking.
//!
//! Run `cargo run --locked -p broca-module-serve --example anthropic-text-tag-probe --
//! [--dry-run] [--seed-only]`. The live run reads the `oauth:anthropic:umutaday` dev-tools handle and makes at
//! most four requests to `claude-sonnet-5-5`: a seed tool call, its tagged tool-result follow-up,
//! a tagged assistant reply across a new user turn, and an untagged follow-up control. Requests
//! go through broca's production Anthropic renderer, parser and transport, including request
//! gzip. The active last-known-good catalog resolves the `high` effort variant to the model's
//! current thinking shape; no thinking JSON is hard-coded. `--dry-run` sends nothing and prints
//! each request's rendered shape without credentials.
//!
//! `--seed-only` sends just the first request and skips the tagged tool loop, new turn, and
//! untagged control, allowing the returned block shape to be checked before more usage is spent.
//! The probe keeps the parser's assistant content as received. It adds the fixed tag only to a
//! cloned text block when building later requests, leaving signed thinking and tool calls alone.
//! The large system prefix is identical in every request so cache reads measure the same prefix.

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

const CREDENTIAL_ID: &str = "oauth:anthropic:umutaday";
const MODEL: &str = "anthropic/claude-sonnet-5-5";
const MODEL_ID: &str = "claude-sonnet-5-5";
// Resolve this thinking-enabled effort choice through the model's catalog row.
const MODEL_VARIANT: &str = "high";
const TOOL: &str = "read_file";
const TAG: &str = "§7§ ";

#[derive(Default)]
struct Answer {
    content: Vec<ContentBlock>,
    input_tokens: u64,
    cache_read: u64,
    cache_write: u64,
    status: Option<u16>,
    error: Option<ProbeError>,
    received_assistant_message: bool,
    stop_reason: Option<String>,
}

struct ProbeError {
    kind: String,
    message: String,
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

fn seed_options() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::System,
            vec![ContentBlock::Text { text: fixed_prefix() }],
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: format!(
                    "First reason about which path to read. Complete that reasoning before calling {TOOL}; then write exactly one short visible text sentence and call {TOOL} exactly once with path sample.txt. Do not finish until the tool result arrives."
                ),
            }],
        ),
    ]);
    opts.tools = vec![file_tool()].into();
    opts.tool_choice = ToolChoice::Auto.into();
    opts
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
) -> Result<broca_catalog::live::ProviderResolution, Box<dyn std::error::Error>> {
    let cache_policy = cache_policy(session)?.ok_or_else(|| {
        std::io::Error::other("Anthropic ephemeral marker cache policy is unavailable")
    })?;
    let options = ResolveOptions {
        auth_method: Some(AuthMethod::Oauth),
        cache_policy: Some(cache_policy),
        variant: Some(MODEL_VARIANT),
        claude_code_persona: Some(persona),
        ..ResolveOptions::default()
    };
    let auth = AuthMode::Secret(token.to_owned());
    resolve_catalog_episode_with_client(
        MODEL,
        Some(2048),
        &auth,
        &options,
        broca_provider::build_provider_http_client(),
        EpisodeRenderConfig {
            thinking_echo_scope: Some(ThinkingEchoScope::AllAssistantMessages),
            ..EpisodeRenderConfig::default()
        },
    )
    .map_err(|error| Box::new(std::io::Error::other(error.to_string())) as _)
}

/// Change only a clone for rendering; the recorded parser output remains untouched.
fn assistant_message_for_request(content: &[ContentBlock], add_tag: bool) -> Message {
    let mut rendered = content.to_vec();
    if add_tag {
        if let Some(block) = rendered.iter_mut().find(|block| {
            matches!(
                block,
                ContentBlock::Text { .. } | ContentBlock::TextWithState { .. }
            )
        }) {
            match block {
                ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                    text.insert_str(0, TAG);
                }
                _ => unreachable!("the selected block is a text block"),
            }
        }
    }
    Message::assistant_from(rendered, ModelHandle::new("anthropic", MODEL_ID))
}

fn has_thinking(content: &[ContentBlock]) -> bool {
    content.iter().any(|block| {
        matches!(
            block,
            ContentBlock::Reasoning { .. } | ContentBlock::RedactedReasoning { .. }
        )
    })
}

fn has_signed_thinking(content: &[ContentBlock]) -> bool {
    content.iter().any(|block| {
        matches!(
            block,
            ContentBlock::Reasoning {
                signature: Some(signature),
                ..
            } if !signature.is_empty()
        )
    })
}

fn seed_missing_requirements(answer: &Answer) -> Vec<&'static str> {
    let mut missing = Vec::new();
    if answer.status != Some(200) || answer.error.is_some() {
        missing.push("successful HTTP response");
    }
    if !answer.received_assistant_message {
        missing.push("assistant message");
    }
    if !has_signed_thinking(&answer.content) {
        missing.push("signed thinking");
    }
    if !has_text(&answer.content) {
        missing.push("text block");
    }
    if tool_results(&answer.content).is_empty() {
        missing.push("tool_use");
    }
    missing
}

fn response_shape(content: &[ContentBlock]) -> String {
    let mut thinking_details = Vec::new();
    let mut redacted_thinking = 0;
    let mut text = 0;
    let mut tool_use = 0;
    for block in content {
        match block {
            ContentBlock::Media(_) => {}
            ContentBlock::Reasoning { text, signature } => thinking_details.push(format!(
                "signature_present={},signature_nonempty={},text_empty={}",
                signature.is_some(),
                signature
                    .as_ref()
                    .is_some_and(|signature| !signature.is_empty()),
                text.is_empty()
            )),
            ContentBlock::RedactedReasoning { .. } => redacted_thinking += 1,
            ContentBlock::Text { .. } | ContentBlock::TextWithState { .. } => text += 1,
            ContentBlock::ToolCall(_) => tool_use += 1,
            ContentBlock::ToolResult(_) => {}
        }
    }
    format!(
        "thinking={} thinking_details=[{}] redacted_thinking={redacted_thinking}[signature_present=not_exposed,text_empty=not_exposed] text={text} tool_use={tool_use}",
        thinking_details.len(),
        thinking_details.join(";")
    )
}

fn print_seed_shape(answer: &Answer) {
    println!(
        "seed_shape: {} stop_reason={}",
        response_shape(&answer.content),
        answer.stop_reason.as_deref().unwrap_or("none")
    );
}

fn has_text(content: &[ContentBlock]) -> bool {
    content.iter().any(|block| {
        matches!(
            block,
            ContentBlock::Text { .. } | ContentBlock::TextWithState { .. }
        )
    })
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

fn options_with_history(mut history: Vec<Message>) -> CallOptions {
    let mut opts = CallOptions::new(std::mem::take(&mut history));
    opts.tools = vec![file_tool()].into();
    opts.tool_choice = ToolChoice::None.into();
    opts
}

fn tagged_tool_loop(seed: &Answer, add_tag: bool) -> CallOptions {
    let mut prompt = seed_options().prompt;
    prompt.push(assistant_message_for_request(&seed.content, add_tag));
    prompt.push(Message::new(Role::Tool, tool_results(&seed.content)));
    options_with_history(prompt)
}

fn tagged_next_turn(seed: &Answer, followup: &Answer) -> CallOptions {
    let mut prompt = seed_options().prompt;
    prompt.push(assistant_message_for_request(&seed.content, true));
    prompt.push(Message::new(Role::Tool, tool_results(&seed.content)));
    prompt.push(assistant_message_for_request(&followup.content, true));
    prompt.push(Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "In one short sentence, summarize what the sample file contained.".into(),
        }],
    ));
    options_with_history(prompt)
}

fn sanitize_message(message: &str, token: &str) -> String {
    let without_token = if token.is_empty() {
        message.to_owned()
    } else {
        message.replace(token, "[redacted]")
    };
    without_token
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
                NormalizedEvent::Control(ProviderControl::StepFinished {
                    finish_reason, ..
                }) => {
                    answer.stop_reason = Some(format!("{finish_reason:?}"));
                }
                NormalizedEvent::Control(ProviderControl::Finished {
                    usage,
                    finish_reason,
                    ..
                }) => {
                    answer.input_tokens = usage.input_tokens.unwrap_or(0);
                    answer.cache_read = usage.cached_input_tokens.unwrap_or(0);
                    answer.cache_write = usage.cache_write_tokens.unwrap_or(0);
                    answer
                        .stop_reason
                        .get_or_insert_with(|| format!("{finish_reason:?}"));
                    finished = true;
                }
                NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                    answer.status = error.status.or(answer.status);
                    answer.error = Some(ProbeError {
                        kind: error
                            .provider_code
                            .unwrap_or_else(|| format!("{:?}", error.class)),
                        message: sanitize_message(&error.message, token),
                    });
                }
                _ => {}
            }
        }
        if !finished && answer.error.is_none() {
            answer.error = Some(ProbeError {
                kind: "incomplete_response".into(),
                message: "stream ended without a terminal success".into(),
            });
        }
        Ok::<_, Box<dyn std::error::Error>>(answer)
    })
    .await?
}

fn print_result(label: &str, answer: &Answer) {
    let thinking = has_thinking(&answer.content);
    print!(
        "{label}: HTTP {} input={} cache_read={} cache_write={} thinking={thinking}",
        answer
            .status
            .map_or_else(|| "none".into(), |status| status.to_string()),
        answer.input_tokens,
        answer.cache_read,
        answer.cache_write
    );
    if let Some(error) = &answer.error {
        print!(
            " error_type={} error_message={:?}",
            error.kind, error.message
        );
    }
    println!();
}

fn print_not_run(label: &str, reason: &str) {
    println!(
        "{label}: HTTP not-run input=0 cache_read=0 cache_write=0 thinking=false error_type=not_run error_message={reason:?}"
    );
}

fn summarize_blocks(value: &Value) -> String {
    value
        .as_array()
        .into_iter()
        .flatten()
        .map(|block| {
            let kind = block["type"].as_str().unwrap_or("unknown");
            if kind == "text" {
                let preview: String = block["text"]
                    .as_str()
                    .unwrap_or_default()
                    .chars()
                    .take(96)
                    .collect();
                format!("text({preview:?})")
            } else {
                kind.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn summarize_system(value: &Value) -> String {
    value
        .as_array()
        .into_iter()
        .flatten()
        .map(|block| {
            let kind = block["type"].as_str().unwrap_or("unknown");
            if kind == "text" {
                let chars = block["text"].as_str().unwrap_or_default().chars().count();
                format!("text(chars={chars})")
            } else {
                kind.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn count_cache_markers(value: &Value) -> usize {
    match value {
        Value::Array(items) => items.iter().map(count_cache_markers).sum(),
        Value::Object(fields) => {
            usize::from(fields.contains_key("cache_control"))
                + fields.values().map(count_cache_markers).sum::<usize>()
        }
        _ => 0,
    }
}

fn print_request_shape(
    label: &str,
    provider: &dyn broca_core::provider::Provider,
    opts: &CallOptions,
) {
    let body: Value = serde_json::from_slice(&provider.render_request(opts))
        .expect("catalog-resolved renderer emits JSON");
    let system = summarize_system(&body["system"]);
    let messages = body["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|message| {
            format!(
                "{}:[{}]",
                message["role"].as_str().unwrap_or("unknown"),
                summarize_blocks(&message["content"])
            )
        })
        .collect::<Vec<_>>()
        .join(";");
    println!(
        "dry-run {label}: model={MODEL} variant={MODEL_VARIANT} thinking={:?} output_config={:?} tool_choice={:?} cache_markers={} system=[{}] messages=[{}]",
        body["thinking"],
        body["output_config"],        body["tool_choice"],
        count_cache_markers(&body),
        system,
        messages
    );
}

fn dry_run(provider: &dyn broca_core::provider::Provider, seed_only: bool) {
    let seed = Answer {
        content: vec![
            ContentBlock::Reasoning {
                text: "brief private thought".into(),
                signature: Some("dry-run-signed-thinking".into()),
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
    let followup = Answer {
        content: vec![
            ContentBlock::Reasoning {
                text: "brief follow-up thought".into(),
                signature: Some("dry-run-followup-signature".into()),
            },
            ContentBlock::Text {
                text: "The sample file contains 42.".into(),
            },
        ],
        received_assistant_message: true,
        ..Answer::default()
    };
    print_request_shape("seed", provider, &seed_options());
    if seed_only {
        return;
    }
    print_request_shape("tagged_tool_loop", provider, &tagged_tool_loop(&seed, true));
    print_request_shape(
        "tagged_next_turn",
        provider,
        &tagged_next_turn(&seed, &followup),
    );
    print_request_shape(
        "untagged_control",
        provider,
        &tagged_tool_loop(&seed, false),
    );
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
        "anthropic-text-tag-probe",
        "anthropic-text-tag-probe",
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
    let mut dry = false;
    let mut seed_only = false;
    for arg in std::env::args().skip(1) {
        match arg.as_str() {
            "--dry-run" if !dry => dry = true,
            "--seed-only" if !seed_only => seed_only = true,
            _ => return Err("usage: anthropic-text-tag-probe [--dry-run] [--seed-only]".into()),
        }
    }

    let opts = seed_options();
    let project_root = std::env::current_dir()?;
    let session = BindIdentity::new(
        project_root,
        "anthropic-text-tag-probe",
        format!("anthropic-text-tag-probe-{}", std::process::id()),
    );
    let state_root = broca_module_serve::state_root::resolve_state_root()?;
    let catalog_cache = Arc::new(broca_catalog::CatalogCache::open(
        state_root.path.join("catalog/fusiform-lkg.json"),
    ));
    broca_catalog::install_global_catalog_cache(catalog_cache);

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
    assert_eq!(
        version, "2.1.302",
        "probe pins the Claude Code 2.1.302 persona"
    );
    let suffix = broca_provider::families::anthropic_messages::cc_version_suffix_for_transcript(
        &opts.prompt,
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
    let provider = resolve_provider(&token, &persona, &session)?;

    if dry {
        dry_run(provider.provider.as_ref(), seed_only);
        return Ok(if seed_only {
            "DRY RUN: no request sent; seed request shape printed without credentials.".into()
        } else {
            "DRY RUN: no request sent; request shapes printed without credentials.".into()
        });
    }

    let seed = send(1, provider.provider.as_ref(), &token, &opts).await?;
    print_result("seed", &seed);
    print_seed_shape(&seed);
    let missing = seed_missing_requirements(&seed);
    if seed_only {
        return Ok(if missing.is_empty() {
            "SEED_ONLY: seed returned signed thinking, text, and tool use; no other requests sent."
                .into()
        } else {
            format!(
                "INCONCLUSIVE: seed-only response missing {}; no other requests sent. {} stop_reason={}",
                missing.join(","),
                response_shape(&seed.content),
                seed.stop_reason.as_deref().unwrap_or("none")
            )
        });
    }
    if !missing.is_empty() {
        return Ok(format!(
            "INCONCLUSIVE: seed missing {}; no follow-up was sent. {} stop_reason={}",
            missing.join(","),
            response_shape(&seed.content),
            seed.stop_reason.as_deref().unwrap_or("none")
        ));
    }

    let tagged = send(
        2,
        provider.provider.as_ref(),
        &token,
        &tagged_tool_loop(&seed, true),
    )
    .await?;
    print_result("tagged_tool_loop", &tagged);

    let next_turn = if tagged.status == Some(200)
        && tagged.error.is_none()
        && tagged.received_assistant_message
        && has_text(&tagged.content)
    {
        let answer = send(
            3,
            provider.provider.as_ref(),
            &token,
            &tagged_next_turn(&seed, &tagged),
        )
        .await?;
        print_result("tagged_next_turn", &answer);
        Some(answer)
    } else {
        print_not_run(
            "tagged_next_turn",
            "the tagged tool-loop request did not return an assistant text block",
        );
        None
    };

    let control = send(
        4,
        provider.provider.as_ref(),
        &token,
        &tagged_tool_loop(&seed, false),
    )
    .await?;
    print_result("untagged_control", &control);

    let tagged_accepted = tagged.status == Some(200) && tagged.error.is_none();
    let next_accepted = next_turn
        .as_ref()
        .is_some_and(|answer| answer.status == Some(200) && answer.error.is_none());
    let prefix_reads = |answer: Option<&Answer>| match answer {
        Some(answer) if answer.cache_read > 0 => "yes",
        Some(_) => "no",
        None => "not-run",
    };
    Ok(format!(
        "tagged tool loop accepted={tagged_accepted}; tagged next turn accepted={next_accepted}; prefix cache reads: tagged tool loop={}, tagged next turn={}, untagged control={}; step-2 cache_read tagged/control={}/{}.",
        prefix_reads(Some(&tagged)),
        prefix_reads(next_turn.as_ref()),
        prefix_reads(Some(&control)),
        tagged.cache_read,
        control.cache_read
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
