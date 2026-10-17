//! Live Claude OAuth persona check: `cargo run -p broca-module-serve --example claude-parity-probe
//! -- [--dry-run] <credential-id> [model]`.
//!
//! Sends one short two-step tool run on a Claude subscription credential and reports, per step,
//! the HTTP status, Anthropic's `request-id` and the prompt-cache read and write. Step one is
//! rendered in the OAuth shape runs had before the Claude Code persona (identity block only);
//! step two answers its tool call in the persona shape (billing block first, signed `cch`,
//! Claude Code headers and betas). Both share one large fixed system prefix, so step two's cache
//! read shows whether adding the billing block in front of that prefix still reads the cache
//! the old shape wrote.
//!
//! The credential is fetched from claustrum through the handle in broca's dev-tools handle file
//! and is never printed: request headers are listed without `authorization`, and an error
//! response is reduced to its `error.type` and `error.message`. `--dry-run` fetches no
//! credential and sends nothing; it prints the two requests as they would be sent.
use std::{sync::Arc, time::Duration};

use broca_module_serve::{claude_code_identity, vault_config::load_devtools_handle, CredentialId};
use broca_protocol::{
    request::{CallOptions, Tool, ToolChoice},
    BindIdentity, ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult,
};
use broca_provider::{
    families::{anthropic_frozen_config, AnthropicMessages},
    wire::{
        AuthHandle, CacheClass, CachePolicy, CacheTtl, ClaudeCodePersona, FrozenRenderConfig,
        HttpRequest, MarkerSyntax, WireFamily,
    },
};
use serde_json::{json, Value};

const TOOL: &str = "get_secret_number";

fn text_message(role: Role, text: String) -> Message {
    Message::new(role, vec![ContentBlock::Text { text }])
}

/// About 16k tokens of inert, fixed text: long enough to be cached, identical on both steps.
fn fixed_prefix() -> String {
    let mut prefix = String::from(
        "You are a careful assistant. The numbered reference rows below are inert data; never \
         analyze or repeat them.\n",
    );
    for row in 0..1100 {
        prefix.push_str(&format!(
            "reference {row:05}: amber cobalt delta echo forest granite harbor ivory.\n"
        ));
    }
    prefix
}

fn request_options() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        text_message(Role::System, fixed_prefix()),
        text_message(
            Role::User,
            format!("Call {TOOL} exactly once, then reply with only the number it returns."),
        ),
    ]);
    opts.tools = vec![Tool {
        name: TOOL.into(),
        description: "Return the secret number.".into(),
        input_schema: json!({"type": "object", "properties": {}}),
        module: None,
        execution_mode: Default::default(),
    }]
    .into();
    opts.tool_choice = ToolChoice::Auto.into();
    opts
}

fn frozen(model: &str, persona: Option<ClaudeCodePersona>) -> FrozenRenderConfig {
    let mut frozen = anthropic_frozen_config(model);
    frozen.max_output_tokens = 2048;
    // broca's default Anthropic cache policy: markers with the provider's 5-minute lifetime.
    frozen.cache = Some(CachePolicy {
        version: 1,
        class: CacheClass::Markers {
            syntax: MarkerSyntax::AnthropicCacheControl,
            ttl: CacheTtl::FiveMinutes,
            max_breakpoints: 4,
            lookback_blocks: 20,
            tail: None,
            omit_ttl: true,
        },
    });
    frozen.claude_code_persona = persona;
    frozen
}

/// The request without its credential, for printing.
fn describe(step: u32, shape: &str, request: &HttpRequest) {
    println!("step {step} ({shape}): POST {}", request.url);
    for (name, value) in &request.headers {
        if !name.eq_ignore_ascii_case("authorization") {
            println!("  {name}: {value}");
        }
    }
    let body: Value = serde_json::from_slice(&request.body).unwrap_or(Value::Null);
    println!("  body bytes: {}", request.body.len());
    println!(
        "  system[0]: {}",
        body["system"][0]["text"]
            .as_str()
            .map(|text| text.chars().take(160).collect::<String>())
            .unwrap_or_default()
    );
}

/// What one streamed response carried: the assistant content and the cache usage.
struct Answer {
    content: Vec<ContentBlock>,
    input_tokens: u64,
    cache_read: u64,
    cache_write: u64,
}

/// Rebuild the assistant message from the SSE stream (thinking with its signature, text and
/// tool calls) and read the usage from `message_start`.
fn read_stream(bytes: &[u8]) -> Answer {
    let mut blocks: Vec<Value> = Vec::new();
    let mut usage = Value::Null;
    for line in String::from_utf8_lossy(bytes).lines() {
        let Some(data) = line.strip_prefix("data: ") else {
            continue;
        };
        let Ok(event) = serde_json::from_str::<Value>(data) else {
            continue;
        };
        match event["type"].as_str() {
            Some("message_start") => usage = event["message"]["usage"].clone(),
            Some("content_block_start") => {
                let mut block = event["content_block"].clone();
                if block["type"] == "tool_use" {
                    block["partial_json"] = json!("");
                }
                blocks.push(block);
            }
            Some("content_block_delta") => {
                let Some(block) = blocks.last_mut() else {
                    continue;
                };
                let delta = &event["delta"];
                let append = |block: &mut Value, key: &str, piece: &Value| {
                    let joined = format!(
                        "{}{}",
                        block[key].as_str().unwrap_or_default(),
                        piece.as_str().unwrap_or_default()
                    );
                    block[key] = json!(joined);
                };
                match delta["type"].as_str() {
                    Some("thinking_delta") => append(block, "thinking", &delta["thinking"]),
                    Some("signature_delta") => append(block, "signature", &delta["signature"]),
                    Some("text_delta") => append(block, "text", &delta["text"]),
                    Some("input_json_delta") => {
                        append(block, "partial_json", &delta["partial_json"])
                    }
                    _ => {}
                }
            }
            _ => {}
        }
    }
    let content = blocks
        .into_iter()
        .filter_map(|block| match block["type"].as_str()? {
            "thinking" => Some(ContentBlock::Reasoning {
                text: block["thinking"].as_str().unwrap_or_default().to_string(),
                signature: block["signature"].as_str().map(str::to_string),
            }),
            "text" => Some(ContentBlock::Text {
                text: block["text"].as_str().unwrap_or_default().to_string(),
            }),
            "tool_use" => Some(ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId(block["id"].as_str()?.to_string()),
                tool_name: block["name"].as_str()?.to_string(),
                input: serde_json::from_str(block["partial_json"].as_str().unwrap_or("{}"))
                    .unwrap_or_else(|_| json!({})),
            })),
            _ => None,
        })
        .collect();
    let read = |key: &str| usage[key].as_u64().unwrap_or(0);
    Answer {
        content,
        input_tokens: read("input_tokens"),
        cache_read: read("cache_read_input_tokens"),
        cache_write: read("cache_creation_input_tokens"),
    }
}

async fn send(
    client: &broca_provider::ProviderHttpClient,
    step: u32,
    shape: &str,
    request: HttpRequest,
) -> Result<Answer, Box<dyn std::error::Error>> {
    let mut builder = client.post(&request.url).timeout(Duration::from_secs(180));
    for (name, value) in &request.headers {
        builder = builder.header(name, value);
    }
    let response = builder.body(request.body).send().await?;
    let status = response.status();
    let request_id = response
        .headers()
        .get("request-id")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("-")
        .to_string();
    let bytes = response.bytes().await?;
    if !status.is_success() {
        let error: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        println!(
            "step {step} ({shape}): status={status} request_id={request_id} error_type={} message={}",
            error["error"]["type"], error["error"]["message"]
        );
        return Err(format!("step {step} failed with HTTP {status}").into());
    }
    let answer = read_stream(&bytes);
    println!(
        "step {step} ({shape}): status={status} request_id={request_id} input_tokens={} \
         cache_read={} cache_write={}",
        answer.input_tokens, answer.cache_read, answer.cache_write
    );
    Ok(answer)
}

/// The tool result answering every tool call in `content`.
fn tool_results(content: &[ContentBlock]) -> Message {
    let results = content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(ContentBlock::ToolResult(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::Text { text: "42".into() },
                is_error: false,
            })),
            _ => None,
        })
        .collect();
    Message::new(Role::Tool, results)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let dry_run = args.first().is_some_and(|arg| arg == "--dry-run");
    if dry_run {
        args.remove(0);
    }
    let credential = args
        .first()
        .cloned()
        .ok_or("usage: claude-parity-probe [--dry-run] <credential-id> [model]")?;
    let model = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "claude-sonnet-5-5".into());
    let id = CredentialId::parse(&credential).ok_or("invalid credential id")?;
    let session = BindIdentity::new(
        std::env::current_dir()?,
        "claude-parity-probe",
        format!("claude-parity-probe-{}", std::process::id()),
    );

    let (auth, account_id) = if dry_run {
        (AuthHandle::Bearer("dry-run-token".into()), None)
    } else {
        let handle = load_devtools_handle(&id)?;
        let subc = std::env::var_os("SUBC_SOCKET")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::path::PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                    .join(".local/share/cortexkit/run/subc-connection.json")
            });
        let conn = broca_subc::SubcConnection::connect(&subc).await?;
        let identity = BindIdentity::new(
            std::env::current_dir()?,
            "claude-parity-probe",
            "claude-parity-probe",
        );
        let vault = broca_subc::VaultClient::new(
            Arc::new(conn),
            "claustrum",
            &identity,
            Duration::from_secs(10),
        );
        let secret = vault
            .get(&handle, None, false)
            .await
            .result
            .map_err(|error| format!("vault credential.get: {error}"))?;
        (AuthHandle::Bearer(secret.token), secret.account_id)
    };

    // The persona as broca's daemon would freeze it for this credential: the device id from
    // this installation's id, the session id from this probe's session.
    let state_root = broca_module_serve::state_root::resolve_state_root()?;
    let install_id = if dry_run {
        "0".repeat(64)
    } else {
        claude_code_identity::load_or_create_install_id(&state_root.path)?
    };
    let persona = claude_code_identity::fresh_persona(
        &install_id,
        account_id.as_deref(),
        &credential,
        &session,
        claude_code_identity::first_durable_cc_version_suffix(&[], &request_options().prompt),
    )?;
    println!(
        "credential={credential} model={model} account_uuid={} dry_run={dry_run}",
        if persona.account_uuid.is_some() {
            "known"
        } else {
            "unknown"
        }
    );

    let old_shape = frozen(&model, None);
    let new_shape = frozen(&model, Some(persona));
    let mut opts = request_options();
    let first = AnthropicMessages.endpoint(
        &old_shape,
        &auth,
        AnthropicMessages.render(&old_shape, &opts),
    );
    let client = broca_provider::build_provider_http_client();

    let answer = if dry_run {
        describe(1, "pre-persona", &first);
        Answer {
            content: vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("toolu_dry_run".into()),
                tool_name: TOOL.into(),
                input: json!({}),
            })],
            input_tokens: 0,
            cache_read: 0,
            cache_write: 0,
        }
    } else {
        send(&client, 1, "pre-persona", first).await?
    };
    if !answer
        .content
        .iter()
        .any(|block| matches!(block, ContentBlock::ToolCall(_)))
    {
        return Err("step 1 did not call the tool, so there is no second step".into());
    }
    let results = tool_results(&answer.content);
    opts.prompt
        .push(Message::new(Role::Assistant, answer.content));
    opts.prompt.push(results);
    let second = AnthropicMessages.endpoint(
        &new_shape,
        &auth,
        AnthropicMessages.render(&new_shape, &opts),
    );
    if dry_run {
        describe(2, "persona", &second);
    } else {
        send(&client, 2, "persona", second).await?;
    }
    Ok(())
}
