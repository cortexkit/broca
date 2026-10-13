//! LIVE check of echoing signed thinking across an Anthropic-to-Anthropic model switch (off by
//! default).
//!
//! broca used to strip, on every request, the signed thinking of every assistant message that a
//! different model produced, because a model once rejected another model's signature. On a
//! session that switches models A -> B -> A that strip edits the conversation's history: under B
//! it removes A's thinking from the middle, and back on A it puts that thinking back. Anthropic's
//! preserved-thinking documentation treats both as invalid edits, and says instead to keep
//! passing every thinking block back on a model switch, because the API itself drops the blocks
//! the target model cannot read.
//!
//! This test drives ONE conversation with thinking on and a tool loop per leg, switching
//! `claude-opus-5-5` -> `claude-opus-5` -> `claude-opus-5-5` -> `claude-fable-5-1`, twice:
//!   - `strip`: the frozen config carries the original `SameModelOnly` rule, so the renderer
//!     strips every other model's signed thinking;
//!   - `keep`: the frozen config carries the current default, `AnyAnthropicModel`, so every
//!     earlier signed thinking block is sent back unchanged.
//!
//! Per request it records the HTTP status, the provider's error text on a rejection, the
//! `input_transformations` the thinking-binding beta reports, and the usage (input, cache read,
//! cache write). Both runs are printed side by side. The `keep` run must be accepted end to end:
//! a rejection there naming a signature is the evidence that a strip is still needed.
//!
//! The `oauth:anthropic` handle comes from `dev-tools-handles.json` under the config home
//! (override: `BROCA_DEVTOOLS_HANDLES`), then the token is fetched from the vault over subc.
//! Mint with `ck auth mint-handle --id oauth:anthropic` (operator only). The daemon's
//! `vault-handles.json` is not used. The token is never printed, logged or formatted.
//!
//! Run (spends real subscription usage, two conversations of up to twelve requests each):
//! `BROCA_LIVE_TESTS=1 cargo test -p broca-module-serve --test it anthropic_model_switch_live:: -- --nocapture`
//! Optional: `BROCA_LIVE_SUBC` (subc connection file, default the standard install's).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use broca_catalog::live::{
    resolve_catalog_episode_with_client, AuthMethod, AuthMode, EpisodeRenderConfig, ResolveOptions,
};
use broca_core::provider::Provider;
use broca_module_serve::vault_config::load_devtools_handle;
use broca_module_serve::{CacheTtlTable, CredentialId};
use broca_protocol::content::{
    AssistantMessage, ContentBlock, Message, Role, ToolOutput, ToolResult,
};
use broca_protocol::request::{
    CacheRequest, CacheRole, CallOptions, ModelHandle, Tool, ToolChoice,
};
use broca_protocol::{
    BindIdentity, InputTransformation, NormalizedEvent, ProviderControl, StepId, Usage,
};
use broca_provider::families::AnthropicMessages;
use broca_provider::policy::{CrossModelThinking, ReasoningPolicy};
use broca_provider::spec::WireFamilyId;
use broca_provider::wire::{AuthHandle, FrozenRenderConfig};
use broca_subc::{SubcConnection, VaultClient};
use serde_json::{json, Value};

const CREDENTIALS_MODULE: &str = "claustrum";
const CREDENTIAL_ID: &str = "oauth:anthropic";
/// The model of each leg, in order. Every leg opens with a fresh user turn and runs a short tool
/// loop, so each model both reads the others' thinking and leaves thinking of its own behind.
const LEGS: [&str; 4] = [
    "claude-opus-5-5",
    "claude-opus-5",
    "claude-opus-5-5",
    "claude-fable-5-1",
];
/// Tool calls asked for per leg. The leg ends after that many calls plus one closing reply, or
/// earlier if the model stops calling the tool.
const CALLS_PER_LEG: usize = 2;

fn live_calls_opted_in() -> bool {
    std::env::var("BROCA_LIVE_TESTS")
        .map(|value| !value.trim().is_empty())
        .unwrap_or(false)
}

fn home() -> PathBuf {
    PathBuf::from(std::env::var("HOME").expect("HOME is set"))
}

/// Fetch the Anthropic OAuth token with the operator dev-tools handle.
async fn vault_token() -> Result<String, String> {
    let credential = CredentialId::parse(CREDENTIAL_ID).expect("a canonical credential id");
    let handle = load_devtools_handle(&credential)?;
    let subc = std::env::var_os("BROCA_LIVE_SUBC")
        .map(PathBuf::from)
        .unwrap_or_else(|| home().join(".local/share/cortexkit/run/subc-connection.json"));
    let conn = SubcConnection::connect(&subc)
        .await
        .map_err(|error| format!("connect subc ({}): {error}", subc.display()))?;
    let identity = BindIdentity::new(home().to_string_lossy().to_string(), "broca", "vault");
    let client = VaultClient::new(
        Arc::new(conn),
        CREDENTIALS_MODULE,
        &identity,
        Duration::from_secs(15),
    );
    let token = client
        .get(&handle, Some(20 * 60 * 1000), false)
        .await
        .result
        .map_err(|error| format!("vault credential.get failed: {error}"))?
        .token;
    // Presence only, as a boolean: the value itself is never formatted anywhere.
    let present = !token.is_empty();
    assert!(present, "the vault returned an empty token");
    Ok(token)
}

fn install_catalog() {
    let catalog = broca_catalog::Catalog::from_json_str(
        include_str!("../../../broca-catalog/data/models.dev.json"),
        include_str!("../../../broca-catalog/data/overlay.json"),
    )
    .expect("vendored catalog snapshot parses");
    let cache = Arc::new(broca_catalog::CatalogCache::empty());
    cache.install_parsed_for_test(&catalog, 0, broca_catalog::CatalogSource::Fusiform);
    broca_catalog::install_global_catalog_cache(cache);
}

/// The production OAuth frozen config for `model`, with the production cache markers. A model
/// the vendored catalog snapshot does not list yet gets `claude-opus-5`'s resolved config with
/// only the wire model id changed: the same family, auth shape, thinking policy and cache policy.
fn frozen_config(token: &str, model: &str, bind: &BindIdentity) -> FrozenRenderConfig {
    let cache_policy = broca_module_serve::resolve_cache_policy(
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
        bind,
    )
    .expect("anthropic has a marker cache row");
    let options = ResolveOptions {
        auth_method: Some(AuthMethod::Oauth),
        cache_policy,
        ..ResolveOptions::default()
    };
    let resolve = |id: &str| {
        resolve_catalog_episode_with_client(
            &format!("anthropic/{id}"),
            Some(8192),
            &AuthMode::Secret(token.to_string()),
            &options,
            broca_provider::build_provider_http_client(),
            EpisodeRenderConfig::default(),
        )
        .map(|resolved| {
            serde_json::from_value::<FrozenRenderConfig>(resolved.provider.render_config_blob())
                .expect("the resolved provider's frozen config round-trips")
        })
    };
    match resolve(model) {
        Ok(frozen) => frozen,
        Err(_) => {
            eprintln!("[{model}] not in the vendored catalog; reusing claude-opus-5's config");
            let mut frozen = resolve("claude-opus-5").expect("claude-opus-5 resolves");
            frozen.wire_model_id = model.to_string();
            frozen
        }
    }
}

/// A system prompt well above the largest minimum cacheable prefix Anthropic documents, so every
/// request is eligible for caching. The run label leads it so the two runs never share a cache.
fn system_prompt(run_label: &str) -> String {
    format!(
        "Session {run_label}. You are assisting with the broca project. Its data model \
         follows for reference.\n\n{}",
        include_str!("../../../../docs/data-model.md")
    )
}

fn tick_tool() -> Tool {
    Tool {
        name: "tick".into(),
        description: "Record tick n and return how to compute the next n.".into(),
        input_schema: json!({
            "type": "object",
            "properties": { "n": { "type": "integer" } },
            "required": ["n"]
        }),
        module: None,
        execution_mode: Default::default(),
    }
}

fn user_turn(first_n: u64) -> Message {
    Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: format!(
                "Call the tick tool {CALLS_PER_LEG} times in a row, one call per response. The \
                 first call uses n = {first_n}. Every result tells you how to compute the next n; \
                 work it out carefully in your thinking before each call. After the result of \
                 the last call, reply with the single word DONE."
            ),
        }],
    )
}

/// One request of a run: what was sent and what came back.
struct Row {
    leg: usize,
    model: &'static str,
    /// Signed thinking blocks in the rendered request, and how many of them another model made.
    thinking_sent: usize,
    foreign_thinking_sent: usize,
    http_status: Option<u16>,
    error: Option<String>,
    transformations: Option<Vec<InputTransformation>>,
    usage: Usage,
}

struct Reply {
    message: Option<AssistantMessage>,
    usage: Usage,
    http_status: Option<u16>,
    transformations: Option<Vec<InputTransformation>>,
    error: Option<String>,
}

async fn drive_step(
    provider: &dyn broca_core::provider::Provider,
    step: u64,
    opts: &CallOptions,
) -> Reply {
    let mut rx = provider.step(StepId(step), opts).await;
    let mut reply = Reply {
        message: None,
        usage: Usage::default(),
        http_status: None,
        transformations: None,
        error: None,
    };
    let mut step_usage = None;
    while let Some(event) = rx.recv().await {
        match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                reply.message = Some(message)
            }
            NormalizedEvent::Control(ProviderControl::StepFinished { usage, .. }) => {
                step_usage = Some(usage)
            }
            NormalizedEvent::Control(ProviderControl::Finished { usage, .. }) => {
                step_usage.get_or_insert(usage);
            }
            NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                reply.http_status = attempt.http_status.or(reply.http_status);
                if attempt.input_transformations.is_some() {
                    reply.transformations = attempt.input_transformations;
                }
            }
            NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                reply.error = Some(format!(
                    "status={:?} class={:?} code={:?}: {}",
                    error.status, error.class, error.provider_code, error.message
                ))
            }
            _ => {}
        }
    }
    reply.usage = step_usage.unwrap_or_default();
    reply
}

/// Run the whole model-switching conversation once under the cross-model thinking rule `rule`.
async fn run_conversation(token: &str, run_label: &str, rule: CrossModelThinking) -> Vec<Row> {
    let bind = BindIdentity::new(home().to_string_lossy().to_string(), "broca", run_label);
    let mut prompt = vec![Message::new(
        Role::System,
        vec![ContentBlock::Text {
            text: system_prompt(run_label),
        }],
    )];
    let mut rows = Vec::new();
    let mut step = 0u64;
    for (leg, model) in LEGS.into_iter().enumerate() {
        let mut frozen = frozen_config(token, model, &bind);
        match &mut frozen.reasoning {
            ReasoningPolicy::AnthropicInterleaved { cross_model, .. } => *cross_model = rule,
            other => panic!("[{model}] thinking must be on, got {other:?}"),
        }
        let provider = broca_provider::LiveProvider::new(
            AnthropicMessages,
            frozen,
            AuthHandle::Bearer(token.to_string()),
        );
        let handle = ModelHandle::new("anthropic", model);
        prompt.push(user_turn(leg as u64 * 100 + 1));
        for _ in 0..=CALLS_PER_LEG {
            step += 1;
            let mut opts = CallOptions::new(prompt.clone());
            opts.tools = vec![tick_tool()].into();
            opts.tool_choice = ToolChoice::Auto.into();
            opts.target_model = Some(handle.clone());
            let rendered: Value =
                serde_json::from_slice(&provider.render_request(&opts)).expect("body is JSON");
            let (thinking_sent, foreign_thinking_sent) = count_thinking(&rendered, &prompt, model);

            let reply = drive_step(&provider, step, &opts).await;
            let rejected = reply.error.is_some() || reply.message.is_none();
            rows.push(Row {
                leg,
                model,
                thinking_sent,
                foreign_thinking_sent,
                http_status: reply.http_status,
                error: reply.error.clone(),
                transformations: reply.transformations.clone(),
                usage: reply.usage,
            });
            if rejected {
                // The conversation cannot continue past a rejected request.
                return rows;
            }
            let message = reply.message.expect("checked above");
            let calls: Vec<_> = message.tool_calls().cloned().collect();
            prompt.push(Message::assistant_from(
                message.content.clone(),
                handle.clone(),
            ));
            if calls.is_empty() {
                break;
            }
            let results = calls
                .iter()
                .map(|call| {
                    let n = call.input.get("n").and_then(Value::as_u64).unwrap_or(0);
                    let dividend = 1000 + (n * 7919 + step * 104_729) % 9000;
                    let divisor = 13 + step;
                    ContentBlock::ToolResult(ToolResult {
                        tool_call_id: call.tool_call_id.clone(),
                        output: ToolOutput::Text {
                            text: format!(
                                "tick {n}. The next n is the remainder of {dividend} divided by \
                                 {divisor}, plus the sum of the digits of {dividend}."
                            ),
                        },
                        is_error: false,
                    })
                })
                .collect();
            prompt.push(Message::new(Role::Tool, results));
        }
    }
    rows
}

/// Count the signed thinking blocks in a rendered body, and how many of them came from assistant
/// messages another model produced. Rendered assistant messages line up with the prompt's
/// assistant messages only while none was omitted, so the foreign count reads the signatures.
fn count_thinking(rendered: &Value, prompt: &[Message], model: &str) -> (usize, usize) {
    let foreign_signatures: Vec<&str> = prompt
        .iter()
        .filter(|m| m.role == Role::Assistant)
        .filter(|m| m.origin.as_ref().is_some_and(|o| o.model_id != model))
        .flat_map(|m| m.content.iter())
        .filter_map(|block| match block {
            ContentBlock::Reasoning {
                signature: Some(signature),
                ..
            } => Some(signature.as_str()),
            _ => None,
        })
        .collect();
    let sent: Vec<&str> = rendered["messages"]
        .as_array()
        .into_iter()
        .flatten()
        .flat_map(|message| message["content"].as_array().into_iter().flatten())
        .filter(|block| block["type"] == "thinking")
        .filter_map(|block| block["signature"].as_str())
        .collect();
    let foreign = sent
        .iter()
        .filter(|signature| foreign_signatures.contains(signature))
        .count();
    (sent.len(), foreign)
}

fn describe_transformations(entries: &Option<Vec<InputTransformation>>) -> String {
    match entries {
        None => "absent".into(),
        Some(entries) if entries.is_empty() => "[]".into(),
        Some(entries) => entries
            .iter()
            .map(|entry| {
                format!(
                    "{}@{} ({})",
                    entry.kind.as_deref().unwrap_or("?"),
                    entry.path.as_deref().unwrap_or("?"),
                    entry.reason.as_deref().unwrap_or("?")
                )
            })
            .collect::<Vec<_>>()
            .join("; "),
    }
}

fn print_table(label: &str, rows: &[Row]) {
    eprintln!("\n=== {label} ===");
    eprintln!(
        "| req | leg | model | thinking sent (foreign) | status | input | cache_read | cache_write | input_transformations | error |"
    );
    eprintln!("|---|---|---|---|---|---|---|---|---|---|");
    for (index, row) in rows.iter().enumerate() {
        eprintln!(
            "| {} | {} | {} | {} ({}) | {} | {} | {} | {} | {} | {} |",
            index + 1,
            row.leg + 1,
            row.model,
            row.thinking_sent,
            row.foreign_thinking_sent,
            row.http_status
                .map_or("-".to_string(), |status| status.to_string()),
            row.usage.input_tokens.unwrap_or(0),
            row.usage.cached_input_tokens.unwrap_or(0),
            row.usage.cache_write_tokens.unwrap_or(0),
            describe_transformations(&row.transformations),
            row.error.as_deref().unwrap_or("-"),
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn anthropic_model_switch_keeps_or_strips_foreign_thinking_live() -> Result<(), String> {
    if !live_calls_opted_in() {
        eprintln!("SKIP anthropic_model_switch_live: set BROCA_LIVE_TESTS to spend real usage");
        return Ok(());
    }
    let token = vault_token().await?;
    install_catalog();
    let base = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    let strip = run_conversation(
        &token,
        &format!("switch-strip-{base}"),
        CrossModelThinking::SameModelOnly,
    )
    .await;
    print_table("strip: foreign thinking removed per request", &strip);
    let keep = run_conversation(
        &token,
        &format!("switch-keep-{base}"),
        CrossModelThinking::AnyAnthropicModel,
    )
    .await;
    print_table("keep: every signed thinking block sent back", &keep);

    // The keep run must be accepted end to end, and it must actually have sent another model's
    // signed thinking, or it proves nothing about cross-model signatures.
    let rejected = keep.iter().find(|row| row.error.is_some());
    assert!(
        rejected.is_none(),
        "the keep run was rejected at {}: {}",
        rejected.map_or("", |row| row.model),
        rejected.and_then(|row| row.error.as_deref()).unwrap_or("")
    );
    assert!(
        keep.iter().any(|row| row.foreign_thinking_sent > 0),
        "the keep run never sent another model's signed thinking"
    );
    Ok(())
}
