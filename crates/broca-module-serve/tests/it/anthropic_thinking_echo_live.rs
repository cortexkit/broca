//! LIVE measurement of the Anthropic keep-all thinking echo against the prompt cache (off by
//! default).
//!
//! Under the family default (`AllAssistantMessages`) every assistant message keeps its signed
//! thinking, so the whole conversation prefix -- across user-turn boundaries too -- stays
//! byte-stable from one request to the next and the prompt cache written at one step is read at
//! the next. This test proves that end to end against the real provider.
//!
//! It drives ONE conversation with TWO user turns: user turn 1 followed by about four reasoning
//! tool steps, then a fresh user turn 2 followed by about four more. Every step thinks and calls
//! a trivial deterministic tool. It prints a per-step usage table and asserts:
//!   - Anthropic accepted every request (any rejected step fails with the provider's error);
//!   - `cache_read` does not fall at the first step after user turn 2 -- the earlier turn's
//!     prefix, thinking included, is still served from the cache across the boundary.
//!
//! The `oauth:anthropic` handle comes from `dev-tools-handles.json` under the config home
//! (override: `BROCA_DEVTOOLS_HANDLES`), then the token is fetched from the vault over subc.
//! Mint with `ck auth mint-handle --id oauth:anthropic` (operator only). The daemon's
//! `vault-handles.json` is not used. The token is never printed, logged or formatted.
//!
//! Run (spends real subscription usage, one conversation of up to eight steps):
//! `BROCA_LIVE_TESTS=1 cargo test -p broca-module-serve --test it anthropic_thinking_echo_live:: -- --nocapture`
//! Optional: `BROCA_LIVE_SUBC` (subc connection file, default the standard install's),
//! `BROCA_LIVE_ANTHROPIC_MODEL` (default `claude-opus-5`).

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use broca_catalog::live::{
    resolve_catalog_episode_with_client, AuthMethod, AuthMode, EpisodeRenderConfig, ResolveOptions,
    ThinkingEchoScope,
};
use broca_module_serve::vault_config::load_devtools_handle;
use broca_module_serve::{CacheTtlTable, CredentialId};
use broca_protocol::content::{
    AssistantMessage, ContentBlock, Message, Role, ToolOutput, ToolResult,
};
use broca_protocol::request::{
    CacheRequest, CacheRole, CallOptions, ModelHandle, Tool, ToolChoice,
};
use broca_protocol::{BindIdentity, NormalizedEvent, ProviderControl, StepId, Usage};
use broca_provider::spec::WireFamilyId;
use broca_subc::{SubcConnection, VaultClient};
use serde_json::{json, Value};

/// Tool steps driven after each of the two user turns -- about four apiece, enough to build a
/// multi-step prefix on each side of the user-turn boundary.
const PHASE1_STEPS: usize = 4;
const PHASE2_STEPS: usize = 4;
const CREDENTIALS_MODULE: &str = "claustrum";
const CREDENTIAL_ID: &str = "oauth:anthropic";

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
    // The vault scopes reads by handle; subc only needs a bind whose project root exists.
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

/// A system prompt well above the largest minimum cacheable prefix Anthropic documents
/// (4,096 tokens), so every step is eligible for caching. The padding is this project's own
/// data-model document: ordinary engineering prose. (Synthetic repeated rule text was tried first
/// and every request came back as a content-filter stop with no output.) The run label leads
/// the prompt so a retry never reads the first attempt's cache entries.
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

/// The user turn text: an instruction to call `tick` `count` times, one call per response.
fn user_turn(count: usize, first_n: u64) -> ContentBlock {
    ContentBlock::Text {
        text: format!(
            "Call the tick tool {count} times in a row, one call per response. The first call \
             uses n = {first_n}. Every result tells you how to compute the next n; work it out \
             carefully in your thinking before each call, and double-check the arithmetic. After \
             the result of the {count}th call, reply with the single word DONE."
        ),
    }
}

struct StepRow {
    step: usize,
    /// True for the first step of the SECOND user turn -- the boundary the keep-all scope has to
    /// carry the cache across.
    first_after_turn2: bool,
    thinking_blocks_sent: usize,
    replied_with_thinking: bool,
    usage: Usage,
}

impl StepRow {
    fn cache_read(&self) -> u64 {
        self.usage.cached_input_tokens.unwrap_or(0)
    }
    fn cache_write(&self) -> u64 {
        self.usage.cache_write_tokens.unwrap_or(0)
    }
    fn prompt_total(&self) -> u64 {
        self.usage.input_tokens.unwrap_or(0) + self.cache_read() + self.cache_write()
    }
}

enum Outcome {
    Ran(Vec<StepRow>),
    Rejected { step: usize, error: String },
}

/// Drive one step: render, stream, and return either the assembled reply + usage or a rejection.
async fn drive_step(
    provider: &dyn broca_core::provider::Provider,
    step: usize,
    opts: &CallOptions,
) -> Result<(AssistantMessage, Usage), String> {
    let mut rx = provider.step(StepId(step as u64), opts).await;
    let mut reply = None;
    let mut usage = None;
    let mut error = None;
    while let Some(event) = rx.recv().await {
        match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                reply = Some(message)
            }
            NormalizedEvent::Control(ProviderControl::StepFinished { usage: u, .. }) => {
                usage = Some(u)
            }
            NormalizedEvent::Control(ProviderControl::Finished { usage: u, .. }) => {
                usage.get_or_insert(u);
            }
            NormalizedEvent::Control(ProviderControl::Error { error: e, .. }) => {
                error = Some(format!(
                    "status={:?} class={:?} code={:?}: {}",
                    e.status, e.class, e.provider_code, e.message
                ))
            }
            _ => {}
        }
    }
    if let Some(error) = error {
        return Err(error);
    }
    let reply =
        reply.ok_or_else(|| "an accepted step assembled no assistant message".to_string())?;
    Ok((reply, usage.unwrap_or_default()))
}

/// Run the whole two-user-turn conversation once under the keep-all scope.
async fn run_conversation(token: &str, model: &str, run_label: &str) -> Outcome {
    let (provider_id, model_id) = model.split_once('/').expect("catalog id");
    let bind = BindIdentity::new(home().to_string_lossy().to_string(), "broca", run_label);
    // The production cache resolver, with the ephemeral (5 minute) role to keep writes cheap.
    let cache_policy = broca_module_serve::resolve_cache_policy(
        WireFamilyId::AnthropicMessages,
        provider_id,
        Some(&CacheRequest {
            role: Some(CacheRole::Ephemeral),
            ttl: None,
            key: None,
            tail: None,
            mode: None,
        }),
        &CacheTtlTable::shipped_defaults(),
        &bind,
    )
    .expect("anthropic has a marker cache row");
    let options = ResolveOptions {
        auth_method: Some(AuthMethod::Oauth),
        cache_policy,
        ..ResolveOptions::default()
    };
    let resolved = resolve_catalog_episode_with_client(
        model,
        Some(8192),
        &AuthMode::Secret(token.to_string()),
        &options,
        broca_provider::build_provider_http_client(),
        // The family default; named explicitly so the measurement pins the keep-all scope.
        EpisodeRenderConfig {
            thinking_echo_scope: Some(ThinkingEchoScope::AllAssistantMessages),
            ..EpisodeRenderConfig::default()
        },
    )
    .expect("anthropic resolves from the catalog");
    let provider = resolved.provider;
    let frozen = provider.render_config_blob();
    eprintln!(
        "[{run_label}] frozen reasoning = {}, cache = {}",
        frozen["reasoning"], frozen["cache"]
    );

    let handle = ModelHandle::new(provider_id, model_id);
    let mut prompt = vec![
        Message::new(
            Role::System,
            vec![ContentBlock::Text {
                text: system_prompt(run_label),
            }],
        ),
        Message::new(Role::User, vec![user_turn(PHASE1_STEPS, 1)]),
    ];

    // Each phase is (label, number of steps to attempt, whether it opens a fresh user turn).
    let phases = [(PHASE1_STEPS, false), (PHASE2_STEPS, true)];
    let mut rows = Vec::new();
    let mut step = 0usize;
    for (phase_index, (phase_steps, opens_turn)) in phases.into_iter().enumerate() {
        if opens_turn {
            // A real user message (text, not tool results) opens the second turn.
            prompt.push(Message::new(Role::User, vec![user_turn(PHASE2_STEPS, 100)]));
        }
        let mut steps_in_phase = 0;
        while steps_in_phase < phase_steps {
            step += 1;
            steps_in_phase += 1;
            let first_after_turn2 = phase_index == 1 && steps_in_phase == 1;

            let mut opts = CallOptions::new(prompt.clone());
            opts.tools = vec![tick_tool()].into();
            opts.tool_choice = ToolChoice::Auto.into();
            opts.target_model = Some(handle.clone());
            let rendered: Value =
                serde_json::from_slice(&provider.render_request(&opts)).expect("body is JSON");
            let thinking_blocks_sent = rendered["messages"]
                .as_array()
                .into_iter()
                .flatten()
                .flat_map(|message| message["content"].as_array().into_iter().flatten())
                .filter(|block| block["type"] == "thinking")
                .count();

            let (reply, usage) = match drive_step(provider.as_ref(), step, &opts).await {
                Ok(pair) => pair,
                Err(error) => return Outcome::Rejected { step, error },
            };
            let replied_with_thinking = reply.content.iter().any(|block| {
                matches!(
                    block,
                    ContentBlock::Reasoning {
                        signature: Some(_),
                        ..
                    }
                )
            });
            rows.push(StepRow {
                step,
                first_after_turn2,
                thinking_blocks_sent,
                replied_with_thinking,
                usage,
            });

            let calls: Vec<_> = reply.tool_calls().cloned().collect();
            prompt.push(Message::assistant_from(
                reply.content.clone(),
                handle.clone(),
            ));
            if calls.is_empty() {
                // The model stopped calling tools; the phase ends early. Model text is not
                // secret, but we only need the fact that it stopped.
                eprintln!("[{run_label}] step {step} produced no tool call; phase ended early");
                break;
            }
            let results = calls
                .iter()
                .map(|call| {
                    // A deterministic small puzzle per result, so each step has something to
                    // reason about and the model thinks before every call.
                    let n = call.input.get("n").and_then(Value::as_u64).unwrap_or(0);
                    let dividend = 1000 + (n * 7919 + step as u64 * 104_729) % 9000;
                    let divisor = 13 + step as u64;
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
    Outcome::Ran(rows)
}

fn print_table(label: &str, rows: &[StepRow]) {
    eprintln!("\n=== {label} ===");
    eprintln!(
        "| step | after turn2 | thinking sent | reply thought | input | cache_read | cache_write | prompt | hit rate | read grew |"
    );
    eprintln!("|---|---|---|---|---|---|---|---|---|---|");
    let mut previous_read: Option<u64> = None;
    for row in rows {
        let read = row.cache_read();
        let total = row.prompt_total();
        let hit = if total == 0 {
            0.0
        } else {
            read as f64 * 100.0 / total as f64
        };
        let grew = previous_read.map_or("-".to_string(), |prev| (read > prev).to_string());
        eprintln!(
            "| {} | {} | {} | {} | {} | {} | {} | {} | {:.1}% | {} |",
            row.step,
            row.first_after_turn2,
            row.thinking_blocks_sent,
            row.replied_with_thinking,
            row.usage.input_tokens.unwrap_or(0),
            read,
            row.cache_write(),
            total,
            hit,
            grew
        );
        previous_read = Some(read);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn keep_all_thinking_echo_cache_live() -> Result<(), String> {
    if !live_calls_opted_in() {
        eprintln!(
            "SKIP keep_all_thinking_echo_cache_live: set BROCA_LIVE_TESTS to spend real usage"
        );
        return Ok(());
    }
    let token = vault_token().await?;
    install_catalog();
    let model = format!(
        "anthropic/{}",
        std::env::var("BROCA_LIVE_ANTHROPIC_MODEL").unwrap_or_else(|_| "claude-opus-5".into())
    );
    let base = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis();

    // One run, plus a single retry on a transient rejection. Each attempt uses a fresh label so
    // a retry never reads the previous attempt's cache and skews the measurement.
    let mut rows = None;
    let mut last_rejection = None;
    for attempt in 0..2 {
        let label = format!("keepall-{base}-{attempt}");
        match run_conversation(&token, &model, &label).await {
            Outcome::Ran(r) => {
                rows = Some(r);
                break;
            }
            Outcome::Rejected { step, error } => {
                eprintln!("attempt {attempt}: REJECTED at step {step}: {error}");
                last_rejection = Some(format!("step {step}: {error}"));
            }
        }
    }

    let rows = rows.unwrap_or_else(|| {
        panic!(
            "Anthropic rejected the conversation on both attempts: {}",
            last_rejection.unwrap_or_default()
        )
    });
    print_table(&format!("{model} keep-all echo"), &rows);

    // The load-bearing claim: the first request of the SECOND user turn still reads the earlier
    // turn's prefix from the cache -- keep-all kept every assistant message's thinking, so the
    // prefix did not change and the cache written before the boundary is read after it.
    let boundary = rows.iter().position(|row| row.first_after_turn2);
    if let Some(boundary) = boundary {
        assert!(
            boundary > 0,
            "the second turn must follow at least one step"
        );
        let before = rows[boundary - 1].cache_read();
        let after = rows[boundary].cache_read();
        assert!(
            after >= before,
            "cache_read fell across the user-turn boundary ({before} -> {after}); the keep-all \
             prefix should still be cached after user turn 2"
        );
        assert!(
            after > 0,
            "cache_read was zero at the first step after user turn 2 ({after}); nothing was \
             served from the cache across the boundary"
        );
    }
    Ok(())
}
