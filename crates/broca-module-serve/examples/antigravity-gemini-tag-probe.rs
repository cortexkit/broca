//! Probe runner-tags/v2 beside Gemini thought signatures through production Code Assist.
//!
//! `cargo run --locked -p broca-module-serve --example antigravity-gemini-tag-probe --
//! [--dry-run|--live] [--model google/antigravity-gemini-3.8-flash] [--gap-secs N]`
//!
//! Dry run is the default: no credential reads or network. It resolves a captured Fusiform
//! catalog row and prints four synthetic request shapes, never signatures or full bodies.
//! Live mode uses the active last-known-good catalog and the `antigravity:google` dev-tools
//! vault credential, including its cloud project. Neither the handle nor its secret is printed.
//!
//! Requests are seed, untagged tool-loop control, tagged tool loop, and tagged next turn.
//! User text is tagged in both arms so the large system/user prefix is identical. Only replayed
//! assistant text and tool results differ between control and tagged loop. Numbering, cleaning,
//! and rendering use broca's computed tag rules on copies; parser output is never modified.
//! All thought signatures, tool calls, and reasoning remain unchanged. Cache comparison uses
//! the two tool-loop requests, not the longer next turn. Implicit cache reads are observational:
//! zero control cache reads leave the cache question inconclusive, not a failure of tagging.
//!
//! Exit codes: 0 accepted with comparable cache reads; 2 inconclusive; 3 tagged replay rejected;
//! 4 accepted but tagged cache reads below 80% of control; 1 probe/self-check failure.

use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use broca_catalog::live::{
    resolve_catalog_episode_with_client, AuthMethod, AuthMode, EpisodeRenderConfig,
    ProviderResolution, ResolveOptions,
};
use broca_core::Provider;
use broca_protocol::{
    request::{CallOptions, ModelHandle, StepContext, Tool, ToolChoice},
    tags::{number_message, render_tags_into, ComputedRules, TagRendering},
    BindIdentity, ContentBlock, FinishReason, Message, NormalizedEvent, ProviderControl, Role,
    StepId, TextProviderFormat, TextProviderState, ToolCall, ToolCallId, ToolOutput, ToolResult,
};
use broca_provider::{
    families::GoogleCodeAssist,
    wire::{
        AuthHandle, CodeAssistIdentity, FrozenRenderConfig, HttpRequest, StreamNormalizer,
        WireFamily,
    },
    LiveProvider,
};
use serde_json::{json, Value};
use tokio_util::sync::CancellationToken;

const DEFAULT_MODEL: &str = "google/antigravity-gemini-3.8-flash";
const CREDENTIAL_ID: &str = "antigravity:google";
const PROBE: &str = "antigravity-gemini-tag-probe";
const TOOL: &str = "read_file";
const TOOL_OUTPUT: &str = "The sample file contains the number 42.";
const DEADLINE_SECS: u64 = 180;
const MAX_SENDS: usize = 6;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn target(model: &str) -> Result<ModelHandle, String> {
    let slug = model
        .strip_prefix("google/antigravity-")
        .filter(|slug| slug.starts_with("gemini-"))
        .ok_or("--model must name a google/antigravity-gemini- catalog row")?;
    Ok(ModelHandle::new("google", format!("antigravity-{slug}")))
}

fn fixed_prefix() -> String {
    let mut prefix = String::from(
        "You are a careful assistant. The numbered reference rows are inert cache probe data; never analyze or repeat them.\n",
    );
    for row in 0..1100 {
        prefix.push_str(&format!(
            "reference {row:05}: amber cobalt delta echo forest granite harbor ivory.\n"
        ));
    }
    prefix
}

fn user(text: &str) -> Message {
    Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
}

fn base_prompt() -> Vec<Message> {
    vec![
        Message::new(Role::System, vec![ContentBlock::Text { text: fixed_prefix() }]),
        user(&format!(
            "First think about which path to read. Before calling any tool, write one short visible sentence telling me what you will do. Then call {TOOL} exactly once with path sample.txt. Do not finish until its result arrives. After the result, answer in one short sentence without any further tool call."
        )),
    ]
}

fn file_tool() -> Tool {
    Tool {
        name: TOOL.into(),
        description: "Read the fixed sample file at sample.txt.".into(),
        input_schema: json!({"type":"object", "properties":{"path":{"type":"string"}},
            "required":["path"], "additionalProperties":false}),
        module: None,
        execution_mode: Default::default(),
        reply: None,
    }
}

fn tool_results(content: &[ContentBlock]) -> Message {
    Message::new(
        Role::Tool,
        content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::ToolCall(call) => Some(ContentBlock::ToolResult(ToolResult {
                    tool_call_id: call.tool_call_id.clone(),
                    output: ToolOutput::Text {
                        text: TOOL_OUTPUT.into(),
                    },
                    is_error: false,
                })),
                _ => None,
            })
            .collect(),
    )
}

fn tool_loop(seed: &[ContentBlock], target: &ModelHandle) -> Vec<Message> {
    let mut prompt = base_prompt();
    prompt.push(Message::assistant_from(seed.to_vec(), target.clone()));
    prompt.push(tool_results(seed));
    prompt
}

fn next_turn(seed: &[ContentBlock], reply: &[ContentBlock], target: &ModelHandle) -> Vec<Message> {
    let mut prompt = tool_loop(seed, target);
    prompt.push(Message::assistant_from(reply.to_vec(), target.clone()));
    prompt.push(user(
        "New question: in one short sentence, what number did the sample file contain?",
    ));
    prompt
}

fn request(
    mut prompt: Vec<Message>,
    tagged: bool,
    target: &ModelHandle,
    scope: &str,
    step: u64,
) -> CallOptions {
    let mut high = 0;
    for message in &mut prompt {
        number_message(
            message,
            ComputedRules {
                render: true,
                clean: true,
            },
            &mut high,
        )
        .expect("the small probe transcript cannot exhaust tag numbers");
        // Keep the seed prefix identical; the control omits only replay tags, not user tags.
        if tagged || message.role == Role::User {
            render_tags_into(std::slice::from_mut(message), TagRendering::default());
        }
    }
    let mut opts = CallOptions::new(prompt);
    opts.ephemeral_session = true;
    opts.tools = vec![file_tool()].into();
    opts.tool_choice = if step == 1 {
        ToolChoice::Auto
    } else {
        ToolChoice::None
    }
    .into();
    opts.target_model = Some(target.clone());
    opts.step_context = Some(StepContext {
        run_id: scope.into(),
        step_index: step,
        session_address: None,
        credential_id: None,
        account_ref: None,
    });
    opts
}

fn sanitize(message: &str, secrets: &[&str]) -> String {
    let mut clean = message.to_owned();
    for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
        clean = clean.replace(secret, "[redacted]");
    }
    clean
        .chars()
        .map(|ch| if ch.is_control() { ' ' } else { ch })
        .take(600)
        .collect()
}

fn visible_text(content: &[ContentBlock]) -> String {
    content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                Some(text.as_str())
            }
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn response_shape(content: &[ContentBlock]) -> Value {
    let mut text = 0;
    let mut signed_text = 0;
    let mut calls = 0;
    let mut signed_calls = 0;
    let mut reasoning = 0;
    for block in content {
        match block {
            ContentBlock::Text { .. } => text += 1,
            ContentBlock::TextWithState { provider_state, .. } => {
                text += 1;
                if provider_state.format == TextProviderFormat::GoogleCodeAssist
                    && serde_json::from_str::<Value>(&provider_state.data)
                        .ok()
                        .and_then(|state| state["thoughtSignature"].as_str().map(str::to_owned))
                        .is_some_and(|signature| !signature.is_empty())
                {
                    signed_text += 1;
                }
            }
            ContentBlock::ToolCall(call) => {
                calls += 1;
                signed_calls += usize::from(
                    call.thought_signature
                        .as_ref()
                        .is_some_and(|s| !s.is_empty()),
                );
            }
            ContentBlock::Reasoning { .. } | ContentBlock::RedactedReasoning { .. } => {
                reasoning += 1
            }
            _ => {}
        }
    }
    json!({"text":text,"signed_text":signed_text,"tool_calls":calls,"signed_calls":signed_calls,"reasoning":reasoning})
}

fn describe_body(body: &Value, bytes: usize) -> Value {
    let inner = &body["request"];
    let messages: Vec<Value> = inner["contents"].as_array().into_iter().flatten().map(|message| {
        let parts: Vec<Value> = message["parts"].as_array().into_iter().flatten().map(|part| {
            let signature_bytes = part["thoughtSignature"].as_str().map_or(0, str::len);
            if let Some(text) = part["text"].as_str() {
                let thought = part["thought"] == true;
                json!({"kind":if thought {"thought"} else {"text"},"bytes":text.len(),
                    "tag_prefix":if thought {None} else {text.split_once(' ').map(|(head, _)| head).filter(|head| head.starts_with('§'))},
                    "signature_bytes":signature_bytes})
            } else if part.get("functionCall").is_some() {
                json!({"kind":"functionCall","name":part["functionCall"]["name"],"signature_bytes":signature_bytes})
            } else {
                let text = part["functionResponse"]["response"]["result"].as_str().unwrap_or_default();
                json!({"kind":"functionResponse","bytes":text.len(),"tag_prefix":text.split_once(' ').map(|(head,_)|head).filter(|head|head.starts_with('§'))})
            }
        }).collect();
        json!({"role":message["role"],"parts":parts})
    }).collect();
    json!({"model":body["model"],"body_bytes":bytes,"thinking":inner["generationConfig"]["thinkingConfig"],
        "tool_config":inner["toolConfig"],"system_bytes":inner["systemInstruction"].to_string().len(),
        "project":"withheld","identity":"withheld","messages":messages})
}

fn rendered(provider: &dyn Provider, opts: &CallOptions) -> Result<(Value, Value), String> {
    let bytes = provider.render_request(opts);
    let body = serde_json::from_slice(&bytes).map_err(|_| "renderer did not emit JSON")?;
    verify_signatures(opts, &body)?;
    let shape = describe_body(&body, bytes.len());
    Ok((body, shape))
}

/// Compare the ordered signed-part kinds and exact signature bytes, without exposing them.
/// A renderer that strips signed state must stop the probe, not produce a false acceptance.
fn verify_signatures(opts: &CallOptions, body: &Value) -> Result<(), String> {
    let expected: Vec<(&str, String)> = opts
        .prompt
        .iter()
        .filter(|m| m.role == Role::Assistant)
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::TextWithState { provider_state, .. } => {
                serde_json::from_str::<Value>(&provider_state.data)
                    .ok()
                    .and_then(|state| {
                        state["thoughtSignature"]
                            .as_str()
                            .map(|sig| ("text", sig.to_owned()))
                    })
            }
            ContentBlock::ToolCall(call) => call.thought_signature.clone().map(|sig| ("call", sig)),
            ContentBlock::Reasoning { signature, .. } => {
                signature.clone().map(|sig| ("thought", sig))
            }
            ContentBlock::RedactedReasoning {
                data,
                provider_format: None,
            } if !data.is_empty() => Some(("thought", data.clone())),
            _ => None,
        })
        .collect();
    let actual: Vec<(&str, String)> = body["request"]["contents"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|message| message["role"] == "model")
        .flat_map(|message| message["parts"].as_array().into_iter().flatten())
        .filter_map(|part| {
            part["thoughtSignature"].as_str().map(|sig| {
                (
                    if part["thought"] == true {
                        "thought"
                    } else if part.get("functionCall").is_some() {
                        "call"
                    } else {
                        "text"
                    },
                    sig.to_owned(),
                )
            })
        })
        .collect();
    if expected == actual {
        Ok(())
    } else {
        Err("rendered replay lost, moved, or changed a thought signature; no request sent".into())
    }
}

#[derive(Default)]
struct Answer {
    content: Vec<ContentBlock>,
    status: Option<u16>,
    error: Option<Value>,
    finished: bool,
    finish_reason: Option<FinishReason>,
    input_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
    attempts: usize,
}

impl Answer {
    fn accepted(&self) -> bool {
        self.status == Some(200)
            && self.finished
            && self.error.is_none()
            && matches!(
                self.finish_reason,
                Some(FinishReason::Stop | FinishReason::ToolCalls)
            )
    }
}

async fn send(
    provider: &dyn Provider,
    opts: &CallOptions,
    label: &str,
    cancel: &CancellationToken,
    secrets: &[&str],
) -> Result<Answer, String> {
    let (_, shape) = rendered(provider, opts)?;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(DEADLINE_SECS);
    let mut events = provider
        .step_with_cancel(
            StepId(opts.step_context.as_ref().unwrap().step_index),
            opts,
            cancel.clone(),
            Some(deadline),
        )
        .await;
    let mut answer = Answer::default();
    let drained = tokio::time::timeout_at(deadline + Duration::from_secs(10), async {
        while let Some(event) = events.recv().await {
            match event {
                NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                    answer.status = attempt.http_status.or(answer.status);
                    answer.attempts += 1;
                }
                NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                    answer.content = message.content
                }
                NormalizedEvent::Control(ProviderControl::Finished {
                    usage,
                    finish_reason,
                    ..
                }) => {
                    answer.finished = true;
                    answer.finish_reason = Some(finish_reason);
                    answer.input_tokens = usage.input_tokens;
                    answer.cached_input_tokens = usage.cached_input_tokens;
                }
                NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                    answer.status = error.status.or(answer.status);
                    answer.error = Some(json!({"class":error.class,
                        "provider_code":error.provider_code.map(|code|sanitize(&code,secrets)),
                        "message":sanitize(&error.message,secrets)}));
                }
                _ => {}
            }
        }
    })
    .await
    .is_ok();
    if !drained {
        cancel.cancel();
        provider.close_run_transport();
        answer.error =
            Some(json!({"class":"probe_deadline","message":"no terminal event before deadline"}));
    } else if !answer.finished && answer.error.is_none() {
        answer.error = Some(
            json!({"class":"incomplete_response","message":"stream ended without terminal success"}),
        );
    }
    println!(
        "{}",
        json!({"event":"request","label":label,"http_status":answer.status,"error":answer.error,
        "input_tokens":answer.input_tokens,"cached_input_tokens":answer.cached_input_tokens,
        "finish_reason":answer.finish_reason,"reply_text":sanitize(&visible_text(&answer.content),secrets),
        "reply_shape":response_shape(&answer.content),"attempt_records":answer.attempts,"request":shape})
    );
    Ok(answer)
}

fn print_not_run(label: &str, reason: &str) {
    println!(
        "{}",
        json!({"event":"request","label":label,"http_status":null,
        "input_tokens":null,"cached_input_tokens":null,"error":{"class":"not_run","message":reason}})
    );
}

/// The transport creates a normalizer before its cancellation-first attempt select. Cancel
/// before attempt seven, including retries, so four logical steps cannot spend 24 requests.
/// Shaping, endpoint construction, and normalization delegate to production Code Assist;
/// the wrapper adds only a shared send budget.
struct BudgetedCodeAssist {
    attempts: Arc<AtomicUsize>,
    cancel: CancellationToken,
}

impl WireFamily for BudgetedCodeAssist {
    fn shape(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
        GoogleCodeAssist.shape(frozen, opts)
    }

    fn endpoint(
        &self,
        frozen: &FrozenRenderConfig,
        auth: &AuthHandle,
        body: Vec<u8>,
    ) -> HttpRequest {
        GoogleCodeAssist.endpoint(frozen, auth, body)
    }

    fn normalizer(&self, frozen: &FrozenRenderConfig, step: StepId) -> Box<dyn StreamNormalizer> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) >= MAX_SENDS {
            self.cancel.cancel();
        }
        GoogleCodeAssist.normalizer(frozen, step)
    }
}

fn resolve(
    model: &str,
    token: &str,
    project: &str,
    scope: &str,
) -> Result<ProviderResolution, String> {
    let identity = CodeAssistIdentity::for_session(scope, 0, now_ms());
    let method = AuthMethod::Antigravity;
    let opts = ResolveOptions {
        auth_method: Some(method),
        family_override: broca_module_serve::family_for_method(method),
        project_id: Some(project),
        variant: Some("high"),
        ..ResolveOptions::default()
    };
    resolve_catalog_episode_with_client(
        model,
        Some(4096),
        &AuthMode::Secret(token.into()),
        &opts,
        broca_provider::build_provider_http_client(),
        EpisodeRenderConfig {
            codeassist_identity: Some(&identity),
            ..EpisodeRenderConfig::default()
        },
    )
    .map_err(|error| {
        format!(
            "catalog resolution failed: {}",
            sanitize(&error.to_string(), &[token, project])
        )
    })
}

fn install_dry_catalog() -> Result<(), String> {
    let response = serde_json::from_str(include_str!(
        "../../broca-catalog/tests/fixtures/routing-parity/fusiform-served-with-wire-facts.json"
    ))
    .map_err(|_| "invalid captured Fusiform catalog")?;
    let cache = Arc::new(broca_catalog::CatalogCache::empty());
    cache
        .refresh(response)
        .map_err(|_| "captured Fusiform catalog failed admission")?;
    broca_catalog::install_global_catalog_cache(cache);
    Ok(())
}

fn synthetic_text(text: &str, signature: &str) -> ContentBlock {
    ContentBlock::TextWithState {
        text: text.into(),
        provider_state: TextProviderState {
            format: TextProviderFormat::GoogleCodeAssist,
            data: json!({"thoughtSignature":signature}).to_string(),
        },
    }
}

fn synthetic_replies() -> (Vec<ContentBlock>, Vec<ContentBlock>) {
    (
        vec![
            ContentBlock::Reasoning {
                text: "Choose the sample path.".into(),
                signature: Some("dry-thought-signature".into()),
            },
            synthetic_text("I will read the sample.", "dry-seed-text-signature"),
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("dry-call".into()),
                tool_name: TOOL.into(),
                input: json!({"path":"sample.txt"}),
                thought_signature: Some("dry-call-signature".into()),
            }),
        ],
        vec![synthetic_text(
            "The file contains 42.",
            "dry-reply-text-signature",
        )],
    )
}

fn dry_plan(provider: &dyn Provider, target: &ModelHandle) -> Result<Vec<(Value, Value)>, String> {
    let (seed, reply) = synthetic_replies();
    [
        ("seed", base_prompt(), true),
        ("control_tool_loop", tool_loop(&seed, target), false),
        ("tagged_tool_loop", tool_loop(&seed, target), true),
        ("tagged_next_turn", next_turn(&seed, &reply, target), true),
    ]
    .into_iter()
    .enumerate()
    .map(|(index, (label, prompt, tagged))| {
        let (body, shape) = rendered(
            provider,
            &request(prompt, tagged, target, "dry-run", index as u64 + 1),
        )?;
        Ok((
            body,
            json!({"event":"planned_request","label":label,"request":shape}),
        ))
    })
    .collect()
}

fn dry_checks(plan: &[(Value, Value)]) -> Value {
    let control = &plan[1].0["request"];
    let tagged = &plan[2].0["request"];
    let next = &plan[3].0["request"];
    let mut restored = tagged.clone();
    // Production correlation labels advance for each send, independently of prompt content.
    restored["labels"]["last_step_index"] = control["labels"]["last_step_index"].clone();
    restored["contents"][1]["parts"][1]["text"] =
        control["contents"][1]["parts"][1]["text"].clone();
    restored["contents"][2]["parts"][0]["functionResponse"]["response"]["result"] =
        control["contents"][2]["parts"][0]["functionResponse"]["response"]["result"].clone();
    let signatures = |body: &Value| {
        body["contents"][1]["parts"][0]["thoughtSignature"] == "dry-thought-signature"
            && body["contents"][1]["parts"][1]["thoughtSignature"] == "dry-seed-text-signature"
            && body["contents"][1]["parts"][2]["thoughtSignature"] == "dry-call-signature"
    };
    json!({
        "thinking_on":plan[0].0["request"]["generationConfig"]["thinkingConfig"]["includeThoughts"] == true,
        "control_text_untagged":control["contents"][1]["parts"][1]["text"] == "I will read the sample."
            && control["contents"][2]["parts"][0]["functionResponse"]["response"]["result"] == TOOL_OUTPUT,
        "computed_tag_numbers":tagged["contents"][0]["parts"][0]["text"].as_str().is_some_and(|text|text.starts_with("§1§ "))
            && tagged["contents"][1]["parts"][1]["text"] == "§2§ I will read the sample."
            && tagged["contents"][2]["parts"][0]["functionResponse"]["response"]["result"] == format!("§3§ {TOOL_OUTPUT}")
            && next["contents"][3]["parts"][0]["text"] == "§4§ The file contains 42."
            && next["contents"][4]["parts"][0]["text"].as_str().is_some_and(|text|text.starts_with("§5§ ")),
        "signatures_unchanged":signatures(control) && signatures(tagged) && signatures(next)
            && next["contents"][3]["parts"][0]["thoughtSignature"] == "dry-reply-text-signature",
        "arms_differ_only_in_replay_text":restored == *control,
        "shared_prefix_stable":control["systemInstruction"] == next["systemInstruction"]
            && control["contents"][0] == tagged["contents"][0]
            && tagged["contents"].as_array().is_some_and(|parts|next["contents"].as_array().is_some_and(|next|next.starts_with(parts))),
    })
}

fn dry_run(model: &str) -> Result<i32, String> {
    let target = target(model)?;
    install_dry_catalog()?;
    let resolved = resolve(model, "dry-token-never-sent", "dry-project", "dry-run")?;
    let plan = dry_plan(resolved.provider.as_ref(), &target)?;
    for (_, described) in &plan {
        println!("{described}");
    }
    let checks = dry_checks(&plan);
    let passed = checks
        .as_object()
        .is_some_and(|checks| checks.values().all(|value| value == true));
    println!(
        "{}",
        json!({"event":"dry_run_finished","provider_calls":0,"credential_reads":0,
        "catalog":"captured Fusiform routing fixture; live uses the active LKG", "checks":checks,"all_checks_passed":passed})
    );
    println!("VERDICT: DRY RUN: no request sent; four request shapes printed; self-checks passed={passed}.");
    Ok(if passed { 0 } else { 1 })
}

async fn live(model: &str, gap: Duration) -> Result<i32, String> {
    use broca_module_serve::{vault_config::load_devtools_handle, CredentialId};
    use broca_subc::{SubcConnection, VaultClient};
    use std::path::PathBuf;

    let target = target(model)?;
    let scope = format!("{PROBE}-{}-{}", std::process::id(), now_ms());
    let bind = BindIdentity::new(
        std::env::current_dir().map_err(|_| "project directory unavailable")?,
        PROBE,
        scope.clone(),
    );
    let connection_file = std::env::var_os("BROCA_LIVE_SUBC")
        .or_else(|| std::env::var_os("SUBC_SOCKET"))
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                .join(".local/share/cortexkit/run/subc-connection.json")
        });
    let state_root = broca_module_serve::state_root::resolve_for_launch(&connection_file)
        .map_err(|_| "state root unavailable")?;
    broca_catalog::install_global_catalog_cache(Arc::new(broca_catalog::CatalogCache::open(
        state_root.path.join("catalog/fusiform-lkg.json"),
    )));
    let credential = CredentialId::parse(CREDENTIAL_ID).ok_or("invalid fixed credential id")?;
    let handle = load_devtools_handle(&credential)
        .map_err(|_| "the antigravity:google dev-tools credential is unavailable")?;
    let connection = SubcConnection::connect(&connection_file)
        .await
        .map_err(|_| "subc connection unavailable")?;
    let vault = VaultClient::new(
        Arc::new(connection),
        "claustrum",
        &bind,
        Duration::from_secs(15),
    );
    // No credential refresh occurs mid-probe; cover all six send slots at their full deadlines.
    let secret = vault
        .get(&handle, Some(25 * 60 * 1000), false)
        .await
        .result
        .map_err(|_| "vault credential unavailable")?;
    let project = secret
        .project_id
        .as_deref()
        .filter(|id| !id.is_empty())
        .ok_or("the Antigravity vault credential has no cloud project")?;
    let secrets = [secret.token.as_str(), project, handle.as_str()];
    let resolved = resolve(model, &secret.token, project, &scope)?;
    let frozen: FrozenRenderConfig = serde_json::from_value(resolved.provider.render_config_blob())
        .map_err(|_| "catalog render config unavailable")?;
    if resolved.family_id != broca_provider::spec::WireFamilyId::GoogleCodeAssist {
        return Err("the catalog did not resolve the production Code Assist family".into());
    }
    let attempts = Arc::new(AtomicUsize::new(0));
    let cancel = CancellationToken::new();
    let provider = LiveProvider::new(
        BudgetedCodeAssist {
            attempts: attempts.clone(),
            cancel: cancel.clone(),
        },
        frozen,
        AuthHandle::Bearer(secret.token.clone()),
    );
    println!(
        "{}",
        json!({"event":"probe_started","model":model,"wire_model":resolved.wire_model,"variant":"high",
        "max_requests":MAX_SENDS,"planned_steps":4,"per_request_deadline_secs":DEADLINE_SECS,"gap_secs":gap.as_secs(),
        "credential":"withheld","project":"withheld","headers":"withheld"})
    );

    let seed = send(
        &provider,
        &request(base_prompt(), true, &target, &scope, 1),
        "seed",
        &cancel,
        &secrets,
    )
    .await?;
    let shape = response_shape(&seed.content);
    if !seed.accepted()
        || visible_text(&seed.content).is_empty()
        || shape["tool_calls"] != 1
        || shape["signed_calls"] != 1
    {
        for label in ["control_tool_loop", "tagged_tool_loop", "tagged_next_turn"] {
            print_not_run(
                label,
                "seed needs successful text and exactly one signed tool call",
            );
        }
        println!("VERDICT: INCONCLUSIVE: seed needs successful text and exactly one signed tool call; no replay sent.");
        return Ok(2);
    }
    let control = send_replay(
        &provider,
        request(tool_loop(&seed.content, &target), false, &target, &scope, 2),
        "control_tool_loop",
        gap,
        &attempts,
        &cancel,
        &secrets,
    )
    .await?;
    let tagged = if control.as_ref().is_some_and(Answer::accepted) {
        send_replay(
            &provider,
            request(tool_loop(&seed.content, &target), true, &target, &scope, 3),
            "tagged_tool_loop",
            gap,
            &attempts,
            &cancel,
            &secrets,
        )
        .await?
    } else {
        print_not_run("tagged_tool_loop", "untagged control was not accepted");
        None
    };
    let next = if let Some(tagged) = tagged.as_ref().filter(|answer| {
        answer.accepted()
            && !visible_text(&answer.content).is_empty()
            && response_shape(&answer.content)["tool_calls"] == 0
    }) {
        send_replay(
            &provider,
            request(
                next_turn(&seed.content, &tagged.content, &target),
                true,
                &target,
                &scope,
                4,
            ),
            "tagged_next_turn",
            gap,
            &attempts,
            &cancel,
            &secrets,
        )
        .await?
    } else {
        print_not_run(
            "tagged_next_turn",
            "tagged loop needs a completed assistant text response without more tool calls",
        );
        None
    };
    provider.close_run_transport();
    let cache = |answer: &Option<Answer>| {
        answer
            .as_ref()
            .and_then(|answer| answer.cached_input_tokens)
    };
    let signed_text_seen = shape["signed_text"].as_u64().unwrap_or(0)
        + tagged
            .as_ref()
            .and_then(|a| response_shape(&a.content)["signed_text"].as_u64())
            .unwrap_or(0)
        > 0;
    println!(
        "{}",
        json!({"event":"probe_finished","send_slots_used":attempts.load(Ordering::SeqCst).min(MAX_SENDS),
        "budget_cancelled":cancel.is_cancelled(),"signed_call_replayed":true,"signed_text_replayed":signed_text_seen && next.is_some(),
        "tool_loop_cached_input_tokens":{"control":cache(&control),"tagged":cache(&tagged)},
        "next_turn_cached_input_tokens":cache(&next)})
    );
    if cancel.is_cancelled() || !control.as_ref().is_some_and(Answer::accepted) || tagged.is_none()
    {
        println!("VERDICT: INCONCLUSIVE: no accepted baseline, the send budget was exhausted, or the deadline expired.");
        return Ok(2);
    }
    if !tagged.as_ref().is_some_and(Answer::accepted)
        || next.as_ref().is_some_and(|a| !a.accepted())
    {
        println!("VERDICT: REJECTED: the untagged control succeeded but a tagged request did not; inspect HTTP status, finish reason, and refusal text above.");
        return Ok(3);
    }
    if next.is_none() || !signed_text_seen {
        println!("VERDICT: INCONCLUSIVE: tagged tool-loop accepted, but the next turn or signed-text replay coverage is missing.");
        return Ok(2);
    }
    let (control_cache, tagged_cache) = (cache(&control).unwrap_or(0), cache(&tagged).unwrap_or(0));
    if control_cache == 0 {
        println!("VERDICT: ACCEPTED; CACHE INCONCLUSIVE: both tagged requests succeeded, but control read no cached tokens.");
        return Ok(2);
    }
    let share = tagged_cache as f64 / control_cache as f64;
    if share < 0.8 {
        println!("VERDICT: CACHE DIVERGED: tagged replay accepted; tool-loop cache tagged/control={tagged_cache}/{control_cache} ({share:.2}), below 0.8.");
        Ok(4)
    } else {
        println!("VERDICT: ACCEPTED: signed call and text replay accepted with tags; tool-loop cache tagged/control={tagged_cache}/{control_cache} ({share:.2}).");
        Ok(0)
    }
}

async fn send_replay(
    provider: &dyn Provider,
    opts: CallOptions,
    label: &str,
    gap: Duration,
    attempts: &AtomicUsize,
    cancel: &CancellationToken,
    secrets: &[&str],
) -> Result<Option<Answer>, String> {
    if cancel.is_cancelled() || attempts.load(Ordering::SeqCst) >= MAX_SENDS {
        print_not_run(
            label,
            "six-send budget exhausted or a previous request timed out",
        );
        return Ok(None);
    }
    tokio::time::sleep(gap).await;
    send(provider, &opts, label, cancel, secrets)
        .await
        .map(Some)
}

const USAGE: &str = "antigravity-gemini-tag-probe [--dry-run|--live] [--model google/antigravity-gemini-MODEL] [--gap-secs 0..60]";

#[tokio::main]
async fn main() {
    let mut model = DEFAULT_MODEL.to_owned();
    let mut mode = None;
    let mut gap = 3;
    let mut args = std::env::args().skip(1);
    let parsed = (|| -> Result<(), String> {
        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--dry-run" | "--live" => {
                    let live = arg == "--live";
                    if mode.is_some_and(|chosen| chosen != live) {
                        return Err("conflicting execution modes".into());
                    }
                    mode = Some(live);
                }
                "--model" => model = args.next().ok_or("--model needs a catalog id")?,
                "--gap-secs" => {
                    gap = args
                        .next()
                        .and_then(|value| value.parse::<u64>().ok())
                        .filter(|gap| *gap <= 60)
                        .ok_or("--gap-secs must be in 0..60")?
                }
                "--help" | "-h" => {
                    println!("{USAGE}");
                    return Ok(());
                }
                _ => return Err("unknown argument".into()),
            }
        }
        let _ = target(&model)?;
        Ok(())
    })();
    if let Err(error) = parsed {
        eprintln!("probe_error={error}\nusage: {USAGE}");
        std::process::exit(1);
    }
    if std::env::args().any(|arg| arg == "--help" || arg == "-h") {
        return;
    }
    let result = if mode == Some(true) {
        live(&model, Duration::from_secs(gap)).await
    } else {
        dry_run(&model)
    };
    match result {
        Ok(code) => std::process::exit(code),
        Err(error) => {
            eprintln!("probe failed: {error}");
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planned_requests_use_computed_tags_and_preserve_signatures() {
        install_dry_catalog().unwrap();
        let resolved = resolve(DEFAULT_MODEL, "dry-token", "dry-project", "dry-run").unwrap();
        let plan = dry_plan(resolved.provider.as_ref(), &target(DEFAULT_MODEL).unwrap()).unwrap();
        let checks = dry_checks(&plan);
        for (name, value) in checks.as_object().unwrap() {
            assert_eq!(value, true, "{name}");
        }
        assert_eq!(
            resolved.family_id,
            broca_provider::spec::WireFamilyId::GoogleCodeAssist
        );
        assert_eq!(resolved.wire_model, "gemini-3.8-flash-high");
    }

    #[tokio::test]
    async fn six_send_budget_cancels_before_seventh_request() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let observed = requests.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                loop {
                    let mut chunk = [0; 4096];
                    let read = stream.read(&mut chunk).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&chunk[..read]);
                    if let Some(end) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
                        let length = String::from_utf8_lossy(&bytes[..end])
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap();
                        if bytes.len() >= end + 4 + length {
                            break;
                        }
                    }
                }
                observed.fetch_add(1, Ordering::SeqCst);
                let body = "data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"ok\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":1,\"candidatesTokenCount\":1}}}\n\n";
                let response = format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
                stream.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let attempts = Arc::new(AtomicUsize::new(0));
        let cancel = CancellationToken::new();
        let family = BudgetedCodeAssist {
            attempts: attempts.clone(),
            cancel: cancel.clone(),
        };
        let mut frozen =
            broca_provider::families::google_codeassist_frozen_config("gemini-3.8-flash-high");
        frozen.base_url = Some(format!("http://{address}"));
        frozen.project_id = Some("dry-project".into());
        let provider = LiveProvider::new(family, frozen, AuthHandle::None);
        let result = tokio::time::timeout(Duration::from_secs(10), async {
            let mut succeeded = Vec::new();
            for step in 1..=7 {
                let opts = request(
                    vec![user("hello")],
                    true,
                    &target(DEFAULT_MODEL).unwrap(),
                    "budget-test",
                    step,
                );
                let mut events = provider
                    .step_with_cancel(StepId(step), &opts, cancel.clone(), None)
                    .await;
                let mut finished = false;
                while let Some(event) = events.recv().await {
                    finished |= matches!(
                        event,
                        NormalizedEvent::Control(ProviderControl::Finished { .. })
                    );
                }
                succeeded.push(finished);
            }
            succeeded
        })
        .await;
        server.abort();
        assert_eq!(
            result.expect("budget test must finish within ten seconds"),
            vec![true, true, true, true, true, true, false]
        );
        assert!(cancel.is_cancelled());
        assert_eq!(
            requests.load(Ordering::SeqCst),
            6,
            "normalizer cancellation must precede the HTTP send"
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 7);
    }

    #[test]
    fn redacts_secrets_before_truncating_and_flattens_controls() {
        assert_eq!(
            sanitize("token\nproject\thandle", &["token", "project", "handle"]),
            "[redacted] [redacted] [redacted]"
        );
    }
}
