//! Append-only rendering across a whole multi-episode session.
//!
//! Anthropic binds every signed thinking block to everything sent before it, and every
//! provider's prompt cache misses on the same edits, so each request must extend the previous one
//! without changing anything already sent: top-level `system` and `tools` byte-identical, and
//! every earlier message byte-identical at the same position. Moving a `cache_control` marker is
//! the one allowed difference, so markers are removed before comparing.
//!
//! These tests run the REAL run loop (repetition guidance, budget messages, replay of earlier
//! episodes, system-prompt de-duplication) and render each request it sends with the Anthropic
//! renderer (thinking echo, cross-model strip, tool id and name aliasing, empty-turn omission,
//! cache markers), then compare consecutive requests.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::{
    provider::Provider, run_loop, run_loop_append_episode, AllowAll, FileLeaseStore, FileWalStore,
    RunRequest, StubReply, StubToolPlane,
};
use broca_protocol::{
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall},
    provider::{NormalizedEvent, ProviderControl},
    request::{
        BudgetConfig, BudgetNudge, CallOptions, ExecutionMode, ModelHandle, StopCondition, Tool,
        ToolChoice,
    },
    FinishReason, GenerationConfig, MessageId, RunId, StepId, ToolCallId, Usage,
};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use super::build_request_body;
use crate::families::anthropic_frozen_config;
use crate::wire::{CacheClass, CachePolicy, CacheTtl, FrozenRenderConfig, MarkerSyntax};

/// Serves scripted steps in call order across episodes and keeps every request it was sent.
#[derive(Clone)]
struct SequenceProvider {
    scripts: Arc<Mutex<VecDeque<Vec<NormalizedEvent>>>>,
    sent: Arc<Mutex<Vec<CallOptions>>>,
}

impl SequenceProvider {
    fn new(scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            scripts: Arc::new(Mutex::new(scripts.into())),
            sent: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl Provider for SequenceProvider {
    async fn step(&self, _step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.sent.lock().unwrap().push(opts.clone());
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for event in script {
                let _ = tx.send(event).await;
            }
        });
        rx
    }
}

fn thinking(step: &str) -> ContentBlock {
    ContentBlock::Reasoning {
        text: format!("thinking for {step}"),
        signature: Some(format!("sig-{step}")),
    }
}

fn scripted(
    id: &str,
    content: Vec<ContentBlock>,
    finish_reason: FinishReason,
) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content,
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason,
            retries_used: 0,
        }),
    ]
}

/// A step that reasons and calls the failing tool. The call id carries a `|`, which Anthropic's
/// id grammar rejects, so the renderer must alias it -- identically on every later request.
fn tool_step(id: &str, input: Value) -> Vec<NormalizedEvent> {
    scripted(
        id,
        vec![
            thinking(id),
            ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId(format!("call|{id}")),
                tool_name: TOOL.into(),
                input,
            }),
        ],
        FinishReason::ToolCalls,
    )
}

fn text_step(id: &str, text: &str) -> Vec<NormalizedEvent> {
    scripted(
        id,
        vec![thinking(id), ContentBlock::Text { text: text.into() }],
        FinishReason::Stop,
    )
}

/// A terminal turn carrying ONLY signed thinking. If any later request strips that thinking the
/// renderer omits the emptied turn, which deletes an earlier assistant message.
fn thinking_only_step(id: &str) -> Vec<NormalizedEvent> {
    scripted(id, vec![thinking(id)], FinishReason::Stop)
}

/// A tool name outside Anthropic's name grammar, so the renderer aliases it.
const TOOL: &str = "fs.read";

fn tool(name: &str) -> Tool {
    Tool {
        name: name.into(),
        description: "read a file".into(),
        input_schema: json!({"type": "object"}),
        module: None,
        execution_mode: ExecutionMode::Sequential,
    }
}

fn request(dir: &std::path::Path, run: &str, model: &str, user: Message) -> RunRequest {
    RunRequest {
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId(run.into()),
        session: broca_protocol::BindIdentity::new(dir, "append-only-anthropic", "s1"),
        model: ModelHandle::new("anthropic", model),
        // Every send repeats the session's system prompt, as clients do; admission drops the
        // byte-identical repeat.
        input: vec![
            Message::new(
                Role::System,
                vec![ContentBlock::Text {
                    text: "You are a careful agent.".into(),
                }],
            ),
            user,
        ],
        tools: vec![tool(TOOL)],
        policy_allowed: [TOOL.to_string()].into_iter().collect(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 8 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
    }
}

fn user(text: &str) -> Message {
    Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
}

fn failing_plane() -> StubToolPlane {
    StubToolPlane::new().with_tool(TOOL, |_| StubReply::ExecError("file not found".into()))
}

async fn drive(
    dir: &std::path::Path,
    req: RunRequest,
    provider: &SequenceProvider,
    plane: &StubToolPlane,
    append: bool,
) {
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    if append {
        run_loop_append_episode(req, provider, plane, &lease, &AllowAll, &wal, None, &tx)
            .await
            .expect("appended episode runs");
    } else {
        run_loop(req, provider, plane, &lease, &AllowAll, &wal, &tx)
            .await
            .expect("first episode runs");
    }
    drop(tx);
    collector.await.unwrap();
}

/// The OAuth Anthropic config with cache markers on, so marker placement is exercised too.
fn frozen_with_markers(wire_model: &str) -> FrozenRenderConfig {
    let mut frozen = anthropic_frozen_config(wire_model);
    frozen.cache = Some(CachePolicy {
        version: 1,
        class: CacheClass::Markers {
            syntax: MarkerSyntax::AnthropicCacheControl,
            ttl: CacheTtl::OneHour,
            max_breakpoints: 4,
            lookback_blocks: 20,
            tail: None,
            omit_ttl: false,
        },
    });
    frozen
}

fn strip_cache_control(value: &mut Value) {
    match value {
        Value::Object(object) => {
            object.remove("cache_control");
            object.values_mut().for_each(strip_cache_control);
        }
        Value::Array(items) => items.iter_mut().for_each(strip_cache_control),
        _ => {}
    }
}

/// Render every request with the frozen config of the model it was sent to.
fn render_all(sent: &[CallOptions]) -> Vec<Value> {
    sent.iter()
        .map(|opts| {
            let model = opts
                .target_model
                .as_ref()
                .map(|model| model.model_id.clone())
                .expect("the run loop names the target model");
            let mut body = build_request_body(&frozen_with_markers(&model), opts);
            strip_cache_control(&mut body);
            body
        })
        .collect()
}

/// The first place a request edits what the previous one sent, or `None` when every request is
/// an append-only extension of the one before.
fn first_edit(bodies: &[Value]) -> Option<String> {
    for (index, pair) in bodies.windows(2).enumerate() {
        let (prev, next) = (&pair[0], &pair[1]);
        let label = format!("request {index} -> {}", index + 1);
        if prev["system"] != next["system"] {
            return Some(format!("{label}: top-level system changed"));
        }
        if prev["tools"] != next["tools"] {
            return Some(format!("{label}: tools changed"));
        }
        let prev_messages = prev["messages"].as_array().expect("messages array");
        let next_messages = next["messages"].as_array().expect("messages array");
        if next_messages.len() < prev_messages.len() {
            return Some(format!("{label}: messages were removed"));
        }
        for (position, (before, after)) in prev_messages.iter().zip(next_messages).enumerate() {
            if before != after {
                return Some(format!(
                    "{label}: message {position} was edited\n before: {before}\n  after: {after}"
                ));
            }
        }
    }
    None
}

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = broca_test_support::scratch_root().join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Three episodes under one model, covering every render-time addition the loop and the renderer
/// make: repetition guidance on an earlier episode's tool result, a budget message, a repeated
/// system prompt, a caller cache-prefix marker on a user turn, signed thinking on every
/// assistant turn (one of them thinking-only), aliased tool ids and an aliased tool name.
#[tokio::test]
async fn a_multi_episode_session_renders_append_only() {
    let dir = scratch("broca-anthropic-append-only");
    let model = "claude-opus-4-8";
    let provider = SequenceProvider::new(vec![
        // Episode 1: the same failing call twice trips the repetition guard.
        tool_step("e1s1", json!({"p": "a"})),
        tool_step("e1s2", json!({"p": "a"})),
        thinking_only_step("e1s3"),
        // Episode 2: two tool calls reach the budget nudge.
        tool_step("e2s1", json!({"p": "b"})),
        tool_step("e2s2", json!({"p": "c"})),
        text_step("e2s3", "second answer"),
        // Episode 3: a plain follow-up.
        text_step("e3s1", "third answer"),
    ]);
    let plane = failing_plane();

    drive(
        &dir,
        request(&dir, "e1", model, user("read a twice")),
        &provider,
        &plane,
        false,
    )
    .await;
    let mut marked = user("continue");
    marked.cache_prefix_blocks = Some(1);
    let mut second = request(&dir, "e2", model, marked);
    second.budget = Some(BudgetConfig {
        nudges: vec![BudgetNudge {
            at_tool_calls: 2,
            text: "Check the work so far before calling more tools.".into(),
        }],
        finalize_at_tool_calls: 99,
        finalize_text: "finalize".into(),
    });
    drive(&dir, second, &provider, &plane, true).await;
    drive(
        &dir,
        request(&dir, "e3", model, user("thanks")),
        &provider,
        &plane,
        true,
    )
    .await;

    let sent = provider.sent.lock().unwrap().clone();
    assert_eq!(sent.len(), 7, "every scripted step was requested");
    let bodies = render_all(&sent);
    let last = serde_json::to_string(bodies.last().unwrap()).unwrap();
    // Each covered case must actually be present, or the comparison proves nothing about it.
    assert!(
        serde_json::to_string(&bodies[2])
            .unwrap()
            .contains("Tool repetition guidance"),
        "the guard fired in episode 1"
    );
    assert!(
        last.contains("Check the work so far"),
        "the budget message fired"
    );
    assert!(
        last.contains("sig-e1s3"),
        "the thinking-only turn is still echoed"
    );
    assert!(last.contains("\"t_"), "tool ids were aliased");
    assert_ne!(
        bodies.last().unwrap()["tools"][0]["name"],
        TOOL,
        "the tool name was aliased"
    );

    assert_eq!(first_edit(&bodies), None);
    let _ = std::fs::remove_dir_all(dir);
}

fn assistant_count(body: &Value) -> usize {
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "assistant")
        .count()
}

/// A session that switches Anthropic models A -> B -> A renders append-only. The original
/// cross-model rule compared each assistant turn's producing model with the request's model, per
/// message and afresh on every request, so under B it omitted A's thinking-only turn (deleting an
/// earlier message) and back on A it put A's removed thinking back and removed B's thinking from
/// the middle of history: all invalid edits for thinking-block binding. The frozen default now
/// echoes every Anthropic model's signed thinking to every other Anthropic model unchanged; the
/// API drops what the target model cannot read.
#[tokio::test]
async fn a_model_round_trip_renders_append_only() {
    let dir = scratch("broca-anthropic-model-round-trip");
    let provider = SequenceProvider::new(vec![
        tool_step("a1", json!({"p": "a"})),
        thinking_only_step("a2"),
        tool_step("b1", json!({"p": "b"})),
        text_step("b2", "from b"),
        tool_step("c1", json!({"p": "c"})),
        text_step("c2", "from a again"),
    ]);
    let plane = failing_plane();
    let (model_a, model_b) = ("claude-opus-4-8", "claude-opus-5");
    drive(
        &dir,
        request(&dir, "a", model_a, user("one")),
        &provider,
        &plane,
        false,
    )
    .await;
    drive(
        &dir,
        request(&dir, "b", model_b, user("two")),
        &provider,
        &plane,
        true,
    )
    .await;
    drive(
        &dir,
        request(&dir, "c", model_a, user("three")),
        &provider,
        &plane,
        true,
    )
    .await;

    let sent = provider.sent.lock().unwrap().clone();
    assert_eq!(sent.len(), 6);
    let bodies = render_all(&sent);
    let text = |index: usize| serde_json::to_string(&bodies[index]).unwrap();
    // Each request really went to the model its leg names, or no switch was exercised.
    let targets: Vec<&str> = sent
        .iter()
        .map(|opts| opts.target_model.as_ref().unwrap().model_id.as_str())
        .collect();
    assert_eq!(
        targets,
        [model_a, model_a, model_b, model_b, model_a, model_a]
    );

    // The load-bearing property: every request extends the one before it unchanged.
    assert_eq!(first_edit(&bodies), None);

    // Under B, A's thinking is still sent, and the thinking-only a2 turn is still there.
    assert!(text(2).contains("sig-a1") && text(2).contains("sig-a2"));
    assert_eq!(
        sent[2]
            .prompt
            .iter()
            .filter(|m| m.role == Role::Assistant)
            .count(),
        2
    );
    assert_eq!(
        assistant_count(&bodies[2]),
        2,
        "the a2 turn is kept under B"
    );
    // Back on A, B's earlier thinking is still sent beside A's later thinking.
    assert!(text(5).contains("sig-b1") && text(5).contains("sig-c1"));
    let _ = std::fs::remove_dir_all(dir);
}

/// KNOWN GAP, reported rather than fixed here: tools are frozen per episode, so a later send
/// that carries a different tool list changes the top-level `tools` of every later request.
#[tokio::test]
async fn known_gap_a_later_episode_can_replace_tools() {
    let dir = scratch("broca-anthropic-tools-replaced");
    let provider = SequenceProvider::new(vec![text_step("t1", "one"), text_step("t2", "two")]);
    let plane = failing_plane().with_tool("grep", |_| StubReply::ExecError("no".into()));
    let model = "claude-opus-4-8";
    drive(
        &dir,
        request(&dir, "t1", model, user("one")),
        &provider,
        &plane,
        false,
    )
    .await;
    let mut second = request(&dir, "t2", model, user("two"));
    second.tools.push(tool("grep"));
    second.policy_allowed.insert("grep".into());
    drive(&dir, second, &provider, &plane, true).await;

    let bodies = render_all(&provider.sent.lock().unwrap());
    assert_eq!(
        first_edit(&bodies).as_deref(),
        Some("request 0 -> 1: tools changed")
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// Every system message in the prompt renders into the top-level `system` field, so a later send
/// that repeats the session's system prompt AND adds a second, different system message would
/// change `system` for every later request. Admission must refuse it the same way it refuses a
/// changed system prompt.
#[tokio::test]
async fn a_later_episode_cannot_add_a_second_system_message() {
    let dir = scratch("broca-anthropic-second-system");
    let provider = SequenceProvider::new(vec![text_step("s1", "one"), text_step("s2", "two")]);
    let plane = failing_plane();
    let model = "claude-opus-4-8";
    drive(
        &dir,
        request(&dir, "s1", model, user("one")),
        &provider,
        &plane,
        false,
    )
    .await;

    let mut second = request(&dir, "s2", model, user("two"));
    second.input.insert(
        1,
        Message::new(
            Role::System,
            vec![ContentBlock::Text {
                text: "An extra instruction.".into(),
            }],
        ),
    );
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let result = run_loop_append_episode(
        second, &provider, &plane, &lease, &AllowAll, &wal, None, &tx,
    )
    .await;
    drop(tx);
    collector.await.unwrap();

    assert!(
        matches!(result, Err(broca_core::RunError::SystemPromptChanged)),
        "a second system message must be refused, got {result:?}"
    );
    assert_eq!(
        provider.sent.lock().unwrap().len(),
        1,
        "no request was sent for it"
    );
    let _ = std::fs::remove_dir_all(dir);
}

/// The thinking-binding beta rides every request that has thinking on, so each response reports
/// the thinking blocks that failed the prefix check. The request body must NEVER set
/// `prefix_mismatch_behavior`: that field opts the account into enforcement.
#[test]
fn thinking_requests_carry_the_binding_beta_and_never_opt_into_enforcement() {
    let frozen = anthropic_frozen_config("claude-opus-5");
    let beta = super::anthropic_beta_header(&frozen).expect("the OAuth shape sends betas");
    assert!(
        beta.split(',')
            .any(|value| value == "thinking-binding-controls-2026-08-01"),
        "{beta}"
    );
    let body = build_request_body(&frozen, &CallOptions::new(vec![user("hi")]));
    let text = serde_json::to_string(&body).unwrap();
    assert!(!text.contains("block_binding") && !text.contains("prefix_mismatch_behavior"));

    // Without thinking there is nothing to bind, so the api-key shape stays beta-free.
    let apikey = crate::families::anthropic_apikey_frozen_config("claude-opus-5");
    assert_eq!(super::anthropic_beta_header(&apikey), None);
}

/// Every entry of `message_start.input_transformations` is kept, whatever its reason, and an
/// entry of unexpected shape still counts.
#[test]
fn message_start_input_transformations_are_read_verbatim() {
    use crate::wire::WireFamily;
    let frozen = anthropic_frozen_config("claude-opus-5-5");
    let mut normalizer = super::AnthropicMessages.normalizer(&frozen, StepId(1));
    assert_eq!(normalizer.input_transformations(), None);
    let event = json!({
        "type": "message_start",
        "message": {
            "id": "msg_1",
            "usage": {"input_tokens": 1, "output_tokens": 0},
            "input_transformations": [
                {"type": "thinking_dropped", "path": "messages.1.content.0",
                 "reason": "prefix_binding_mismatch"},
                {"type": "thinking_mismatch_allowed", "reason": "some_future_reason"},
                "not an object"
            ]
        }
    });
    normalizer
        .push(format!("event: message_start\ndata: {event}\n\n").as_bytes())
        .expect("message_start decodes");
    let entries = normalizer.input_transformations().expect("reported");
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[0].kind.as_deref(), Some("thinking_dropped"));
    assert_eq!(entries[0].path.as_deref(), Some("messages.1.content.0"));
    assert_eq!(entries[1].reason.as_deref(), Some("some_future_reason"));
    assert_eq!(entries[2].kind, None);
}
