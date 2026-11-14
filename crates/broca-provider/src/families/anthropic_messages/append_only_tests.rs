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
    RunRequest, StubReply, StubToolPlane, WalStore,
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
use broca_wal::WalRecord;
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
    /// The render config each run freezes, when a test rebuilds requests offline from the WAL,
    /// which reads it back; the trait's default otherwise.
    frozen: Option<Value>,
}

impl SequenceProvider {
    fn new(scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            scripts: Arc::new(Mutex::new(scripts.into())),
            sent: Arc::new(Mutex::new(Vec::new())),
            frozen: None,
        }
    }

    /// Freezes `frozen` into every run it serves.
    fn freezing(mut self, frozen: &FrozenRenderConfig) -> Self {
        self.frozen = Some(serde_json::to_value(frozen).unwrap());
        self
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

    fn render_config_blob(&self) -> Value {
        match &self.frozen {
            Some(frozen) => frozen.clone(),
            None => json!({ "render_config": "stateless" }),
        }
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
                input: input.into(),
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
        reply: None,
    }
}

fn request(dir: &std::path::Path, run: &str, model: &str, user: Message) -> RunRequest {
    RunRequest {
        account_route: None,
        route_decision_id: None,
        input_written_at_ms: None,
        directives: None,
        session_class: None,
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
        structured_output: None,
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
        max_input_tokens: None,
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

/// Whether any request in `bodies[index]` carries the signature `sig`.
fn sends_signature(bodies: &[Value], index: usize, sig: &str) -> bool {
    serde_json::to_string(&bodies[index]["messages"])
        .unwrap()
        .contains(sig)
}

/// Tools are frozen per episode, so a later send that carries a different tool list changes the
/// top-level `tools` of every later request. Anthropic refuses a request that still carries
/// signed thinking sent under another tool set, so the later episode's requests send none of the
/// earlier episode's, while the stored transcript keeps it.
#[tokio::test]
async fn a_later_episode_with_another_tool_sends_no_earlier_signed_thinking() {
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

    let sent = provider.sent.lock().unwrap().clone();
    let bodies = render_all(&sent);
    assert_eq!(
        first_edit(&bodies).as_deref(),
        Some("request 0 -> 1: tools changed")
    );
    assert!(
        !sends_signature(&bodies, 1, "sig-t1"),
        "the earlier episode's signed thinking is sent under the new tools"
    );
    // Only the outgoing copy loses it: the transcript the request renders from keeps it.
    assert!(sent[1].prompt.iter().any(|message| message
        .content
        .iter()
        .any(|block| matches!(block, ContentBlock::Reasoning { signature: Some(sig), .. } if sig == "sig-t1"))));
    assert!(serde_json::to_string(&bodies[1]).unwrap().contains("one"));
    let _ = std::fs::remove_dir_all(dir);
}

/// A refresh that changes only one tool's description changes the tool set: the episode after it
/// sends no earlier signed thinking. The episode after that, under the same tools, keeps the
/// thinking sent under them byte for byte, so its requests extend the cached prefix unchanged.
#[tokio::test]
async fn a_changed_description_drops_earlier_thinking_and_an_unchanged_set_keeps_it() {
    let dir = scratch("broca-anthropic-description-changed");
    let provider = SequenceProvider::new(vec![
        tool_step("a1", json!({"p": "a"})),
        text_step("a2", "one"),
        tool_step("b1", json!({"p": "b"})),
        text_step("b2", "two"),
        text_step("c1", "three"),
    ]);
    let plane = failing_plane();
    let model = "claude-opus-4-8";
    drive(
        &dir,
        request(&dir, "a", model, user("one")),
        &provider,
        &plane,
        false,
    )
    .await;
    let refreshed = |run: &str, text: &str| {
        let mut next = request(&dir, run, model, user(text));
        next.tools[0].description = "read a file, skipping binary content".into();
        next
    };
    drive(&dir, refreshed("b", "two"), &provider, &plane, true).await;
    drive(&dir, refreshed("c", "three"), &provider, &plane, true).await;

    let bodies = render_all(&provider.sent.lock().unwrap());
    assert_eq!(bodies.len(), 5);
    assert_eq!(
        first_edit(&bodies).as_deref(),
        Some("request 1 -> 2: tools changed")
    );
    // Episode a's own requests keep its thinking; episode b's requests send none of it.
    assert!(sends_signature(&bodies, 1, "sig-a1"));
    for index in 2..5 {
        assert!(!sends_signature(&bodies, index, "sig-a1"));
        assert!(!sends_signature(&bodies, index, "sig-a2"));
    }
    // From episode b on, every request extends the one before it unchanged, b's thinking
    // included: episode c's tool set is b's.
    assert_eq!(first_edit(&bodies[2..]), None);
    assert!(sends_signature(&bodies, 4, "sig-b1") && sends_signature(&bodies, 4, "sig-b2"));
    let _ = std::fs::remove_dir_all(dir);
}

/// Only the set of tools counts: a later episode that declares the same tools in another order
/// keeps the earlier episode's signed thinking.
#[tokio::test]
async fn reordered_tools_keep_earlier_signed_thinking() {
    let dir = scratch("broca-anthropic-tools-reordered");
    let provider = SequenceProvider::new(vec![text_step("t1", "one"), text_step("t2", "two")]);
    let plane = failing_plane().with_tool("grep", |_| StubReply::ExecError("no".into()));
    let model = "claude-opus-4-8";
    let mut first = request(&dir, "t1", model, user("one"));
    first.tools.push(tool("grep"));
    first.policy_allowed.insert("grep".into());
    drive(&dir, first, &provider, &plane, false).await;
    let mut second = request(&dir, "t2", model, user("two"));
    second.tools.insert(0, tool("grep"));
    second.policy_allowed.insert("grep".into());
    drive(&dir, second, &provider, &plane, true).await;

    let bodies = render_all(&provider.sent.lock().unwrap());
    assert_eq!(
        first_edit(&bodies).as_deref(),
        Some("request 0 -> 1: tools changed"),
        "the request lists the tools in the order declared"
    );
    assert!(sends_signature(&bodies, 1, "sig-t1"));
    let _ = std::fs::remove_dir_all(dir);
}

/// The floor comes from the runs' start records, so a run resumed after a crash and a request
/// rebuilt offline from the WAL (cache-dump's render) send exactly the bytes the live run sent
/// after its tools changed.
#[tokio::test]
async fn a_resumed_run_and_an_offline_rebuild_send_the_live_bytes_after_a_tool_change() {
    let dir = scratch("broca-anthropic-tools-changed-resume");
    let model = "claude-opus-4-8";
    let frozen = frozen_with_markers(model);
    let b_steps = || vec![tool_step("b1", json!({"p": "b"})), text_step("b2", "two")];
    let mut steps = vec![tool_step("a1", json!({"p": "a"})), text_step("a2", "one")];
    steps.extend(b_steps());
    let provider = SequenceProvider::new(steps).freezing(&frozen);
    let plane = failing_plane();
    drive(
        &dir,
        request(&dir, "a", model, user("one")),
        &provider,
        &plane,
        false,
    )
    .await;
    let refreshed = || {
        let mut next = request(&dir, "b", model, user("two"));
        next.tools[0].description = "read a file, skipping binary content".into();
        next
    };
    drive(&dir, refreshed(), &provider, &plane, true).await;
    let live = render_all(&provider.sent.lock().unwrap());
    assert_eq!(live.len(), 4);
    assert!(sends_signature(&live, 1, "sig-a1"));
    assert!(!sends_signature(&live, 2, "sig-a1") && !sends_signature(&live, 3, "sig-a1"));

    let request_b = refreshed();
    let wal = FileWalStore::new(dir.join("wal"));
    let records: Vec<WalRecord> = WalStore::read_records(&wal, &request_b.session)
        .unwrap()
        .into_iter()
        .map(|framed| framed.record)
        .collect();
    let b_start = records
        .iter()
        .position(
            |record| matches!(record, WalRecord::RunStarted { run_id, .. } if run_id.0 == "b"),
        )
        .expect("run b started");
    let WalRecord::RunStarted { config, .. } = &records[b_start] else {
        unreachable!()
    };

    // Offline: each of run b's steps rebuilt from the records through its `step_started`.
    for (step, live_body) in [(1, &live[2]), (2, &live[3])] {
        let through = b_start
            + records[b_start..]
                .iter()
                .position(|record| {
                    matches!(record, WalRecord::StepStarted { step_id } if step_id.0 == step)
                })
                .expect("the step started");
        let (_, opts, _) =
            broca_module_serve::wal_render::render_step(&records[..=through], config, "b", step)
                .expect("the step renders offline");
        let mut body = build_request_body(&frozen, &opts);
        strip_cache_control(&mut body);
        assert_eq!(&body, live_body, "offline rebuild of step {step}");
    }

    // Resume: a crash right after run b's start, resumed from a copy of the log through it.
    let resume_dir = dir.join("resume");
    let resume_wal = FileWalStore::new(resume_dir.join("wal"));
    std::fs::create_dir_all(resume_wal.wal_path(&request_b.session).parent().unwrap()).unwrap();
    {
        let mut log = broca_wal::FileWal::open(resume_wal.wal_path(&request_b.session))
            .unwrap()
            .with_feature_floor(resume_wal.feature_floor_path());
        for record in &records[..=b_start] {
            broca_wal::Wal::append(&mut log, 1, record, true).unwrap();
        }
    }
    let resumed_provider = SequenceProvider::new(b_steps()).freezing(&frozen);
    let lease = FileLeaseStore::new(resume_dir.join("lease"));
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    run_loop(
        request_b,
        &resumed_provider,
        &plane,
        &lease,
        &AllowAll,
        &resume_wal,
        &tx,
    )
    .await
    .expect("run b resumes");
    drop(tx);
    collector.await.unwrap();
    let resumed = render_all(&resumed_provider.sent.lock().unwrap());
    assert_eq!(resumed, live[2..], "a resumed run b sends the live bytes");
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

/// On the pre-persona OAuth shape the thinking-binding beta rides every request that has thinking
/// on, so each response reports the thinking blocks that failed the prefix check. A Claude Code
/// persona request does not carry it (the reference client sends it only with an enforcing
/// `block_binding`), so its attempts are recorded as not measured: see
/// `a_persona_request_is_not_measured_for_input_transformations`. The request body must NEVER set
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
/// Which requests ask for the input-transformations report: a pre-persona OAuth request with
/// thinking does; a Claude Code persona request and an api-key request do not, so their
/// attempts read as not measured rather than as zero transformations; 2.1.302 main requests
/// carry that beta again and must be measured even though they also use a persona.
#[test]
fn a_persona_request_is_not_measured_for_input_transformations() {
    use crate::wire::WireFamily;
    let unmeasured = |frozen: &crate::wire::FrozenRenderConfig| {
        super::AnthropicMessages
            .normalizer(frozen, broca_protocol::StepId(1))
            .input_transformations_unmeasured()
    };
    let oauth = anthropic_frozen_config("claude-opus-5");
    assert!(
        !unmeasured(&oauth),
        "the pre-persona OAuth shape asks for the report"
    );

    let mut persona = anthropic_frozen_config("claude-opus-5");
    persona.claude_code_persona = Some(crate::wire::ClaudeCodePersona {
        version: "2.1.280".into(),
        device_id: "0".repeat(64),
        account_uuid: None,
        session_id: "11111111-1111-4111-8111-111111111111".into(),
        prompt_id: None,
        cc_version_suffix: None,
        prompt_ordinal: None,
    });
    assert!(
        unmeasured(&persona),
        "a resumed 2.1.280 persona request does not ask for the report"
    );

    persona.claude_code_persona.as_mut().unwrap().version = "2.1.302".into();
    assert!(
        !unmeasured(&persona),
        "2.1.302 main requests ask for the report"
    );

    let apikey = crate::families::anthropic_apikey_frozen_config("claude-opus-5");
    assert!(
        unmeasured(&apikey),
        "an api-key request sends no beta at all"
    );
}

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
