//! Thinking tied to the old conversation prefix is removed from requests, not storage.
use std::sync::Arc;

use broca_protocol::{
    content::{stamp_ordinals, ContentBlock, Message, Role},
    BindIdentity, CallOptions, FinishReason, ModelHandle, RunId, StepId, ToolCall, ToolCallId,
    ToolOutput, ToolResult, Usage,
};
use broca_provider::{
    families::{anthropic_frozen_config, gemini_frozen_config, AnthropicMessages, Gemini},
    wire::WireFamily,
};
use broca_records::{
    record::CompactionMessageKind, record::CompactionRange, record::RunConfig, record::RunOrigin,
    record::WalRecord,
};
use serde_json::{json, Value};

fn text(role: Role, value: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: value.into() }])
}

fn thinking(label: &str) -> Message {
    let mut message = Message::new(
        Role::Assistant,
        vec![
            ContentBlock::Reasoning {
                text: format!("{label} thinking").into(),
                signature: Some(format!("{label}-signature").into()),
            },
            ContentBlock::RedactedReasoning {
                data: format!("{label}-redacted").into(),
                provider_format: None,
            },
            ContentBlock::Text {
                text: format!("{label} text").into(),
            },
        ],
    );
    message.origin = Some(ModelHandle::new("anthropic", "claude-opus-5-5"));
    message
}

fn seed() -> Vec<Message> {
    let mut pending = thinking("pending");
    pending.content.push(ContentBlock::ToolCall(ToolCall {
        tool_call_id: ToolCallId("call-1".into()),
        tool_name: "record_note".into(),
        input: json!({"value": 599}).into(),
        thought_signature: None,
        provider_state: None,
    }));
    vec![
        text(Role::User, "original prefix"),
        thinking("older"),
        text(Role::User, "record stage 2"),
        pending,
        Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId("call-1".into()),
                output: ToolOutput::Text {
                    text: "recorded 599".into(),
                },
                is_error: false,
            })],
        ),
    ]
}

fn start(input: Vec<Message>) -> WalRecord {
    let frozen = anthropic_frozen_config("claude-opus-5-5");
    WalRecord::RunStarted {
        message_marks: None,
        compaction_plan_version: None,
        sent_by: None,
        written_at_ms: None,
        tags: None,
        session_class: None,
        retention: None,
        hooks: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        marker_order_version: None,
        manifest: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        ts_ms: None,
        run_id: RunId("compacted-run".into()),
        session: BindIdentity::new("/tmp/p", "h", "s"),
        config: Box::new(RunConfig {
            global_system: None,
            account_route: None,
            route_decision_id: None,
            served_provider: None,
            account_limit_pause: false,
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: ModelHandle::new("anthropic", "claude-opus-5-5"),
            tools: vec![].into(),
            tool_choice: Default::default(),
            generation: Default::default(),
            response_format: Default::default(),
            provider_options: Value::Null,
            cache_tiers: Value::Null,
            context_epoch: 0,
            renderer_version: frozen.renderer_version,
            frozen_render_config: serde_json::to_value(frozen).unwrap(),
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: vec![],
            context_management: None,
        }),
        input: input.into(),
        origin: RunOrigin::Fresh,
        submission_id: None,
    }
}

fn applied(version: u64, to: u64) -> WalRecord {
    let replacement = vec![text(
        Role::User,
        "Summary: recorded stage 1; continue stage 2.",
    )];
    WalRecord::CompactionMessage {
        provider: "compactor".into(),
        kind: if version == 1 {
            CompactionMessageKind::Setup
        } else {
            CompactionMessageKind::Step
        },
        compaction_id: "view".into(),
        version,
        range: CompactionRange { from: 0, to },
        sha256: broca_records::record::compaction_replacement_sha256(&replacement).unwrap(),
        replacement: Arc::new(replacement),
        request_id: None,
        cursor: None,
        call_when: None,
        stability: None,
        ceiling: None,
        tag_checkpoint: None,
        ride_wait_ms: None,
        view_features: None,
        view_due: None,
    }
}

fn completion(label: &str) -> WalRecord {
    WalRecord::ModelStepFinished {
        raw_finish_reason: None,
        fallback: Default::default(),
        written_at_ms: None,
        tags: None,
        step_id: StepId(0),
        assistant_message: Arc::new(broca_protocol::AssistantMessage {
            message_id: broca_protocol::MessageId(label.into()),
            content: thinking(label).content,
        }),
        usage: Usage::default(),
        finish_reason: Some(FinishReason::Stop),
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
        hooks: None,
    }
}

fn options(records: &[WalRecord]) -> CallOptions {
    let prior = broca_core::replay::replay(records).unwrap();
    let mut opts = CallOptions::new(prior.prompt);
    stamp_ordinals(&mut opts.prompt);
    opts.render_episodes = Arc::new(prior.render_episodes.unwrap().into());
    if let Some(state) = prior.compaction.state() {
        opts = broca_core::compaction::model_options(&opts, state).unwrap();
    }
    opts
}

fn bytes(records: &[WalRecord]) -> Vec<u8> {
    AnthropicMessages.render(
        &anthropic_frozen_config("claude-opus-5-5"),
        &options(records),
    )
}

#[test]
fn compaction_v2_globals_preserve_real_serializer_system_bytes_without_transcript_positions() {
    use broca_core::compaction_v2::systems;
    for systems_at in [0, 1, 2] {
        let mut legacy = vec![text(Role::User, "earlier turn")];
        legacy.splice(
            systems_at.min(legacy.len())..systems_at.min(legacy.len()),
            [
                text(Role::System, "first instructions"),
                text(Role::System, "second instructions"),
            ],
        );
        legacy.push(text(Role::User, "next question"));
        let mut transcript = legacy.clone();
        let globals = systems::take_globals(&mut transcript);
        stamp_ordinals(&mut transcript);
        assert_eq!(transcript[0].ordinal, Some(0));
        assert_eq!(transcript[1].ordinal, Some(1));
        let legacy = CallOptions::new(legacy);
        let mode3 = systems::view(&CallOptions::new(transcript), &globals)
            .unwrap()
            .unwrap();
        assert_eq!(
            AnthropicMessages.render(&anthropic_frozen_config("claude-opus-5-5"), &legacy),
            AnthropicMessages.render(&anthropic_frozen_config("claude-opus-5-5"), &mode3)
        );
        assert_eq!(
            Gemini.render(&gemini_frozen_config("gemini-3-pro"), &legacy),
            Gemini.render(&gemini_frozen_config("gemini-3-pro"), &mode3)
        );
    }
}

fn blocks(records: &[WalRecord]) -> Vec<Value> {
    let body: Value = serde_json::from_slice(&bytes(records)).unwrap();
    body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|message| message["content"].as_array().unwrap().clone())
        .collect()
}

fn reasoning(records: &[WalRecord]) -> Vec<Value> {
    blocks(records)
        .into_iter()
        .filter(|block| {
            matches!(
                block["type"].as_str(),
                Some("thinking" | "redacted_thinking")
            )
        })
        .collect()
}

#[test]
fn view_strips_older_and_pending_tool_turn_without_changing_other_blocks_or_wal() {
    let original = vec![start(seed())];
    let mut compacted = original.clone();
    compacted.push(applied(1, 1));
    let stored = serde_json::to_vec(&compacted).unwrap();
    assert_eq!(
        reasoning(&compacted),
        Vec::<Value>::new(),
        "stale thinking includes the pending tool_use turn"
    );
    let expected: Vec<_> = blocks(&original)
        .into_iter()
        .skip(1)
        .filter(|block| {
            !matches!(
                block["type"].as_str(),
                Some("thinking" | "redacted_thinking")
            )
        })
        .collect();
    assert_eq!(&blocks(&compacted)[1..], &expected);
    assert_eq!(serde_json::to_vec(&compacted).unwrap(), stored);
    assert_eq!(
        broca_core::replay::replay(&compacted).unwrap().prompt,
        broca_core::replay::replay(&original).unwrap().prompt
    );
}

#[test]
fn post_view_thinking_is_kept_without_restoring_pre_view_thinking() {
    let mut records = vec![start(seed()), applied(1, 1)];
    records.push(completion("post"));
    assert_eq!(
        reasoning(&records),
        vec![
            json!({"type":"thinking", "thinking":"post thinking", "signature":"post-signature"}),
            json!({"type":"redacted_thinking", "data":"post-redacted"}),
        ]
    );
    let first = bytes(&records);
    records.push(WalRecord::Unknown);
    assert_eq!(
        first,
        bytes(&records),
        "informational records must not move the boundary"
    );
}

#[test]
fn second_compaction_moves_the_boundary_and_keeps_only_new_thinking() {
    let mut records = vec![
        start(seed()),
        applied(1, 1),
        completion("post"),
        applied(2, 3),
    ];
    assert!(
        reasoning(&records).is_empty(),
        "second view invalidates the first view's completion"
    );
    records.push(completion("new"));
    assert_eq!(
        reasoning(&records),
        vec![
            json!({"type":"thinking", "thinking":"new thinking", "signature":"new-signature"}),
            json!({"type":"redacted_thinking", "data":"new-redacted"}),
        ]
    );
}

#[test]
fn replayed_compaction_renders_identical_bytes_and_never_restores_stale_thinking() {
    let records = vec![start(seed()), applied(1, 1), completion("post")];
    let encoded = serde_json::to_vec(&records).unwrap();
    let replayed: Vec<WalRecord> = serde_json::from_slice(&encoded).unwrap();
    assert_eq!(bytes(&records), bytes(&replayed));
    assert_eq!(
        reasoning(&replayed).len(),
        2,
        "only post-view thinking survives replay"
    );
}

#[test]
fn non_anthropic_family_keeps_signed_reasoning_on_the_same_compacted_view() {
    let records = vec![start(seed()), applied(1, 1)];
    let mut opts = options(&records);
    // Follow signed Reasoning into Gemini's thoughtSignature field. Anthropic's
    // redacted thinking is unrelated to that signature and is excluded here.
    for message in &mut opts.prompt {
        message
            .content
            .retain(|block| !matches!(block, ContentBlock::RedactedReasoning { .. }));
    }
    let body = Gemini.shape(&gemini_frozen_config("gemini-3-pro"), &opts);
    assert!(
        body["contents"].as_array().unwrap().iter().any(|message| {
            message["parts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|part| part["thoughtSignature"] == "pending-signature")
        }),
        "compaction does not invalidate Gemini signatures"
    );
}

#[test]
fn compacted_anthropic_request_matches_golden() {
    let records = vec![start(seed()), applied(1, 1), completion("post")];
    assert_eq!(
        String::from_utf8(bytes(&records)).unwrap(),
        include_str!("../fixtures/anthropic_compacted_request.json").trim_end()
    );
}

#[test]
fn no_compaction_preserves_all_thinking_and_request_bytes() {
    let records = vec![start(seed())];
    let direct = CallOptions::new(seed());
    assert_eq!(reasoning(&records).len(), 4);
    assert_eq!(
        bytes(&records),
        AnthropicMessages.render(&anthropic_frozen_config("claude-opus-5-5"), &direct)
    );
}

#[test]
fn carried_replacement_thinking_is_stripped_without_editing_the_replacement() {
    let mut record = applied(1, 2);
    if let WalRecord::CompactionMessage {
        replacement,
        sha256,
        ..
    } = &mut record
    {
        *replacement = Arc::new(vec![text(Role::User, "summary"), thinking("older")]);
        *sha256 = broca_records::record::compaction_replacement_sha256(replacement).unwrap();
    }
    let records = vec![start(seed()), record];
    let stored = serde_json::to_vec(&records).unwrap();
    assert!(reasoning(&records).is_empty());
    assert!(blocks(&records)
        .iter()
        .any(|block| block["text"] == "older text"));
    assert_eq!(serde_json::to_vec(&records).unwrap(), stored);
}

#[test]
fn family_policy_is_anthropic_only_not_a_model_name_check() {
    use broca_provider::spec::WireFamilyId;
    for family in [
        WireFamilyId::OpenAiChat,
        WireFamilyId::OpenAiResponses,
        WireFamilyId::Gemini,
        WireFamilyId::GoogleCodeAssist,
        WireFamilyId::OpenAiResponsesChatgpt,
        WireFamilyId::BedrockConverse,
    ] {
        assert!(!family.thinking_is_prefix_bound(), "{family:?}");
    }
    assert!(WireFamilyId::AnthropicMessages.thinking_is_prefix_bound());
    let records = vec![start(seed()), applied(1, 1)];
    assert!(!AnthropicMessages
        .render(&anthropic_frozen_config("any-model-id"), &options(&records))
        .windows(b"pending-signature".len())
        .any(|bytes| bytes == b"pending-signature"));
}
