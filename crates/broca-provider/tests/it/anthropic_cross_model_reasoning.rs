//! B2 — the cross-model signed-reasoning gate (a render-side cache/correctness fix).
//!
//! Anthropic signed `thinking` is echoed back on the next turn for interleaved-thinking
//! continuity. The original rule, still frozen into runs admitted before the current one,
//! assumed a signature minted by one model is REJECTED by a different one, and stripped every
//! signed-thinking block whose producing model (`Message.origin`) differs from the request's
//! current model (`CallOptions.target_model`). The legacy tests below pin that rule.
//!
//! Measured live since, Anthropic's API accepts another Anthropic model's signed thinking and
//! drops what the target model cannot read, so the current rule keeps thinking between models
//! of the first-party `anthropic` provider and strips only thinking from any other provider
//! (for example a Gemini turn), whose blocks are not Anthropic signatures at all.
//!
//! This is the exact provider-switch-mid-lineage scenario: episode 1 runs on model X and
//! ends with a signed-thinking assistant turn; episode 2 continues on model Y. At episode
//! 2's first render that model-X turn is the LAST assistant turn in the prompt, so the
//! positional interleaved-thinking rule WOULD echo it — and without this gate the signature
//! goes to model Y and 400s.
//!
//! ## Why both arms hold `last_turn_only && is_last_assistant_turn` TRUE
//! The render keeps thinking only when `last_turn_only && is_last_assistant_turn &&
//! is_same_model`. To prove the gate is the `is_same_model` term (and not green because the
//! positional rule already stripped the block), the signed block MUST be the LAST assistant
//! turn in BOTH arms, and the ONLY variable between arms is the target model (X vs Y). If the
//! block weren't the last assistant turn, the positional rule would pre-strip it and the test
//! would prove nothing.

use broca_protocol::{
    content::{ContentBlock, Message, Role},
    request::{CallOptions, GenerationConfig, ModelHandle, ResponseFormat, ToolChoice},
    BindIdentity, RunFinishReason, RunId, Usage,
};
use broca_provider::families::{anthropic_frozen_config, AnthropicMessages};
use broca_provider::policy::{CrossModelThinking, ReasoningPolicy};
use broca_provider::wire::{FrozenRenderConfig, WireFamily};
use broca_wal::{RunConfig, RunOrigin, SeedOriginKind, WalRecord};
use serde_json::Value;

const PROVIDER: &str = "anthropic";
const MODEL_X: &str = "claude-opus-4-8";
const MODEL_Y: &str = "claude-sonnet-4-5";

/// A model of another provider whose reasoning never is an Anthropic signature.
const FOREIGN_PROVIDER: &str = "google";
const FOREIGN_MODEL: &str = "gemini-3.5-pro";

fn origin(model: &str) -> ModelHandle {
    ModelHandle::new(PROVIDER, model)
}

fn foreign_origin() -> ModelHandle {
    ModelHandle::new(FOREIGN_PROVIDER, FOREIGN_MODEL)
}

/// The Anthropic config for `model` under the original same-model-only rule. Runs admitted
/// before the rule became part of the frozen config still render with it when resumed.
fn legacy_frozen_config(model: &str) -> FrozenRenderConfig {
    let mut frozen = anthropic_frozen_config(model);
    match &mut frozen.reasoning {
        ReasoningPolicy::AnthropicInterleaved { cross_model, .. } => {
            *cross_model = CrossModelThinking::SameModelOnly
        }
        other => panic!("expected interleaved thinking, got {other:?}"),
    }
    frozen
}

/// The prompt the provider-switch scenario produces: a user turn, then a model-X assistant
/// turn carrying a SIGNED thinking block followed by a final text block. The signed block is
/// the last assistant turn's reasoning — the interleaved-thinking echo candidate.
fn prompt_with_model_x_signed_thinking() -> Vec<Message> {
    prompt_with_signed_thinking_from(origin(MODEL_X))
}

fn prompt_with_signed_thinking_from(producer: ModelHandle) -> Vec<Message> {
    vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "continue".into(),
            }],
        ),
        Message::assistant_from(
            vec![
                ContentBlock::Reasoning {
                    text: "deliberating".into(),
                    signature: Some("sig-minted-by-model-x".into()),
                },
                ContentBlock::Text { text: "ok".into() },
            ],
            producer,
        ),
    ]
}

/// Render the prompt for `target` under the legacy rule and return the `messages` array of the
/// request body.
fn rendered_messages(target_model: &str) -> Vec<Value> {
    rendered_messages_with(
        &legacy_frozen_config(target_model),
        prompt_with_model_x_signed_thinking(),
        target_model,
    )
}

fn rendered_messages_with(
    frozen: &FrozenRenderConfig,
    prompt: Vec<Message>,
    target_model: &str,
) -> Vec<Value> {
    let mut opts = CallOptions::new(prompt);
    opts.target_model = Some(origin(target_model));
    let body: Value = serde_json::from_slice(&AnthropicMessages.render(frozen, &opts)).unwrap();
    body["messages"].as_array().cloned().unwrap_or_default()
}

/// Does any rendered assistant message carry a `thinking` block (the signed-thinking echo)?
fn has_thinking_block(messages: &[Value]) -> bool {
    messages.iter().any(|m| {
        m["content"]
            .as_array()
            .is_some_and(|blocks| blocks.iter().any(|b| b["type"] == "thinking"))
    })
}

#[test]
fn same_model_keeps_signed_thinking() {
    // Episode 2 continues on the SAME model that produced the signed block (X → X). All
    // three gate conditions hold (last turn, positional, same model) → the signature is
    // echoed back for interleaved-thinking continuity.
    let messages = rendered_messages(MODEL_X);
    assert!(
        has_thinking_block(&messages),
        "same-model render must KEEP the signed thinking block (interleaved-thinking echo); \
         got {messages:#?}"
    );
}

#[test]
fn legacy_rule_cross_model_strips_signed_thinking() {
    // Episode 2 continues on a DIFFERENT model (X → Y). The block is STILL the last
    // assistant turn and the positional rule still fires — the ONLY thing that changed from
    // the same-model arm is the target model. So a stripped signature here is attributable
    // to `is_same_model` alone, not to the positional rule.
    let messages = rendered_messages(MODEL_Y);
    assert!(
        !has_thinking_block(&messages),
        "cross-model render must STRIP the model-X signature (echoing it to model Y is a hard \
         provider error); got {messages:#?}"
    );
}

#[test]
fn legacy_rule_cross_model_strip_is_the_only_difference() {
    // Belt-and-braces: the X→X and X→Y renders differ EXACTLY in the presence of the
    // thinking block. Both share the same prompt, same family, same positional state; the
    // discriminating variable is solely the target model. This pins that B2 (is_same_model)
    // is what flips the decision — not some other byte difference between the two models'
    // configs (the wire_model_id differs, but that only changes the `model` field, not the
    // thinking echo).
    let same = rendered_messages(MODEL_X);
    let cross = rendered_messages(MODEL_Y);
    assert!(has_thinking_block(&same));
    assert!(!has_thinking_block(&cross));

    // The assistant message in the cross-model render keeps its NON-reasoning content
    // (the strip removes only the signed thinking, not the whole turn).
    let cross_assistant = cross
        .iter()
        .find(|m| m["role"] == "assistant")
        .expect("assistant turn present in cross-model render");
    let has_text = cross_assistant["content"]
        .as_array()
        .unwrap()
        .iter()
        .any(|b| b["type"] == "text");
    assert!(
        has_text,
        "cross-model strip must remove only the signature block, keeping the text content"
    );
}

/// Under the current rule a switch between two models of the first-party `anthropic` provider
/// sends the earlier model's signed thinking back unchanged: Anthropic's API accepts it and drops
/// what the target cannot read, while stripping it would edit the conversation's history.
#[test]
fn anthropic_to_anthropic_switch_keeps_signed_thinking() {
    let messages = rendered_messages_with(
        &anthropic_frozen_config(MODEL_Y),
        prompt_with_model_x_signed_thinking(),
        MODEL_Y,
    );
    assert!(
        has_thinking_block(&messages),
        "a switch between Anthropic models must keep the earlier model's thinking; got \
         {messages:#?}"
    );
}

/// Under the current rule reasoning from another provider is still stripped: it is not an
/// Anthropic signature at all. Only the thinking goes; the turn keeps its text.
#[test]
fn another_providers_signed_reasoning_is_still_stripped() {
    let messages = rendered_messages_with(
        &anthropic_frozen_config(MODEL_X),
        prompt_with_signed_thinking_from(foreign_origin()),
        MODEL_X,
    );
    assert!(
        !has_thinking_block(&messages),
        "another provider's reasoning must not reach an Anthropic model; got {messages:#?}"
    );
    assert!(messages[1]["content"]
        .as_array()
        .unwrap()
        .iter()
        .any(|b| b["type"] == "text"));
}

/// A message whose producing model is unknown is stripped under the current rule too: nothing
/// proves it is an Anthropic signature.
#[test]
fn unknown_origin_signed_thinking_is_still_stripped() {
    let mut prompt = prompt_with_model_x_signed_thinking();
    prompt[1].origin = None;
    let messages = rendered_messages_with(&anthropic_frozen_config(MODEL_X), prompt, MODEL_X);
    assert!(
        !has_thinking_block(&messages),
        "thinking of unknown origin must be stripped; got {messages:#?}"
    );
}

#[test]
fn no_target_model_preserves_positional_behavior() {
    // When no target model is threaded (an isolated render / golden that doesn't carry run
    // identity), the cross-model check is skipped and the positional rule stands unchanged —
    // so existing golden bytes are preserved. The signed last-assistant-turn block is kept.
    let frozen = anthropic_frozen_config(MODEL_X);
    let opts = CallOptions::new(prompt_with_model_x_signed_thinking()); // target_model = None
    let body: Value = serde_json::from_slice(&AnthropicMessages.render(&frozen, &opts)).unwrap();
    let messages = body["messages"].as_array().cloned().unwrap_or_default();
    assert!(
        has_thinking_block(&messages),
        "with no target model, the positional rule must stand and KEEP the block (golden \
         byte-stability); got {messages:#?}"
    );
}

#[test]
fn keep_all_scope_strips_only_the_foreign_signed_thinking() {
    // Under the family default (keep-all) EVERY assistant message is positionally eligible to
    // echo its thinking, not just the last. So an EARLIER assistant message from another
    // provider is still stripped by the cross-model gate alone, while a same-model message
    // keeps its signature. This proves keep-all did not widen the echo past the cross-model
    // guard.
    let prompt = vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "start".into(),
            }],
        ),
        // An earlier assistant message produced by ANOTHER provider's model.
        Message::assistant_from(
            vec![
                ContentBlock::Reasoning {
                    text: "foreign deliberation".into(),
                    signature: Some("sig-minted-by-model-y".into()),
                },
                ContentBlock::Text {
                    text: "foreign answer".into(),
                },
            ],
            foreign_origin(),
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "continue".into(),
            }],
        ),
        // The last assistant message signed by the TARGET model.
        Message::assistant_from(
            vec![
                ContentBlock::Reasoning {
                    text: "native deliberation".into(),
                    signature: Some("sig-minted-by-model-x".into()),
                },
                ContentBlock::Text { text: "ok".into() },
            ],
            origin(MODEL_X),
        ),
    ];
    let frozen = anthropic_frozen_config(MODEL_X);
    let mut opts = CallOptions::new(prompt);
    opts.target_model = Some(origin(MODEL_X));
    let body: Value = serde_json::from_slice(&AnthropicMessages.render(&frozen, &opts)).unwrap();
    let messages = body["messages"].as_array().cloned().unwrap_or_default();

    // The earlier foreign-signed message keeps its text but loses the thinking block.
    let foreign = &messages[1];
    assert_eq!(foreign["role"], "assistant");
    let foreign_blocks = foreign["content"].as_array().unwrap();
    assert!(
        foreign_blocks.iter().all(|b| b["type"] != "thinking"),
        "a foreign signature must be stripped even mid-conversation under keep-all; got {foreign:#?}"
    );
    assert!(foreign_blocks.iter().any(|b| b["type"] == "text"));

    // The same-model last assistant message keeps its signed thinking.
    let native = &messages[3];
    assert_eq!(native["role"], "assistant");
    assert!(
        native["content"]
            .as_array()
            .unwrap()
            .iter()
            .any(|b| b["type"] == "thinking"),
        "a same-model signature must be echoed under keep-all; got {native:#?}"
    );
}

#[test]
fn imported_seed_replay_strips_foreign_signed_reasoning_on_cross_model_render() {
    let records = vec![
        WalRecord::RunStarted {
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
            run_id: RunId("run-import".into()),
            session: BindIdentity::new("/project", "runner", "imported"),
            config: Box::new(RunConfig {
                turn_directives_v1: None,
                render_tags: false,
                model: ModelHandle::new("import", "import"),
                tools: Vec::new().into(),
                tool_choice: ToolChoice::None,
                generation: GenerationConfig::default(),
                response_format: ResponseFormat::default(),
                provider_options: Value::Null,
                cache_tiers: Value::Null,
                context_epoch: 0,
                renderer_version: 0,
                frozen_render_config: Value::Null,
                auth_selection: None,
                tools_were_omitted: false,
                budget: None,
                context_limit: None,
                work_class: None,
                variant: None,
                service_tier: None,
                stop_when: Vec::new(),
                context_management: None,
            }),
            input: prompt_with_signed_thinking_from(foreign_origin()).into(),
            origin: RunOrigin::SeededFrom {
                origin_kind: SeedOriginKind::Import,
                source_ref: "cc:session-1".into(),
                origin_device_pseudonym: None,
                base_seq: None,
            },
            submission_id: None,
        },
        WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ];
    let replayed = broca_core::replay(&records).expect("replay imported episode");
    let frozen = anthropic_frozen_config(MODEL_Y);
    let mut opts = CallOptions::new(replayed.prompt);
    opts.target_model = Some(origin(MODEL_Y));
    let body: Value = serde_json::from_slice(&AnthropicMessages.render(&frozen, &opts)).unwrap();
    let messages = body["messages"].as_array().cloned().unwrap_or_default();
    assert!(
        !has_thinking_block(&messages),
        "a foreign signature carried through import replay must not reach the target model"
    );
}
