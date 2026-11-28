use super::*;
use broca_protocol::ContentBlock;
use broca_provider::{
    families::{anthropic_apikey_frozen_config, AnthropicMessages},
    policy::{CrossModelThinking, ReasoningPolicy},
    wire::{FrozenRenderConfig, WireFamily},
};

const SIG: &str = "reasoning-from-the-old-prefix";

fn signed(id: &str, text: Option<&str>) -> Vec<NormalizedEvent> {
    let mut step = text_step(id, text.unwrap_or_default());
    for event in &mut step {
        if let NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) = event {
            if text.is_none() {
                message.content.clear();
            }
            message.content.insert(
                0,
                ContentBlock::Reasoning {
                    text: "Earlier thinking".into(),
                    signature: Some(SIG.into()),
                },
            );
        }
    }
    step
}

fn config() -> FrozenRenderConfig {
    let mut frozen = anthropic_apikey_frozen_config("claude-opus-5-5");
    frozen.renderer_version = 4;
    frozen.anthropic_turn_system = true;
    frozen.reasoning = ReasoningPolicy::AnthropicInterleaved {
        budget_tokens: 1024,
        last_turn_only: true,
        echo_scope: broca_provider::policy::ThinkingEchoScope::AllAssistantMessages,
        cross_model: CrossModelThinking::SameModelOnly,
    };
    frozen
}

fn on_route(steps: Vec<Vec<NormalizedEvent>>, frozen: &FrozenRenderConfig) -> LoopExecutor {
    let mut executor = executor(steps, None);
    executor.provider.render_config = Some(serde_json::to_value(frozen).unwrap());
    executor
}

async fn flush(module: &Module) {
    module
        .registry
        .session_flush_prefix(
            &module.key,
            Some(&core()),
            &SendScope::default(),
            "rebuild".into(),
        )
        .await
        .unwrap();
}

fn body(frozen: &FrozenRenderConfig, opts: &broca_protocol::CallOptions) -> serde_json::Value {
    AnthropicMessages.shape(frozen, opts)
}

async fn apply_without_baseline(seal: bool, incomplete_attempt: bool) {
    let first = manifest(&plan("old instructions", &["read"], None, None), "plan-1");
    let pending = manifest(&plan("new instructions", &["read"], None, None), "plan-2");
    let frozen = config();
    // The old log carries completed assistant messages but has no Attempt evidence.
    let mut first_reply = text_step("a1", "ready");
    if incomplete_attempt {
        first_reply = confirmed(first_reply);
    }
    let (module, executor) = module_with_executor(
        on_route(
            vec![
                first_reply,
                signed("a2", Some("noted")),
                text_step("a3", "done"),
            ],
            &frozen,
        ),
        true,
        true,
    )
    .await;
    send_claude(&module, "r1", Some(&first)).await;
    refresh(&module, 1, pending.clone(), Some(NOTE)).await;
    send_claude(&module, "r2", None).await;
    if incomplete_attempt {
        let mut attempt = records(&module)
            .await
            .into_iter()
            .find_map(|record| match record {
                Record::Attempt(attempt) => Some(attempt),
                _ => None,
            })
            .unwrap();
        // An old provider receipt survived without its step-start record. It cannot be
        // rebuilt but must not be used for admitting the next send.
        attempt.step_id = 99;
        let mut session = module.load().await;
        session
            .append(&[Record::Attempt(attempt)], true)
            .await
            .unwrap();
    } else {
        assert!(records(&module)
            .await
            .iter()
            .all(|record| !matches!(record, Record::Attempt(_))));
    }
    if seal {
        let mut session = module.load().await;
        session.seal(None, 0).await.unwrap();
        drop(session);
    }
    flush(&module).await;
    send_claude(&module, "r3", Some(&pending)).await;
    let requests = executor.provider.inner.calls();
    assert_eq!(requests.len(), 3);
    let sent = body(&frozen, &requests[2]);
    assert!(sent["system"].to_string().contains("new instructions"));
    assert!(!sent["system"].to_string().contains("old instructions"));
    assert!(!sent.to_string().contains(SIG));
    assert!(requests[2].render_episodes.signed_floor.is_some());
}

#[tokio::test]
async fn a_text_only_full_apply_needs_no_attempt_baseline() {
    apply_without_baseline(false, false).await;
}

#[tokio::test]
async fn a_text_only_full_apply_needs_no_attempt_baseline_after_a_seal() {
    apply_without_baseline(true, false).await;
}

#[tokio::test]
async fn a_text_only_full_apply_sends_even_if_the_old_attempt_cannot_be_rebuilt() {
    apply_without_baseline(false, true).await;
}
async fn apply_off_route(addition: bool) {
    let first = manifest(&plan("before", &["read"], None, None), "plan-1");
    let pending = if addition {
        manifest(&plan("before", &["read", "search"], None, None), "plan-2")
    } else {
        manifest(&plan("after", &["read"], None, None), "plan-2")
    };
    let frozen = config();
    let (module, executor) = module_with_executor(
        on_route(
            vec![
                confirmed(text_step("a1", "ready")),
                confirmed(signed("a2", Some("signed after the note"))),
                confirmed(text_step("a3", "off route")),
                text_step("a4", "back"),
            ],
            &frozen,
        ),
        true,
        true,
    )
    .await;
    send_claude(&module, "r1", Some(&first)).await;
    refresh(&module, 1, pending.clone(), (!addition).then_some(NOTE)).await;
    send_claude(&module, "r2", None).await;
    let original = body(&frozen, &executor.provider.inner.calls()[1]);
    assert_eq!(original.to_string().contains("tool_addition"), addition);
    assert_eq!(original.to_string().contains(NOTE), !addition);

    flush(&module).await;
    let away = openai_chat_frozen_config("gpt-4o");
    *executor.next_render_config.lock().unwrap() = Some(serde_json::to_value(away).unwrap());
    let mut request = request("r3");
    request.model = ModelHandle::new("openai", "gpt-4o");
    send_request(&module, "r3", None, &mut request).await;
    send_claude(&module, "r4", Some(&pending)).await;
    let calls = executor.provider.inner.calls();
    assert_eq!(calls.len(), 4);
    assert_eq!(
        calls[2].target_model.as_ref().unwrap().provider_module_id,
        "openai"
    );
    let returned = body(&frozen, &calls[3]);
    assert!(returned["messages"]
        .to_string()
        .contains("signed after the note"));
    assert!(!returned.to_string().contains(SIG));
    assert!(records(&module)
        .await
        .iter()
        .any(|record| matches!(record, Record::RefreshApplied(apply) if apply.generation == 1)));
}

#[tokio::test]
async fn addition_apply_off_route_strips_old_signatures_when_claude_returns() {
    apply_off_route(true).await;
}

#[tokio::test]
async fn text_apply_off_route_strips_old_signatures_when_claude_returns() {
    apply_off_route(false).await;
}

#[tokio::test]
async fn a_plain_full_apply_derives_a_floor_without_a_new_ledger_field() {
    let first = manifest(&plan("before", &["read"], None, None), "plan-1");
    let pending = manifest(&plan("after", &["read"], None, None), "plan-2");
    let frozen = config();
    let (module, executor) = module_with_executor(
        on_route(
            vec![signed("a1", Some("old reply")), text_step("a2", "done")],
            &frozen,
        ),
        true,
        false,
    )
    .await;
    send_claude(&module, "r1", Some(&first)).await;
    module
        .registry
        .route_session_refresh(
            &module.key,
            1,
            "immediately".into(),
            Box::new(pending),
            vec![],
            None,
        )
        .await
        .unwrap();
    send_claude(&module, "r2", None).await;
    let calls = executor.provider.inner.calls();
    let sent = body(&frozen, &calls[1]);
    assert!(sent["messages"].to_string().contains("old reply"));
    assert!(sent["system"].to_string().contains("after"));
    assert!(!sent.to_string().contains(SIG));
    assert!(records(&module)
        .await
        .iter()
        .any(|record| matches!(record, Record::RefreshApplied(apply) if apply.generation == 1)));
}
