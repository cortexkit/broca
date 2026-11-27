use super::*;
use broca_provider::{
    families::AnthropicMessages,
    wire::{RefusalFallbackConfig, WireFamily},
};

pub(super) fn fallback_config(
    source: &str,
    fallback: &str,
) -> (ModelHandle, FrozenRenderConfig, FallbackScript) {
    let model = ModelHandle::new("anthropic", source);
    let fallback_model = ModelHandle::new("anthropic", fallback);
    let mut frozen = broca_provider::families::anthropic_frozen_config(source);
    frozen.refusal_fallback = Some(Box::new(RefusalFallbackConfig {
        model: fallback_model.clone(),
        context_limit: Some(100_000),
        frozen: broca_provider::families::anthropic_frozen_config(fallback),
    }));
    (
        model,
        frozen,
        FallbackScript {
            model: fallback_model,
            context_limit: Some(100_000),
        },
    )
}

pub(super) fn render(frozen: &FrozenRenderConfig, opts: &CallOptions) -> Vec<u8> {
    let frozen = frozen
        .refusal_fallback
        .as_ref()
        .filter(|fallback| opts.target_model.as_ref() == Some(&fallback.model))
        .map_or(frozen, |fallback| &fallback.frozen);
    AnthropicMessages.render(frozen, opts)
}

pub(super) async fn session_at(records: &[Record]) -> Session<MemoryBackend> {
    let mut session = Session::create(
        MemoryBackend::default(),
        SessionAddr("0123456789abcdef".into()),
        SessionTriple {
            project_root: "/project".into(),
            harness: "differential".into(),
            session: "s1".into(),
        },
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    session.append(records, true).await.unwrap();
    session
}

#[tokio::test]
async fn live_bodies_match_offline_at_same_step_and_scheduled_fallback_cuts() {
    let (model, mut frozen, fallback) = fallback_config("claude-opus-5", "claude-opus-5-5");
    frozen.anthropic_turn_system = true;
    frozen
        .refusal_fallback
        .as_mut()
        .unwrap()
        .frozen
        .anthropic_turn_system = true;
    ROUTE
        .scope(
            (model, frozen.clone()),
            FALLBACK.scope(
                Some(fallback),
                Box::pin(async {
                    let scenario = reader_scenario(vec![
                        attempted(refusal("refused"), vec![attempt(1, 200, "complete")]),
                        attempted(
                            answer(
                                "retry",
                                vec![call("c1", "read", json!({}))],
                                FinishReason::ToolCalls,
                            ),
                            vec![attempt(2, 200, "complete")],
                        ),
                        attempted(
                            text_step("scheduled", "done"),
                            vec![attempt(3, 200, "complete")],
                        ),
                    ]);
                    let base = new_source(&scenario, None, scenario.steps.clone()).await;
                    let live = new_source(
                        &scenario,
                        Some(writer_start(&base, "anthropic_tool_addition", 1)),
                        scenario.steps.clone(),
                    )
                    .await;
                    assert_eq!(live.outcome.requests.len(), 3);
                    let parts = MemoryParts {
                        parts: live.parts.clone(),
                    };
                    for ((at, sent), request) in live
                        .records
                        .iter()
                        .enumerate()
                        .filter_map(|(at, record)| match record {
                            Record::Attempt(sent) => Some((at, sent)),
                            _ => None,
                        })
                        .zip(&live.outcome.requests)
                    {
                        let session = session_at(&live.records[..=at]).await;
                        let rebuilt = broca_session_loop::run::offline_request(
                            &session,
                            &parts,
                            &bind(),
                            sent.step_id,
                        )
                        .await
                        .unwrap();
                        assert_eq!(
                            rebuilt.target_model, request.target_model,
                            "ledger cut after attempt {}",
                            sent.attempt
                        );
                        assert_eq!(
                            render(&frozen, &rebuilt),
                            render(&frozen, request),
                            "ledger cut after attempt {}",
                            sent.attempt
                        );
                    }
                    assert!(live.outcome.requests[0]
                        .prompt
                        .iter()
                        .any(|message| message.tool_addition.is_some()));
                    for request in &live.outcome.requests[1..] {
                        assert!(request
                            .prompt
                            .iter()
                            .any(|message| message.tool_addition.is_some()));
                        assert_eq!(request.tools, live.outcome.requests[0].tools);
                    }
                    assert_eq!(
                        live.outcome.requests[1].target_model,
                        live.outcome.requests[2].target_model
                    );
                }),
            ),
        )
        .await;
}
