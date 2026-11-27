use super::*;
use broca_protocol::content::EncryptedReasoningFormat;
use broca_provider::families::{
    openai_responses_chatgpt_frozen_config, openai_responses_frozen_config, OpenAiResponses,
    OpenAiResponsesChatgpt,
};
use broca_provider::wire::{FrozenRenderConfig, WireFamily};

tokio::task_local! {
    pub(super) static CONFIG: FrozenRenderConfig;
}

fn reasoning(id: &str, format: EncryptedReasoningFormat) -> ContentBlock {
    ContentBlock::RedactedReasoning {
        data: json!({"type": "reasoning", "id": id, "encrypted_content": format!("cipher:{id}")})
            .to_string()
            .into(),
        provider_format: Some(format),
    }
}

fn with_report(step: Vec<NormalizedEvent>, ids: &[&str]) -> Vec<NormalizedEvent> {
    let mut event = attempt(0, 200, "complete");
    let NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) = &mut event else {
        unreachable!();
    };
    attempt.input_transformations = Some(
        ids.iter()
            .map(|id| broca_protocol::InputTransformation {
                kind: Some("reasoning".into()),
                id: Some((*id).into()),
                action: Some("removed".into()),
                reason: Some("prefix_mismatch".into()),
                path: None,
            })
            .collect(),
    );
    attempted(step, vec![event])
}

fn warn_steps(logs: &[broca_test_support::LogEvent]) -> Vec<&str> {
    logs.iter()
        .filter(|event| {
            event.target == "provider"
                && event.level == tracing::Level::WARN
                && event
                    .message()
                    .starts_with("provider transformed the request input")
        })
        .map(|event| event.fields["step"].as_str())
        .collect()
}

#[tokio::test]
async fn responses_reasoning_removals_take_effect_on_next_step_and_after_actor_restart() {
    for (config, family, format) in [
        (
            openai_responses_frozen_config("gpt-5"),
            &OpenAiResponses as &dyn WireFamily,
            EncryptedReasoningFormat::OpenAiResponses,
        ),
        (
            openai_responses_chatgpt_frozen_config("gpt-5"),
            &OpenAiResponsesChatgpt as &dyn WireFamily,
            EncryptedReasoningFormat::OpenAiResponsesChatgpt,
        ),
    ] {
        let cipher = |index: &str| format!("cipher:rs_{index}");
        let render = |opts: &CallOptions| String::from_utf8(family.render(&config, opts)).unwrap();
        let text = |text: &str| ContentBlock::Text { text: text.into() };
        let read_step = |id: &str| {
            answer(
                id,
                vec![call(id, "read", json!({"path": id}))],
                FinishReason::ToolCalls,
            )
        };
        let scenario = Scenario {
            input: vec![user("read the files")],
            tools: vec![tool("read", ExecutionMode::Sequential)],
            manifest: None,
            stop_when: vec![],
            steps: vec![
                answer(
                    "a1",
                    vec![
                        text("left"),
                        reasoning("rs_1", format),
                        text("right"),
                        reasoning("rs_2", format),
                        call("c1", "read", json!({"path": "first"})),
                    ],
                    FinishReason::ToolCalls,
                ),
                with_report(read_step("a2"), &["rs_1", "rs_1"]),
                with_report(read_step("a3"), &["rs_1"]),
                with_report(read_step("a4"), &["rs_1", "rs_2"]),
                text_step("a5", "done"),
            ],
        };
        let run = |start, steps| {
            ACTOR.scope(
                true,
                CONFIG.scope(
                    config.clone(),
                    Box::pin(new_source(&scenario, start, steps)),
                ),
            )
        };
        let (source, logs) =
            broca_test_support::capture_logs(run(None, scenario.steps.clone())).await;
        let requests = &source.outcome.requests;
        assert_eq!(requests.len(), 5);
        assert!(
            render(&requests[1]).contains(&cipher("1")),
            "the reported item was sent"
        );
        assert!(render(&requests[1]).contains(&cipher("2")));
        for request in &requests[2..4] {
            let rendered = render(request);
            assert!(
                !rendered.contains(&cipher("1")),
                "a reported item must not be echoed: {rendered}"
            );
            assert!(
                rendered.contains(&cipher("2")),
                "the unreported item is still echoed"
            );
            let mut without_report = request.clone();
            Arc::make_mut(&mut without_report.render_episodes)
                .removed_responses_reasoning
                .clear();
            let baseline = render(&without_report);
            let parsed: Value = serde_json::from_str(&baseline).unwrap();
            let removed = parsed["input"]
                .as_array()
                .unwrap()
                .iter()
                .find(|item| item["encrypted_content"] == cipher("1"))
                .unwrap();
            let fragment = format!("{removed},");
            assert_eq!(baseline.matches(&fragment).count(), 1);
            assert_eq!(
                rendered,
                baseline.replacen(&fragment, "", 1),
                "the reasoning echo is the only changed bytes"
            );
        }
        assert!(!render(&requests[4]).contains("cipher:rs_"));
        assert_eq!(warn_steps(&logs), ["2", "4"]);

        let restart = source
            .records
            .iter()
            .position(|r| matches!(r, Record::StepStart(start) if start.step_id == 3))
            .unwrap();
        let cut = source.cut(restart);
        assert!(
            cut.snapshot.is_some(),
            "resume must reopen the actor's stored session"
        );
        let (restarted, logs) =
            broca_test_support::capture_logs(run(Some(cut), scenario.steps[2..].to_vec())).await;
        assert_eq!(
            restarted
                .outcome
                .requests
                .iter()
                .map(render)
                .collect::<Vec<_>>(),
            requests[2..].iter().map(render).collect::<Vec<_>>()
        );
        assert_eq!(
            warn_steps(&logs),
            ["4"],
            "the restarted actor already knows the earlier removal"
        );
    }
}
