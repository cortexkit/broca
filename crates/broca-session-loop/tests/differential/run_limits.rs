use super::*;

fn tool_step(n: usize, name: &str, reason: FinishReason) -> Vec<NormalizedEvent> {
    // The property is canonical input, not correlation ID or incoming object key order.
    let args = if n.is_multiple_of(2) {
        r#"{"a":1,"z":2}"#
    } else {
        r#"{"z":2,"a":1}"#
    };
    answer(
        &format!("a{n}"),
        vec![call(
            &format!("c{n}"),
            name,
            serde_json::from_str(args).unwrap(),
        )],
        reason,
    )
}

fn scenario(name: &str, steps: Vec<Vec<NormalizedEvent>>) -> Scenario {
    Scenario {
        input: vec![user("Run the scripted tool")],
        manifest: None,
        tools: vec![tool(name, ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps,
    }
}

fn repeat(name: &str, count: usize) -> Scenario {
    let mut steps: Vec<_> = (1..=count)
        .map(|n| tool_step(n, name, FinishReason::ToolCalls))
        .collect();
    steps.push(text_step("last", "done"));
    scenario(name, steps)
}

fn assert_error(outcome: &Outcome, code: &str) {
    assert_eq!(run_end_reason(outcome), "error");
    let end = named(outcome, "RunEnd")[0];
    assert_eq!(end["error"]["provider_code"], code);
    assert_eq!(end["error"]["class"], "permanent");
}

#[tokio::test]
async fn three_length_cuts_end_the_run_and_only_the_first_two_get_cut_results() {
    let cut = |n| {
        cut_call(
            tool_step(n, "json", FinishReason::Length),
            &format!("c{n}"),
            "json",
        )
    };
    let scenario = scenario(
        "json",
        vec![
            cut(1),
            cut(2),
            cut(3),
            text_step("last", "must not be requested"),
        ],
    );
    let source = new_source(&scenario, None, scenario.steps.clone()).await;
    let outcome = &source.outcome;
    assert_error(outcome, "output_limit");
    assert_eq!(outcome.requests.len(), 3);
    assert_eq!(
        call_outcomes(outcome),
        [
            "c1:NotDispatched:arguments_incomplete",
            "c2:NotDispatched:arguments_incomplete",
            "c3:Interrupted:",
        ]
    );
    assert!(named(outcome, "ToolIntent").is_empty());
    assert!(serde_json::to_string(&outcome.requests[1]).unwrap().contains(
        "Tool call `json` was not run: the reply hit the output limit before its arguments were complete. Re-issue it with complete arguments, and split large payloads into smaller calls."
    ));
    let kept = source
        .records
        .iter()
        .position(|record| matches!(record, Record::StepEnd(end) if end.step_id == 3))
        .unwrap()
        + 1;
    let resumed = new_from(
        &scenario,
        Some(source.cut(kept)),
        scenario.steps[3..].to_vec(),
    )
    .await;
    assert_error(&resumed, "output_limit");
    assert!(resumed.requests.is_empty());
    assert_eq!(call_outcomes(&resumed), ["c3:Interrupted:"]);
}

#[tokio::test]
async fn a_length_cut_settles_only_the_incomplete_call_and_then_continues() {
    let step = cut_call(
        answer(
            "a1",
            vec![
                call("bad", "json", json!({})),
                call("good", "json", json!({"a": 1})),
            ],
            FinishReason::Length,
        ),
        "bad",
        "json",
    );
    let stem = scenario("json", vec![step, text_step("last", "done")]);
    let outcome = new_loop(&stem).await;
    assert_eq!(run_end_reason(&outcome), "completed");
    assert_eq!(
        call_outcomes(&outcome),
        ["bad:NotDispatched:arguments_incomplete", "good:Completed:"]
    );
    let intents = named(&outcome, "ToolIntent");
    assert_eq!(intents.len(), 1);
    assert_eq!(intents[0]["tool_call_id"], "good");
}

#[tokio::test]
async fn the_third_identical_call_adds_one_model_facing_note() {
    let outcome = new_loop(&repeat("json", 3)).await;
    let note = "You called `json` with the same arguments 3 times in a row. Repeating it is unlikely to help. Change your approach: use different arguments or another tool, or answer with what you have.";
    assert!(!serde_json::to_string(&outcome.requests[2])
        .unwrap()
        .contains(note));
    assert_eq!(
        serde_json::to_string(&outcome.requests[3])
            .unwrap()
            .matches(note)
            .count(),
        1
    );
    assert!(!serde_json::to_string(&outcome.records)
        .unwrap()
        .contains(note));
}

#[tokio::test]
async fn the_twentieth_identical_call_and_result_is_not_dispatched() {
    let outcome = new_loop(&repeat("json", 20)).await;
    assert_error(&outcome, "tool_loop");
    assert_eq!(outcome.requests.len(), 20);
    assert_eq!(named(&outcome, "ToolIntent").len(), 19);
    assert_eq!(call_outcomes(&outcome).last().unwrap(), "c20:Interrupted:");
}

#[tokio::test]
async fn a_poll_with_changing_results_can_pass_twenty_identical_calls() {
    let outcome = new_loop(&repeat("loop_test_poll", 22)).await;
    assert_eq!(run_end_reason(&outcome), "completed");
    assert_eq!(named(&outcome, "ToolIntent").len(), 22);
    assert_eq!(outcome.requests.len(), 23);
    assert!(serde_json::to_string(&outcome.requests[3])
        .unwrap()
        .contains("the same arguments 3 times in a row"));
}

#[tokio::test]
async fn resume_mid_loop_preserves_requests_and_stops_at_the_same_call() {
    let scenario = repeat("json", 20);
    let source = new_source(&scenario, None, scenario.steps.clone()).await;
    let kept = source
        .records
        .iter()
        .position(
            |record| matches!(record, Record::BatchEnd(end) if end.batch_id.ends_with("-b10")),
        )
        .unwrap()
        + 1;
    let resumed = new_from(
        &scenario,
        Some(source.cut(kept)),
        scenario.steps[10..].to_vec(),
    )
    .await;
    assert_error(&resumed, "tool_loop");
    assert_eq!(named(&resumed, "ToolIntent").len(), 9);
    assert_eq!(
        serde_json::to_vec(&resumed.requests).unwrap(),
        serde_json::to_vec(&source.outcome.requests[10..]).unwrap()
    );
}
