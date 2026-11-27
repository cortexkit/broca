use super::conservative::{apply, refresh, APPLY};
use super::revision14::{body, route, signed, start};
use super::*;

const SIGNATURE: &str = "signature-floor-anchor";

fn tools() -> Vec<Tool> {
    let mut read = tool("read", ExecutionMode::Sequential);
    read.input_schema =
        serde_json::from_str(r#"{"type":"object","properties":{"path":{"type":"string"}}}"#)
            .unwrap();
    vec![read, tool("search", ExecutionMode::Sequential)]
}

async fn next(source: Option<Start>, run: &'static str, tools: Vec<Tool>, reply: &str) -> Source {
    let mut script = reader_scenario(vec![signed(reply, false)]);
    script.tools = tools;
    FRESH_RUN
        .scope(
            run,
            Box::pin(new_source(&script, source, script.steps.clone())),
        )
        .await
}

#[tokio::test]
async fn an_applied_tool_reorder_keeps_signed_thinking_but_a_definition_edit_cuts() {
    let (model, wire) = route("claude-opus-5-5", 4);
    ROUTE.scope((model, wire.clone()), Box::pin(async {
        let original = tools();
        let first = next(None, "r1", original.clone(), "floor-anchor").await;
        let before = next(Some(start(first)), "r2", original.clone(), "before").await;
        assert!(body(&wire, &before.outcome.requests[0]).contains(SIGNATURE));
        let mut reordered = original.clone();
        reordered[0].input_schema = serde_json::from_str(r#"{"properties":{"path":{"type":"string"}},"type":"object"}"#).unwrap();
        reordered.reverse();
        let after = APPLY.scope(apply(1, None), next(Some(refresh(before, 1)), "r3", reordered.clone(), "retained")).await;
        let sent: Value = serde_json::from_str(&body(&wire, &after.outcome.requests[0])).unwrap();
        assert_eq!(sent["tools"][0]["name"], "search");
        assert_eq!(sent["tools"][1]["name"], "read");
        assert!(sent.to_string().contains(SIGNATURE), "{sent}");
        assert!(after.records.iter().any(|record| matches!(record, Record::RefreshApplied(record) if record.generation == 1)));
        let expected = body(&wire, &after.outcome.requests[0]);
        let before_attempt = after.records.iter().position(|record| matches!(record, Record::Attempt(attempt) if attempt.run_id == "r3")).unwrap();
        let mut replay_script = reader_scenario(vec![signed("retained", false)]);
        replay_script.tools = reordered.clone();
        let resumed = new_source(&replay_script, Some(after.cut(before_attempt)), replay_script.steps.clone()).await;
        assert_eq!(body(&wire, &resumed.outcome.requests[0]), expected);
        let stored = historical::session_at(&after.records).await;
        let records = stored.open_log_records().await.unwrap();
        let parts = MemoryParts { parts: after.parts.clone() };
        let rebuilt = broca_session_loop::recorded_request::rebuild(&stored, &records, &parts, &bind(), "r3", Some(1), broca_session_loop::recorded_request::Through::Request).await.unwrap();
        assert_eq!(body(&wire, &rebuilt.opts), expected);
        let mut changed = reordered;
        changed[0].description = "Search by path and content".into();
        let changed = APPLY.scope(apply(2, None), next(Some(refresh(after, 2)), "r4", changed, "after-edit")).await;
        let sent = body(&wire, &changed.outcome.requests[0]);
        assert!(sent.contains("Search by path and content") && sent.contains("floor-anchor"), "{sent}");
        assert!(!sent.contains(SIGNATURE) && !sent.contains("signature-retained"), "{sent}");
    })).await;
}

#[tokio::test]
async fn an_appended_native_tool_note_keeps_top_level_tools_and_earlier_messages() {
    let (model, mut wire) = route("claude-opus-5-5", 4);
    wire.anthropic_turn_system = true;
    ROUTE
        .scope(
            (model, wire.clone()),
            Box::pin(async {
                let first = next(None, "r1", tools(), "floor-anchor").await;
                let second = next(Some(start(first)), "r2", tools(), "second").await;
                let plain: Value =
                    serde_json::from_str(&body(&wire, &second.outcome.requests[0])).unwrap();
                let note = writer_start(&second, "anthropic_tool_addition", 3)
                    .records
                    .pop()
                    .unwrap();
                let before_step = second
                    .records
                    .iter()
                    .position(
                        |record| matches!(record, Record::StepStart(step) if step.run_id == "r2"),
                    )
                    .unwrap();
                let mut records = second.records[..before_step].to_vec();
                records.push(note);
                let mut script = reader_scenario(vec![signed("second", false)]);
                script.tools = tools();
                let noted = new_source(
                    &script,
                    Some(Start {
                        records,
                        parts: second.parts,
                        snapshot: None,
                        converted: false,
                    }),
                    script.steps.clone(),
                )
                .await;
                let sent: Value =
                    serde_json::from_str(&body(&wire, &noted.outcome.requests[0])).unwrap();
                assert_eq!(sent["tools"], plain["tools"]);
                let messages = sent["messages"].as_array().unwrap();
                let earlier = plain["messages"].as_array().unwrap();
                assert_eq!(&messages[..earlier.len()], earlier.as_slice());
                assert_eq!(
                    messages.last().unwrap()["content"][0]["type"],
                    "tool_addition"
                );
                assert!(sent.to_string().contains(SIGNATURE));

                let after = APPLY
                    .scope(
                        apply(2, None),
                        next(Some(refresh(noted, 2)), "r3", tools(), "after-apply"),
                    )
                    .await;
                let sent = body(&wire, &after.outcome.requests[0]);
                assert!(!sent.contains("tool_addition"));
                assert!(
                    !sent.contains(SIGNATURE) && !sent.contains("signature-second"),
                    "{sent}"
                );
            }),
        )
        .await;
}

#[tokio::test]
async fn unreadable_apply_note_evidence_cuts_without_refusing_the_send() {
    let (model, wire) = route("claude-opus-5-5", 4);
    ROUTE
        .scope(
            (model, wire.clone()),
            Box::pin(async {
                let first = next(None, "r1", tools(), "floor-anchor").await;
                let after = APPLY_NOTE_READ_FAILS
                    .scope(
                        (),
                        APPLY.scope(
                            apply(1, None),
                            next(Some(refresh(first, 1)), "r2", tools(), "sent"),
                        ),
                    )
                    .await;
                assert_eq!(after.outcome.requests.len(), 1);
                let sent = body(&wire, &after.outcome.requests[0]);
                assert!(
                    sent.contains("floor-anchor") && !sent.contains(SIGNATURE),
                    "{sent}"
                );
                assert_eq!(
                    after.outcome.requests[0].render_episodes.signed_floor,
                    Some(3)
                );
            }),
        )
        .await;
}
