use super::*;
use broca_protocol::content::{ToolCall, ToolOutput, ToolResult};
use broca_session_store::record::{Assistant, BatchStart, Input, StepEnd};

fn multi_message_batch() -> Vec<Record> {
    vec![
        Record::Input(Input {
            run_id: "synthetic-run".into(),
            placement: Placement::RunInput,
            message: std::sync::Arc::new(Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: "input".into(),
                }],
            )),
            written_at_ms: Some(10),
            submission_id: None,
            notice: None,
            tags_v1: None,
            receipt_seq: None,
            media: Vec::new(),
            pruned: Vec::new(),
        }),
        Record::Assistant(Assistant {
            run_id: "synthetic-run".into(),
            step_id: 4,
            provider_message_id: broca_protocol::MessageId("provider-id-not-mid".into()),
            message: std::sync::Arc::new(Message::new(
                Role::Assistant,
                vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("call".into()),
                    tool_name: "read".into(),
                    input: serde_json::json!({}).into(),
                })],
            )),
            usage: None,
            written_at_ms: Some(11),
            last_content_at_ms: None,
            tags_v1: None,
            budget_finalize: None,
            pruned: Vec::new(),
        }),
        Record::StepEnd(StepEnd {
            run_id: "synthetic-run".into(),
            step_id: 4,
            usage: None,
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
            call_count: 1,
            fallback: None,
            hooks: None,
            budget_finalize: None,
            total_usage: None,
            stop_met: None,
            limiting_stop_met: None,
            by_model: Vec::new(),
        }),
        Record::BatchStart(BatchStart {
            run_id: "synthetic-run".into(),
            batch_id: "batch".into(),
            step_id: 4,
            planned: vec![ToolCallId("call".into())],
            mid: Some("results-id".into()),
        }),
        Record::ToolReply(ToolReply {
            run_id: "synthetic-run".into(),
            batch_id: Some("batch".into()),
            result: ToolResult {
                tool_call_id: ToolCallId("call".into()),
                output: ToolOutput::Text {
                    text: "result".into(),
                },
                is_error: false,
            },
            finished_at_ms: Some(12),
            tags_v1: None,
            media: Vec::new(),
        }),
    ]
}

#[test]
fn message_ids_pair_only_the_assistant_with_its_final_entry_facts() {
    let batch = multi_message_batch();
    let conversation = Conversation::new(false);
    let operations = Operations::default();
    let calls = OpenCalls::default();
    let at = Placing {
        conversation: &conversation,
        operations: &operations,
        calls: &calls,
        first_seq: 10,
        prefix: "gsynthetic",
        session: "synthetic-session",
        tags: Tags {
            entries: true,
            ..Default::default()
        },
        view: None,
        rich_ui: None,
        runner: None,
    };
    let (records, messages) = with_facts_and_messages(batch, &at).unwrap();
    let facts: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            Record::EntryFacts(facts) => Some(facts),
            _ => None,
        })
        .collect();
    assert_eq!(facts.len(), 3);
    assert_eq!(
        facts
            .iter()
            .map(|facts| (facts.ordinal, facts.mid.as_str()))
            .collect::<Vec<_>>(),
        vec![
            (1, "gsynthetic.11"),
            (2, "gsynthetic.14"),
            (3, "results-id")
        ]
    );
    let assistant = facts.iter().find(|facts| facts.ordinal == 2).unwrap();
    assert_eq!(
        messages,
        vec![PlacedMessage {
            run_id: "synthetic-run".into(),
            step_id: 4,
            mid: assistant.mid.clone(),
        }]
    );
    assert!(
        !assistant.mid.is_empty(),
        "flush must have minted the final id"
    );
    assert_ne!(assistant.mid, facts[0].mid);
    assert_ne!(assistant.mid, facts[2].mid);
    assert_ne!(assistant.mid, "provider-id-not-mid");
}

#[test]
fn message_ids_copy_rebased_facts_not_a_converted_steps_earlier_announcement() {
    use broca_session_store::fold::operations::Run;
    use broca_session_store::record::StepStart;
    let mut batch = multi_message_batch();
    let mut conversation = Conversation::new(false);
    let seed = with_facts(
        vec![batch[0].clone()],
        &Placing {
            conversation: &conversation,
            operations: &Operations::default(),
            calls: &OpenCalls::default(),
            first_seq: 1,
            prefix: "gseed",
            session: "synthetic-session",
            tags: Tags {
                entries: true,
                ..Default::default()
            },
            view: None,
            rich_ui: None,
            runner: None,
        },
    )
    .unwrap();
    for record in seed {
        conversation.apply(&record).unwrap();
    }
    let operations = Operations {
        run: Some(Run {
            run_id: "synthetic-run".into(),
            finished: None,
            paused: None,
            seeded: false,
            completed_steps: 0,
            tool_call_count: 0,
            usage: Default::default(),
            retries_used: 0,
            last_finish_reason: None,
            natural_end: None,
            open_batch: None,
            unknown: Vec::new(),
            hard_stopped: None,
            pending_steers: Vec::new(),
            cancel: None,
            budget: Default::default(),
            last_step_usage: None,
            provider_call_started: false,
            account_limit_pause: None,
            context_ceiling: None,
            output_cap: None,
            structured_downgraded: false,
            refrozen_render: None,
            refrozen_auth: None,
            refused_step: None,
            step_request: None,
            step_attempt: None,
            end_error: None,
            end_usage: None,
            step_mid: Some("m1".into()),
            warms: Vec::new(),
            keep_warm: None,
            on_restart: None,
            start: None,
        }),
        ..Default::default()
    };
    // The converted step reserved zero-based position 1; a delivery takes it before retry.
    if let Record::Input(input) = &mut batch[0] {
        input.placement = Placement::Delivered;
    }
    batch.insert(
        1,
        Record::StepStart(StepStart {
            run_id: "synthetic-run".into(),
            step_id: 4,
            mid: None,
        }),
    );
    let calls = OpenCalls::default();
    let (records, messages) = with_facts_and_messages(
        batch,
        &Placing {
            conversation: &conversation,
            operations: &operations,
            calls: &calls,
            first_seq: 10,
            prefix: "gsynthetic",
            session: "synthetic-session",
            tags: Tags {
                entries: true,
                ..Default::default()
            },
            view: None,
            rich_ui: None,
            runner: None,
        },
    )
    .unwrap();
    let facts: Vec<_> = records
        .iter()
        .filter_map(|record| match record {
            Record::EntryFacts(facts) => Some(facts),
            _ => None,
        })
        .collect();
    assert_eq!(
        facts.iter().find(|facts| facts.ordinal == 2).unwrap().mid,
        "m1"
    );
    let served = facts.iter().find(|facts| facts.ordinal == 3).unwrap();
    assert_eq!(served.mid, "m2");
    assert_eq!(
        messages,
        vec![PlacedMessage {
            run_id: "synthetic-run".into(),
            step_id: 4,
            mid: served.mid.clone(),
        }]
    );
    assert_ne!(
        messages[0].mid,
        operations
            .run
            .as_ref()
            .unwrap()
            .step_mid
            .as_deref()
            .unwrap()
    );
}
