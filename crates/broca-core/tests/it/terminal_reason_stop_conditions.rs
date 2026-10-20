//! Real-loop WAL cuts preserve the admitted stop policy and its first-match ordering.

use broca_core::{
    replay, run_loop, AllowAll, FileLeaseStore, FileWalStore, ReplayResolution, RunRequest,
    StubProvider, StubToolPlane, WalStore,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall},
    provider::{NormalizedEvent, ProviderControl},
    request::{GenerationConfig, ModelHandle, StopCondition, ToolChoice},
    BindIdentity, FinishReason, MessageId, RunId, ToolCallId, Usage,
};
use broca_wal::WalRecord;
use tokio::sync::mpsc;

async fn live_records(
    stop_when: Vec<StopCondition>,
    tool_turn: bool,
) -> (RunFinishReason, Vec<WalRecord>) {
    // A counter, not the clock: the tests run in parallel and the clock's resolution is coarse
    // enough on macOS that two of them got the same directory, so one found the other's lease
    // held (`LeaseHeld("s1")`).
    static NEXT_DIR: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-stop-policy-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let session = BindIdentity::new(&dir, "terminal-reason", "s1");
    let request = RunRequest {
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
        run_id: RunId("r1".into()),
        session: session.clone(),
        model: ModelHandle::new("stub", "m"),
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "Say done.".into(),
            }],
        )],
        tools: vec![],
        policy_allowed: Default::default(),
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when,
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    };
    let content = if tool_turn {
        vec![ContentBlock::ToolCall(ToolCall {
            tool_call_id: ToolCallId("t1".into()),
            tool_name: "finish".into(),
            input: serde_json::json!({}),
            thought_signature: None,
        })]
    } else {
        vec![ContentBlock::Text {
            text: "done".into(),
        }]
    };
    let provider = StubProvider::new(vec![vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m1".into()),
                content,
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: if tool_turn {
                FinishReason::ToolCalls
            } else {
                FinishReason::Stop
            },
            retries_used: 0,
        }),
    ]]);
    let wal = FileWalStore::new(dir.join("wal"));
    let lease = FileLeaseStore::new(dir.join("lease"));
    let (tx, mut rx) = mpsc::channel(64);
    let collector = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = run_loop(
        request,
        &provider,
        &StubToolPlane::new(),
        &lease,
        &AllowAll,
        &wal,
        &tx,
    )
    .await
    .expect("uninterrupted run");
    drop(tx);
    collector.await.unwrap();
    let records = wal
        .read_records(&session)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    std::fs::remove_dir_all(dir).unwrap();
    assert_eq!(summary.steps, 1);
    (summary.reason, records)
}

fn assert_terminal_cut(records: &[WalRecord], expected: RunFinishReason) {
    assert!(
        matches!(records.last(), Some(WalRecord::RunFinished { reason, .. }) if *reason == expected)
    );
    let cut = &records[..records.len() - 1];
    assert!(!cut
        .iter()
        .any(|record| matches!(record, WalRecord::RunFinished { .. })));
    assert!(cut
        .iter()
        .any(|record| matches!(record, WalRecord::ModelStepFinished { .. })));
    for prefix in [records, cut] {
        assert_eq!(
            replay(prefix).unwrap().resolution,
            ReplayResolution::LineageIdle {
                last_reason: expected
            }
        );
    }
}

#[tokio::test]
async fn step_count_and_ordered_stop_conditions_preserve_live_reason_at_terminal_cut() {
    use StopCondition::{LoopFinished, StepCount};
    for (conditions, expected) in [
        (vec![StepCount { n: 1 }], RunFinishReason::MaxSteps),
        (vec![StepCount { n: 2 }], RunFinishReason::Completed),
        (
            vec![StepCount { n: 1 }, LoopFinished],
            RunFinishReason::MaxSteps,
        ),
        (
            vec![LoopFinished, StepCount { n: 1 }],
            RunFinishReason::Completed,
        ),
    ] {
        let (live, records) = live_records(conditions.clone(), false).await;
        // Independent expected reasons keep a shared evaluator bug from making both paths agree wrongly.
        assert_eq!(live, expected, "{conditions:?}");
        assert_terminal_cut(&records, live);
    }
}

#[tokio::test]
async fn loop_finished_preserves_live_completed_at_terminal_cut() {
    let (live, records) = live_records(vec![StopCondition::LoopFinished], false).await;
    assert_eq!(live, RunFinishReason::Completed);
    assert_terminal_cut(&records, live);
}

#[tokio::test]
async fn has_tool_call_named_and_any_preserve_live_completed_at_terminal_cut() {
    for name in [None, Some("finish".into())] {
        // Text-only natural ends cannot satisfy HasToolCall. Cover both that fallback and
        // a matching tool turn, whose existing hard-stop repair batch remains authoritative.
        for tool_turn in [false, true] {
            let (live, records) = live_records(
                vec![StopCondition::HasToolCall { name: name.clone() }],
                tool_turn,
            )
            .await;
            assert_eq!(live, RunFinishReason::Completed);
            assert_terminal_cut(&records, live);
        }
    }
}

#[tokio::test]
async fn no_stop_when_preserves_live_completed_and_legacy_missing_field_at_terminal_cut() {
    let (live, records) = live_records(vec![], false).await;
    assert_eq!(live, RunFinishReason::Completed);
    assert_terminal_cut(&records, live);

    let mut legacy = records.clone();
    let config = legacy
        .iter_mut()
        .find_map(|record| match record {
            WalRecord::RunStarted { config, .. } => Some(config),
            _ => None,
        })
        .unwrap();
    let mut value = serde_json::to_value(&**config).unwrap();
    value.as_object_mut().unwrap().remove("stop_when");
    assert!(value.get("stop_when").is_none());
    **config = serde_json::from_value(value).unwrap();
    assert!(config.stop_when.is_empty());
    assert_terminal_cut(&legacy, RunFinishReason::Completed);
}
