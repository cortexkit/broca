//! A terminal reason must not depend on whether recovery seals the final model step.

use broca_core::{
    replay, run_loop, AllowAll, FileLeaseStore, FileWalStore, ReplayResolution, RunRequest,
    StubProvider, StubToolPlane, WalStore,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role},
    provider::{NormalizedEvent, ProviderControl},
    request::{GenerationConfig, ModelHandle, StopCondition, ToolChoice},
    BindIdentity, FinishReason, MessageId, RunId, Usage,
};
use broca_wal::WalRecord;
use tokio::sync::mpsc;

#[tokio::test]
async fn replay_before_run_finished_preserves_live_max_steps_reason_for_text_only_step() {
    let dir = broca_test_support::scratch_root().join(format!(
        "broca-terminal-reason-crash-cut-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let session = BindIdentity::new(&dir, "terminal-reason", "s1");
    let request = RunRequest {
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
        stop_when: vec![StopCondition::StepCount { n: 1 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
    };
    let provider = StubProvider::new(vec![vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::Text {
                    text: "done".into(),
                }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
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
    let records: Vec<_> = wal
        .read_records(&session)
        .expect("read the real loop's WAL")
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    std::fs::remove_dir_all(&dir).unwrap();

    assert_eq!(summary.steps, 1);
    assert_eq!(summary.reason, RunFinishReason::MaxSteps);
    assert!(matches!(
        records.last(),
        Some(WalRecord::RunFinished {
            reason: RunFinishReason::MaxSteps,
            ..
        })
    ));
    assert_eq!(
        replay(&records).expect("uncut replay").resolution,
        ReplayResolution::LineageIdle {
            last_reason: summary.reason,
        }
    );

    // Keep the actual durable prefix, not a hand-built proxy for the loop's records.
    // A text-only response also hits the step cap here. Losing only RunFinished must
    // not change the caller-visible reason from MaxSteps to a natural Completed stop.
    let cut = &records[..records.len() - 1];
    assert!(!cut
        .iter()
        .any(|record| matches!(record, WalRecord::RunFinished { .. })));
    assert!(matches!(
        cut.last(),
        Some(WalRecord::ModelStepFinished { assistant_message, .. })
            if assistant_message.tool_calls().next().is_none()
    ));
    assert_eq!(
        replay(cut).expect("replay before terminal").resolution,
        ReplayResolution::LineageIdle {
            last_reason: summary.reason,
        },
        "a crash after the text-only step must preserve the uninterrupted terminal reason"
    );
}
