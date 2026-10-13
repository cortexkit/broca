//! The resume close-out, on the shared harness.
//!
//! A resume that finds a tool call whose intent is durable and whose result is not (it may or
//! may not have run) never sends it again and makes no model call. It writes an
//! `outcome_unknown` result for it, commits the batch, and seals the episode Interrupted. The
//! next send to the session (a new episode with a new run id and input) then renders the call
//! paired with that result.

mod scenarios;

use broca_core::{outcome_unknown_result, FileWalStore, RunControl};
use broca_crash_harness::{
    drive, HarnessPlane, InFlight, KillPoint, KillReport, KillTrigger, LogImage, Render,
    RunnerHandle, ScriptedProvider, Suite,
};
use broca_protocol::{
    caller::RunFinishReason,
    content::{AssistantMessage, ContentBlock, Message, Role},
    provider::{NormalizedEvent, ProviderControl},
    request::CallOptions,
    FinishReason, MessageId, RunId, ToolCallId, Usage,
};
use broca_wal::{DispatchOutcome, WalRecord};

broca_crash_harness::crash_harness_child!(scenarios::by_name);

fn t(id: &str) -> ToolCallId {
    ToolCallId(id.into())
}

/// The record count just after the dispatch intent of `call` in the reference log.
fn after_intent_of(handle: &RunnerHandle, call: &str) -> usize {
    handle
        .reference
        .log
        .records()
        .iter()
        .position(|record| {
            matches!(record, WalRecord::ToolDispatchIntent { tool_call_id, .. } if tool_call_id.0 == call)
        })
        .expect("the reference run records the intent")
        + 1
}

async fn kill_after_intent_of(handle: &RunnerHandle, call: &str) -> KillReport {
    handle
        .kill_at(
            KillPoint::RecordBoundary {
                records: after_intent_of(handle, call),
                torn: false,
            },
            KillTrigger::Truncate,
        )
        .await
}

/// Each tool result written after the first `from` records: (call, outcome).
fn results_after(records: &[WalRecord], from: usize) -> Vec<(String, DispatchOutcome)> {
    records[from..]
        .iter()
        .filter_map(|record| match record {
            WalRecord::ToolResult {
                result, outcome, ..
            } => Some((result.tool_call_id.0.clone(), *outcome)),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn a_resume_with_an_indeterminate_call_writes_outcome_unknown_seals_interrupted_and_calls_no_provider(
) {
    let handle = RunnerHandle::new(scenarios::TWO_EDITS).await;

    // After both intents were synced: both calls may have run.
    let kill = kill_after_intent_of(&handle, "t2").await;
    assert_eq!(kill.after_sync, Some(true), "{}", kill.label());
    let resumed = handle.resume(&kill).await;
    handle.check(&kill, &resumed);
    assert_eq!(resumed.reason, RunFinishReason::Interrupted);
    assert_eq!(
        resumed.provider.request_count(),
        0,
        "the close-out makes no model call"
    );
    assert!(
        resumed.plane.dispatched().is_empty(),
        "nothing is sent again"
    );
    let prefix = kill.durable.record_count();
    let records = resumed.log.records();
    assert!(
        matches!(
            &records[prefix..],
            [
                WalRecord::ResumeStarted { indeterminate, .. },
                WalRecord::ToolResult { .. },
                WalRecord::ToolResult { .. },
                WalRecord::ToolBatchFinished { .. },
                WalRecord::TurnFinished { .. },
                WalRecord::RunFinished {
                    reason: RunFinishReason::Interrupted,
                    ..
                },
            ] if indeterminate == &vec![t("t1"), t("t2")]
        ),
        "resume record, the results, the batch commit, then the seal: {:#?}",
        &records[prefix..]
    );
    assert_eq!(
        results_after(&records, prefix),
        vec![
            ("t1".into(), DispatchOutcome::Unknown),
            ("t2".into(), DispatchOutcome::Unknown)
        ]
    );
    for record in &records[prefix..] {
        if let WalRecord::ToolResult { result, .. } = record {
            assert_eq!(**result, outcome_unknown_result(&result.tool_call_id));
            assert!(result.is_error);
        }
    }

    // Inside the intents' barrier: only t1's intent survived, so t2 never left the process. It
    // is closed as interrupted, never dispatched, and only t1 is indeterminate.
    let kill = kill_after_intent_of(&handle, "t1").await;
    assert_eq!(kill.after_sync, Some(false), "{}", kill.label());
    let resumed = handle.resume(&kill).await;
    handle.check(&kill, &resumed);
    assert_eq!(resumed.provider.request_count(), 0);
    assert!(resumed.plane.dispatched().is_empty());
    let prefix = kill.durable.record_count();
    assert_eq!(
        results_after(&resumed.log.records(), prefix),
        vec![
            ("t1".into(), DispatchOutcome::Unknown),
            ("t2".into(), DispatchOutcome::Interrupted)
        ]
    );
}

#[tokio::test]
async fn the_next_send_renders_the_call_paired_with_its_outcome_unknown_result_and_the_provider_accepts_it(
) {
    let handle = RunnerHandle::new(scenarios::TWO_EDITS).await;
    let kill = kill_after_intent_of(&handle, "t2").await;
    let resumed = handle.resume(&kill).await;
    handle.check(&kill, &resumed);

    // The owner's next send: a new episode with a new run id and new input.
    let mut next = scenarios::edit_request();
    next.run_id = RunId("r2".into());
    next.input = vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "continue after review".into(),
        }],
    )];
    let provider = ScriptedProvider::new(
        vec![vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: MessageId("m-next".into()),
                    content: vec![ContentBlock::Text {
                        text: "continued".into(),
                    }],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: FinishReason::Stop,
                retries_used: 0,
            }),
        ]],
        Render::Canonical,
    );
    let plane = HarnessPlane::new(scenarios::edit_plane());
    let outcome = drive(
        next,
        RunControl::default(),
        &kill.state_root,
        &FileWalStore::new(kill.state_root.join("wal")),
        &provider,
        &plane,
    )
    .await;
    assert_eq!(outcome.reason, RunFinishReason::Completed);
    // The scripted provider refuses a prompt holding a tool call with no matching result, as
    // Anthropic refuses a `tool_use` with no `tool_result` after it.
    assert!(provider.rejected().is_empty(), "{:?}", provider.rejected());
    assert_eq!(provider.request_count(), 1);
    assert!(
        plane.dispatched().is_empty(),
        "the next send re-sends nothing"
    );

    let sent = provider.sent();
    let opts: CallOptions = serde_json::from_slice(&sent[&0]).expect("canonical request bytes");
    assert_eq!(
        opts.prompt.iter().map(|m| m.role).collect::<Vec<_>>(),
        vec![Role::User, Role::Assistant, Role::Tool, Role::User]
    );
    let calls: Vec<&ToolCallId> = opts.prompt[1]
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(&call.tool_call_id),
            _ => None,
        })
        .collect();
    assert_eq!(calls, vec![&t("t1"), &t("t2")]);
    let results: Vec<_> = opts.prompt[2]
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolResult(result) => Some(result.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        results,
        vec![
            outcome_unknown_result(&t("t1")),
            outcome_unknown_result(&t("t2"))
        ],
        "each call is paired with the result the close-out wrote"
    );

    // The new episode's log still holds exactly one terminal per run and one result per call.
    let log = LogImage::read(&broca_crash_harness::wal_path(
        &kill.state_root,
        &scenarios::session(),
    ));
    let terminals = log
        .records()
        .iter()
        .filter(|record| matches!(record, WalRecord::RunFinished { .. }))
        .count();
    assert_eq!(terminals, 2);
}

#[tokio::test]
async fn a_cut_between_the_outcome_unknown_results_and_the_seal_resumes_without_duplicates() {
    let handle = RunnerHandle::new(scenarios::TWO_EDITS).await;
    let kill = kill_after_intent_of(&handle, "t2").await;
    let prefix = kill.durable.record_count();

    // Every boundary of the first resume's close-out, each resumed again and checked: both
    // sides of the resume record, of each result, of the batch commit, and of the seal.
    let nested = handle.every_boundary_inside_resume(&kill).await;
    assert_eq!(
        nested.len(),
        6,
        "resume record, two results, batch and turn commits, seal"
    );

    // Every close-out record is durable and the Interrupted `RunFinished` is not: the resume
    // after this cut must add no second result and still end with exactly one terminal.
    let before_seal = nested
        .iter()
        .find(|cut| {
            matches!(
                cut.durable.records().last(),
                Some(WalRecord::TurnFinished { .. })
            )
        })
        .expect("a cut just before the seal");
    let resumed = handle.resume(before_seal).await;
    handle.check(before_seal, &resumed);
    let records = resumed.log.records();
    assert_eq!(
        results_after(&records, prefix),
        vec![
            ("t1".into(), DispatchOutcome::Unknown),
            ("t2".into(), DispatchOutcome::Unknown)
        ],
        "the second resume adds no result for a call the first already closed"
    );
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::RunFinished { .. }))
            .count(),
        1,
        "still one terminal"
    );
    assert_eq!(
        broca_crash_harness::resume_records(&records),
        2,
        "each resume writes its own resume record"
    );
    assert_eq!(resumed.provider.request_count(), 0);
    assert!(resumed.plane.dispatched().is_empty());
}

#[tokio::test]
async fn a_runner_sigkilled_inside_a_tool_dispatch_resumes_into_the_close_out() {
    let mut suite = Suite::new("resume close-out");
    let handle = RunnerHandle::new(scenarios::TWO_EDITS).await;
    let (kill, resumed) = handle
        .kill_resume_check(
            KillPoint::InFlight(InFlight::ToolDispatch(t("t1"))),
            KillTrigger::ProcessKill,
        )
        .await;
    // The runner died with both intents durable and no result: t1 was inside its dispatch and
    // t2, dispatched alongside it, had run but its result waits for the batch's barrier.
    let durable = kill.durable.records();
    let intents = durable
        .iter()
        .filter(|record| matches!(record, WalRecord::ToolDispatchIntent { .. }))
        .count();
    assert_eq!(intents, 2, "{}", kill.label());
    assert!(!durable
        .iter()
        .any(|record| matches!(record, WalRecord::ToolResult { .. })));
    assert_eq!(resumed.reason, RunFinishReason::Interrupted);
    assert_eq!(resumed.provider.request_count(), 0);
    assert!(resumed.plane.dispatched().is_empty());
    assert_eq!(
        results_after(&resumed.log.records(), kill.durable.record_count()),
        vec![
            ("t1".into(), DispatchOutcome::Unknown),
            ("t2".into(), DispatchOutcome::Unknown)
        ]
    );
    suite.record([kill]);
    suite.finish();
}
