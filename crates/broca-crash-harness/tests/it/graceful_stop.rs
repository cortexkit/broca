//! Crash cuts of a run the module asked to stop.
//!
//! The stop request arrives while the run's first tool call runs. That call finishes and its
//! result is recorded; nothing after it starts (no further call of the batch, no next model
//! step), and the run ends Interrupted. These cut the reference log at every record boundary,
//! torn appends included, and resume each cut under a stop that is still requested. Every
//! resume must satisfy the harness's invariants (each call dispatched at most once, one
//! terminal per run and never Cancelled, every step it requests rendered byte-identically) and,
//! checked by hand here rather than through replay, start nothing: no provider call and no
//! dispatch.

use crate::scenarios;

use broca_crash_harness::{KillPoint, KillTrigger, RunnerHandle, StopAt};
use broca_protocol::caller::RunFinishReason;
use broca_wal::{DispatchOutcome, WalRecord};

/// The kind of each record of `log`, in order, for comparison with a log written out by hand.
fn kinds(records: &[WalRecord]) -> Vec<String> {
    records
        .iter()
        .map(|record| match record {
            WalRecord::ToolDispatchIntent { tool_call_id, .. } => {
                format!("tool_dispatch_intent {}", tool_call_id.0)
            }
            WalRecord::ToolResult {
                result, outcome, ..
            } => format!("tool_result {} {outcome:?}", result.tool_call_id.0),
            WalRecord::RunFinished { reason, .. } => format!("run_finished {reason:?}"),
            other => serde_json::to_value(other).expect("a record serializes")["type"]
                .as_str()
                .expect("a record names its type")
                .to_string(),
        })
        .collect()
}

/// Cut `handle`'s reference log at every record boundary (torn too), resume, check the
/// harness's invariants, and check that no resume started anything.
async fn every_cut_starts_nothing(handle: &RunnerHandle) {
    for point in handle.record_boundaries(true) {
        let (kill, resumed) = handle.kill_resume_check(point, KillTrigger::Truncate).await;
        let label = kill.label();
        assert_eq!(
            resumed.reason,
            RunFinishReason::Interrupted,
            "{label}: the resume ends Interrupted"
        );
        assert_eq!(
            resumed.provider.request_count(),
            0,
            "{label}: a resume under a standing stop calls no model"
        );
        assert!(
            resumed.plane.dispatched().is_empty(),
            "{label}: a resume under a standing stop sends no tool call"
        );
    }
}

/// One edit: the edit the stop arrives during finishes `completed`, and the run ends right
/// after its turn, before a second step.
#[tokio::test]
async fn every_record_boundary_of_a_run_stopped_during_its_tool_call() {
    let handle = RunnerHandle::stopping(scenarios::STOPPED_ONE_EDIT, StopAt::FirstToolCall).await;
    assert_eq!(
        kinds(&handle.reference.log.records()),
        [
            "run_started",
            "step_started",
            "model_step_finished",
            "tool_batch_started",
            "tool_dispatch_intent t1",
            "tool_result t1 Completed",
            "tool_batch_finished",
            "turn_finished",
            "run_finished Interrupted",
        ],
        "the reference run finishes its call and starts nothing after it"
    );
    every_cut_starts_nothing(&handle).await;
}

/// Two parallel edits: the first is sent and finishes, the second is never sent and is closed
/// as not dispatched. A cut that keeps the second call's intent but not its result is resumed
/// like any indeterminate call (`outcome_unknown`), because the log alone cannot tell that it
/// never left.
#[tokio::test]
async fn every_record_boundary_of_a_run_stopped_inside_a_parallel_batch() {
    let handle = RunnerHandle::stopping(scenarios::STOPPED_TWO_EDITS, StopAt::FirstToolCall).await;
    assert_eq!(
        kinds(&handle.reference.log.records()),
        [
            "run_started",
            "step_started",
            "model_step_finished",
            "tool_batch_started",
            "tool_dispatch_intent t1",
            "tool_dispatch_intent t2",
            "tool_result t1 Completed",
            "tool_result t2 Interrupted",
            "tool_batch_finished",
            "turn_finished",
            "run_finished Interrupted",
        ],
        "the reference run sends only the first call and closes the second as not dispatched"
    );
    every_cut_starts_nothing(&handle).await;
}

/// A cut that keeps the step's tool call but none of its batch: the resume, still asked to
/// stop, closes the call as not dispatched instead of sending it. That resume is then cut at
/// every record it appended and resumed again, which must converge the same way.
#[tokio::test]
async fn every_boundary_inside_a_stopped_resume_of_an_unsent_call() {
    let handle = RunnerHandle::stopping(scenarios::STOPPED_ONE_EDIT, StopAt::FirstToolCall).await;
    let kill = handle
        .kill_at(
            KillPoint::RecordBoundary {
                records: 3,
                torn: false,
            },
            KillTrigger::Truncate,
        )
        .await;
    let resumed = handle.resume(&kill).await;
    handle.check(&kill, &resumed);
    let appended = &resumed.log.records()[3..];
    assert_eq!(
        kinds(appended),
        [
            "resume_started",
            "tool_batch_started",
            "tool_result t1 Interrupted",
            "tool_batch_finished",
            "turn_finished",
            "run_finished Interrupted",
        ]
    );
    assert!(
        appended.iter().all(|record| !matches!(
            record,
            WalRecord::ToolResult {
                outcome: DispatchOutcome::Unknown,
                ..
            }
        )),
        "a call that never left is never outcome_unknown"
    );
    let nested = handle.every_boundary_inside_resume(&kill).await;
    assert_eq!(nested.len(), appended.len());
}

fn restart_request() -> broca_core::RunRequest {
    let mut request = (scenarios::STOPPED_ONE_EDIT.request)();
    request.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    request
}

#[tokio::test]
async fn restart_pause_and_resume_are_safe_at_every_record_boundary() {
    let scenario = broca_crash_harness::Scenario {
        name: "restart_pause_cuts",
        request: restart_request,
        expected: RunFinishReason::Completed,
        ..scenarios::STOPPED_ONE_EDIT
    };
    let stopped = RunnerHandle::stopping(
        broca_crash_harness::Scenario {
            expected: RunFinishReason::Interrupted,
            ..scenario
        },
        StopAt::FirstToolCall,
    )
    .await;
    assert!(matches!(
        stopped.reference.log.records().last(),
        Some(WalRecord::RunPaused {
            reason: broca_protocol::PauseReason::Restart,
            ..
        })
    ));
    let continuing = RunnerHandle::new(scenario).await;
    for point in stopped.record_boundaries(true) {
        let kill = stopped.kill_at(point, KillTrigger::Truncate).await;
        let resumed = continuing.resume(&kill).await;
        continuing.check(&kill, &resumed);
    }
    let kill = stopped
        .kill_at(
            KillPoint::RecordBoundary {
                records: stopped.reference.log.records().len(),
                torn: false,
            },
            KillTrigger::Truncate,
        )
        .await;
    let resumed = continuing.resume(&kill).await;
    assert_eq!(resumed.reason, RunFinishReason::Completed);
    assert_eq!(resumed.provider.request_count(), 1);
    assert!(
        resumed.plane.dispatched().is_empty(),
        "the completed tool must never be dispatched again"
    );
    assert_eq!(
        resumed
            .log
            .records()
            .iter()
            .filter(|record| matches!(record, WalRecord::ResumeStarted { .. }))
            .count(),
        1
    );
    continuing.every_boundary_inside_resume(&kill).await;
}
