//! A cross-check of the harness that does not come from replay.
//!
//! `invariants::check` predicts a resume's terminal reason, and whether it may call the
//! provider, from `replay(prefix)`, so a wrong classification in replay would be mirrored by the
//! check rather than caught. This table is written by hand from the one-edit scenario's log and
//! the durability contract, one row per record boundary, and the harness's actual results are
//! asserted against it.

use crate::scenarios;

use broca_crash_harness::{KillPoint, KillTrigger, RunnerHandle};
use broca_protocol::{caller::RunFinishReason, ToolCallId};
use broca_wal::{DispatchOutcome, WalRecord};

use RunFinishReason::{Completed, Interrupted};

/// What a resume from one cut must do.
struct Row {
    /// Records the cut keeps, and the last kept record's kind, so the table can be read
    /// against the log below without running anything.
    records: usize,
    last_kept: &'static str,
    reason: RunFinishReason,
    /// Whether the resume calls the provider at all.
    provider_called: bool,
    /// Whether the resume dispatches the edit call `t1`.
    dispatches_t1: bool,
    /// `ToolResult` records with outcome `Unknown` in the log after the resume.
    outcome_unknown: usize,
}

/// The uninterrupted log of the one-edit scenario is:
///  1 run_started (synced)    2 step_started            3 model_step_finished (calls t1)
///  4 tool_batch_started      5 tool_dispatch_intent t1 (synced; t1 then runs)
///  6 tool_result t1          7 tool_batch_finished     8 turn_finished (synced)
///  9 step_started            10 model_step_finished (text)   11 run_finished (synced)
const TABLE: &[Row] = &[
    // Nothing durable: a fresh run from the start.
    Row {
        records: 0,
        last_kept: "(none)",
        reason: Completed,
        provider_called: true,
        dispatches_t1: true,
        outcome_unknown: 0,
    },
    // No step output durable: step 1 is requested again and t1 runs for the first time.
    Row {
        records: 1,
        last_kept: "run_started",
        reason: Completed,
        provider_called: true,
        dispatches_t1: true,
        outcome_unknown: 0,
    },
    Row {
        records: 2,
        last_kept: "step_started",
        reason: Completed,
        provider_called: true,
        dispatches_t1: true,
        outcome_unknown: 0,
    },
    // Step 1 is durable but t1 has no intent: it never left the process, so it is dispatched,
    // then step 2 is requested.
    Row {
        records: 3,
        last_kept: "model_step_finished",
        reason: Completed,
        provider_called: true,
        dispatches_t1: true,
        outcome_unknown: 0,
    },
    Row {
        records: 4,
        last_kept: "tool_batch_started",
        reason: Completed,
        provider_called: true,
        dispatches_t1: true,
        outcome_unknown: 0,
    },
    // t1's intent is durable and its result is not: it may have run. Never sent again, no model
    // call, one outcome_unknown result, sealed Interrupted.
    Row {
        records: 5,
        last_kept: "tool_dispatch_intent",
        reason: Interrupted,
        provider_called: false,
        dispatches_t1: false,
        outcome_unknown: 1,
    },
    // t1's result is durable: reused as recorded; only step 2 is requested.
    Row {
        records: 6,
        last_kept: "tool_result",
        reason: Completed,
        provider_called: true,
        dispatches_t1: false,
        outcome_unknown: 0,
    },
    Row {
        records: 7,
        last_kept: "tool_batch_finished",
        reason: Completed,
        provider_called: true,
        dispatches_t1: false,
        outcome_unknown: 0,
    },
    Row {
        records: 8,
        last_kept: "turn_finished",
        reason: Completed,
        provider_called: true,
        dispatches_t1: false,
        outcome_unknown: 0,
    },
    Row {
        records: 9,
        last_kept: "step_started",
        reason: Completed,
        provider_called: true,
        dispatches_t1: false,
        outcome_unknown: 0,
    },
    // The final text step is durable: the run ended naturally; the resume only writes the
    // missing terminal.
    Row {
        records: 10,
        last_kept: "model_step_finished",
        reason: Completed,
        provider_called: false,
        dispatches_t1: false,
        outcome_unknown: 0,
    },
    // The terminal is durable: nothing to resume.
    Row {
        records: 11,
        last_kept: "run_finished",
        reason: Completed,
        provider_called: false,
        dispatches_t1: false,
        outcome_unknown: 0,
    },
];

#[tokio::test]
async fn every_cut_of_one_tool_call_resumes_as_written_down_by_hand() {
    let handle = RunnerHandle::new(scenarios::ONE_EDIT).await;
    let records = handle.reference.log.records();
    assert_eq!(
        TABLE.len(),
        records.len() + 1,
        "one row per record boundary of the log documented above TABLE"
    );
    for row in TABLE {
        let last_kept = match row.records {
            0 => "(none)".to_string(),
            n => serde_json::to_value(&records[n - 1]).unwrap()["type"]
                .as_str()
                .unwrap()
                .to_string(),
        };
        assert_eq!(
            last_kept, row.last_kept,
            "the log no longer matches the table at row {}",
            row.records
        );

        let (kill, resumed) = handle
            .kill_resume_check(
                KillPoint::RecordBoundary {
                    records: row.records,
                    torn: false,
                },
                KillTrigger::Truncate,
            )
            .await;
        let label = kill.label();
        assert_eq!(resumed.reason, row.reason, "{label}: terminal reason");
        assert_eq!(
            resumed.provider.request_count() > 0,
            row.provider_called,
            "{label}: provider called on resume"
        );
        assert_eq!(
            resumed.plane.dispatch_count(&ToolCallId("t1".into())) == 1,
            row.dispatches_t1,
            "{label}: t1 dispatched on resume"
        );
        let unknown = resumed
            .log
            .records()
            .iter()
            .filter(|record| {
                matches!(
                    record,
                    WalRecord::ToolResult {
                        outcome: DispatchOutcome::Unknown,
                        ..
                    }
                )
            })
            .count();
        assert_eq!(
            unknown, row.outcome_unknown,
            "{label}: outcome_unknown results"
        );
    }
}
