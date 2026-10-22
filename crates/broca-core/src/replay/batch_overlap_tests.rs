//! Replay refuses a tool batch or tool-calling step that would replace an uncommitted batch
//! still holding call evidence, instead of forgetting that evidence and offering the call for
//! dispatch again.

use super::*;
use crate::replay::ReplayError;

fn started_batch(step: u64, call: &str) -> WalRecord {
    WalRecord::ToolBatchStarted {
        batch_id: BatchId(format!("r1-b{step}")),
        step_id: StepId(step),
        planned_calls: vec![ToolCallId(call.into())],
    }
}

/// The run's first tool turn, cut after `t1`'s dispatch intent and before its result.
fn intent_without_result() -> Vec<WalRecord> {
    let mut records = vec![run_started(Vec::new()), step_started(1), call_step(1, "t1")];
    records.extend(tool_turn(1, "t1").0);
    records
}

fn assert_refused(records: &[WalRecord]) {
    match replay(records) {
        Err(ReplayError::CorruptLineage { reason, .. }) => {
            assert!(reason.contains("uncommitted batch"), "{reason}")
        }
        Ok(state) => panic!(
            "replay forgot an unresolved dispatch intent: {:?}",
            state.resolution
        ),
        Err(other) => panic!("refused for the wrong reason: {other}"),
    }
}

#[test]
fn a_second_batch_start_over_an_unresolved_intent_is_refused() {
    let mut records = intent_without_result();
    records.push(started_batch(1, "t1"));
    assert_refused(&records);
}

#[test]
fn a_new_tool_step_over_an_unresolved_intent_is_refused() {
    let mut records = intent_without_result();
    records.extend([step_started(2), call_step(2, "t2")]);
    assert_refused(&records);
}

/// A repeated start over a batch nothing has been recorded for loses nothing, so it still
/// replays: the turn resumes with its one planned call.
#[test]
fn a_repeated_start_over_an_empty_batch_still_replays() {
    let records = vec![
        run_started(Vec::new()),
        step_started(1),
        call_step(1, "t1"),
        started_batch(1, "t1"),
        started_batch(1, "t1"),
    ];
    let state = replay(&records).expect("an evidence-free batch can be restarted");
    match state.resolution {
        ReplayResolution::Resume {
            pending: Some(pending),
        } => assert_eq!(pending.planned, vec![ToolCallId("t1".into())]),
        other => panic!("expected the turn to resume, got {other:?}"),
    }
}
