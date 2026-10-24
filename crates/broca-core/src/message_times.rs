//! Envelope times follow the shared placement decisions, never message content or replay clocks.

use crate::placement::{AppendKind, HoldKind, Placement, PlacementAction};
use broca_wal::WalRecord;
use std::collections::BTreeMap;

pub fn written_times<'a>(
    records: impl IntoIterator<Item = &'a WalRecord>,
    placement: Placement,
) -> Result<BTreeMap<u64, u64>, String> {
    written_times_with_queued(records, placement, BTreeMap::new())
}

pub fn written_times_with_queued<'a>(
    records: impl IntoIterator<Item = &'a WalRecord>,
    mut placement: Placement,
    mut queued: BTreeMap<String, u64>,
) -> Result<BTreeMap<u64, u64>, String> {
    let mut times = BTreeMap::new();
    let mut attempts = BTreeMap::new();
    let mut steers = std::collections::VecDeque::new();
    let mut assistant = None;
    let mut tool_time = None;
    let mut tools_known = true;
    for record in records {
        if matches!(record, WalRecord::RunStarted { .. }) {
            times.split_off(&placement.next_ordinal());
            attempts.clear();
            steers.clear();
            assistant = None;
        }
        match record {
            WalRecord::Queued {
                submission_id,
                written_at_ms,
                ts_ms,
                ..
            } => {
                if let Some(at) = written_at_ms.or(*ts_ms).filter(|at| *at != 0) {
                    queued.insert(submission_id.0.clone(), at);
                }
            }
            WalRecord::ModelAttemptFinished {
                step_id,
                outcome:
                    broca_protocol::AttemptOutcome::Complete
                    | broca_protocol::AttemptOutcome::ToolArgsTrailingGarbage,
                ts_ms,
                ..
            } => {
                attempts.insert(*step_id, *ts_ms);
            }
            WalRecord::ToolBatchStarted { .. } => {
                tool_time = None;
                tools_known = true;
            }
            WalRecord::ToolResult { finished_at_ms, .. }
            | WalRecord::ToolNoDispatch {
                dispatched_at_ms: finished_at_ms,
                ..
            } => {
                tools_known &= finished_at_ms.is_some_and(|at| at != 0);
                tool_time = tool_time.max(*finished_at_ms);
            }
            WalRecord::ApprovalRecorded {
                closing_result: Some(_),
                ..
            } => {
                tools_known = false;
            }
            _ => {}
        }
        let source_time = match record {
            WalRecord::RunStarted {
                written_at_ms,
                ts_ms,
                submission_id,
                ..
            } => written_at_ms.or_else(|| match submission_id {
                Some(id) => queued.remove(&id.0),
                None => *ts_ms,
            }),
            WalRecord::Steered {
                written_at_ms,
                ts_ms,
                ..
            } => written_at_ms.or(*ts_ms),
            WalRecord::SendDelivered {
                written_at_ms,
                submission_id,
                ..
            } => written_at_ms.or_else(|| queued.remove(&submission_id.0)),
            WalRecord::ModelStepFinished {
                written_at_ms,
                step_id,
                ..
            } => written_at_ms.or_else(|| attempts.remove(step_id)),
            _ => None,
        }
        .filter(|at| *at != 0);
        for action in placement.observe(record)? {
            match action {
                PlacementAction::Hold {
                    kind: HoldKind::Assistant,
                    ..
                } => assistant = source_time,
                PlacementAction::Hold {
                    count,
                    kind: HoldKind::Steer,
                } => {
                    steers.extend(std::iter::repeat_n(source_time, count as usize));
                }
                PlacementAction::PlaceHeld { first, count } => {
                    for ordinal in first..first + count {
                        if let Some(at) = steers.pop_front().flatten() {
                            times.insert(ordinal, at);
                        }
                    }
                }
                PlacementAction::Append {
                    first,
                    count,
                    kind: AppendKind::ToolTurn,
                } => {
                    if count > 0 {
                        if let Some(at) = assistant.take() {
                            times.insert(first, at);
                        }
                        if tools_known {
                            if let Some(at) = tool_time {
                                times.insert(first + 1, at);
                            }
                        }
                    }
                }
                PlacementAction::Append { first, count, .. } => {
                    if let Some(at) = source_time {
                        for ordinal in first..first + count {
                            times.insert(ordinal, at);
                        }
                    }
                }
                PlacementAction::Truncate { to } => {
                    times.split_off(&to);
                }
            }
        }
        // Keep the held assistant's time at its reserved ordinal. Reads can expose the message
        // after ToolDispatchIntent or ToolCallDeferred makes it survive recovery, before results commit.
        if let (Some(ordinal), Some(at)) = (placement.held_assistant_ordinal(), assistant) {
            times.insert(ordinal, at);
        }
    }
    Ok(times)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn legacy_start() -> WalRecord {
        serde_json::from_str(
            include_str!("../../broca-wal/tests/fixtures/before_message_times.jsonl")
                .lines()
                .next()
                .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn admission_attempt_and_tool_times_follow_shared_placement() {
        let mut start = legacy_start();
        if let WalRecord::RunStarted { ts_ms, .. } = &mut start {
            *ts_ms = Some(10);
        }
        let values = [
            json!({"type":"step_started","step_id":1}),
            json!({"type":"model_attempt_finished","step_id":1,"attempt":0,"outcome":"complete","ts_ms":12,
                "request_body_len":0,"request_body_sha256":"a","response_body_len":0,"response_body_sha256":"b"}),
            json!({"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"a","content":[
                {"type":"tool_call","tool_call_id":"call","tool_name":"work","input":{}}
            ]},"usage":{},"retries_used":0}),
            json!({"type":"tool_batch_started","batch_id":"b","step_id":1,"planned_calls":["call"]}),
            json!({"type":"steered","ts_ms":13,"input":[{"role":"user","content":[{"type":"text","text":"steer"}]}]}),
            json!({"type":"tool_result","finished_at_ms":14,"batch_id":"b","result":{
                "tool_call_id":"call","output":{"kind":"text","text":"done"},"is_error":false},"outcome":"completed"}),
            json!({"type":"tool_batch_finished","batch_id":"b"}),
            json!({"type":"turn_finished","step_id":1}),
            json!({"type":"step_started","step_id":2}),
            json!({"type":"model_step_finished","written_at_ms":15,"step_id":2,"assistant_message":{
                "message_id":"last","content":[{"type":"text","text":"done"}]},"usage":{},"retries_used":0}),
        ];
        let records: Vec<WalRecord> = std::iter::once(start)
            .chain(
                values
                    .into_iter()
                    .map(|value| serde_json::from_value(value).unwrap()),
            )
            .collect();
        let times = written_times(&records, Placement::default()).unwrap();
        assert_eq!(
            times,
            BTreeMap::from([(0, 10), (1, 12), (2, 14), (3, 13), (4, 15)])
        );
        let mut untimed = records.clone();
        if let WalRecord::ToolResult { finished_at_ms, .. } = &mut untimed[6] {
            *finished_at_ms = None;
        }
        let times = written_times(&untimed, Placement::default()).unwrap();
        assert!(
            !times.contains_key(&2),
            "legacy tool results have no replacement clock"
        );
        assert_eq!(times.get(&1), Some(&12));
        assert_eq!(times.get(&3), Some(&13));
    }

    #[test]
    fn unrecorded_times_and_failed_attempts_stay_absent() {
        let mut records = [legacy_start(), serde_json::from_value(json!({"type":"model_attempt_finished","step_id":1,
            "attempt":0,"outcome":"http_error","ts_ms":99,"request_body_len":0,"request_body_sha256":"a",
            "response_body_len":0,"response_body_sha256":"b"})).unwrap(),
            serde_json::from_value(json!({"type":"model_step_finished","step_id":1,"assistant_message":{
                "message_id":"last","content":[]},"usage":{},"retries_used":0})).unwrap()];
        assert!(written_times(&records, Placement::default())
            .unwrap()
            .is_empty());
        if let WalRecord::ModelAttemptFinished { outcome, .. } = &mut records[1] {
            *outcome = broca_protocol::AttemptOutcome::ToolArgsTrailingGarbage;
        }
        assert_eq!(
            written_times(&records, Placement::default())
                .unwrap()
                .get(&1),
            Some(&99)
        );
    }

    #[test]
    fn recorded_queue_admission_survives_a_projection_watermark() {
        let queued: WalRecord = serde_json::from_value(json!({"type":"queued","submission_id":"q","ts_ms":123,
            "request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[],"policy_allowed":[],
                "tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}})).unwrap();
        let mut head = crate::head::HeadProjection::default();
        head.observe(&queued);
        let mut start = legacy_start();
        if let WalRecord::RunStarted {
            submission_id,
            ts_ms,
            ..
        } = &mut start
        {
            *submission_id = Some(broca_protocol::SubmissionId("q".into()));
            *ts_ms = Some(999);
        }
        let times =
            written_times_with_queued([&start], Placement::new(2), head.queued_times().clone())
                .unwrap();
        assert_eq!(times, BTreeMap::from([(2, 123)]));
        assert_eq!(
            written_times([&queued, &start], Placement::default()).unwrap(),
            BTreeMap::from([(0, 123)])
        );
    }
}
