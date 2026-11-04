//! Host creation facts use recorded acceptance and request-send stamps, never row clocks.

use std::collections::{BTreeMap, VecDeque};

use broca_protocol::{AttemptOutcome, StepId};
use broca_wal::WalRecord;

use crate::placement::{AppendKind, HoldKind, Placement, PlacementAction};

/// Only unfinished placement needs a continuation. No message bodies are cached here.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreationTimeSeed {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    requests: BTreeMap<StepId, u64>,
    #[serde(default, skip_serializing_if = "VecDeque::is_empty")]
    steers: VecDeque<Option<u64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    assistant: Option<u64>,
}

impl CreationTimeSeed {
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty() && self.steers.is_empty() && self.assistant.is_none()
    }

    fn new_episode(&mut self) {
        self.requests.clear();
        self.steers.clear();
        self.assistant = None;
    }
}

pub struct CreationTimes {
    pub times: BTreeMap<u64, u64>,
    pub seed: CreationTimeSeed,
}

/// One projector serves materialization and uncached reads. `queued` contains the
/// recorded acceptance stamps of pending sends, not materialized message timestamps.
pub fn project<'a>(
    records: impl IntoIterator<Item = &'a WalRecord>,
    mut placement: Placement,
    mut queued: BTreeMap<String, u64>,
    mut seed: CreationTimeSeed,
) -> Result<CreationTimes, String> {
    let mut times = BTreeMap::new();
    for record in records {
        if matches!(record, WalRecord::RunStarted { .. }) {
            times.split_off(&placement.next_ordinal());
            seed.new_episode();
        }
        if let WalRecord::Queued {
            submission_id,
            written_at_ms,
            ts_ms,
            ..
        } = record
        {
            // Both fields on Queued record send admission, before later delivery.
            if let Some(at) = written_at_ms.or(*ts_ms) {
                queued.insert(submission_id.0.clone(), at);
            }
        }
        if let WalRecord::ModelAttemptFinished {
            step_id,
            outcome,
            sent_at_ms,
            reservation,
            ..
        } = record
        {
            if matches!(
                outcome,
                AttemptOutcome::Complete | AttemptOutcome::ToolArgsTrailingGarbage
            ) && !reservation
                .as_ref()
                .is_some_and(|reservation| reservation.warm)
            {
                if let Some(at) = sent_at_ms {
                    seed.requests.insert(*step_id, *at);
                } else {
                    seed.requests.remove(step_id);
                }
            }
        }
        if let WalRecord::CacheWarmed { step_id, .. } = record {
            seed.requests.remove(step_id);
        }
        let source = match record {
            WalRecord::RunStarted {
                written_at_ms,
                submission_id,
                ..
            } => {
                let accepted = submission_id.as_ref().and_then(|id| queued.remove(&id.0));
                // ts_ms dates episode admission and may follow queue acceptance. Only
                // the dedicated input stamp or original queued admission dates this input.
                written_at_ms.or(accepted)
            }
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
            WalRecord::ModelStepFinished { step_id, .. } => seed.requests.remove(step_id),
            _ => None,
        };
        for action in placement.observe(record)? {
            match action {
                PlacementAction::Hold {
                    kind: HoldKind::Assistant,
                    ..
                } => seed.assistant = source,
                PlacementAction::Hold {
                    count,
                    kind: HoldKind::Steer,
                } => {
                    seed.steers
                        .extend(std::iter::repeat_n(source, count as usize));
                }
                PlacementAction::PlaceHeld { first, count } => {
                    for ordinal in first..first + count {
                        if let Some(at) = seed.steers.pop_front().flatten() {
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
                        if let Some(at) = seed.assistant.take() {
                            times.insert(first, at);
                        }
                    }
                }
                PlacementAction::Append { first, count, .. } => {
                    if let Some(at) = source {
                        for ordinal in first..first + count {
                            times.insert(ordinal, at);
                        }
                    }
                }
                PlacementAction::Truncate { to } => {
                    times.split_off(&to);
                    seed.requests.clear();
                }
            }
        }
        if let (Some(ordinal), Some(at)) = (placement.held_assistant_ordinal(), seed.assistant) {
            times.insert(ordinal, at);
        }
    }
    Ok(CreationTimes { times, seed })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(value: serde_json::Value) -> WalRecord {
        serde_json::from_value(value).unwrap()
    }

    fn start(id: &str, accepted: Option<u64>, submission: Option<&str>) -> WalRecord {
        let mut value: serde_json::Value = serde_json::from_str(
            include_str!("../../broca-wal/tests/fixtures/before_message_times.jsonl")
                .lines()
                .next()
                .unwrap(),
        )
        .unwrap();
        value["run_id"] = json!(id);
        value["written_at_ms"] = json!(accepted);
        value["ts_ms"] = json!(9999);
        value["submission_id"] = json!(submission);
        record(value)
    }

    fn attempt(at: Option<u64>, outcome: &str) -> WalRecord {
        record(
            json!({"type":"model_attempt_finished","step_id":1,"attempt":0,
            "request_body_len":0,"request_body_sha256":"a","response_body_len":0,"response_body_sha256":"b",
            "outcome":outcome,"ts_ms":9000,"sent_at_ms":at}),
        )
    }

    fn message() -> WalRecord {
        record(
            json!({"type":"model_step_finished","step_id":1,"written_at_ms":505,
            "assistant_message":{"message_id":"a","content":[{"type":"text","text":"answer"}]},
            "usage":{},"retries_used":0}),
        )
    }

    #[test]
    fn recorded_acceptance_and_successful_request_send_are_not_other_clocks() {
        let records = [
            start("r", Some(250), None),
            attempt(Some(100), "error"),
            attempt(Some(600), "complete"),
            message(),
        ];
        let facts = project(
            &records,
            Placement::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        assert_eq!(facts.times.get(&0), Some(&250));
        assert_eq!(facts.times.get(&1), Some(&600));
        assert!(
            facts.times[&1] > 505,
            "the host clock may step before completion"
        );
    }

    #[test]
    fn accepted_input_creation_is_available_for_system_and_user_roles() {
        let mut event = start("r", Some(250), None);
        if let WalRecord::RunStarted { input, .. } = &mut event {
            std::sync::Arc::make_mut(input).insert(
                0,
                broca_protocol::Message::new(
                    broca_protocol::Role::System,
                    vec![broca_protocol::ContentBlock::Text {
                        text: "system".into(),
                    }],
                ),
            );
        }
        let facts = project(
            [&event],
            Placement::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        assert_eq!(facts.times.get(&0), Some(&250));
        assert_eq!(facts.times.get(&1), Some(&250));
    }

    #[test]
    fn missing_creation_stamps_do_not_use_episode_finish_or_write_times() {
        let records = [
            start("r", None, None),
            attempt(Some(100), "error"),
            attempt(None, "complete"),
            message(),
        ];
        let facts = project(
            &records,
            Placement::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        assert!(facts.times.is_empty());
    }

    #[test]
    fn queued_and_held_steer_inputs_keep_original_acceptance_across_a_cut() {
        let mut queued: serde_json::Value = serde_json::from_str(
            include_str!("../../broca-wal/tests/golden/pre_step_transforms/queued.json").trim(),
        )
        .unwrap();
        queued["submission_id"] = json!("q");
        queued["written_at_ms"] = json!(200);
        let records = vec![
            record(queued),
            start("r", None, Some("q")),
            record(json!({"type":"step_started","step_id":1})),
            attempt(Some(600), "complete"),
            record(json!({"type":"steered","written_at_ms":300,"ts_ms":8888,
                "input":[{"role":"user","content":[{"type":"text","text":"steer"}]}]})),
            message(),
            record(json!({"type":"turn_finished","step_id":1})),
            record(json!({"type":"step_started","step_id":2})),
        ];
        let full = project(
            &records,
            Placement::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        assert_eq!(full.times.get(&0), Some(&200));
        assert_eq!(full.times.get(&1), Some(&600));
        assert_eq!(full.times.get(&2), Some(&300));
        let cut = 5;
        let first = project(
            &records[..cut],
            Placement::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        let mut placement = Placement::default();
        for event in &records[..cut] {
            placement.observe(event).unwrap();
        }
        let persisted = serde_json::to_vec(&first.seed).unwrap();
        let restored = serde_json::from_slice::<CreationTimeSeed>(&persisted).unwrap();
        let second = project(&records[cut..], placement, Default::default(), restored).unwrap();
        let mut combined = first.times;
        combined.extend(second.times);
        assert_eq!(combined, full.times);
    }

    #[test]
    fn a_warm_request_is_not_an_assistant_creation_stamp() {
        let records = [
            start("r", Some(250), None),
            attempt(Some(600), "complete"),
            record(json!({"type":"cache_warmed","run_id":"r","step_id":1,
                "request_body_sha256":"a","warm_body_sha256":"b","usage":{},"sent_at_ms":600})),
            message(),
        ];
        let facts = project(
            &records,
            Placement::default(),
            Default::default(),
            Default::default(),
        )
        .unwrap();
        assert_eq!(facts.times.get(&0), Some(&250));
        assert!(!facts.times.contains_key(&1));
    }
}
