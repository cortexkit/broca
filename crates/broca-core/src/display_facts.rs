//! Rebuildable display-envelope facts, folded once and continued from a per-session seed.

use std::collections::{BTreeMap, VecDeque};

use broca_wal::{DispatchOutcome, FramedRecord, NoDispatchReason, WalRecord};
use cortexkit_role_llm_runner::read::ToolTime;
use serde::{Deserialize, Serialize};

use crate::placement::{AppendKind, Placement, PlacementAction};

pub const DISPLAY_FACTS_VERSION: u32 = 2;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisplayFacts {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_times: Vec<ToolTime>,
}

/// A versioned continuation, committed atomically with every fact row it covers. It retains
/// only pending sender metadata and the current tool batch, never the transcript prefix.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DisplayFactsSeed {
    pub version: u32,
    pub through_seq: u64,
    pub anchor: Option<(u64, u64, u64)>,
    placement: Placement,
    budget: Option<broca_protocol::BudgetConfig>,
    queued: BTreeMap<String, Option<String>>,
    pending: VecDeque<Option<String>>,
    assistant: Option<(u64, DisplayFacts)>,
    tool_names: BTreeMap<String, String>,
}

impl Default for DisplayFactsSeed {
    fn default() -> Self {
        Self {
            version: DISPLAY_FACTS_VERSION,
            through_seq: 0,
            anchor: None,
            placement: Placement::default(),
            budget: None,
            queued: BTreeMap::new(),
            pending: VecDeque::new(),
            assistant: None,
            tool_names: BTreeMap::new(),
        }
    }
}

/// A fact version is identified by the WAL sequence that supplied it. Keeping versions lets
/// an older read snapshot use the cache even when another reader has already advanced it.
#[derive(Debug, Clone)]
pub struct DisplayFactVersion {
    pub ordinal: u64,
    pub seq: u64,
    pub facts: DisplayFacts,
}

#[derive(Debug, Clone, Default)]
pub struct DisplayFactsCache {
    pub seed: Option<DisplayFactsSeed>,
    pub rows: BTreeMap<u64, DisplayFacts>,
}

impl DisplayFactsSeed {
    pub fn fold(
        &mut self,
        frames: &[FramedRecord],
        lineage: &str,
    ) -> Result<Vec<DisplayFactVersion>, String> {
        self.placement = Placement::from_index_seed(self.placement.clone(), self.budget.clone());
        let mut versions = Vec::new();
        for frame in frames {
            if frame.seq <= self.through_seq {
                continue;
            }
            let record = &frame.record;
            let mut client = None;
            match record {
                WalRecord::Queued {
                    submission_id,
                    request_identity_digest,
                    ..
                } => {
                    self.queued.insert(
                        submission_id.0.clone(),
                        request_identity_digest
                            .as_deref()
                            .and_then(crate::digest_client_message_id),
                    );
                }
                WalRecord::RunStarted {
                    config,
                    request_identity_digest,
                    submission_id,
                    origin,
                    ..
                } => {
                    self.budget = config.budget.clone();
                    self.pending.clear();
                    self.assistant = None;
                    self.tool_names.clear();
                    if matches!(origin, broca_wal::RunOrigin::Fresh) {
                        client = submission_id
                            .as_ref()
                            .and_then(|id| self.queued.remove(&id.0))
                            .flatten()
                            .or_else(|| {
                                request_identity_digest
                                    .as_deref()
                                    .and_then(crate::digest_client_message_id)
                            });
                    }
                }
                WalRecord::Steered {
                    request_identity_digest,
                    input,
                    ..
                } => {
                    let id = request_identity_digest
                        .as_deref()
                        .and_then(crate::digest_client_message_id);
                    self.pending.extend(std::iter::repeat_n(id, input.len()));
                }
                WalRecord::SendDelivered {
                    submission_id,
                    input: Some(input),
                    ..
                } => {
                    let id = self.queued.remove(&submission_id.0).flatten();
                    self.pending.extend(std::iter::repeat_n(id, input.len()));
                }
                WalRecord::Retracted { submission_id, .. }
                | WalRecord::SendEnded { submission_id, .. } => {
                    self.queued.remove(&submission_id.0);
                }
                _ => {}
            }
            let actions = self.placement.observe(record)?;
            for action in &actions {
                match *action {
                    PlacementAction::Append { first, count, kind } => {
                        for ordinal in first..first + count {
                            let facts = if kind == AppendKind::Input {
                                DisplayFacts {
                                    client_message_id: client.clone(),
                                    ..Default::default()
                                }
                            } else {
                                self.assistant
                                    .as_ref()
                                    .filter(|(at, _)| {
                                        *at == ordinal && kind == AppendKind::ToolTurn
                                    })
                                    .map(|(_, facts)| facts.clone())
                                    .unwrap_or_default()
                            };
                            versions.push(DisplayFactVersion {
                                ordinal,
                                seq: frame.seq,
                                facts,
                            });
                        }
                    }
                    PlacementAction::PlaceHeld { first, count } => {
                        for ordinal in first..first + count {
                            let id = self.pending.pop_front().flatten();
                            versions.push(DisplayFactVersion {
                                ordinal,
                                seq: frame.seq,
                                facts: DisplayFacts {
                                    client_message_id: id,
                                    ..Default::default()
                                },
                            });
                        }
                    }
                    _ => {}
                }
            }
            if let WalRecord::ModelStepFinished {
                assistant_message, ..
            } = record
            {
                self.tool_names = assistant_message
                    .tool_calls()
                    .map(|call| (call.tool_call_id.0.clone(), call.tool_name.clone()))
                    .collect();
                let ordinal = self.placement.held_assistant_ordinal().or_else(|| {
                    actions.iter().find_map(|action| match action {
                        PlacementAction::Append {
                            first,
                            kind: AppendKind::Assistant | AppendKind::Finalize,
                            ..
                        } => Some(*first),
                        _ => None,
                    })
                });
                self.assistant = ordinal.map(|ordinal| {
                    (
                        ordinal,
                        DisplayFacts {
                            client_message_id: None,
                            tool_times: assistant_message
                                .tool_calls()
                                .map(|call| ToolTime::new(&call.tool_call_id.0))
                                .collect(),
                        },
                    )
                });
            }
            if let Some((ordinal, facts)) = &mut self.assistant {
                match record {
                    WalRecord::ToolDispatchIntent {
                        tool_call_id,
                        dispatched_at_ms,
                        call_key,
                        schema_pin,
                        tool_name,
                        ..
                    } => {
                        if let Some(call) = facts
                            .tool_times
                            .iter_mut()
                            .find(|call| call.tool_call_id == tool_call_id.0)
                        {
                            call.started_at = *dispatched_at_ms;
                            if let Some(key) =
                                crate::tool_call_key(Some(lineage), frame.seq, call_key.as_deref())
                            {
                                *call = call.clone().with_call_key(key);
                            }
                            if let Some((name, pin)) = tool_name
                                .as_ref()
                                .or_else(|| self.tool_names.get(&tool_call_id.0))
                                .zip(schema_pin.as_ref())
                            {
                                if let Ok(pin) = cortexkit_role_tool_provider::call::SchemaPin::new(
                                    name,
                                    &pin.schema_digest,
                                    pin.semantics,
                                )
                                .encode()
                                {
                                    *call = call.clone().with_schema_pin(pin);
                                }
                            }
                        }
                    }
                    WalRecord::ToolCallDeferred { call_id, .. } => {
                        if let Some(call) = facts
                            .tool_times
                            .iter_mut()
                            .find(|call| call.tool_call_id == call_id.0)
                        {
                            if let Some(key) = crate::tool_call_key(Some(lineage), frame.seq, None)
                            {
                                *call = call.clone().with_call_key(key);
                            }
                        }
                    }
                    WalRecord::ToolNoDispatch {
                        tool_call_id,
                        reason,
                        ..
                    } => {
                        if let Some(call) = facts
                            .tool_times
                            .iter_mut()
                            .find(|call| call.tool_call_id == tool_call_id.0)
                        {
                            call.unanswered = Some(
                                match reason {
                                    NoDispatchReason::Denied { .. }
                                    | NoDispatchReason::PolicyBlocked { .. } => "denied",
                                    NoDispatchReason::ValidationFailed { .. }
                                    | NoDispatchReason::Unavailable { .. } => "not_executed",
                                }
                                .into(),
                            );
                        }
                    }
                    WalRecord::ApprovalRecorded {
                        call_id,
                        call_key,
                        decision,
                        closing_result: Some(_),
                        ..
                    } => {
                        if let Some(call) = facts
                            .tool_times
                            .iter_mut()
                            .find(|call| call.tool_call_id == call_id.0)
                        {
                            *call = call.clone().with_call_key(call_key);
                            call.unanswered = match decision {
                                broca_wal::DeferredDecision::Declined => Some("denied".into()),
                                broca_wal::DeferredDecision::Expired
                                | broca_wal::DeferredDecision::Unavailable => {
                                    Some("not_executed".into())
                                }
                                broca_wal::DeferredDecision::Approved => None,
                            };
                        }
                    }
                    WalRecord::ToolResult {
                        result,
                        finished_at_ms,
                        outcome,
                        ..
                    } => {
                        if let Some(call) = facts
                            .tool_times
                            .iter_mut()
                            .find(|call| call.tool_call_id == result.tool_call_id.0)
                        {
                            match outcome {
                                DispatchOutcome::Completed => match &result.output {
                                    broca_protocol::ToolOutput::Denied { .. } => {
                                        call.unanswered = Some("denied".into())
                                    }
                                    broca_protocol::ToolOutput::Error {
                                        nothing_sent: Some(true),
                                        ..
                                    } => call.unanswered = Some("not_executed".into()),
                                    _ => call.finished_at = *finished_at_ms,
                                },
                                DispatchOutcome::Unknown | DispatchOutcome::Interrupted => {
                                    call.unanswered = Some("interrupted".into())
                                }
                                DispatchOutcome::MaxSteps => {
                                    call.unanswered = Some("cancelled".into())
                                }
                            }
                        }
                    }
                    WalRecord::RunFinished {
                        reason: broca_protocol::RunFinishReason::Cancelled,
                        ..
                    } => {
                        for call in &mut facts.tool_times {
                            if call.unanswered.as_deref() == Some("interrupted") {
                                call.unanswered = Some("cancelled".into());
                            }
                        }
                    }
                    _ => {}
                }
                if !facts.tool_times.is_empty()
                    && matches!(
                        record,
                        WalRecord::ModelStepFinished { .. }
                            | WalRecord::ToolDispatchIntent { .. }
                            | WalRecord::ToolCallDeferred { .. }
                            | WalRecord::ToolNoDispatch { .. }
                            | WalRecord::ToolResult { .. }
                            | WalRecord::ApprovalRecorded { .. }
                            | WalRecord::RunFinished { .. }
                    )
                {
                    versions.push(DisplayFactVersion {
                        ordinal: *ordinal,
                        seq: frame.seq,
                        facts: facts.clone(),
                    });
                }
            }
            self.through_seq = frame.seq;
        }
        Ok(versions)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn frame(seq: u64, value: serde_json::Value) -> FramedRecord {
        FramedRecord {
            seq,
            fence: 1,
            record: serde_json::from_value(value).unwrap(),
        }
    }

    #[test]
    fn per_call_times_keep_parallel_answers_absence_and_recorded_closures() {
        let ids = ["a", "b", "c", "d", "e", "f", "g"];
        let mut frames = vec![frame(
            1,
            json!({"type":"model_step_finished","step_id":0,"assistant_message":{"message_id":"m","content":ids.map(|id| json!({"type":"tool_call","tool_call_id":id,"tool_name":"work","input":{}}))},"usage":{}}),
        )];
        for (id, at) in [("a", Some(100)), ("b", Some(101)), ("c", None)] {
            frames.push(frame(frames.len() as u64 + 1, json!({"type":"tool_dispatch_intent","batch_id":"batch","tool_call_id":id,"args":{},"dispatched_at_ms":at})));
        }
        for (id, at, outcome) in [
            ("b", 125, "completed"),
            ("a", 150, "completed"),
            ("c", 160, "interrupted"),
        ] {
            frames.push(frame(frames.len() as u64 + 1, json!({"type":"tool_result","batch_id":"batch","result":{"tool_call_id":id,"output":{"kind":"text","text":"answer"},"is_error":false},"outcome":outcome,"finished_at_ms":at})));
        }
        for (id, reason) in [
            ("d", json!({"kind":"denied","reason":"no"})),
            ("e", json!({"kind":"unavailable","detail":"offline"})),
        ] {
            frames.push(frame(frames.len() as u64 + 1, json!({"type":"tool_no_dispatch","batch_id":"batch","tool_call_id":id,"reason":reason,"dispatched_at_ms":170})));
        }
        let mut prepared = crate::replay::deferred_tests::prepared("g", "batch");
        if let WalRecord::ToolCallDeferred { schema_pin, .. } = &mut prepared {
            *schema_pin = Some(broca_wal::DispatchSchemaPin {
                schema_digest: "b".repeat(64),
                semantics: 9,
            });
        }
        frames.push(FramedRecord {
            seq: frames.len() as u64 + 1,
            fence: 1,
            record: prepared,
        });
        frames.push(frame(frames.len() as u64 + 1, json!({"type":"approval_recorded","call_key":"lineage:10","call_id":"g","decision":"declined","closing_result":{"tool_call_id":"g","output":{"kind":"denied","reason":"no"},"is_error":true}})));
        frames.push(frame(
            frames.len() as u64 + 1,
            json!({"type":"run_finished","reason":"cancelled","usage":{}}),
        ));
        let mut seed = DisplayFactsSeed::default();
        let before_stop = seed.fold(&frames[..frames.len() - 1], "lineage").unwrap();
        assert_eq!(
            before_stop.last().unwrap().facts.tool_times[2]
                .unanswered
                .as_deref(),
            Some("interrupted")
        );
        assert_eq!(
            before_stop.last().unwrap().facts.tool_times[2].finished_at,
            None
        );
        let mut seed = DisplayFactsSeed::default();
        let versions = seed.fold(&frames, "lineage").unwrap();
        let expected = json!([
            {"tool_call_id":"a","started_at":100,"finished_at":150,"call_key":"lineage:2"},
            {"tool_call_id":"b","started_at":101,"finished_at":125,"call_key":"lineage:3"},
            {"tool_call_id":"c","unanswered":"cancelled","call_key":"lineage:4"},
            {"tool_call_id":"d","unanswered":"denied"},
            {"tool_call_id":"e","unanswered":"not_executed"},
            {"tool_call_id":"f"},
            {"tool_call_id":"g","unanswered":"denied","call_key":"lineage:10"}
        ]);
        assert_eq!(
            serde_json::to_value(&versions.last().unwrap().facts.tool_times).unwrap(),
            expected
        );
        for split in 0..=frames.len() {
            let mut seed = DisplayFactsSeed::default();
            let mut versions = seed.fold(&frames[..split], "lineage").unwrap();
            let mut restored: DisplayFactsSeed =
                serde_json::from_str(&serde_json::to_string(&seed).unwrap()).unwrap();
            versions.extend(restored.fold(&frames[split..], "lineage").unwrap());
            assert_eq!(
                serde_json::to_value(&versions.last().unwrap().facts.tool_times).unwrap(),
                expected,
                "continuation cut {split}"
            );
        }
    }
}
