//! Per-occurrence durable call anchors, shared by reader and live projections.
use broca_protocol::ToolCallId;
use broca_wal::WalRecord;

/// The calls of the current step, one slot per copy in the assistant message's order, each
/// anchored by the records that answer it. A record matches the first fitting copy of its
/// call id, so a repeated id's copies keep distinct anchors.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CallAnchors {
    slots: Vec<CallAnchor>,
}

/// One copy of a tool call and what anchored it: the seq of the first record that did (its
/// dispatch intent, deferral or terminal), and the facts its intent recorded.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CallAnchor {
    pub id: ToolCallId,
    pub seq: Option<u64>,
    pub intent_seq: Option<u64>,
    pub recorded_key: Option<String>,
    pub dispatched_to: Option<String>,
    pub schema_pin: Option<String>,
    pub dispatched: bool,
    pub terminal: bool,
    tool_name: String,
}

impl CallAnchor {
    /// The call's key: the one a record names, else the log's lineage and the anchoring seq.
    pub fn key(&self, lineage: Option<&str>) -> Option<String> {
        self.recorded_key
            .clone()
            .or_else(|| Some(format!("{}:{}", lineage?, self.seq?)))
    }
}

impl CallAnchors {
    /// The step's call copies, in the assistant message's order.
    pub fn slots(&self) -> &[CallAnchor] {
        &self.slots
    }

    /// Takes in the record at `seq`: a run, step or turn boundary resets the copies, and a
    /// record about a call anchors the copy it answers. Returns that copy's index and its
    /// state after the record, or `None` for a record about no copy.
    pub fn observe(&mut self, record: &WalRecord, seq: u64) -> Option<(usize, CallAnchor)> {
        match record {
            WalRecord::RunStarted { .. }
            | WalRecord::TurnFinished { .. }
            | WalRecord::RunFinished { .. } => {
                self.slots.clear();
                return None;
            }
            WalRecord::ModelStepFinished {
                assistant_message, ..
            } => {
                self.slots = assistant_message
                    .tool_calls()
                    .map(|call| CallAnchor {
                        id: call.tool_call_id.clone(),
                        seq: None,
                        intent_seq: None,
                        recorded_key: None,
                        dispatched_to: None,
                        schema_pin: None,
                        dispatched: false,
                        terminal: false,
                        tool_name: call.tool_name.clone(),
                    })
                    .collect();
                return None;
            }
            _ => {}
        }
        // Imported input can synthesize several results under one RunStarted anchor.
        // Only singular native terminal records can anchor previously keyless calls.
        let (id, terminal) = match record {
            WalRecord::ToolDispatchIntent { tool_call_id, .. }
            | WalRecord::ToolNoDispatch { tool_call_id, .. } => (
                tool_call_id,
                matches!(record, WalRecord::ToolNoDispatch { .. }),
            ),
            WalRecord::ToolResult { result, .. } => (&result.tool_call_id, true),
            WalRecord::ToolCallDeferred { call_id, .. } => (call_id, false),
            WalRecord::ApprovalRecorded {
                call_id,
                closing_result,
                ..
            } => (call_id, closing_result.is_some()),
            _ => return None,
        };
        let index = if let WalRecord::ApprovalRecorded { call_key, .. } = record {
            self.slots
                .iter()
                .position(|slot| {
                    &slot.id == id && slot.recorded_key.as_ref() == Some(call_key) && !slot.terminal
                })
                .or_else(|| {
                    self.slots
                        .iter()
                        .position(|slot| &slot.id == id && !slot.terminal)
                })?
        } else if matches!(record, WalRecord::ToolNoDispatch { .. }) {
            // A refusal can precede the results of already-dispatched siblings.
            self.slots
                .iter()
                .position(|slot| &slot.id == id && slot.seq.is_none() && !slot.terminal)
                .or_else(|| {
                    self.slots
                        .iter()
                        .position(|slot| &slot.id == id && !slot.terminal)
                })?
        } else if terminal {
            // Resolved results, including duplicate-ID synthetic errors, are in call order.
            self.slots
                .iter()
                .position(|slot| &slot.id == id && !slot.terminal)?
        } else {
            self.slots
                .iter()
                .position(|slot| &slot.id == id && !slot.dispatched && !slot.terminal)?
        };
        let slot = &mut self.slots[index];
        match record {
            WalRecord::ToolDispatchIntent {
                call_key,
                module_id,
                schema_pin,
                tool_name,
                ..
            } => {
                slot.seq = Some(seq);
                slot.intent_seq = Some(seq);
                if call_key.is_some() {
                    slot.recorded_key.clone_from(call_key);
                }
                slot.dispatched = true;
                slot.dispatched_to.clone_from(module_id);
                slot.schema_pin = schema_pin.as_ref().and_then(|pin| {
                    cortexkit_role_tool_provider::call::SchemaPin::new(
                        tool_name.as_deref().unwrap_or(&slot.tool_name),
                        &pin.schema_digest,
                        pin.semantics,
                    )
                    .encode()
                    .ok()
                });
            }
            WalRecord::ToolCallDeferred { .. } => {
                slot.seq.get_or_insert(seq);
            }
            WalRecord::ApprovalRecorded { call_key, .. } => {
                slot.recorded_key = Some(call_key.clone());
            }
            WalRecord::ToolNoDispatch { .. } | WalRecord::ToolResult { .. } => {
                slot.seq.get_or_insert(seq);
            }
            _ => unreachable!(),
        }
        slot.terminal |= terminal;
        Some((index, slot.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn record(value: serde_json::Value) -> WalRecord {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn native_negative_anchors_are_distinct_and_preserve_dispatched_and_prepared_keys() {
        let mut anchors = CallAnchors::default();
        let calls = ["sent", "prepared", "denied", "denied"]
            .map(|id| json!({"type":"tool_call","tool_call_id":id,"tool_name":"work","input":{}}));
        anchors.observe(&record(json!({"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m","content":calls},"usage":{}})), 1);
        anchors.observe(&record(json!({"type":"tool_dispatch_intent","batch_id":"batch","tool_call_id":"sent","args":{}})), 2);
        anchors.observe(
            &crate::replay::deferred_tests::prepared("prepared", "batch"),
            3,
        );
        anchors.observe(&record(json!({"type":"tool_dispatch_intent","batch_id":"batch","tool_call_id":"prepared","call_key":"lineage:3","args":{}})), 4);
        for seq in [5, 6] {
            anchors.observe(&record(json!({"type":"tool_no_dispatch","batch_id":"batch","tool_call_id":"denied","reason":{"kind":"denied","reason":"no"}})), seq);
        }
        assert_eq!(
            anchors
                .slots()
                .iter()
                .map(|slot| slot.key(Some("lineage")).unwrap())
                .collect::<Vec<_>>(),
            ["lineage:2", "lineage:3", "lineage:5", "lineage:6"]
        );
    }

    #[test]
    fn duplicate_id_results_preserve_the_dispatched_occurrence_and_anchor_the_synthetic_one() {
        let mut anchors = CallAnchors::default();
        let calls = ["first", "duplicate"].map(|name| {
            json!({
                "type":"tool_call", "tool_call_id":"same", "tool_name":"work", "input":{"name":name}
            })
        });
        anchors.observe(&record(json!({"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m","content":calls},"usage":{}})), 1);
        anchors.observe(&record(json!({"type":"tool_dispatch_intent","batch_id":"batch","tool_call_id":"same","args":{"name":"first"}})), 2);
        let first = anchors.observe(&record(json!({"type":"tool_result","batch_id":"batch","result":{"tool_call_id":"same","output":{"kind":"text","text":"ok"},"is_error":false},"outcome":"completed"})), 3).unwrap();
        let duplicate = anchors.observe(&record(json!({"type":"tool_result","batch_id":"batch","result":{"tool_call_id":"same","output":{"kind":"error","message":"duplicate tool_call_id from provider"},"is_error":true},"outcome":"completed"})), 4).unwrap();
        assert_eq!(first.0, 0);
        assert_eq!(first.1.key(Some("lineage")).as_deref(), Some("lineage:2"));
        assert_eq!(duplicate.0, 1);
        assert_eq!(
            duplicate.1.key(Some("lineage")).as_deref(),
            Some("lineage:4")
        );
    }
}
