//! Rebuildable display-envelope facts, folded once and continued from a per-session seed.

use std::collections::{BTreeMap, VecDeque};

use broca_wal::{DispatchOutcome, FramedRecord, NoDispatchReason, WalRecord};
use cortexkit_role_llm_runner::read::ToolTime;
use serde::{Deserialize, Serialize};

use crate::placement::{AppendKind, Placement, PlacementAction};

pub const DISPLAY_FACTS_VERSION: u32 = 6;

/// Exact response-only outputs by zero-based message ordinal and content part.
pub type NativeToolOutputs = BTreeMap<u64, BTreeMap<u32, broca_protocol::ToolOutput>>;

fn module_sender(principal: Option<&broca_protocol::BindPrincipal>) -> Option<String> {
    match principal {
        Some(broca_protocol::BindPrincipal::Reserved { module_id }) => Some(module_id.clone()),
        _ => None,
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisplayFacts {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_marks: Option<broca_protocol::MessageMarks>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sender: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub notices: Vec<crate::projection::ProjectedNotice>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client_message_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_times: Vec<ToolTime>,
    /// Application-only `ToolResult` output with `SendDelivered.ride` text excluded.
    /// Materialized messages remain model-facing and must not be rewritten by a read.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub native_tool_outputs: BTreeMap<u32, broca_protocol::ToolOutput>,
}

impl DisplayFacts {
    pub fn resolve_notices(
        &mut self,
        keys: &crate::notice_keys::NoticeSendKeys,
    ) -> Result<(), String> {
        let mut notices = Vec::new();
        for notice in std::mem::take(&mut self.notices) {
            if keys.allows(
                &notice.submission_id,
                &notice.fact.sender,
                &notice.fact.send_id,
            )? {
                notices.push(notice);
            }
        }
        self.notices = notices;
        Ok(())
    }
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
    #[serde(default)]
    queued_marks: BTreeMap<String, Option<broca_protocol::MessageMarks>>,
    #[serde(default)]
    pending_marks: VecDeque<Option<broca_protocol::MessageMarks>>,
    queued: BTreeMap<String, Option<String>>,
    pending: VecDeque<Option<String>>,
    assistant: Option<(u64, DisplayFacts)>,
    tool_names: BTreeMap<String, String>,
    #[serde(default)]
    call_anchors: crate::call_identity::CallAnchors,
    #[serde(default)]
    pub send_keys: crate::notice_keys::NoticeSendKeys,
    #[serde(default)]
    queued_senders: BTreeMap<String, Option<String>>,
    #[serde(default)]
    pending_senders: VecDeque<Option<String>>,
    #[serde(default)]
    tool_row: Option<(u64, DisplayFacts)>,
    #[serde(default)]
    tool_calls: Vec<String>,
    #[serde(default)]
    tool_results: BTreeMap<String, broca_protocol::ToolResult>,
}

impl Default for DisplayFactsSeed {
    fn default() -> Self {
        Self {
            version: DISPLAY_FACTS_VERSION,
            through_seq: 0,
            anchor: None,
            placement: Placement::default(),
            budget: None,
            queued_marks: Default::default(),
            pending_marks: Default::default(),
            queued: BTreeMap::new(),
            pending: VecDeque::new(),
            assistant: None,
            tool_names: BTreeMap::new(),
            call_anchors: Default::default(),
            send_keys: Default::default(),
            queued_senders: Default::default(),
            pending_senders: Default::default(),
            tool_row: None,
            tool_calls: Vec::new(),
            tool_results: BTreeMap::new(),
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
    /// A read-side publication ticket for stores that fence same-lineage recovery.
    /// It is not persisted in a display fact or included in a request identity.
    pub publication_epoch: Option<i64>,
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
            self.send_keys.observe(std::slice::from_ref(frame));
            let current_call = self.call_anchors.observe(record, frame.seq);
            let mut client = None;
            let mut input_sender = None;
            let mut input_marks = None;
            match record {
                WalRecord::Queued {
                    message_marks,
                    submission_id,
                    request_identity_digest,
                    ..
                } => {
                    self.queued_marks
                        .insert(submission_id.0.clone(), message_marks.clone());
                    self.queued_senders.insert(
                        submission_id.0.clone(),
                        self.send_keys
                            .origin(&submission_id.0)
                            .and_then(|(_, sender)| sender),
                    );
                    self.queued.insert(
                        submission_id.0.clone(),
                        request_identity_digest
                            .as_deref()
                            .and_then(crate::digest_client_message_id),
                    );
                }
                WalRecord::RunStarted {
                    message_marks,
                    config,
                    request_identity_digest,
                    submission_id,
                    origin,
                    sent_by,
                    principal,
                    ..
                } => {
                    self.budget = config.budget.clone();
                    self.pending.clear();
                    self.pending_senders.clear();
                    self.pending_marks.clear();
                    self.tool_row = None;
                    self.tool_calls.clear();
                    self.tool_results.clear();
                    self.assistant = None;
                    self.tool_names.clear();
                    if matches!(origin, broca_wal::RunOrigin::Fresh) {
                        let queued_marks = submission_id
                            .as_ref()
                            .and_then(|id| self.queued_marks.remove(&id.0))
                            .flatten();
                        input_marks = message_marks.clone().or(queued_marks);
                        input_sender = submission_id
                            .as_ref()
                            .and_then(|id| self.queued_senders.remove(&id.0))
                            .flatten()
                            .or_else(|| module_sender(sent_by.as_ref().or(principal.as_ref())));
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
                    message_marks,
                    request_identity_digest,
                    input,
                    sent_by,
                    ..
                } => {
                    let id = request_identity_digest
                        .as_deref()
                        .and_then(crate::digest_client_message_id);
                    self.pending.extend(std::iter::repeat_n(id, input.len()));
                    self.pending_marks
                        .extend(std::iter::repeat_n(message_marks.clone(), input.len()));
                    self.pending_senders.extend(std::iter::repeat_n(
                        module_sender(sent_by.as_ref()),
                        input.len(),
                    ));
                }
                WalRecord::SendDelivered {
                    submission_id,
                    input: Some(input),
                    ..
                } => {
                    let id = self.queued.remove(&submission_id.0).flatten();
                    self.pending.extend(std::iter::repeat_n(id, input.len()));
                    let sender = self.queued_senders.remove(&submission_id.0).flatten();
                    let marks = self.queued_marks.remove(&submission_id.0).flatten();
                    self.pending_marks
                        .extend(std::iter::repeat_n(marks, input.len()));
                    self.pending_senders
                        .extend(std::iter::repeat_n(sender, input.len()));
                }
                WalRecord::Retracted { submission_id, .. }
                | WalRecord::SendEnded { submission_id, .. } => {
                    self.queued.remove(&submission_id.0);
                    self.queued_marks.remove(&submission_id.0);
                    self.queued_senders.remove(&submission_id.0);
                }
                _ => {}
            }
            let actions = self.placement.observe(record)?;
            for action in &actions {
                match *action {
                    PlacementAction::Append { first, count, kind } => {
                        if kind == AppendKind::ToolTurn {
                            self.tool_row = Some((first + 1, DisplayFacts::default()));
                        }
                        for ordinal in first..first + count {
                            let facts = if kind == AppendKind::Input {
                                DisplayFacts {
                                    message_marks: input_marks.clone(),
                                    sender: input_sender.clone(),
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
                                    message_marks: self.pending_marks.pop_front().flatten(),
                                    sender: self.pending_senders.pop_front().flatten(),
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
                self.tool_results.clear();
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
                            ..Default::default()
                        },
                    )
                });
                self.tool_calls = assistant_message
                    .content
                    .iter()
                    .filter_map(|block| match block {
                        broca_protocol::ContentBlock::ToolCall(call) => {
                            Some(call.tool_call_id.0.clone())
                        }
                        _ => None,
                    })
                    .collect();
            }
            if let WalRecord::ToolResult { result, .. } = record {
                self.tool_results
                    .insert(result.tool_call_id.0.clone(), (**result).clone());
            }
            if let WalRecord::SendDelivered {
                submission_id,
                ride,
                tool_call_id: Some(call),
                appended_text: Some(text),
                mode: broca_protocol::DeliveryMode::ToolResult,
                ..
            } = record
            {
                if let Some((ordinal, facts)) = &mut self.tool_row {
                    let part = self.tool_calls.iter().rposition(|id| id == &call.0);
                    if let Some(part) = part {
                        if ride.is_none() {
                            if let Some(result) = self.tool_results.get_mut(&call.0) {
                                crate::delivery_order::append_to_result(result, text);
                            }
                        }
                        if ride.is_some() || facts.native_tool_outputs.contains_key(&(part as u32))
                        {
                            let result = self
                                .tool_results
                                .get(&call.0)
                                .ok_or("ride carrier has no native result")?;
                            facts
                                .native_tool_outputs
                                .insert(part as u32, result.output.clone());
                        }
                        if let (Some(ride), Some(text)) = (ride, text.strip_prefix("\n\n")) {
                            facts.notices.push(crate::projection::ProjectedNotice {
                                submission_id: submission_id.0.clone(),
                                fact: {
                                    let mut fact =
                                        cortexkit_role_llm_runner::read::NoticeFact::new(
                                            part as u32,
                                            &ride.send_id,
                                            &ride.sender,
                                            text,
                                        )
                                        .with_call_key(&ride.call_key);
                                    fact.speaks_for = ride.speaks_for.clone();
                                    fact
                                },
                            });
                        }
                        versions.push(DisplayFactVersion {
                            ordinal: *ordinal,
                            seq: frame.seq,
                            facts: facts.clone(),
                        });
                    }
                }
                self.queued_marks.remove(&submission_id.0);
                self.queued_senders.remove(&submission_id.0);
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
                if let Some((index, anchor)) = current_call {
                    if let Some(call) = facts.tool_times.get_mut(index) {
                        call.call_key = anchor.key(Some(lineage));
                        call.schema_pin = anchor.schema_pin;
                    }
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

/// Reuse already-decoded message source spans without reading a historical prefix.
/// Deliveries without `SendDelivered.ride` remain appended; deliveries with it are
/// excluded from this response overlay.
pub fn native_tool_outputs(
    records: &[FramedRecord],
    lineage: &str,
) -> Result<NativeToolOutputs, String> {
    if !records.iter().any(|frame| {
        matches!(
            &frame.record,
            WalRecord::SendDelivered { ride: Some(_), .. }
        )
    }) {
        return Ok(Default::default());
    }
    let mut outputs: NativeToolOutputs = DisplayFactsSeed::default()
        .fold(records, lineage)?
        .into_iter()
        .map(|version| (version.ordinal, version.facts.native_tool_outputs))
        .collect();
    outputs.retain(|_, parts| !parts.is_empty());
    Ok(outputs)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    #[ignore = "set BROCA_DISPLAY_REBUILD_WAL to a copied WAL to time a cold display seed rebuild"]
    fn time_cold_display_seed_rebuild_from_wal() {
        use std::io::{Read, Seek, SeekFrom};
        let path = std::env::var_os("BROCA_DISPLAY_REBUILD_WAL")
            .expect("BROCA_DISPLAY_REBUILD_WAL is required");
        let file = std::sync::Mutex::new(std::fs::File::open(&path).unwrap());
        let bytes = file.lock().unwrap().metadata().unwrap().len();
        let (_, lineage) = crate::projection::log_header_info(&|off, len| {
            let mut file = file.lock().unwrap();
            file.seek(SeekFrom::Start(off))?;
            let mut bytes = Vec::new();
            file.by_ref().take(len).read_to_end(&mut bytes)?;
            Ok(bytes)
        })
        .unwrap();
        let started = std::time::Instant::now();
        let records = broca_wal::replay_file(path, u64::MAX).unwrap();
        let decoded = started.elapsed();
        let mut seed = DisplayFactsSeed::default();
        let facts = seed
            .fold(
                &records,
                &lineage.expect("WAL needs an incarnation").to_string(),
            )
            .unwrap();
        eprintln!("cold display rebuild: bytes={bytes} records={} fact_versions={} seed_bytes={} decode_ms={} total_ms={}", records.len(), facts.len(), serde_json::to_vec(&seed).unwrap().len(), decoded.as_millis(), started.elapsed().as_millis());
    }

    fn frame(seq: u64, value: serde_json::Value) -> FramedRecord {
        FramedRecord {
            seq,
            fence: 1,
            record: serde_json::from_value(value).unwrap(),
        }
    }

    #[test]
    fn native_output_sidecar_survives_every_cut_and_preserves_only_legacy_appends() {
        use broca_protocol::{ToolOutput, ToolOutputPart};
        for output in [
            ToolOutput::Text {
                text: "worked".into(),
            },
            ToolOutput::error("failed"),
            ToolOutput::Json {
                value: json!({"answer":42}),
            },
            ToolOutput::Parts {
                parts: vec![ToolOutputPart::Text {
                    text: "worked".into(),
                }],
            },
        ] {
            let mut result = broca_protocol::ToolResult {
                tool_call_id: broca_protocol::ToolCallId("call".into()),
                output: output.clone(),
                is_error: false,
            };
            crate::delivery_order::append_to_result(&mut result, "\n\nold-before");
            crate::delivery_order::append_to_result(&mut result, "\n\nold-after");
            let values = vec![
                json!({"type":"model_step_finished","step_id":0,"assistant_message":{"message_id":"m","content":[{"type":"tool_call","tool_call_id":"call","tool_name":"work","input":{}}]},"usage":{}}),
                json!({"type":"tool_batch_started","step_id":0,"batch_id":"b","planned_calls":["call"]}),
                json!({"type":"tool_result","batch_id":"b","result":{"tool_call_id":"call","output":output,"is_error":false},"outcome":"completed"}),
                json!({"type":"tool_batch_finished","batch_id":"b"}),
                json!({"type":"send_delivered","submission_id":"before","run_id":"r","mode":"tool_result","tool_call_id":"call","appended_text":"\n\nold-before"}),
                json!({"type":"send_delivered","submission_id":"frozen","run_id":"r","mode":"tool_result","tool_call_id":"call","appended_text":"\n\nnew","ride":{"call_key":"lineage:1","send_id":"frozen","sender":"sender","speaks_for":{"agent_id":"Ada"},"forms":["tool_result"],"form":"append"}}),
                json!({"type":"send_delivered","submission_id":"after","run_id":"r","mode":"tool_result","tool_call_id":"call","appended_text":"\n\nold-after"}),
            ];
            let frames: Vec<_> = values
                .into_iter()
                .enumerate()
                .map(|(index, value)| frame(index as u64 + 1, value))
                .collect();
            for cut in 0..=frames.len() {
                let mut seed = DisplayFactsSeed::default();
                let mut versions = seed.fold(&frames[..cut], "lineage").unwrap();
                let mut seed: DisplayFactsSeed =
                    serde_json::from_slice(&serde_json::to_vec(&seed).unwrap()).unwrap();
                versions.extend(seed.fold(&frames[cut..], "lineage").unwrap());
                let facts = &versions.last().unwrap().facts;
                assert_eq!(
                    facts.native_tool_outputs.get(&0),
                    Some(&result.output),
                    "cut {cut}"
                );
                assert_eq!(facts.notices.len(), 1);
                assert_eq!(facts.notices[0].fact.text, "new");
                assert_eq!(
                    facts.notices[0].fact.speaks_for.as_ref().unwrap().agent_id,
                    "Ada"
                );
            }
        }
    }

    #[test]
    fn send_marks_survive_every_seed_cut_and_ordered_delivery() {
        let mut start: serde_json::Value = serde_json::from_str(include_str!(
            "../../broca-wal/tests/golden/pre_step_transforms/run_started.json"
        ))
        .unwrap();
        start["message_marks"] = json!({"speaks_for":{"agent_id":"Ada"}});
        let mut queued: serde_json::Value = serde_json::from_str(include_str!(
            "../../broca-wal/tests/golden/pre_step_transforms/queued.json"
        ))
        .unwrap();
        queued["message_marks"] = json!({"notice":true});
        let id = queued["submission_id"].clone();
        let mut steer: serde_json::Value = serde_json::from_str(include_str!(
            "../../broca-wal/tests/golden/pre_step_transforms/steered.json"
        ))
        .unwrap();
        steer["message_marks"] = json!({"speaks_for":{"agent_id":"Grace"}});
        let frames = [
            frame(1, start),
            frame(2, queued),
            frame(
                3,
                json!({"type":"send_delivered","submission_id":id,"run_id":"r","mode":"steer","input":[{"role":"user","content":[{"type":"text","text":"notice"}]}]}),
            ),
            frame(4, steer),
            frame(5, json!({"type":"step_started","step_id":0})),
        ];
        for cut in 0..=frames.len() {
            let mut seed = DisplayFactsSeed::default();
            let mut versions = seed.fold(&frames[..cut], "lineage").unwrap();
            let mut seed: DisplayFactsSeed =
                serde_json::from_slice(&serde_json::to_vec(&seed).unwrap()).unwrap();
            versions.extend(seed.fold(&frames[cut..], "lineage").unwrap());
            let marks: Vec<_> = versions
                .iter()
                .filter_map(|version| version.facts.message_marks.as_ref())
                .collect();
            assert_eq!(marks.len(), 3, "cut {cut}");
            assert_eq!(marks[0].speaks_for.as_ref().unwrap().agent_id, "Ada");
            assert!(marks[1].notice);
            assert!(marks[1].speaks_for.is_none());
            assert_eq!(marks[2].speaks_for.as_ref().unwrap().agent_id, "Grace");
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
            {"tool_call_id":"d","unanswered":"denied","call_key":"lineage:8"},
            {"tool_call_id":"e","unanswered":"not_executed","call_key":"lineage:9"},
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
