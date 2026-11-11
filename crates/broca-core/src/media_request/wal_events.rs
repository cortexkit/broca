//! The media events an old-storage record stands for.
//!
//! For tests and for an offline check of a converted session only: both drive one engine
//! through `SessionMedia::fold` and another through `SessionMedia::apply` with these events,
//! and compare them. Nothing on a production path may call this. It works the ordinals out
//! with the old placement rules, which is exactly what events exist to avoid, and it has no
//! answer for the old compaction protocol's records.

use broca_protocol::media_event::{MediaEvent, MediaInput, MediaLimits, MediaMessage, MediaPart};
use broca_protocol::{ToolOutput, ToolOutputPart};
use broca_wal::WalRecord;

use super::SessionMedia;
use crate::placement::{AppendKind, Placement, PlacementAction};

/// Why an old record has no events.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalMediaEventsError {
    /// A record of the old compaction protocol (`compaction_message` or
    /// `compaction_view_applied`). Its effect on which images show is decided by compaction
    /// code that no event carries, so the engine has to continue from an index saved after it
    /// (a `MediaCheckpoint` of the old storage). Never guessed.
    NeedsSavedIndex { record: &'static str },
    /// The old placement rules refused the record, as `fold` would.
    Placement(String),
}

impl std::fmt::Display for WalMediaEventsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NeedsSavedIndex { record } => {
                write!(
                    f,
                    "media events: a {record} record needs a saved media index"
                )
            }
            Self::Placement(error) => write!(f, "media events: {error}"),
        }
    }
}

impl std::error::Error for WalMediaEventsError {}

/// Turns old records into media events, keeping the old placement state that supplies their
/// ordinals. Not for production paths: see the module comment.
#[derive(Debug, Clone, Default)]
pub struct WalMediaEvents {
    placement: Placement,
}

impl WalMediaEvents {
    /// For a session from its first record.
    pub fn new() -> Self {
        Self::default()
    }

    /// For records after those `session` has folded, such as after a restored checkpoint:
    /// starts from the placement state its fold reached.
    pub fn continuing(session: &SessionMedia) -> Self {
        Self {
            placement: session.placement.clone(),
        }
    }

    /// The events `record`, the session's record `seq`, stands for, in the order `apply`
    /// takes them. `seq` is the number `fold` gives the record: one more than the records
    /// folded before it. A record the engine doesn't read has none.
    pub fn events(
        &mut self,
        seq: u64,
        record: &WalRecord,
    ) -> Result<Vec<MediaEvent>, WalMediaEventsError> {
        match record {
            WalRecord::CompactionMessage { .. } => {
                return Err(WalMediaEventsError::NeedsSavedIndex {
                    record: "compaction_message",
                })
            }
            WalRecord::CompactionViewApplied { .. } => {
                return Err(WalMediaEventsError::NeedsSavedIndex {
                    record: "compaction_view_applied",
                })
            }
            _ => {}
        }
        // A tool result's message follows the assistant message waiting for it, as placed
        // before this record.
        let tool_ordinal = self.placement.held_tool_result_ordinal();
        let actions = self
            .placement
            .observe(record)
            .map_err(WalMediaEventsError::Placement)?;
        let mut events = Vec::new();
        match record {
            WalRecord::RunStarted { input, config, .. } => {
                let first = actions
                    .iter()
                    .find_map(|action| match action {
                        PlacementAction::Append {
                            first,
                            kind: AppendKind::Input,
                            ..
                        } => Some(*first),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        WalMediaEventsError::Placement("a run start placed no input".into())
                    })?;
                let inputs = input
                    .iter()
                    .enumerate()
                    .filter_map(|(i, message)| {
                        let skeleton = MediaMessage::of(message);
                        skeleton.holds_media().then_some(MediaInput {
                            message: i,
                            ordinal: first + i as u64,
                            skeleton,
                        })
                    })
                    .collect();
                events.push(MediaEvent::RunStart {
                    record: seq,
                    inputs,
                    limits: media_limits(&config.frozen_render_config),
                });
            }
            WalRecord::Steered { input, .. }
            | WalRecord::SendDelivered {
                input: Some(input), ..
            } => events.push(MediaEvent::Accepted {
                record: seq,
                messages: input.iter().map(MediaMessage::of).collect(),
            }),
            WalRecord::ToolBatchStarted { planned_calls, .. } => {
                events.push(MediaEvent::BatchStart {
                    calls: planned_calls.clone(),
                })
            }
            WalRecord::ToolResult { result, .. } | WalRecord::LateResult { result, .. } => {
                if let ToolOutput::Parts { parts } = &result.output {
                    events.push(MediaEvent::ToolResult {
                        record: seq,
                        call: result.tool_call_id.clone(),
                        ordinal: tool_ordinal,
                        shown: matches!(record, WalRecord::ToolResult { .. }),
                        media: parts
                            .iter()
                            .enumerate()
                            .filter_map(|(part, value)| match value {
                                ToolOutputPart::Media { media } => Some(MediaPart {
                                    part,
                                    reference: media.clone(),
                                }),
                                ToolOutputPart::Text { .. } => None,
                            })
                            .collect(),
                    });
                }
            }
            WalRecord::RunConfigRefrozen {
                frozen_render_config,
                ..
            } => {
                if let Some(limits) = media_limits(frozen_render_config) {
                    events.push(MediaEvent::LimitsChanged { limits });
                }
            }
            _ => {}
        }
        for action in actions {
            if let PlacementAction::PlaceHeld { first, .. } = action {
                events.push(MediaEvent::Placed { first });
            }
        }
        match record {
            WalRecord::StepStarted { step_id } => {
                events.push(MediaEvent::StepStart { step: *step_id })
            }
            WalRecord::ModelAttemptFinished { .. } => events.push(MediaEvent::AttemptEnd),
            WalRecord::ModelStepFinished { .. } => events.push(MediaEvent::StepEnd),
            _ => {}
        }
        Ok(events)
    }
}

/// The `media` member of a frozen render config, when it has a non-null one: the run's media
/// limits.
fn media_limits(frozen_render_config: &serde_json::Value) -> Option<MediaLimits> {
    frozen_render_config
        .get("media")
        .filter(|media| !media.is_null())
        .cloned()
        .map(MediaLimits)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn reference() -> serde_json::Value {
        json!({"kind":"image","mime":"image/png","byte_size":1,"sha256":"a".repeat(64),
            "width":1,"height":1})
    }

    /// One run with a person's image, a tool image and a steered image, around `words`
    /// words of text in every message, tool result and config, and `tools` declared tools.
    fn history(words: usize, tools: usize) -> Vec<WalRecord> {
        let text = "word ".repeat(words);
        let mut image = reference();
        image["type"] = json!("media");
        let message = json!({"role":"user","content":[{"type":"text","text":text}, image]});
        let tools: Vec<_> = (0..tools)
            .map(|n| {
                json!({"name":format!("tool{n}"),"description":text,
                    "input_schema":{"type":"object","description":text}})
            })
            .collect();
        [
            json!({"type":"run_started","run_id":"run",
                "session":{"project_root":"/fixture","harness":"media","session":"session"},
                "config":{"model":{"provider_module_id":"fake","model_id":"fake"},"tools":tools,
                    "tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},
                    "context_epoch":0,"renderer_version":1,"frozen_render_config":{"system":text}},
                "input":[message, {"role":"user","content":[{"type":"text","text":text}]}],
                "origin":{"kind":"fresh"}}),
            json!({"type":"step_started","step_id":1}),
            json!({"type":"model_step_finished","step_id":1,"usage":{},
                "assistant_message":{"message_id":"answer","content":[{"type":"text","text":text},
                    {"type":"tool_call","tool_call_id":"call","tool_name":"screen",
                        "input":{"text":text}}]}}),
            json!({"type":"tool_batch_started","step_id":1,"batch_id":"batch",
                "planned_calls":["call"]}),
            json!({"type":"tool_result","batch_id":"batch","outcome":"completed",
                "result":{"tool_call_id":"call","is_error":false,"output":{"kind":"parts",
                    "parts":[{"kind":"text","text":text},{"kind":"media","media":reference()}]}}}),
            json!({"type":"tool_batch_finished","batch_id":"batch"}),
            json!({"type":"steered","input":[message]}),
            json!({"type":"run_config_refrozen","renderer_version":1,
                "frozen_render_config":{"system":text}}),
            json!({"type":"step_started","step_id":2}),
        ]
        .into_iter()
        .map(|value| serde_json::from_value(value).unwrap())
        .collect()
    }

    fn events(records: &[WalRecord]) -> Vec<MediaEvent> {
        let mut events = WalMediaEvents::new();
        records
            .iter()
            .enumerate()
            .flat_map(|(index, record)| events.events(index as u64 + 1, record).unwrap())
            .collect()
    }

    #[test]
    fn events_do_not_grow_with_text_or_tools() {
        let small = history(1, 0);
        let large = history(20_000, 40);
        assert!(
            serde_json::to_vec(&large).unwrap().len() > 4_000_000,
            "the large history really is large"
        );
        let small_events = events(&small);
        let large_events = events(&large);
        assert_eq!(small_events.len(), large_events.len());
        assert!(
            small_events
                .iter()
                .filter(|event| matches!(
                    event,
                    MediaEvent::RunStart { .. }
                        | MediaEvent::Accepted { .. }
                        | MediaEvent::ToolResult { .. }
                ))
                .count()
                == 3,
            "every image-bearing record has its event"
        );
        assert_eq!(
            serde_json::to_vec(&small_events).unwrap().len(),
            serde_json::to_vec(&large_events).unwrap().len(),
        );
        assert_eq!(small_events, large_events);
    }

    #[test]
    fn fold_and_apply_agree_on_distinct_and_shared_references() {
        let image = |digit: char| {
            json!({"type":"media","kind":"image","mime":"image/png","byte_size":1,
                "sha256":digit.to_string().repeat(64),"width":1,"height":1})
        };
        let records: Vec<WalRecord> = [
            json!({"type":"run_started","run_id":"run",
                "session":{"project_root":"/fixture","harness":"media","session":"session"},
                "config":{"model":{"provider_module_id":"fake","model_id":"fake"},"tools":[],
                    "tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},
                    "context_epoch":0,"renderer_version":1,"frozen_render_config":null},
                "input":[{"role":"user","content":[{"type":"text","text":"original"},image('a')]}],
                "origin":{"kind":"fresh"}}),
            json!({"type":"steered","input":[
                {"role":"user","content":[{"type":"text","text":"steered"},image('b')]}]}),
            json!({"type":"step_started","step_id":1}),
            json!({"type":"steered","input":[{"role":"user","content":[image('a'),image('b')]},
                {"role":"user","content":[{"type":"text","text":"no media"}]}]}),
            json!({"type":"step_started","step_id":2}),
            json!({"type":"step_started","step_id":2}),
        ]
        .into_iter()
        .map(|value| serde_json::from_value(value).unwrap())
        .collect();
        let engine = || {
            SessionMedia::read_only(
                std::path::Path::new("/nonexistent"),
                crate::media_store::MediaStoreClass::Durable,
            )
        };
        let (mut folded, mut applied) = (engine(), engine());
        let mut events = WalMediaEvents::new();
        for (index, record) in records.iter().enumerate() {
            folded.fold(record).unwrap();
            for event in events.events(index as u64 + 1, record).unwrap() {
                applied.apply(&event).unwrap();
            }
            let seq = index + 1;
            assert_eq!(folded.facts(), applied.facts(), "facts after record {seq}");
            assert_eq!(
                folded.take_changes(),
                applied.take_changes(),
                "changes after record {seq}"
            );
        }
        assert_eq!(folded.facts().captures.len(), 4);
    }

    #[test]
    fn old_compaction_records_need_a_saved_index() {
        let setup: WalRecord = serde_json::from_value(json!({
            "type":"compaction_message", "provider":"compactor", "kind":"setup",
            "compaction_id":"setup", "version":1, "range":{"from":0,"to":1},
            "replacement":[], "sha256":"fixture"
        }))
        .unwrap();
        assert_eq!(
            WalMediaEvents::new().events(1, &setup),
            Err(WalMediaEventsError::NeedsSavedIndex {
                record: "compaction_message"
            })
        );
    }
}
