//! DISPLAY lane payload — lossy, coalesced, droppable under backpressure.
//! The runner also uses DisplayReset plus re-streaming after a write-time hook changes a
//! completed message; all other provider deltas remain passthrough.
//! Sent on the lower-priority background lane. Each delta
//! carries the id linking it to its authoritative control unit.
//!
//! Because the provider module COALESCES deltas pre-enqueue, a frame here is a
//! coalesced chunk, not one-token-one-frame — so a JSON body on this lane is not
//! a per-token cost.

use serde::{Deserialize, Serialize};

use crate::ids::{MessageId, ToolCallId};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DisplayEvent {
    /// Coalesced assistant text for live rendering.
    TextDelta {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mid: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none", with = "part_key")]
        part: Option<u64>,
        message_id: MessageId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<ToolCallId>,
        delta: String,
    },
    /// Coalesced reasoning text for live rendering.
    ReasoningDelta {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mid: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none", with = "part_key")]
        part: Option<u64>,
        message_id: MessageId,
        delta: String,
    },
    /// Coalesced partial tool-call input JSON (rendering only; the authoritative
    /// assembled input arrives on the control lane).
    ToolInputDelta {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mid: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none", with = "part_key")]
        part: Option<u64>,
        tool_call_id: ToolCallId,
        delta: String,
    },
    /// Backpressure marker: `dropped` display frames were coalesced/dropped
    /// before this point, so the renderer knows the live text has a gap.
    DisplayGap { dropped: u64 },
    /// Discard all live display state for the in-flight step. A retried generation may use
    /// different message ids, so consumers must treat this as a full-generation wipe rather than
    /// resetting state for one message id.
    ///
    /// Also emitted after a post_assistant hook changes a completed message, followed by
    /// its final reasoning/text/tool-input deltas in content order. It is not emitted for a
    /// message every hook left unchanged. This reset belongs to
    /// the step whose deltas preceded it on the display lane, not the control lane's latest
    /// step. Non-empty message ids bound that re-stream when the next id differs. Gemini
    /// uses empty ids, so the re-stream/next-step boundary is unmarked on this lossy lane.
    /// Unknown future display events use the same conservative reset fallback.
    #[serde(other)]
    DisplayReset,
}

impl DisplayEvent {
    /// Transcript coordinates are independent of provider message and tool-call ids.
    pub fn coordinates(&self) -> Option<(Option<&str>, u64)> {
        match self {
            Self::TextDelta {
                mid,
                part: Some(part),
                ..
            }
            | Self::ReasoningDelta {
                mid,
                part: Some(part),
                ..
            }
            | Self::ToolInputDelta {
                mid,
                part: Some(part),
                ..
            } => Some((mid.as_deref(), *part)),
            _ => None,
        }
    }

    pub fn set_mid(&mut self, value: Option<String>) {
        match self {
            Self::TextDelta { mid, .. }
            | Self::ReasoningDelta { mid, .. }
            | Self::ToolInputDelta { mid, .. } => *mid = value,
            _ => {}
        }
    }

    pub fn set_part(&mut self, value: u64) {
        match self {
            Self::TextDelta { part, .. }
            | Self::ReasoningDelta { part, .. }
            | Self::ToolInputDelta { part, .. } => *part = Some(value),
            _ => {}
        }
    }

    pub fn delta(&self) -> Option<&str> {
        match self {
            Self::TextDelta { delta, .. }
            | Self::ReasoningDelta { delta, .. }
            | Self::ToolInputDelta { delta, .. } => Some(delta),
            _ => None,
        }
    }

    pub fn append_delta(&mut self, text: &str) {
        match self {
            Self::TextDelta { delta, .. }
            | Self::ReasoningDelta { delta, .. }
            | Self::ToolInputDelta { delta, .. } => delta.push_str(text),
            _ => {}
        }
    }

    /// Owned string bytes retained by a coalesced delta, including its provider identities.
    pub fn retained_bytes(&self) -> usize {
        match self {
            Self::TextDelta {
                mid,
                message_id,
                tool_call_id,
                delta,
                ..
            } => mid
                .as_ref()
                .map_or(0, String::len)
                .saturating_add(message_id.0.len())
                .saturating_add(tool_call_id.as_ref().map_or(0, |id| id.0.len()))
                .saturating_add(delta.len()),
            Self::ReasoningDelta {
                mid,
                message_id,
                delta,
                ..
            } => mid
                .as_ref()
                .map_or(0, String::len)
                .saturating_add(message_id.0.len())
                .saturating_add(delta.len()),
            Self::ToolInputDelta {
                mid,
                tool_call_id,
                delta,
                ..
            } => mid
                .as_ref()
                .map_or(0, String::len)
                .saturating_add(tool_call_id.0.len())
                .saturating_add(delta.len()),
            _ => 0,
        }
    }
}

mod part_key {
    use serde::{Deserialize, Serialize};
    pub fn serialize<S: serde::Serializer>(
        value: &Option<u64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.map(|v| v.to_string()).serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u64>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|v| v.parse().map_err(serde::de::Error::custom))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::DisplayEvent;

    #[test]
    fn unknown_display_event_uses_the_reset_fallback() {
        let event: DisplayEvent = serde_json::from_value(serde_json::json!({
            "type": "future_display_delta",
            "payload": "ignored by this reader"
        }))
        .unwrap();
        assert_eq!(event, DisplayEvent::DisplayReset);
    }
}
