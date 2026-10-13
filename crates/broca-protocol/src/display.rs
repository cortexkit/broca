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
        message_id: MessageId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<ToolCallId>,
        delta: String,
    },
    /// Coalesced reasoning text for live rendering.
    ReasoningDelta {
        message_id: MessageId,
        delta: String,
    },
    /// Coalesced partial tool-call input JSON (rendering only; the authoritative
    /// assembled input arrives on the control lane).
    ToolInputDelta {
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
    DisplayReset,
}
