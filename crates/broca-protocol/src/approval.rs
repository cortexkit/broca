//! Tool-approval round-trip. The runner emits `ApprovalRequest` on the control
//! lane; the caller replies with `ApprovalResponse` on the control back-channel,
//! correlated by `tool_call_id` (and the envelope `corr`). Only the one tool
//! blocks — not the whole loop.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::ids::ToolCallId;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ApprovalRequest {
    pub tool_call_id: ToolCallId,
    pub tool_name: String,
    pub input: Value,
}

/// An approval may NARROW a call's input, never widen it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum ApprovalResponse {
    Allow {
        tool_call_id: ToolCallId,
    },
    /// Allow, but with narrowed arguments (a subset/restriction of the request).
    AllowNarrowed {
        tool_call_id: ToolCallId,
        input: Value,
    },
    Deny {
        tool_call_id: ToolCallId,
        reason: String,
    },
}

impl ApprovalResponse {
    pub fn tool_call_id(&self) -> &ToolCallId {
        match self {
            ApprovalResponse::Allow { tool_call_id }
            | ApprovalResponse::AllowNarrowed { tool_call_id, .. }
            | ApprovalResponse::Deny { tool_call_id, .. } => tool_call_id,
        }
    }
}
