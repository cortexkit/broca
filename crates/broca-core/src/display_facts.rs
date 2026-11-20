//! Rebuildable display-envelope facts, folded once and continued from a per-session seed.

use std::collections::BTreeMap;

use cortexkit_role_llm_runner::read::ToolTime;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisplayFacts {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub send_id: Option<String>,
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

impl DisplayFacts {}
