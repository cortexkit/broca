//! Envelope times follow the shared placement decisions, never message content or replay clocks.

use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MessageTimeSeed {
    pub last_record_ms: Option<u64>,
    pub last_entry_ms: u64,
}

pub struct MessageTimeFold {
    pub times: BTreeMap<u64, u64>,
    pub seed: MessageTimeSeed,
    /// Source-derived positions of host-produced assistants, excluding supplied input.
    pub native_assistants: std::collections::BTreeSet<u64>,
}

/// Tool-bearing assistant entries finish only when every tool part is final, including
/// held calls and approvals. The caller supplies finality from the same entry snapshot.
pub fn completed_at(
    role: broca_protocol::Role,
    written_at: Option<u64>,
    all_tool_parts_final: bool,
) -> Option<u64> {
    match role {
        broca_protocol::Role::Assistant | broca_protocol::Role::Tool if all_tool_parts_final => {
            written_at
        }
        _ => None,
    }
}
