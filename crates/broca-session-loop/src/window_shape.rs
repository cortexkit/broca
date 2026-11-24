//! What the run loop reads from a window message's shape: the parts of a message that stay in
//! memory when its content is released (see `broca_session_store::fold::conversation`'s
//! `WindowMessage`). Each reader here gives the same answer for a released message as for the
//! held one.

use broca_core::compaction_v2::systems::is_global_role;
use broca_protocol::ids::ToolCallId;
use broca_protocol::tags::{cleans, number_kinds, ComputedRules, NumberingOverflow};
use broca_session_store::fold::conversation::{BlockKind, WindowMessage};

pub(crate) use broca_session_store::placement::window_numbered_parts as numbered_parts;

/// Whether the message is a global system message (see `systems::is_global`).
pub(crate) fn is_global(message: &WindowMessage) -> bool {
    is_global_role(message.role(), message.turn_system())
}

/// The ids of the message's tool calls, in order.
pub(crate) fn call_ids(message: &WindowMessage) -> Vec<ToolCallId> {
    message
        .blocks()
        .filter(|block| block.kind() == BlockKind::ToolCall)
        .filter_map(|block| block.tool_call_id().cloned())
        .collect()
}

/// The id of the call the block at `part` makes, when that block is a tool call.
pub(crate) fn call_at(message: &WindowMessage, part: usize) -> Option<&ToolCallId> {
    message
        .blocks()
        .nth(part)
        .filter(|block| block.kind() == BlockKind::ToolCall)
        .and_then(|block| block.tool_call_id())
}

/// The highest number the message's parts take in the runner's own numbering when numbered
/// after `high` under `rules`, or `high` when none counts: what
/// `broca_protocol::tags::number_message` leaves in `high`.
pub(crate) fn runner_high(
    message: &WindowMessage,
    rules: ComputedRules,
    high: u64,
) -> Result<u64, NumberingOverflow> {
    let clean = cleans(rules, message.role());
    let kinds = message
        .blocks()
        .map(|block| block.runner_kind(message.role(), clean));
    let parts = number_kinds(kinds, rules.render, high)?;
    Ok(parts.into_iter().flatten().max().unwrap_or(high))
}
