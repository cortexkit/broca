//! Part numbering for `session.entries`, separate from the runner's own tag numbering.

use crate::{ContentBlock, Message, Role as HostRole};
use cortexkit_role_compaction::{
    part::{PartBody, ToolPart, ToolStatus},
    read::Role,
};

/// The view library sets the numbering rules; the host maps native text and calls to its parts.
pub fn numbered_parts(message: &Message) -> u64 {
    numbered_parts_of(message.role, message.content.iter().map(NumberingKind::of))
}

/// The part of a block's shape the numbering needs, even when the content isn't held.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumberingKind {
    Text,
    ToolCall,
    Other,
}

impl NumberingKind {
    pub fn of(block: &ContentBlock) -> Self {
        match block {
            ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. }
                if !text.is_empty() =>
            {
                Self::Text
            }
            ContentBlock::ToolCall(_) => Self::ToolCall,
            _ => Self::Other,
        }
    }
}

/// The count over a message's role and block shapes. Results share their call's number.
pub fn numbered_parts_of(role: HostRole, kinds: impl IntoIterator<Item = NumberingKind>) -> u64 {
    let role = match role {
        HostRole::Tool => return 0,
        HostRole::User => Role::User,
        HostRole::Assistant => Role::Assistant,
        HostRole::System => Role::System,
    };
    kinds
        .into_iter()
        .filter(|kind| {
            let body = match kind {
                NumberingKind::Text => PartBody::Text {
                    text: Default::default(),
                },
                NumberingKind::ToolCall => PartBody::Tool(ToolPart::new(
                    "",
                    "",
                    serde_json::Value::Null,
                    ToolStatus::Running,
                )),
                NumberingKind::Other => return false,
            };
            cortexkit_compaction_view::tags::is_numbered(&role, &body)
        })
        .count() as u64
}
