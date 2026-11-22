//! Fixtures the compaction tests share.

use super::*;

pub(super) fn text(role: Role, text: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: text.into() }])
}
