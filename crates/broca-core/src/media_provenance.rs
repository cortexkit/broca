//! Where a `MediaRef` may enter a session.
//!
//! A media reference names bytes in the session's media store by digest. broca writes one into a
//! WAL record only from its own capture path: committing a person's upload, or capturing a tool
//! reply. Both publish bytes broca holds and register them in the session's manifest before the
//! record names them. A transcript a caller hands over (a wire message, a session import) holds
//! no bytes, only claims, so a `media` block or a `Parts` media part in it would name an object
//! the session never captured, possibly one another session stored. Such content is refused
//! right after the supplied transcript is decoded, before it can reach a WAL record; a person
//! names media only through upload references.

use broca_protocol::content::{ContentBlock, Message, ToolOutput, ToolOutputPart};

/// The first place a caller-supplied transcript names media directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SuppliedMedia {
    /// Index of the message in the supplied list.
    pub message: usize,
    /// Index of the content block in that message.
    pub block: usize,
    /// For a tool result, the index of the media part in its `Parts` output; `None` when the
    /// block itself is a `media` block.
    pub part: Option<usize>,
}

impl SuppliedMedia {
    /// The member path inside the supplied `messages` list, e.g.
    /// `messages[1].content[0].output.parts[2]`.
    pub fn path(&self) -> String {
        let mut path = format!("messages[{}].content[{}]", self.message, self.block);
        if let Some(part) = self.part {
            path.push_str(&format!(".output.parts[{part}]"));
        }
        path
    }
}

impl std::fmt::Display for SuppliedMedia {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let what = if self.part.is_some() {
            "a tool result media part"
        } else {
            "a media block"
        };
        write!(
            f,
            "{} carries {what}; media cannot be supplied inline and enter a session only \
             through upload references or tool capture, so nothing was written",
            self.path()
        )
    }
}

/// Find the first `media` block or `Parts` media part in a caller-supplied transcript.
///
/// Text-only `Parts` outputs are not media references and pass; only a part that names a
/// `MediaRef` is refused.
pub fn find_supplied_media(messages: &[Message]) -> Option<SuppliedMedia> {
    messages.iter().enumerate().find_map(|(message, entry)| {
        entry
            .content
            .iter()
            .enumerate()
            .find_map(|(block, content)| {
                supplied_media_in_block(content).map(|part| SuppliedMedia {
                    message,
                    block,
                    part,
                })
            })
    })
}

/// `Some(None)` for a media block, `Some(Some(j))` for a tool result whose part `j` is media.
fn supplied_media_in_block(block: &ContentBlock) -> Option<Option<usize>> {
    match block {
        ContentBlock::Media(_) => Some(None),
        ContentBlock::ToolResult(result) => match &result.output {
            ToolOutput::Parts { parts } => parts
                .iter()
                .position(|part| matches!(part, ToolOutputPart::Media { .. }))
                .map(Some),
            ToolOutput::Text { .. }
            | ToolOutput::Json { .. }
            | ToolOutput::Denied { .. }
            | ToolOutput::Error { .. } => None,
        },
        ContentBlock::Text { .. }
        | ContentBlock::TextWithState { .. }
        | ContentBlock::Reasoning { .. }
        | ContentBlock::RedactedReasoning { .. }
        | ContentBlock::ProviderFallback { .. }
        | ContentBlock::ToolCall(_) => None,
    }
}

#[cfg(test)]
#[path = "media_provenance_tests.rs"]
mod tests;
