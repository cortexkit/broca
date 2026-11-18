//! The released form of a content block.
//!
//! A long session keeps many messages whose content its requests no longer send: the compaction
//! view leaves the part out, or sends replacement text in its place. The content is in the
//! session's log, so memory keeps only what a reader needs without it: the block's kind, its ids
//! and its error flag, and whether each content field is empty, which the part numberings read.
//! Each non-empty content field holds the released value instead ([`Text::released`],
//! [`Json::released`]), which is recognised by identity, so a reader can tell a released block
//! from one whose content happens to read the same.

use super::{ContentBlock, ToolOutput, ToolOutputPart};
use crate::json::Json;
use crate::text::Text;

impl ContentBlock {
    /// This block with its content released, or `None` when it holds nothing worth releasing: a
    /// media reference, a fallback marker, empty text, or a tool result whose output is a denial
    /// or an error, which hold short plain strings rather than shared text, so releasing them
    /// would free almost nothing. Media parts of a tool result stay as they are,
    /// in place, so a renderer that keeps a result's attachments by index finds them unchanged.
    pub fn released(&self) -> Option<Self> {
        let released = match self {
            Self::Media(_) | Self::ProviderFallback { .. } => return None,
            Self::Text { text } => Self::Text {
                text: released_text(text)?,
            },
            Self::TextWithState {
                text,
                provider_state,
            } => Self::TextWithState {
                text: released_text(text)?,
                provider_state: provider_state.clone(),
            },
            Self::Reasoning { text, signature } => {
                let text_released = released_text(text);
                let signature_released = signature.as_ref().and_then(released_text);
                if text_released.is_none() && signature_released.is_none() {
                    return None;
                }
                Self::Reasoning {
                    text: text_released.unwrap_or_else(|| text.clone()),
                    signature: signature_released.or_else(|| signature.clone()),
                }
            }
            Self::RedactedReasoning {
                data,
                provider_format,
            } => Self::RedactedReasoning {
                data: released_text(data)?,
                provider_format: *provider_format,
            },
            Self::ToolCall(call) => {
                // `{}` is the shortest input there is; releasing it frees nothing.
                if call.input.text().len() <= 2 {
                    return None;
                }
                let mut call = call.clone();
                call.input = Json::released();
                Self::ToolCall(call)
            }
            Self::ToolResult(result) => {
                let output = released_output(&result.output)?;
                let mut result = result.clone();
                result.output = output;
                Self::ToolResult(result)
            }
        };
        Some(released)
    }

    /// Whether any content field of this block holds the released value.
    pub fn holds_released(&self) -> bool {
        match self {
            Self::Media(_) | Self::ProviderFallback { .. } => false,
            Self::Text { text } | Self::TextWithState { text, .. } => text.is_released(),
            Self::Reasoning { text, signature } => {
                text.is_released() || signature.as_ref().is_some_and(Text::is_released)
            }
            Self::RedactedReasoning { data, .. } => data.is_released(),
            Self::ToolCall(call) => call.input.is_released(),
            Self::ToolResult(result) => result.output.holds_released(),
        }
    }
}

impl ToolOutput {
    /// Whether any content of this output holds the released value.
    pub fn holds_released(&self) -> bool {
        match self {
            Self::Text { text } => text.is_released(),
            Self::Json { value } => value.is_released(),
            Self::Parts { parts } => parts
                .iter()
                .any(|part| matches!(part, ToolOutputPart::Text { text } if text.is_released())),
            Self::Denied { .. } | Self::Error { .. } => false,
        }
    }
}

/// The released text in place of `text`, unless `text` is empty: an empty text stays empty, so
/// whether it is empty reads the same either way.
fn released_text(text: &Text) -> Option<Text> {
    (!text.is_empty()).then(Text::released)
}

/// `output` with its content released, or `None` when nothing in it is released.
fn released_output(output: &ToolOutput) -> Option<ToolOutput> {
    match output {
        ToolOutput::Text { text } => Some(ToolOutput::Text {
            text: released_text(text)?,
        }),
        ToolOutput::Json { .. } => Some(ToolOutput::Json {
            value: Json::released(),
        }),
        ToolOutput::Parts { parts } => {
            let mut released_any = false;
            let parts = parts
                .iter()
                .map(|part| match part {
                    ToolOutputPart::Text { text } => match released_text(text) {
                        Some(text) => {
                            released_any = true;
                            ToolOutputPart::Text { text }
                        }
                        None => part.clone(),
                    },
                    ToolOutputPart::Media { .. } => part.clone(),
                })
                .collect();
            released_any.then_some(ToolOutput::Parts { parts })
        }
        ToolOutput::Denied { .. } | ToolOutput::Error { .. } => None,
    }
}

#[cfg(test)]
mod tests;
