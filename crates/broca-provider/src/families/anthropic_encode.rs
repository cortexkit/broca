//! Encode CK canonical messages back into Anthropic `messages` array elements.
//!
//! The encoder returns raw JSON message objects rather than `serde_json::Value`s so decoded
//! Anthropic `Opaque` blocks can ride back out byte-verbatim when a caller splices the encoded
//! `messages` array into a larger intercepted request body.

use std::fmt;

/// A structural failure proving a CK message array has no exact Anthropic `messages[]` inverse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncodeError {
    MediaBlock {
        message_index: usize,
        block_index: usize,
        result_block_index: Option<usize>,
    },
    OpaqueWithoutRaw {
        message_index: usize,
        block_index: usize,
        result_block_index: Option<usize>,
        kind: String,
    },
    InvalidOpaqueJson {
        message_index: usize,
        block_index: usize,
        result_block_index: Option<usize>,
        kind: String,
        error: String,
    },
    ReasoningWithoutSignature {
        message_index: usize,
        block_index: usize,
    },
    ForeignEncryptedReasoning {
        message_index: usize,
        block_index: usize,
    },
    ToolRoleNonResultBlock {
        message_index: usize,
        block_index: usize,
        kind: &'static str,
    },
    UnsupportedToolResultOutput {
        message_index: usize,
        block_index: usize,
        kind: &'static str,
    },
    ScalarFormIncompatible {
        message_index: usize,
        reason: &'static str,
    },
    Serialize(String),
}

impl fmt::Display for EncodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncodeError::MediaBlock {
                message_index,
                block_index,
                result_block_index,
            } => match result_block_index {
                Some(result_block_index) => write!(
                    f,
                    "Anthropic tool_result block at message {message_index}, block {block_index}, nested result block {result_block_index} cannot encode a typed Media block"
                ),
                None => write!(
                    f,
                    "Anthropic message {message_index} block {block_index} cannot encode a typed Media block"
                ),
            },
            EncodeError::OpaqueWithoutRaw {
                message_index,
                block_index,
                result_block_index,
                kind,
            } => match result_block_index {
                Some(result_block_index) => write!(
                    f,
                    "Anthropic tool_result block at message {message_index}, block {block_index}, nested result block {result_block_index} has Opaque kind {kind:?} without raw JSON bytes"
                ),
                None => write!(
                    f,
                    "Anthropic message {message_index} block {block_index} has Opaque kind {kind:?} without raw JSON bytes"
                ),
            },
            EncodeError::InvalidOpaqueJson {
                message_index,
                block_index,
                result_block_index,
                kind,
                error,
            } => match result_block_index {
                Some(result_block_index) => write!(
                    f,
                    "Anthropic tool_result block at message {message_index}, block {block_index}, nested result block {result_block_index} has Opaque kind {kind:?} with invalid raw JSON: {error}"
                ),
                None => write!(
                    f,
                    "Anthropic message {message_index} block {block_index} has Opaque kind {kind:?} with invalid raw JSON: {error}"
                ),
            },
            EncodeError::ReasoningWithoutSignature {
                message_index,
                block_index,
            } => write!(
                f,
                "Anthropic message {message_index} block {block_index} cannot encode a typed Reasoning block without a signature"
            ),
            EncodeError::ForeignEncryptedReasoning {
                message_index,
                block_index,
            } => write!(
                f,
                "Anthropic message {message_index} block {block_index} contains encrypted reasoning owned by another wire family"
            ),
            EncodeError::ToolRoleNonResultBlock {
                message_index,
                block_index,
                kind,
            } => write!(
                f,
                "Anthropic tool-role message {message_index} block {block_index} has unsupported kind {kind}; every Tool role block must encode as tool_result"
            ),
            EncodeError::UnsupportedToolResultOutput {
                message_index,
                block_index,
                kind,
            } => write!(
                f,
                "Anthropic message {message_index} block {block_index} has unsupported tool_result output kind {kind}"
            ),
            EncodeError::ScalarFormIncompatible {
                message_index,
                reason,
            } => write!(
                f,
                "Anthropic message {message_index} is marked content_form=scalar but its content \
                 cannot be spelled as a bare string: {reason}"
            ),
            EncodeError::Serialize(error) => write!(f, "failed to serialize Anthropic messages: {error}"),
        }
    }
}

impl std::error::Error for EncodeError {}
