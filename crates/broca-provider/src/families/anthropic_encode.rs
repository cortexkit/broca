//! Encode CK canonical messages back into Anthropic `messages` array elements.
//!
//! The encoder returns raw JSON message objects rather than `serde_json::Value`s so decoded
//! Anthropic `Opaque` blocks can ride back out byte-verbatim when a caller splices the encoded
//! `messages` array into a larger intercepted request body.

use std::{collections::BTreeMap, fmt};

use broca_ck::{
    CkMessage, ContentBlock, ContentKind, OpaqueBlock, ProviderExtras, ResultBlock,
    ResultBlockKind, Role, ToolResultOutput, ToolResultOutputKind,
};
use serde::Serialize;
use serde_json::{value::RawValue, Value};

const ANTHROPIC_PROVIDER: &str = "anthropic";

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

#[derive(Serialize)]
struct WireMessage<'a> {
    role: &'a str,
    content: Content,
}

/// The two wire spellings of `content`. Anthropic accepts both; which one a message uses
/// is byte-affecting for MITM legs, so the decoder records the scalar form in message-level
/// provider extras and this encoder reproduces it.
enum Content {
    Blocks(Vec<Box<RawValue>>),
    Scalar(String),
}

impl Serialize for Content {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Content::Blocks(blocks) => blocks.serialize(serializer),
            Content::Scalar(text) => serializer.serialize_str(text),
        }
    }
}

#[derive(Serialize)]
struct WireTextBlock<'a> {
    #[serde(rename = "type")]
    block_type: &'static str,
    text: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<Box<RawValue>>,
    #[serde(flatten)]
    residual: BTreeMap<String, Box<RawValue>>,
}

#[derive(Serialize)]
struct WireThinkingBlock<'a> {
    #[serde(rename = "type")]
    block_type: &'static str,
    thinking: &'a str,
    signature: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<Box<RawValue>>,
    #[serde(flatten)]
    residual: BTreeMap<String, Box<RawValue>>,
}

#[derive(Serialize)]
struct WireRedactedThinkingBlock<'a> {
    #[serde(rename = "type")]
    block_type: &'static str,
    data: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<Box<RawValue>>,
    #[serde(flatten)]
    residual: BTreeMap<String, Box<RawValue>>,
}

#[derive(Serialize)]
struct WireToolUseBlock<'a> {
    #[serde(rename = "type")]
    block_type: &'static str,
    id: &'a str,
    name: &'a str,
    input: &'a Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<Box<RawValue>>,
    #[serde(flatten)]
    residual: BTreeMap<String, Box<RawValue>>,
}

#[derive(Serialize)]
struct WireToolResultBlock<'a> {
    #[serde(rename = "type")]
    block_type: &'static str,
    tool_use_id: &'a str,
    content: Box<RawValue>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    is_error: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    cache_control: Option<Box<RawValue>>,
    #[serde(flatten)]
    residual: BTreeMap<String, Box<RawValue>>,
}

/// Encode CK messages into raw Anthropic `messages[]` elements.
///
/// The return type is `Vec<Box<RawValue>>` instead of `Vec<Value>` so any decoded Anthropic
/// `Opaque` block can be re-emitted byte-verbatim inside its containing message object.
pub fn encode_messages(messages: &[CkMessage]) -> Result<Vec<Box<RawValue>>, EncodeError> {
    messages
        .iter()
        .enumerate()
        .map(|(message_index, message)| encode_message(message_index, message))
        .collect()
}

fn encode_message(message_index: usize, message: &CkMessage) -> Result<Box<RawValue>, EncodeError> {
    let role = match message.role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "user",
    };
    let scalar = match content_form(message_index, &message.provider_extras)? {
        Some(_) => scalar_text(&message.content),
        None => None,
    };
    let content = match scalar {
        Some(text) => Content::Scalar(text),
        None => Content::Blocks(encode_content(
            message_index,
            message.role,
            &message.content,
        )?),
    };
    to_raw_json(&WireMessage { role, content })
}

/// Read the decoder's `content_form` marker. Absent (every owned-path message) keeps the
/// block-array spelling; the scalar marker is a HINT, not a command. Only an unrecognized
/// marker value fails loud, since that proves a contract mismatch rather than drift.
fn content_form(message_index: usize, extras: &ProviderExtras) -> Result<Option<()>, EncodeError> {
    let Some(form) = extras
        .get(ANTHROPIC_PROVIDER)
        .and_then(|anthropic| anthropic.get(super::anthropic_decode::CONTENT_FORM_EXTRA))
    else {
        return Ok(None);
    };
    if form.as_str() == Some(super::anthropic_decode::CONTENT_FORM_SCALAR) {
        return Ok(Some(()));
    }
    Err(EncodeError::ScalarFormIncompatible {
        message_index,
        reason: "unrecognized content_form marker value",
    })
}

/// The scalar spelling can carry exactly one plain text block with no block-level extras
/// (a `cache_control` would be silently dropped by a bare string). Content that has since
/// outgrown the scalar form (mutation added blocks or extras after decode) falls back to
/// the complete block-array spelling: nothing may ever be dropped to honor a spelling hint.
fn scalar_text(content: &[ContentBlock]) -> Option<String> {
    let [block] = content else {
        return None;
    };
    if !block.provider_extras.is_empty() {
        return None;
    }
    match &block.kind {
        ContentKind::Text { text } => Some(text.clone()),
        _ => None,
    }
}

fn encode_content(
    message_index: usize,
    role: Role,
    content: &[ContentBlock],
) -> Result<Vec<Box<RawValue>>, EncodeError> {
    content
        .iter()
        .enumerate()
        .map(|(block_index, block)| {
            if role == Role::Tool && !matches!(block.kind, ContentKind::ToolResult { .. }) {
                return Err(EncodeError::ToolRoleNonResultBlock {
                    message_index,
                    block_index,
                    kind: content_kind_name(&block.kind),
                });
            }
            encode_content_block(message_index, block_index, block)
        })
        .collect()
}

fn encode_content_block(
    message_index: usize,
    block_index: usize,
    block: &ContentBlock,
) -> Result<Box<RawValue>, EncodeError> {
    let cache_control = anthropic_cache_control(&block.provider_extras);
    let residual = anthropic_residual_fields(&block.provider_extras)?;
    match &block.kind {
        ContentKind::Text { text } => to_raw_json(&WireTextBlock {
            block_type: "text",
            text,
            cache_control,
            residual: residual.clone(),
        }),
        ContentKind::Reasoning { text, signature } => {
            let Some(signature) = signature.as_deref() else {
                return Err(EncodeError::ReasoningWithoutSignature {
                    message_index,
                    block_index,
                });
            };
            to_raw_json(&WireThinkingBlock {
                block_type: "thinking",
                thinking: text,
                signature,
                cache_control,
                residual: residual.clone(),
            })
        }
        ContentKind::RedactedReasoning {
            data,
            provider_format,
        } => {
            if provider_format.is_some() {
                return Err(EncodeError::ForeignEncryptedReasoning {
                    message_index,
                    block_index,
                });
            }
            to_raw_json(&WireRedactedThinkingBlock {
                block_type: "redacted_thinking",
                data,
                cache_control,
                residual: residual.clone(),
            })
        }
        ContentKind::ToolCall {
            id, name, input, ..
        } => to_raw_json(&WireToolUseBlock {
            block_type: "tool_use",
            id,
            name,
            input,
            cache_control,
            residual: residual.clone(),
        }),
        ContentKind::ToolResult { id, output, .. } => {
            let (content, is_error) =
                encode_tool_result_output(message_index, block_index, output)?;
            to_raw_json(&WireToolResultBlock {
                block_type: "tool_result",
                tool_use_id: id,
                content,
                is_error,
                cache_control,
                residual,
            })
        }
        ContentKind::Media(_) => Err(EncodeError::MediaBlock {
            message_index,
            block_index,
            result_block_index: None,
        }),
        ContentKind::Opaque(opaque) => {
            raw_value_from_opaque(opaque, message_index, block_index, None)
        }
    }
}

fn encode_tool_result_output(
    message_index: usize,
    block_index: usize,
    output: &ToolResultOutput,
) -> Result<(Box<RawValue>, bool), EncodeError> {
    match &output.kind {
        ToolResultOutputKind::Text { text } => Ok((to_raw_json(text)?, false)),
        ToolResultOutputKind::Json { value } => Ok((to_raw_json(value)?, false)),
        ToolResultOutputKind::ErrorText { text } => Ok((to_raw_json(text)?, true)),
        ToolResultOutputKind::ErrorJson { value } => Ok((to_raw_json(value)?, true)),
        ToolResultOutputKind::Content { blocks } => {
            let blocks = blocks
                .iter()
                .enumerate()
                .map(|(result_block_index, block)| {
                    encode_result_block(message_index, block_index, result_block_index, block)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((to_raw_json(&blocks)?, false))
        }
        ToolResultOutputKind::ErrorContent { blocks } => {
            // Anthropic renders error content blocks exactly like successful content blocks;
            // only the enclosing tool_result's is_error bit distinguishes the outcome.
            let blocks = blocks
                .iter()
                .enumerate()
                .map(|(result_block_index, block)| {
                    encode_result_block(message_index, block_index, result_block_index, block)
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok((to_raw_json(&blocks)?, true))
        }
        ToolResultOutputKind::ExecutionDenied { .. } => {
            Err(EncodeError::UnsupportedToolResultOutput {
                message_index,
                block_index,
                kind: tool_result_output_kind_name(&output.kind),
            })
        }
    }
}

fn encode_result_block(
    message_index: usize,
    block_index: usize,
    result_block_index: usize,
    block: &ResultBlock,
) -> Result<Box<RawValue>, EncodeError> {
    let cache_control = anthropic_cache_control(&block.provider_extras);
    let residual = anthropic_residual_fields(&block.provider_extras)?;
    match &block.kind {
        ResultBlockKind::Text { text } => to_raw_json(&WireTextBlock {
            block_type: "text",
            text,
            cache_control,
            residual: residual.clone(),
        }),
        ResultBlockKind::Media { .. } => Err(EncodeError::MediaBlock {
            message_index,
            block_index,
            result_block_index: Some(result_block_index),
        }),
        ResultBlockKind::Opaque { opaque } => {
            raw_value_from_opaque(opaque, message_index, block_index, Some(result_block_index))
        }
    }
}

fn anthropic_cache_control(provider_extras: &ProviderExtras) -> Option<Box<RawValue>> {
    provider_extras
        .get(ANTHROPIC_PROVIDER)
        .and_then(|anthropic| anthropic.get("cache_control"))
        .map(ordered_cache_control)
}

/// Collect retained Anthropic extension fields after the typed fields and cache marker.
/// `ProviderExtras`' B-tree maps make this residual-field order deterministic; that ordering is
/// identity-bearing, while each retained value itself remains a source `RawValue`.
fn anthropic_residual_fields(
    provider_extras: &ProviderExtras,
) -> Result<BTreeMap<String, Box<RawValue>>, EncodeError> {
    let Some(anthropic) = provider_extras.get(ANTHROPIC_PROVIDER) else {
        return Ok(BTreeMap::new());
    };
    anthropic
        .iter()
        .filter(|(key, _)| key.as_str() != "cache_control")
        .map(|(key, value)| {
            let raw = provider_extras
                .raw_value(ANTHROPIC_PROVIDER, key)
                .map(ToOwned::to_owned)
                .unwrap_or(to_raw_json(value)?);
            Ok((key.clone(), raw))
        })
        .collect()
}

/// Serialize a `cache_control` object with Anthropic's authored key order: `type` first,
/// `ttl` second, any remaining fields in sorted order. The semantic value rides CK as a
/// plain JSON object whose map sorts keys alphabetically, which would re-spell an authored
/// `{"type":...,"ttl":...}` as `{"ttl":...,"type":...}` — byte drift that busts the
/// provider's prefix cache on MITM legs even though the meaning is identical.
fn ordered_cache_control(value: &Value) -> Box<RawValue> {
    let Value::Object(fields) = value else {
        // Non-object spellings pass through with their default serialization.
        return RawValue::from_string(value.to_string()).expect("serde_json Value is valid JSON");
    };
    let leading = ["type", "ttl"]
        .into_iter()
        .filter_map(|key| fields.get(key).map(|val| (key, val)));
    let rest = fields
        .iter()
        .filter(|(key, _)| key.as_str() != "type" && key.as_str() != "ttl")
        .map(|(key, val)| (key.as_str(), val));
    let entries: Vec<String> = leading
        .chain(rest)
        .map(|(key, val)| {
            format!(
                "{}:{}",
                serde_json::to_string(key).expect("string key"),
                val
            )
        })
        .collect();
    RawValue::from_string(format!("{{{}}}", entries.join(",")))
        .expect("hand-built JSON object is valid")
}

fn raw_value_from_opaque(
    opaque: &OpaqueBlock,
    message_index: usize,
    block_index: usize,
    result_block_index: Option<usize>,
) -> Result<Box<RawValue>, EncodeError> {
    if opaque.raw.is_empty() {
        return Err(EncodeError::OpaqueWithoutRaw {
            message_index,
            block_index,
            result_block_index,
            kind: opaque.kind.clone(),
        });
    }
    let raw =
        String::from_utf8(opaque.raw.clone()).map_err(|error| EncodeError::InvalidOpaqueJson {
            message_index,
            block_index,
            result_block_index,
            kind: opaque.kind.clone(),
            error: error.to_string(),
        })?;
    RawValue::from_string(raw).map_err(|error| EncodeError::InvalidOpaqueJson {
        message_index,
        block_index,
        result_block_index,
        kind: opaque.kind.clone(),
        error: error.to_string(),
    })
}

fn to_raw_json<T: ?Sized + Serialize>(value: &T) -> Result<Box<RawValue>, EncodeError> {
    let raw =
        serde_json::to_string(value).map_err(|error| EncodeError::Serialize(error.to_string()))?;
    RawValue::from_string(raw).map_err(|error| EncodeError::Serialize(error.to_string()))
}

fn content_kind_name(kind: &ContentKind) -> &'static str {
    match kind {
        ContentKind::Text { .. } => "text",
        ContentKind::Reasoning { .. } => "reasoning",
        ContentKind::RedactedReasoning { .. } => "redacted_reasoning",
        ContentKind::ToolCall { .. } => "tool_call",
        ContentKind::ToolResult { .. } => "tool_result",
        ContentKind::Media(_) => "media",
        ContentKind::Opaque(_) => "opaque",
    }
}

fn tool_result_output_kind_name(kind: &ToolResultOutputKind) -> &'static str {
    match kind {
        ToolResultOutputKind::Text { .. } => "text",
        ToolResultOutputKind::Json { .. } => "json",
        ToolResultOutputKind::ErrorText { .. } => "error_text",
        ToolResultOutputKind::ErrorJson { .. } => "error_json",
        ToolResultOutputKind::ExecutionDenied { .. } => "execution_denied",
        ToolResultOutputKind::Content { .. } => "content",
        ToolResultOutputKind::ErrorContent { .. } => "error_content",
    }
}
