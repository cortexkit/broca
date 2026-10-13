//! Decode Anthropic `/v1/messages` request bodies into CK canonical messages.
//!
//! The decoder is deliberately permissive at the content-block layer: only the small
//! Anthropic typed core is recognized, and every other or malformed block is carried as an
//! Anthropic wire `Opaque` block instead of failing the request. That keeps a MITM proxy safe
//! when Anthropic adds new block types before this crate knows about them.

use std::{collections::BTreeMap, fmt};

use broca_ck::{
    CkMessage, ContentBlock as CkContentBlock, ContentKind, DecodedRequest, MessageOrigin,
    OpaqueArc, OpaqueArcKind, OpaqueArcRole, OpaqueBlock, OpaqueSource, ProviderExtras,
    ResultBlock, ResultBlockKind, Role, ToolResultOutput, ToolResultOutputKind,
};
use broca_protocol::request::Tool;
use serde_json::{value::RawValue, Map, Value};

const ANTHROPIC_PROVIDER: &str = "anthropic";

/// Message-level provider-extras key recording the wire spelling of `content`.
/// Shared contract with the encoder (and consumed by MITM legs): stamped at decode ONLY
/// when the wire carried the scalar-string form, and honored at encode by re-emitting
/// that form. Never minted for the block-array spelling.
pub const CONTENT_FORM_EXTRA: &str = "content_form";
/// The only `content_form` value: the wire spelled `content` as a bare JSON string.
pub const CONTENT_FORM_SCALAR: &str = "scalar";

/// A structural failure proving the bytes are not an Anthropic Messages request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    InvalidJson(String),
    MessagesNotArray,
    MessageWithoutRole { index: usize },
    InvalidRole { index: usize, role: String },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::InvalidJson(err) => write!(f, "invalid JSON: {err}"),
            DecodeError::MessagesNotArray => {
                write!(f, "Anthropic request messages must be an array")
            }
            DecodeError::MessageWithoutRole { index } => {
                write!(f, "Anthropic message at index {index} has no string role")
            }
            DecodeError::InvalidRole { index, role } => {
                write!(
                    f,
                    "Anthropic message at index {index} has unsupported role {role:?}"
                )
            }
        }
    }
}

impl std::error::Error for DecodeError {}

#[derive(Debug)]
struct RawRequest {
    model: Option<Value>,
    tools: Option<Value>,
    messages: Option<Box<RawValue>>,
    system: Option<Box<RawValue>>,
    residual: Map<String, Value>,
}

#[derive(Debug)]
struct RawMessage {
    role: Option<Value>,
    content: Option<Box<RawValue>>,
}

#[derive(Debug)]
struct RawContentBlock {
    block_type: Option<Value>,
    text: Option<Value>,
    thinking: Option<Value>,
    signature: Option<Value>,
    data: Option<Value>,
    id: Option<Value>,
    name: Option<Value>,
    input: Option<Value>,
    tool_use_id: Option<Value>,
    is_error: Option<Value>,
    content: Option<Box<RawValue>>,
    cache_control: Option<Value>,
    residual: BTreeMap<String, Box<RawValue>>,
}

impl RawContentBlock {
    fn from_raw(raw: &RawValue) -> Option<Self> {
        let mut map: BTreeMap<String, Box<RawValue>> = serde_json::from_str(raw.get()).ok()?;
        let block_type = map
            .remove("type")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let text = map
            .remove("text")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let thinking = map
            .remove("thinking")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let signature = map
            .remove("signature")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let data = map
            .remove("data")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let id = map
            .remove("id")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let name = map
            .remove("name")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let input = map
            .remove("input")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let tool_use_id = map
            .remove("tool_use_id")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let is_error = map
            .remove("is_error")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let content = map.remove("content");
        let cache_control = map
            .remove("cache_control")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        Some(RawContentBlock {
            block_type,
            text,
            thinking,
            signature,
            data,
            id,
            name,
            input,
            tool_use_id,
            is_error,
            content,
            cache_control,
            residual: map,
        })
    }
}

#[derive(Debug)]
struct RawResultBlock {
    block_type: Option<Value>,
    text: Option<Value>,
    cache_control: Option<Value>,
    residual: BTreeMap<String, Box<RawValue>>,
}

impl RawResultBlock {
    fn from_raw(raw: &RawValue) -> Option<Self> {
        let mut map: BTreeMap<String, Box<RawValue>> = serde_json::from_str(raw.get()).ok()?;
        let block_type = map
            .remove("type")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let text = map
            .remove("text")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let cache_control = map
            .remove("cache_control")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        Some(RawResultBlock {
            block_type,
            text,
            cache_control,
            residual: map,
        })
    }
}

enum JsonStringOrArray {
    String(String),
    Array(Vec<Box<RawValue>>),
    Other(Value),
}

struct ParsedMessage {
    role: WireRole,
    content: ParsedMessageContent,
}

enum ParsedMessageContent {
    Missing,
    String(String),
    Blocks(Vec<ParsedContentBlock>),
    Other,
}

struct ParsedContentBlock {
    raw: Box<RawValue>,
    fields: Option<RawContentBlock>,
}

struct ParsedResultBlock {
    raw: Box<RawValue>,
    fields: Option<RawResultBlock>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WireRole {
    System,
    User,
    Assistant,
}

impl RawContentBlock {
    fn block_type(&self) -> Option<&str> {
        string_value(self.block_type.as_ref())
    }

    fn text(&self) -> Option<&str> {
        string_value(self.text.as_ref())
    }

    fn thinking(&self) -> Option<&str> {
        string_value(self.thinking.as_ref())
    }

    fn signature(&self) -> Option<&str> {
        string_value(self.signature.as_ref())
    }

    fn data(&self) -> Option<&str> {
        string_value(self.data.as_ref())
    }

    fn id(&self) -> Option<&str> {
        string_value(self.id.as_ref())
    }

    fn name(&self) -> Option<&str> {
        string_value(self.name.as_ref())
    }

    fn tool_use_id(&self) -> Option<&str> {
        string_value(self.tool_use_id.as_ref())
    }

    fn is_error(&self) -> bool {
        bool_value(self.is_error.as_ref()).unwrap_or(false)
    }
}

impl RawResultBlock {
    fn block_type(&self) -> Option<&str> {
        string_value(self.block_type.as_ref())
    }

    fn text(&self) -> Option<&str> {
        string_value(self.text.as_ref())
    }
}

/// Decode an Anthropic Messages request body into CK canonical messages and request metadata.
pub fn decode(body: &[u8]) -> Result<DecodedRequest, DecodeError> {
    let RawRequest {
        model,
        tools,
        messages,
        system,
        residual,
    } = parse_request(body)?;
    let Some(raw_messages) = messages.as_deref() else {
        return Err(DecodeError::MessagesNotArray);
    };
    let raw_messages = parse_raw_array(raw_messages).map_err(|_| DecodeError::MessagesNotArray)?;
    let parsed_messages = raw_messages
        .iter()
        .enumerate()
        .map(|(index, raw_message)| parse_message(index, raw_message.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;

    let wire_model_id = model
        .as_ref()
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let mut messages = decode_system(system);
    let mut current_tool_call_names = BTreeMap::new();
    for parsed_message in &parsed_messages {
        if parsed_message.role == WireRole::Assistant {
            current_tool_call_names = tool_call_names_in_message(parsed_message);
        }
        messages.push(decode_message(
            parsed_message,
            &current_tool_call_names,
            &wire_model_id,
        ));
        if parsed_message.role != WireRole::Assistant {
            current_tool_call_names.clear();
        }
    }

    Ok(DecodedRequest {
        messages,
        tools: decode_tools(tools.as_ref()),
        wire_model_id,
        residual: Value::Object(residual),
    })
}

fn parse_request(body: &[u8]) -> Result<RawRequest, DecodeError> {
    let mut map: BTreeMap<String, Box<RawValue>> = match serde_json::from_slice(body) {
        Ok(m) => m,
        Err(err) => match serde_json::from_slice::<Value>(body) {
            Ok(_) => return Err(DecodeError::MessagesNotArray),
            Err(_) => return Err(invalid_json(err)),
        },
    };
    let model = map
        .remove("model")
        .and_then(|v| serde_json::from_str(v.get()).ok());
    let tools = map
        .remove("tools")
        .and_then(|v| serde_json::from_str(v.get()).ok());
    let messages = map.remove("messages");
    let system = map.remove("system");
    let mut residual = Map::new();
    for (k, v) in map {
        if let Ok(val) = serde_json::from_str(v.get()) {
            residual.insert(k, val);
        }
    }
    Ok(RawRequest {
        model,
        tools,
        messages,
        system,
        residual,
    })
}

fn parse_message(index: usize, raw_message: &RawValue) -> Result<ParsedMessage, DecodeError> {
    let mut map: BTreeMap<String, Box<RawValue>> = serde_json::from_str(raw_message.get())
        .map_err(|_| DecodeError::MessageWithoutRole { index })?;
    let role = map
        .remove("role")
        .and_then(|v| serde_json::from_str(v.get()).ok());
    let content = map.remove("content");
    let fields = RawMessage { role, content };
    let role_text =
        string_value(fields.role.as_ref()).ok_or(DecodeError::MessageWithoutRole { index })?;
    let role = match role_text {
        "user" => WireRole::User,
        "assistant" => WireRole::Assistant,
        // If the incoming turn has role="system", treat it as a normal system message.
        // Claude Code payloads send a plain-text <system-reminder> entry inside the message
        // array alongside the top-level system field, so this role stays typed instead of
        // being preserved as opaque data.
        "system" => WireRole::System,
        other => {
            return Err(DecodeError::InvalidRole {
                index,
                role: other.to_string(),
            })
        }
    };
    Ok(ParsedMessage {
        role,
        content: parse_message_content(fields.content),
    })
}

fn parse_message_content(content: Option<Box<RawValue>>) -> ParsedMessageContent {
    match content.as_deref().map(parse_json_string_or_array) {
        None => ParsedMessageContent::Missing,
        Some(JsonStringOrArray::String(text)) => ParsedMessageContent::String(text),
        Some(JsonStringOrArray::Array(raw_blocks)) => {
            ParsedMessageContent::Blocks(raw_blocks.into_iter().map(parse_content_block).collect())
        }
        Some(JsonStringOrArray::Other(_)) => ParsedMessageContent::Other,
    }
}

fn parse_content_block(raw_block: Box<RawValue>) -> ParsedContentBlock {
    ParsedContentBlock {
        fields: RawContentBlock::from_raw(raw_block.as_ref()),
        raw: raw_block,
    }
}

fn parse_result_block(raw_block: Box<RawValue>) -> ParsedResultBlock {
    ParsedResultBlock {
        fields: RawResultBlock::from_raw(raw_block.as_ref()),
        raw: raw_block,
    }
}

fn parse_json_string_or_array(raw: &RawValue) -> JsonStringOrArray {
    match first_non_ws_byte(raw.get()) {
        Some(b'"') => JsonStringOrArray::String(
            serde_json::from_str(raw.get()).expect("raw string came from valid JSON"),
        ),
        Some(b'[') => {
            JsonStringOrArray::Array(parse_raw_array(raw).expect("raw array came from valid JSON"))
        }
        _ => JsonStringOrArray::Other(
            serde_json::from_str(raw.get()).expect("raw JSON value came from valid JSON"),
        ),
    }
}

fn parse_raw_array(raw: &RawValue) -> Result<Vec<Box<RawValue>>, serde_json::Error> {
    serde_json::from_str(raw.get())
}

fn first_non_ws_byte(raw: &str) -> Option<u8> {
    raw.as_bytes()
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
}

fn invalid_json(err: serde_json::Error) -> DecodeError {
    DecodeError::InvalidJson(err.to_string())
}

fn decode_tools(value: Option<&Value>) -> Vec<Tool> {
    let Some(Value::Array(tools)) = value else {
        return Vec::new();
    };
    tools
        .iter()
        .filter_map(Value::as_object)
        .map(|tool| Tool {
            name: string_field(tool, "name").unwrap_or_default().to_string(),
            description: string_field(tool, "description")
                .unwrap_or_default()
                .to_string(),
            input_schema: tool.get("input_schema").cloned().unwrap_or(Value::Null),
            module: None,
            execution_mode: Default::default(),
        })
        .collect()
}

fn tool_call_names_in_message(message: &ParsedMessage) -> BTreeMap<String, String> {
    let ParsedMessageContent::Blocks(blocks) = &message.content else {
        return BTreeMap::new();
    };
    blocks
        .iter()
        .filter_map(|block| {
            let fields = block.fields.as_ref()?;
            if fields.block_type() != Some("tool_use") {
                return None;
            }
            Some((fields.id()?.to_string(), fields.name()?.to_string()))
        })
        .collect()
}

fn decode_system(system: Option<Box<RawValue>>) -> Vec<CkMessage> {
    let Some(system) = system else {
        return Vec::new();
    };
    match parse_json_string_or_array(system.as_ref()) {
        JsonStringOrArray::String(text) => {
            vec![system_message(vec![CkContentBlock::bare(
                ContentKind::Text { text },
            )])]
        }
        JsonStringOrArray::Array(raw_blocks) => {
            let tool_names = BTreeMap::new();
            raw_blocks
                .into_iter()
                .map(parse_content_block)
                .map(|block| {
                    system_message(vec![decode_content_block(
                        &block,
                        WireRole::System,
                        &tool_names,
                    )])
                })
                .collect()
        }
        JsonStringOrArray::Other(_) => Vec::new(),
    }
}

fn system_message(content: Vec<CkContentBlock>) -> CkMessage {
    CkMessage {
        role: Role::System,
        content,
        origin: None,
        provider_extras: ProviderExtras::new(),
        meta: Default::default(),
    }
}

fn decode_message(
    message: &ParsedMessage,
    tool_names: &BTreeMap<String, String>,
    wire_model_id: &str,
) -> CkMessage {
    let content = decode_content(&message.content, message.role, tool_names);
    let role = match message.role {
        WireRole::Assistant => Role::Assistant,
        WireRole::User if is_tool_result_turn(&message.content) => Role::Tool,
        WireRole::User => Role::User,
        WireRole::System => Role::System,
    };

    CkMessage {
        role,
        content,
        origin: (role == Role::Assistant).then(|| MessageOrigin {
            provider: ANTHROPIC_PROVIDER.to_string(),
            model: wire_model_id.to_string(),
            api: ANTHROPIC_PROVIDER.to_string(),
        }),
        provider_extras: scalar_content_provenance(&message.content),
        meta: Default::default(),
    }
}

/// Anthropic accepts `content` as either a scalar string or a block array; both decode to
/// the same typed blocks, but a byte-faithful re-encode must reproduce the original
/// spelling. Stamp message-level provenance ONLY for the scalar form: array-form messages
/// (the owned path's only spelling) carry no marker, so their encoding is unchanged.
fn scalar_content_provenance(content: &ParsedMessageContent) -> ProviderExtras {
    let mut extras = ProviderExtras::new();
    if matches!(content, ParsedMessageContent::String(_)) {
        let mut anthropic = BTreeMap::new();
        anthropic.insert(
            CONTENT_FORM_EXTRA.to_string(),
            Value::String(CONTENT_FORM_SCALAR.to_string()),
        );
        extras.insert(ANTHROPIC_PROVIDER.to_string(), anthropic);
    }
    extras
}

fn is_tool_result_turn(content: &ParsedMessageContent) -> bool {
    let ParsedMessageContent::Blocks(blocks) = content else {
        return false;
    };
    !blocks.is_empty()
        && blocks.iter().all(|block| {
            block.fields.as_ref().and_then(RawContentBlock::block_type) == Some("tool_result")
        })
}

fn decode_content(
    content: &ParsedMessageContent,
    wire_role: WireRole,
    tool_names: &BTreeMap<String, String>,
) -> Vec<CkContentBlock> {
    match content {
        ParsedMessageContent::Missing => Vec::new(),
        ParsedMessageContent::String(text) => {
            vec![CkContentBlock::bare(ContentKind::Text {
                text: text.clone(),
            })]
        }
        ParsedMessageContent::Blocks(blocks) => blocks
            .iter()
            .map(|block| decode_content_block(block, wire_role, tool_names))
            .collect(),
        ParsedMessageContent::Other => Vec::new(),
    }
}

fn decode_content_block(
    block: &ParsedContentBlock,
    wire_role: WireRole,
    tool_names: &BTreeMap<String, String>,
) -> CkContentBlock {
    let Some(fields) = block.fields.as_ref() else {
        return opaque_content_block(block.raw.as_ref(), None);
    };
    match fields.block_type() {
        Some("text") => fields
            .text()
            .map(|text| content_block(ContentKind::Text { text: text.into() }, fields))
            .unwrap_or_else(|| opaque_content_block(block.raw.as_ref(), Some(fields))),
        Some("thinking") => match (fields.thinking(), fields.signature()) {
            (Some(text), Some(signature)) => content_block(
                ContentKind::Reasoning {
                    text: text.into(),
                    signature: Some(signature.into()),
                },
                fields,
            ),
            _ => opaque_content_block(block.raw.as_ref(), Some(fields)),
        },
        Some("redacted_thinking") => fields
            .data()
            .map(|data| {
                content_block(
                    ContentKind::RedactedReasoning {
                        data: data.into(),
                        provider_format: None,
                    },
                    fields,
                )
            })
            .unwrap_or_else(|| opaque_content_block(block.raw.as_ref(), Some(fields))),
        Some("tool_use") if wire_role == WireRole::Assistant => {
            match (fields.id(), fields.name(), fields.input.as_ref()) {
                (Some(id), Some(name), Some(input)) => content_block(
                    ContentKind::ToolCall {
                        thought_signature: None,
                        id: id.into(),
                        name: name.into(),
                        input: input.clone(),
                        provider_executed: false,
                    },
                    fields,
                ),
                _ => opaque_content_block(block.raw.as_ref(), Some(fields)),
            }
        }
        Some("tool_result") if wire_role == WireRole::User => {
            decode_tool_result_block(fields, tool_names)
                .unwrap_or_else(|| opaque_content_block(block.raw.as_ref(), Some(fields)))
        }
        _ => opaque_content_block(block.raw.as_ref(), Some(fields)),
    }
}

fn decode_tool_result_block(
    fields: &RawContentBlock,
    tool_names: &BTreeMap<String, String>,
) -> Option<CkContentBlock> {
    let tool_use_id = fields.tool_use_id()?;
    let raw_content = fields.content.as_deref()?;
    let output = decode_tool_result_output(raw_content, fields.is_error());
    Some(content_block(
        ContentKind::ToolResult {
            id: tool_use_id.into(),
            tool_name: tool_names.get(tool_use_id).cloned().unwrap_or_default(),
            output,
            provider_executed: false,
        },
        fields,
    ))
}

fn decode_tool_result_output(content: &RawValue, is_error: bool) -> ToolResultOutput {
    let kind = match parse_json_string_or_array(content) {
        JsonStringOrArray::String(text) if is_error => ToolResultOutputKind::ErrorText { text },
        JsonStringOrArray::String(text) => ToolResultOutputKind::Text { text },
        JsonStringOrArray::Array(raw_blocks) => {
            let blocks = raw_blocks
                .into_iter()
                .map(parse_result_block)
                .map(|block| decode_result_block(&block))
                .collect();
            if is_error {
                ToolResultOutputKind::ErrorContent { blocks }
            } else {
                ToolResultOutputKind::Content { blocks }
            }
        }
        JsonStringOrArray::Other(value) if is_error => ToolResultOutputKind::ErrorJson { value },
        JsonStringOrArray::Other(value) => ToolResultOutputKind::Json { value },
    };
    ToolResultOutput {
        kind,
        provider_extras: ProviderExtras::new(),
    }
}

fn decode_result_block(block: &ParsedResultBlock) -> ResultBlock {
    let Some(fields) = block.fields.as_ref() else {
        return opaque_result_block(block.raw.as_ref(), None);
    };
    match fields.block_type() {
        Some("text") => fields
            .text()
            .map(|text| ResultBlock {
                kind: ResultBlockKind::Text { text: text.into() },
                provider_extras: provider_extras_from_block(
                    &fields.residual,
                    fields.cache_control.as_ref(),
                ),
            })
            .unwrap_or_else(|| opaque_result_block(block.raw.as_ref(), Some(fields))),
        _ => opaque_result_block(block.raw.as_ref(), Some(fields)),
    }
}

fn content_block(kind: ContentKind, fields: &RawContentBlock) -> CkContentBlock {
    CkContentBlock {
        kind,
        provider_extras: provider_extras_from_block(
            &fields.residual,
            fields.cache_control.as_ref(),
        ),
    }
}

fn opaque_content_block(raw_block: &RawValue, fields: Option<&RawContentBlock>) -> CkContentBlock {
    CkContentBlock {
        kind: ContentKind::Opaque(opaque_block(raw_block, fields)),
        provider_extras: provider_extras_from_cache_control(
            fields.and_then(|block| block.cache_control.as_ref()),
        ),
    }
}

fn opaque_result_block(raw_block: &RawValue, fields: Option<&RawResultBlock>) -> ResultBlock {
    ResultBlock {
        kind: ResultBlockKind::Opaque {
            opaque: opaque_block_from_type(
                raw_block,
                fields.and_then(RawResultBlock::block_type),
                None,
            ),
        },
        provider_extras: provider_extras_from_cache_control(
            fields.and_then(|block| block.cache_control.as_ref()),
        ),
    }
}

fn opaque_block(raw_block: &RawValue, fields: Option<&RawContentBlock>) -> OpaqueBlock {
    opaque_block_from_type(
        raw_block,
        fields.and_then(RawContentBlock::block_type),
        fields.and_then(derive_arc),
    )
}

fn opaque_block_from_type(
    raw_block: &RawValue,
    kind: Option<&str>,
    arc: Option<OpaqueArc>,
) -> OpaqueBlock {
    OpaqueBlock {
        source: OpaqueSource::Wire {
            wire: ANTHROPIC_PROVIDER.to_string(),
        },
        kind: kind.unwrap_or("unknown").to_string(),
        raw: raw_block.get().as_bytes().to_vec(),
        arc,
    }
}

fn derive_arc(fields: &RawContentBlock) -> Option<OpaqueArc> {
    let kind = fields.block_type()?;
    if kind == "server_tool_use" {
        return fields.id().map(|id| OpaqueArc {
            kind: OpaqueArcKind::Tool,
            id: id.to_string(),
            role: OpaqueArcRole::Request,
        });
    }
    if kind.ends_with("_tool_result") {
        return fields.tool_use_id().map(|id| OpaqueArc {
            kind: OpaqueArcKind::Tool,
            id: id.to_string(),
            role: OpaqueArcRole::Response,
        });
    }
    None
}

fn provider_extras_from_block(
    residual: &BTreeMap<String, Box<RawValue>>,
    cache_control: Option<&Value>,
) -> ProviderExtras {
    let mut extras = provider_extras_from_cache_control(cache_control);
    for (key, value) in residual {
        extras.insert_raw(ANTHROPIC_PROVIDER.to_string(), key.clone(), value.clone());
    }
    extras
}

fn provider_extras_from_cache_control(cache_control: Option<&Value>) -> ProviderExtras {
    let mut extras = ProviderExtras::new();
    if let Some(cache_control) = cache_control {
        let mut anthropic = BTreeMap::new();
        anthropic.insert("cache_control".to_string(), cache_control.clone());
        extras.insert(ANTHROPIC_PROVIDER.to_string(), anthropic);
    }
    extras
}

fn bool_value(value: Option<&Value>) -> Option<bool> {
    value.and_then(Value::as_bool)
}

fn string_value(value: Option<&Value>) -> Option<&str> {
    value.and_then(Value::as_str)
}

fn string_field<'a>(obj: &'a Map<String, Value>, field: &str) -> Option<&'a str> {
    obj.get(field).and_then(Value::as_str)
}
