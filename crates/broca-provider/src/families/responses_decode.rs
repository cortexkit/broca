//! Decode OpenAI `/responses` request bodies into CK canonical messages.
//!
//! The decoder is deliberately permissive below the small typed core: only OpenAI Responses
//! `message`, `function_call`, and `function_call_output` items (plus text result parts) are
//! recognized, and every other or malformed item rides through as an `Opaque` block instead of
//! failing the request. That keeps a MITM splice proxy safe when the Responses API adds new item
//! or part types before this crate learns about them.
//!
//! Responses `reasoning` items are the load-bearing example. Codex sends encrypted reasoning as
//! standalone request items that the client must echo back byte-verbatim on the next turn. The
//! decoder therefore carries each one as an ATOMIC `Opaque` block with a `Reasoning` arc keyed by
//! the item id. The wire contract pairs that reasoning item with the `function_call` that
//! immediately follows it, so adjacent assistant-side items coalesce into one assistant message
//! and preserve block order: the reasoning opaque stays directly in front of its paired tool call
//! without inventing a new typed field.

use std::{collections::BTreeMap, fmt};

use broca_ck::{
    CkMessage, ContentBlock as CkContentBlock, ContentKind, DecodedRequest, MessageOrigin,
    OpaqueArc, OpaqueArcKind, OpaqueArcRole, OpaqueBlock, OpaqueSource, ProviderExtras,
    ResultBlock, ResultBlockKind, Role, ToolResultOutput, ToolResultOutputKind,
};
use broca_protocol::request::Tool;
use serde_json::{value::RawValue, Map, Value};

const OPENAI_PROVIDER: &str = "openai";
const OPENAI_RESPONSES_FAMILY: &str = "openai_responses";

/// A structural failure proving the bytes are not an OpenAI Responses request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DecodeError {
    InvalidJson(String),
    InputNotArray,
    ItemWithoutType { index: usize },
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DecodeError::InvalidJson(err) => write!(f, "invalid JSON: {err}"),
            DecodeError::InputNotArray => {
                write!(
                    f,
                    "OpenAI Responses request input must be an array or string"
                )
            }
            DecodeError::ItemWithoutType { index } => {
                write!(
                    f,
                    "OpenAI Responses input item at index {index} has no string type"
                )
            }
        }
    }
}

impl std::error::Error for DecodeError {}

#[derive(Debug)]
struct RawRequest {
    model: Option<Value>,
    input: Option<Box<RawValue>>,
    instructions: Option<Value>,
    residual: Map<String, Value>,
}

#[derive(Debug)]
struct RawInputItem {
    item_type: Option<Value>,
    role: Option<Value>,
    id: Option<Value>,
    call_id: Option<Value>,
    name: Option<Value>,
    arguments: Option<Value>,
    content: Option<Box<RawValue>>,
    output: Option<Box<RawValue>>,
    residual: BTreeMap<String, Box<RawValue>>,
}

impl RawInputItem {
    fn from_raw(raw: &RawValue) -> Option<Self> {
        let mut map: BTreeMap<String, Box<RawValue>> = serde_json::from_str(raw.get()).ok()?;
        let item_type = map
            .remove("type")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let role = map
            .remove("role")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let id = map
            .remove("id")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let call_id = map
            .remove("call_id")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let name = map
            .remove("name")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let arguments = map
            .remove("arguments")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let content = map.remove("content");
        let output = map.remove("output");
        Some(RawInputItem {
            item_type,
            role,
            id,
            call_id,
            name,
            arguments,
            content,
            output,
            residual: map,
        })
    }
}

#[derive(Debug)]
struct RawMessagePart {
    part_type: Option<Value>,
    text: Option<Value>,
    residual: BTreeMap<String, Box<RawValue>>,
}

impl RawMessagePart {
    fn from_raw(raw: &RawValue) -> Option<Self> {
        let mut map: BTreeMap<String, Box<RawValue>> = serde_json::from_str(raw.get()).ok()?;
        let part_type = map
            .remove("type")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        let text = map
            .remove("text")
            .and_then(|v| serde_json::from_str(v.get()).ok());
        Some(RawMessagePart {
            part_type,
            text,
            residual: map,
        })
    }
}

#[derive(Debug)]
struct RawResultBlock {
    block_type: Option<Value>,
    text: Option<Value>,
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
        Some(RawResultBlock {
            block_type,
            text,
            residual: map,
        })
    }
}

enum JsonStringOrArray {
    String(String),
    Array(Vec<Box<RawValue>>),
    Other(Value),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AssistantCoalesce {
    Yes,
    No,
}

struct ParsedInputItem {
    raw: Box<RawValue>,
    fields: RawInputItem,
}

struct ParsedMessagePart {
    raw: Box<RawValue>,
    fields: Option<RawMessagePart>,
}

struct ParsedResultBlock {
    raw: Box<RawValue>,
    fields: Option<RawResultBlock>,
}

struct DecodedItem {
    role: Role,
    content: Vec<CkContentBlock>,
    coalesce_assistant: AssistantCoalesce,
}

impl RawInputItem {
    fn item_type(&self) -> Option<&str> {
        string_value(self.item_type.as_ref())
    }

    fn role(&self) -> Option<&str> {
        string_value(self.role.as_ref())
    }

    fn id(&self) -> Option<&str> {
        string_value(self.id.as_ref())
    }

    fn call_id(&self) -> Option<&str> {
        string_value(self.call_id.as_ref())
    }

    fn name(&self) -> Option<&str> {
        string_value(self.name.as_ref())
    }

    fn arguments(&self) -> Option<&str> {
        string_value(self.arguments.as_ref())
    }
}

impl RawMessagePart {
    fn part_type(&self) -> Option<&str> {
        string_value(self.part_type.as_ref())
    }

    fn text(&self) -> Option<&str> {
        string_value(self.text.as_ref())
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

/// Decode an OpenAI Responses request body into CK canonical messages and request metadata.
pub fn decode(body: &[u8]) -> Result<DecodedRequest, DecodeError> {
    let RawRequest {
        model,
        input,
        instructions,
        residual,
    } = parse_request(body)?;
    let wire_model_id = model
        .as_ref()
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let tools = decode_tools(residual.get("tools"));

    let mut messages = decode_instructions(instructions.as_ref());
    messages.extend(decode_input(input, &wire_model_id)?);

    Ok(DecodedRequest {
        messages,
        tools,
        wire_model_id,
        residual: Value::Object(residual),
    })
}

fn parse_request(body: &[u8]) -> Result<RawRequest, DecodeError> {
    let mut map: BTreeMap<String, Box<RawValue>> = match serde_json::from_slice(body) {
        Ok(m) => m,
        Err(err) => match serde_json::from_slice::<Value>(body) {
            Ok(_) => return Err(DecodeError::InputNotArray),
            Err(_) => return Err(invalid_json(err)),
        },
    };
    let model = map
        .remove("model")
        .and_then(|v| serde_json::from_str(v.get()).ok());
    let input = map.remove("input");
    let instructions = map
        .remove("instructions")
        .and_then(|v| serde_json::from_str(v.get()).ok());
    let mut residual = Map::new();
    for (k, v) in map {
        if let Ok(val) = serde_json::from_str(v.get()) {
            residual.insert(k, val);
        }
    }
    Ok(RawRequest {
        model,
        input,
        instructions,
        residual,
    })
}

fn parse_item(index: usize, raw_item: Box<RawValue>) -> Result<ParsedInputItem, DecodeError> {
    let fields =
        RawInputItem::from_raw(raw_item.as_ref()).ok_or(DecodeError::ItemWithoutType { index })?;
    if fields.item_type().is_none() {
        return Err(DecodeError::ItemWithoutType { index });
    }
    Ok(ParsedInputItem {
        raw: raw_item,
        fields,
    })
}

fn parse_message_part(raw_part: Box<RawValue>) -> ParsedMessagePart {
    ParsedMessagePart {
        fields: RawMessagePart::from_raw(raw_part.as_ref()),
        raw: raw_part,
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

fn decode_instructions(instructions: Option<&Value>) -> Vec<CkMessage> {
    match instructions.and_then(Value::as_str) {
        Some(text) => vec![system_message(vec![CkContentBlock::bare(
            ContentKind::Text {
                text: text.to_string(),
            },
        )])],
        None => Vec::new(),
    }
}

fn decode_input(
    input: Option<Box<RawValue>>,
    wire_model_id: &str,
) -> Result<Vec<CkMessage>, DecodeError> {
    let Some(input) = input else {
        return Err(DecodeError::InputNotArray);
    };
    match parse_json_string_or_array(input.as_ref()) {
        JsonStringOrArray::String(text) => Ok(vec![plain_message(
            Role::User,
            vec![CkContentBlock::bare(ContentKind::Text { text })],
        )]),
        JsonStringOrArray::Array(raw_items) => {
            let parsed_items = raw_items
                .into_iter()
                .enumerate()
                .map(|(index, raw_item)| parse_item(index, raw_item))
                .collect::<Result<Vec<_>, _>>()?;
            let mut out = Vec::new();
            let mut assistant_buf: Option<CkMessage> = None;
            let mut current_tool_call_names = BTreeMap::new();
            let mut previous_item_type = None;
            for item in &parsed_items {
                let item_type = item
                    .fields
                    .item_type()
                    .expect("parse_item guarantees a string type");
                match item_type {
                    "function_call" => {
                        if previous_item_type != Some("function_call") {
                            current_tool_call_names.clear();
                        }
                        if let (Some(call_id), Some(name)) =
                            (item.fields.call_id(), item.fields.name())
                        {
                            current_tool_call_names.insert(call_id.to_string(), name.to_string());
                        }
                    }
                    "function_call_output" => {
                        if !matches!(
                            previous_item_type,
                            Some("function_call" | "function_call_output")
                        ) {
                            current_tool_call_names.clear();
                        }
                    }
                    _ => current_tool_call_names.clear(),
                }

                let decoded = decode_item(item, &current_tool_call_names);
                push_decoded_item(&mut out, &mut assistant_buf, decoded, wire_model_id);
                previous_item_type = Some(item_type);
            }
            if let Some(message) = assistant_buf {
                out.push(message);
            }
            Ok(out)
        }
        JsonStringOrArray::Other(_) => Err(DecodeError::InputNotArray),
    }
}

fn push_decoded_item(
    out: &mut Vec<CkMessage>,
    assistant_buf: &mut Option<CkMessage>,
    decoded: DecodedItem,
    wire_model_id: &str,
) {
    if decoded.role == Role::Assistant {
        let message =
            assistant_buf.get_or_insert_with(|| assistant_message(Vec::new(), wire_model_id));
        message.content.extend(decoded.content);
        if decoded.coalesce_assistant == AssistantCoalesce::No {
            out.push(
                assistant_buf
                    .take()
                    .expect("assistant buffer just inserted"),
            );
        }
        return;
    }
    if let Some(message) = assistant_buf.take() {
        out.push(message);
    }
    out.push(plain_message(decoded.role, decoded.content));
}

fn decode_item(item: &ParsedInputItem, tool_names: &BTreeMap<String, String>) -> DecodedItem {
    match item.fields.item_type() {
        Some("message") => decode_message_item(item.raw.as_ref(), &item.fields),
        Some("function_call") => decode_function_call_item(item.raw.as_ref(), &item.fields),
        Some("function_call_output") => {
            decode_function_call_output_item(item.raw.as_ref(), &item.fields, tool_names)
        }
        Some("reasoning") => decode_reasoning_item(item.raw.as_ref(), &item.fields),
        Some(_) => decode_unknown_item(item.raw.as_ref(), &item.fields),
        None => unreachable!("parse_item guarantees a string type"),
    }
}

fn decode_message_item(raw_item: &RawValue, fields: &RawInputItem) -> DecodedItem {
    let Some(role_text) = fields.role() else {
        return opaque_item(
            raw_item,
            fields.item_type(),
            Some(Role::User),
            AssistantCoalesce::No,
        );
    };
    let role = match role_text {
        "system" | "developer" => Role::System,
        "user" => Role::User,
        "assistant" => Role::Assistant,
        _ => {
            return opaque_item(
                raw_item,
                fields.item_type(),
                Some(Role::User),
                AssistantCoalesce::No,
            )
        }
    };
    let Some(content) = fields.content.as_deref() else {
        return opaque_item(
            raw_item,
            fields.item_type(),
            Some(role),
            coalesce_for_role(role),
        );
    };
    match parse_json_string_or_array(content) {
        JsonStringOrArray::String(text) => DecodedItem {
            role,
            content: vec![CkContentBlock {
                kind: ContentKind::Text { text },
                provider_extras: provider_extras_from_residual(&fields.residual),
            }],
            coalesce_assistant: coalesce_for_role(role),
        },
        JsonStringOrArray::Array(raw_parts) => {
            let mut content = raw_parts
                .into_iter()
                .map(parse_message_part)
                .map(|part| decode_message_part(&part))
                .collect::<Vec<_>>();
            // An item-level extension has one CK block-level home. Attach it to the first
            // decoded part instead of duplicating the field across every part.
            if let Some(first) = content.first_mut() {
                first
                    .provider_extras
                    .merge_from(provider_extras_from_residual(&fields.residual));
            }
            DecodedItem {
                role,
                content,
                coalesce_assistant: coalesce_for_role(role),
            }
        }
        JsonStringOrArray::Other(_) => opaque_item(
            raw_item,
            fields.item_type(),
            Some(role),
            coalesce_for_role(role),
        ),
    }
}

fn decode_message_part(part: &ParsedMessagePart) -> CkContentBlock {
    let Some(fields) = part.fields.as_ref() else {
        return opaque_content_block(part.raw.as_ref(), None, None);
    };
    match fields.part_type() {
        Some("input_text") | Some("output_text") | Some("text") => fields
            .text()
            .map(|text| CkContentBlock {
                kind: ContentKind::Text {
                    text: text.to_string(),
                },
                provider_extras: provider_extras_from_residual(&fields.residual),
            })
            .unwrap_or_else(|| opaque_content_block(part.raw.as_ref(), fields.part_type(), None)),
        _ => opaque_content_block(part.raw.as_ref(), fields.part_type(), None),
    }
}

fn decode_function_call_item(raw_item: &RawValue, fields: &RawInputItem) -> DecodedItem {
    let (Some(call_id), Some(name), Some(arguments)) =
        (fields.call_id(), fields.name(), fields.arguments())
    else {
        return opaque_item(
            raw_item,
            fields.item_type(),
            Some(Role::Assistant),
            AssistantCoalesce::Yes,
        );
    };
    let Ok(input) = serde_json::from_str::<Value>(arguments) else {
        return opaque_item(
            raw_item,
            fields.item_type(),
            Some(Role::Assistant),
            AssistantCoalesce::Yes,
        );
    };
    let mut provider_extras = provider_extras_from_item_id(fields.id());
    provider_extras.merge_from(provider_extras_from_residual(&fields.residual));
    DecodedItem {
        role: Role::Assistant,
        content: vec![CkContentBlock {
            kind: ContentKind::ToolCall {
                id: call_id.to_string(),
                name: name.to_string(),
                input,
                thought_signature: None,
                provider_executed: false,
            },
            provider_extras,
        }],
        coalesce_assistant: AssistantCoalesce::Yes,
    }
}

fn decode_function_call_output_item(
    raw_item: &RawValue,
    fields: &RawInputItem,
    tool_names: &BTreeMap<String, String>,
) -> DecodedItem {
    let (Some(call_id), Some(output)) = (fields.call_id(), fields.output.as_deref()) else {
        return opaque_item(
            raw_item,
            fields.item_type(),
            Some(Role::Tool),
            AssistantCoalesce::No,
        );
    };
    DecodedItem {
        role: Role::Tool,
        content: vec![CkContentBlock {
            kind: ContentKind::ToolResult {
                id: call_id.to_string(),
                tool_name: tool_names.get(call_id).cloned().unwrap_or_default(),
                output: decode_tool_result_output(output),
                provider_executed: false,
            },
            provider_extras: provider_extras_from_residual(&fields.residual),
        }],
        coalesce_assistant: AssistantCoalesce::No,
    }
}

fn decode_reasoning_item(raw_item: &RawValue, fields: &RawInputItem) -> DecodedItem {
    let arc = fields.id().map(|id| OpaqueArc {
        kind: OpaqueArcKind::Reasoning,
        id: id.to_string(),
        role: OpaqueArcRole::Request,
    });
    DecodedItem {
        role: Role::Assistant,
        content: vec![opaque_content_block(raw_item, fields.item_type(), arc)],
        coalesce_assistant: AssistantCoalesce::Yes,
    }
}

fn decode_unknown_item(raw_item: &RawValue, fields: &RawInputItem) -> DecodedItem {
    let role = match fields.role() {
        Some("system") | Some("developer") => Role::System,
        Some("user") => Role::User,
        Some("tool") => Role::Tool,
        _ => Role::Assistant,
    };
    opaque_item(
        raw_item,
        fields.item_type(),
        Some(role),
        coalesce_for_role(role),
    )
}

fn decode_tool_result_output(output: &RawValue) -> ToolResultOutput {
    let kind = match parse_json_string_or_array(output) {
        JsonStringOrArray::String(text) => ToolResultOutputKind::Text { text },
        JsonStringOrArray::Array(raw_blocks) => ToolResultOutputKind::Content {
            blocks: raw_blocks
                .into_iter()
                .map(parse_result_block)
                .map(|block| decode_result_block(&block))
                .collect(),
        },
        JsonStringOrArray::Other(value) => ToolResultOutputKind::Json { value },
    };
    ToolResultOutput::bare(kind)
}

fn decode_result_block(block: &ParsedResultBlock) -> ResultBlock {
    let Some(fields) = block.fields.as_ref() else {
        return opaque_result_block(block.raw.as_ref(), None);
    };
    match fields.block_type() {
        Some("input_text") | Some("output_text") | Some("text") => fields
            .text()
            .map(|text| ResultBlock {
                kind: ResultBlockKind::Text {
                    text: text.to_string(),
                },
                provider_extras: provider_extras_from_residual(&fields.residual),
            })
            .unwrap_or_else(|| opaque_result_block(block.raw.as_ref(), fields.block_type())),
        _ => opaque_result_block(block.raw.as_ref(), fields.block_type()),
    }
}

fn decode_tools(value: Option<&Value>) -> Vec<Tool> {
    let Some(Value::Array(tools)) = value else {
        return Vec::new();
    };
    tools
        .iter()
        .filter_map(Value::as_object)
        .filter(|tool| string_field(tool, "type") == Some("function"))
        .map(|tool| Tool {
            name: string_field(tool, "name").unwrap_or_default().to_string(),
            description: string_field(tool, "description")
                .unwrap_or_default()
                .to_string(),
            input_schema: tool.get("parameters").cloned().unwrap_or(Value::Null),
            module: None,
            execution_mode: Default::default(),
            reply: None,
        })
        .collect()
}

fn assistant_message(content: Vec<CkContentBlock>, wire_model_id: &str) -> CkMessage {
    CkMessage {
        role: Role::Assistant,
        content,
        origin: Some(MessageOrigin {
            provider: OPENAI_PROVIDER.to_string(),
            model: wire_model_id.to_string(),
            api: OPENAI_RESPONSES_FAMILY.to_string(),
        }),
        provider_extras: ProviderExtras::new(),
        meta: Default::default(),
    }
}

fn plain_message(role: Role, content: Vec<CkContentBlock>) -> CkMessage {
    CkMessage {
        role,
        content,
        origin: None,
        provider_extras: ProviderExtras::new(),
        meta: Default::default(),
    }
}

fn system_message(content: Vec<CkContentBlock>) -> CkMessage {
    plain_message(Role::System, content)
}

fn opaque_item(
    raw_item: &RawValue,
    kind: Option<&str>,
    role: Option<Role>,
    coalesce_assistant: AssistantCoalesce,
) -> DecodedItem {
    DecodedItem {
        role: role.unwrap_or(Role::Assistant),
        content: vec![opaque_content_block(raw_item, kind, None)],
        coalesce_assistant,
    }
}

fn opaque_content_block(
    raw_block: &RawValue,
    kind: Option<&str>,
    arc: Option<OpaqueArc>,
) -> CkContentBlock {
    CkContentBlock {
        kind: ContentKind::Opaque(opaque_block(raw_block, kind, arc)),
        provider_extras: ProviderExtras::new(),
    }
}

fn opaque_result_block(raw_block: &RawValue, kind: Option<&str>) -> ResultBlock {
    ResultBlock {
        kind: ResultBlockKind::Opaque {
            opaque: opaque_block(raw_block, kind, None),
        },
        provider_extras: ProviderExtras::new(),
    }
}

fn opaque_block(raw_block: &RawValue, kind: Option<&str>, arc: Option<OpaqueArc>) -> OpaqueBlock {
    OpaqueBlock {
        source: OpaqueSource::Wire {
            wire: OPENAI_RESPONSES_FAMILY.to_string(),
        },
        kind: kind.unwrap_or("unknown").to_string(),
        raw: raw_block.get().as_bytes().to_vec(),
        arc,
    }
}

fn provider_extras_from_residual(residual: &BTreeMap<String, Box<RawValue>>) -> ProviderExtras {
    let mut extras = ProviderExtras::new();
    for (key, value) in residual {
        extras.insert_raw(OPENAI_PROVIDER.to_string(), key.clone(), value.clone());
    }
    extras
}

fn provider_extras_from_item_id(id: Option<&str>) -> ProviderExtras {
    let Some(id) = id else {
        return ProviderExtras::new();
    };
    let mut extras = ProviderExtras::new();
    let mut family = BTreeMap::new();
    family.insert("id".to_string(), Value::String(id.to_string()));
    extras.insert(OPENAI_RESPONSES_FAMILY.to_string(), family);
    extras
}

fn coalesce_for_role(role: Role) -> AssistantCoalesce {
    if role == Role::Assistant {
        AssistantCoalesce::Yes
    } else {
        AssistantCoalesce::No
    }
}

fn string_value(value: Option<&Value>) -> Option<&str> {
    value.and_then(Value::as_str)
}

fn string_field<'a>(obj: &'a Map<String, Value>, field: &str) -> Option<&'a str> {
    obj.get(field).and_then(Value::as_str)
}
