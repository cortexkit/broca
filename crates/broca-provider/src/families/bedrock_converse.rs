//! AWS Bedrock ConverseStream wire family.
//!
//! Bedrock ConverseStream is a JSON request grammar over a binary AWS event-stream
//! response (`application/vnd.amazon.eventstream`). The request renderer remains a
//! pure canonical-JSON function; SigV4 material is attached later by `endpoint` via
//! `AuthHandle`, outside the cache/resume hash domain.

use std::collections::BTreeMap;

use broca_protocol::{
    content::{AssistantMessage, ContentBlock, FinishReason, Message, Role, ToolCall, ToolOutput},
    display::DisplayEvent,
    error::{ErrorClass, ProviderError},
    ids::{MessageId, StepId, ToolCallId},
    provider::{NormalizedEvent, ProviderControl},
    request::{CallOptions, Tool, ToolChoice},
    usage::Usage,
};
use bytes::{Bytes, BytesMut};
use serde_json::{json, Map, Value};

use crate::cache_markers::{plan_markers, MarkerPlan, MarkerViewMessage};
use crate::policy::SystemPromptPolicy;
use crate::wire::{
    AuthHandle, CacheClass, CacheTail, CacheTtl, FrozenRenderConfig, HttpMethod, HttpRequest,
    MarkerSyntax, NormalizeError, StreamNormalizer, WireFamily,
};

const BEDROCK_RUNTIME_SERVICE: &str = "bedrock-runtime";
const DEFAULT_REGION: &str = "us-east-1";
const EVENT_STREAM_CONTENT_TYPE: &str = "application/vnd.amazon.eventstream";
const JSON_CONTENT_TYPE: &str = "application/json";
const MAX_EVENT_STREAM_MESSAGE_BYTES: usize = 16 * 1024 * 1024;

/// The AWS Bedrock ConverseStream family. Stateless — all per-run config is in the
/// frozen render config passed to each method.
pub struct BedrockConverse;

impl WireFamily for BedrockConverse {
    fn shape(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
        build_request_body(frozen, opts)
    }

    fn endpoint(
        &self,
        frozen: &FrozenRenderConfig,
        auth: &AuthHandle,
        body: Vec<u8>,
    ) -> HttpRequest {
        // Use the region from the frozen endpoint data (`frozen`) for the request host.
        // This keeps a resumed request pointed at the same Bedrock region it was
        // originally created with, even if the SigV4 signing data in `auth` was built
        // for a different region. The SigV4 region is used only for request signing.
        // An absent OR empty region falls back to the default. The catalog path supplies
        // `Some("")` for a Bedrock provider resolved with no explicit region (the auth kind
        // carries an empty region string), so filtering empty here keeps the host
        // well-formed instead of `bedrock-runtime..amazonaws.com`.
        let region = frozen
            .aws_region
            .as_deref()
            .filter(|s| !s.is_empty())
            .unwrap_or(DEFAULT_REGION);
        let path = format!(
            "/model/{}/converse-stream",
            percent_encode_path_label(&frozen.wire_model_id)
        );
        // A base_url override (a mock server in tests, or a VPC/proxy endpoint)
        // replaces the default origin and may include a path prefix; otherwise the host is
        // derived from the AWS region. base_url lives in the endpoint, not the rendered body,
        // so it never changes a golden. The `host` header carries only the target authority
        // because SigV4 signs over it.
        let (url, host) = match frozen.base_url.as_deref() {
            Some(base) => {
                let trimmed = base.trim_end_matches('/');
                let host = base_url_authority(trimmed).to_string();
                (format!("{trimmed}{path}"), host)
            }
            None => {
                let host = format!("{BEDROCK_RUNTIME_SERVICE}.{region}.amazonaws.com");
                (format!("https://{host}{path}"), host)
            }
        };
        let mut headers = vec![
            ("host".to_string(), host),
            ("content-type".to_string(), JSON_CONTENT_TYPE.to_string()),
            ("accept".to_string(), EVENT_STREAM_CONTENT_TYPE.to_string()),
        ];
        // Auth is attached HERE, after render — never on the rendered/hashed bytes.
        // Bedrock accepts two credential forms: a long-term BEARER api key
        // (`Authorization: Bearer <key>`, the newer AWS Bedrock API key) OR classic
        // SigV4 request signing (the transport/auth layer computes the signed headers
        // over method+URL+headers+body). The bearer form needs no signing, so the host
        // header it rides with is the literal target host above.
        match auth {
            AuthHandle::Bearer(token) => {
                headers.push(("authorization".to_string(), format!("Bearer {token}")));
            }
            AuthHandle::SigV4 { signed_headers, .. } => {
                headers.extend(signed_headers.iter().cloned());
            }
            AuthHandle::ApiKeyHeader { header, value } => {
                headers.push((header.clone(), value.clone()));
            }
            AuthHandle::Headers(hs) => headers.extend(hs.iter().cloned()),
            AuthHandle::None => {}
        }
        HttpRequest {
            method: HttpMethod::Post,
            url,
            headers,
            body,
        }
    }

    fn normalizer(
        &self,
        _frozen: &FrozenRenderConfig,
        step_id: StepId,
    ) -> Box<dyn StreamNormalizer> {
        // The output cap is left unknown here: the body renders `maxTokens` from the per-step
        // generation cap when one is set, which the normalizer never sees, so naming the frozen
        // default could state the wrong number. The provider's stop reason proves the cut, and
        // the metadata trailer's output-token count rides in the error beside it.
        Box::new(BedrockStreamNormalizer::new(step_id))
    }
}

// ───────────────────────── request body ─────────────────────────

fn build_request_body(frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
    // A run with structured output adds its submit tool or schema instructions here; a run
    // without renders from `opts` unchanged.
    let prepared = crate::structured_output::prepare(frozen, opts);
    let opts = prepared.as_ref();
    let view = crate::families::compaction_view(frozen.family_id, opts);
    let opts = view.as_ref();
    let marker_policy = bedrock_marker_policy(frozen);
    let mut body = Map::new();

    let name_map = crate::tool_name_alias::ToolNameMap::build(&opts.tools, &opts.prompt);
    let id_map = crate::tool_id_alias::ToolIdMap::build(
        &opts.prompt,
        &crate::tool_id_alias::IdGrammar::bedrock(),
    );
    let system = convert_system(frozen, &opts.prompt, marker_policy.is_some());
    let messages = convert_messages(&opts.prompt, &name_map, &id_map);

    if !system.is_empty() {
        body.insert("system".to_string(), Value::Array(system));
    }

    body.insert("messages".to_string(), Value::Array(messages));
    body.insert(
        "inferenceConfig".to_string(),
        convert_inference_config(frozen, opts),
    );

    if let Some(tool_config) = convert_tool_config(&opts.tools, &opts.tool_choice, &name_map) {
        body.insert("toolConfig".to_string(), tool_config);
    }

    merge_top_level_options(&mut body, &frozen.static_options);
    merge_top_level_options(&mut body, &opts.provider_options);

    if let Some((ttl, max_breakpoints, lookback_blocks, tail)) = marker_policy {
        apply_bedrock_marker_policy_to_body(
            &mut body,
            frozen,
            ttl,
            max_breakpoints,
            lookback_blocks,
            tail,
            opts.marker_order_version,
        );
    }

    Value::Object(body)
}

fn convert_system(
    frozen: &FrozenRenderConfig,
    prompt: &[Message],
    coalesce_instruction_tail: bool,
) -> Vec<Value> {
    let mut system = Vec::new();
    if let SystemPromptPolicy::RequiredIdentityLead { identity } = &frozen.system_prompt {
        system.push(json!({ "text": identity }));
    }
    for message in prompt {
        if message.role == Role::System {
            for block in &message.content {
                // Drop an empty text part — Bedrock Converse rejects an empty `{text:""}`
                // block. Defensive against foreign input; the owned normalizers never emit
                // empty text.
                if let ContentBlock::Text { text } = block {
                    if !text.is_empty() {
                        system.push(json!({ "text": text }));
                    }
                }
            }
        }
    }

    if coalesce_instruction_tail {
        coalesce_system_tail(frozen, &mut system);
    }
    system
}

fn coalesce_system_tail(frozen: &FrozenRenderConfig, system: &mut Vec<Value>) {
    let tail_start = system_tail_start(frozen);
    if tail_start >= system.len().saturating_sub(1) {
        return;
    }

    let merged = system[tail_start..]
        .iter()
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n");
    if merged.is_empty() {
        return;
    }

    system.truncate(tail_start);
    system.push(json!({ "text": merged }));
}

fn system_tail_start(frozen: &FrozenRenderConfig) -> usize {
    if matches!(
        frozen.system_prompt,
        SystemPromptPolicy::RequiredIdentityLead { .. }
    ) {
        1
    } else {
        0
    }
}

fn system_tail_block_count(frozen: &FrozenRenderConfig, system: &[Value]) -> usize {
    system
        .get(system_tail_start(frozen)..)
        .unwrap_or_default()
        .iter()
        .filter(|block| is_bedrock_cacheable_block(block))
        .count()
}

fn apply_bedrock_marker_policy_to_body(
    body: &mut Map<String, Value>,
    frozen: &FrozenRenderConfig,
    ttl: CacheTtl,
    max_breakpoints: u32,
    lookback_blocks: u32,
    tail: Option<CacheTail>,
    marker_order_version: Option<u32>,
) {
    let mut system = take_array(body, "system");
    let mut messages = take_array(body, "messages");
    strip_bedrock_inbound_markers(&mut system, &mut messages);
    coalesce_system_tail(frozen, &mut system);

    // Marker positions are based on the merged rendered body, so passthrough
    // options cannot reintroduce unplanned cache points or shift breakpoints.
    let marker_view = bedrock_marker_view(&messages);
    debug_assert_eq!(marker_view.len(), messages.len());
    let plan = plan_markers(
        lookback_blocks,
        max_breakpoints,
        system_tail_block_count(frozen, &system),
        tail,
        &marker_view,
        marker_order_version,
        &[],
    );
    apply_bedrock_marker_plan(&mut system, &mut messages, frozen, ttl, &plan);

    if system.is_empty() {
        body.remove("system");
    } else {
        body.insert("system".to_string(), Value::Array(system));
    }
    body.insert("messages".to_string(), Value::Array(messages));
}

fn take_array(body: &mut Map<String, Value>, key: &str) -> Vec<Value> {
    match body.remove(key) {
        Some(Value::Array(values)) => values,
        _ => Vec::new(),
    }
}

fn strip_bedrock_inbound_markers(system: &mut Vec<Value>, messages: &mut [Value]) {
    strip_bedrock_marker_blocks(system);
    for message in messages {
        if let Some(content) = message.get_mut("content").and_then(Value::as_array_mut) {
            strip_bedrock_marker_blocks(content);
        }
    }
}

fn strip_bedrock_marker_blocks(blocks: &mut Vec<Value>) {
    blocks.retain(|block| block.get("cachePoint").is_none());
    for block in blocks {
        if let Some(object) = block.as_object_mut() {
            object.remove("cache_control");
            object.remove("cacheControl");
        }
    }
}

fn bedrock_marker_view(messages: &[Value]) -> Vec<MarkerViewMessage> {
    messages
        .iter()
        .map(|message| {
            let role = match message.get("role").and_then(Value::as_str) {
                Some("user") => Role::User,
                Some("assistant") => Role::Assistant,
                _ => Role::Assistant,
            };
            let block_count = message
                .get("content")
                .and_then(Value::as_array)
                .map(|content| {
                    content
                        .iter()
                        .filter(|block| is_bedrock_cacheable_block(block))
                        .count()
                })
                .unwrap_or(0);
            MarkerViewMessage {
                role,
                block_count,
                has_cacheable: block_count > 0,
                // Bedrock cache points are separate list elements, not block attributes.
                cache_prefix_blocks: None,
            }
        })
        .collect()
}

fn bedrock_marker_policy(
    frozen: &FrozenRenderConfig,
) -> Option<(CacheTtl, u32, u32, Option<CacheTail>)> {
    let cache = frozen.cache.as_ref()?;
    match &cache.class {
        CacheClass::Markers {
            syntax: MarkerSyntax::BedrockCachePoint,
            ttl,
            max_breakpoints,
            lookback_blocks,
            tail,
            // Only the first-party Anthropic default sets this, and Bedrock's cache points are
            // rendered the same way either way.
            omit_ttl: _,
        } => Some((*ttl, *max_breakpoints, *lookback_blocks, *tail)),
        _ => None,
    }
}

fn apply_bedrock_marker_plan(
    system: &mut Vec<Value>,
    messages: &mut [Value],
    frozen: &FrozenRenderConfig,
    ttl: CacheTtl,
    plan: &MarkerPlan,
) {
    if plan.system_tail {
        let tail_start = system_tail_start(frozen);
        if let Some(relative_index) = system
            .get(tail_start..)
            .and_then(|tail| tail.iter().rposition(is_bedrock_cacheable_block))
        {
            system.insert(tail_start + relative_index + 1, bedrock_cache_point(ttl));
        }
    }

    if let Some(target) = plan.bridge_message {
        insert_bedrock_message_marker(messages, target.message_index, ttl);
    }
    for target in &plan.message_anchors {
        insert_bedrock_message_marker(messages, target.message_index, ttl);
    }
}

fn insert_bedrock_message_marker(messages: &mut [Value], index: usize, ttl: CacheTtl) {
    let Some(content) = messages
        .get_mut(index)
        .and_then(|message| message.get_mut("content"))
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    if let Some(position) = content.iter().rposition(is_bedrock_cacheable_block) {
        content.insert(position + 1, bedrock_cache_point(ttl));
    }
}

fn is_bedrock_cacheable_block(block: &Value) -> bool {
    block.get("text").is_some()
        || block.get("toolUse").is_some()
        || block.get("toolResult").is_some()
}

fn bedrock_cache_point(ttl: CacheTtl) -> Value {
    json!({ "cachePoint": { "type": "default", "ttl": ttl.as_str() } })
}

fn convert_messages(
    prompt: &[Message],
    name_map: &crate::tool_name_alias::ToolNameMap,
    id_map: &crate::tool_id_alias::ToolIdMap,
) -> Vec<Value> {
    prompt
        .iter()
        .filter_map(|message| convert_message(message, name_map, id_map))
        // Bedrock rejects messages left empty after unsupported reasoning is stripped.
        .filter(|message| {
            message
                .get("content")
                .and_then(Value::as_array)
                .is_none_or(|content| !content.is_empty())
        })
        .collect()
}

fn convert_message(
    message: &Message,
    name_map: &crate::tool_name_alias::ToolNameMap,
    id_map: &crate::tool_id_alias::ToolIdMap,
) -> Option<Value> {
    match message.role {
        Role::System => None,
        Role::User => Some(json!({
            "role": "user",
            "content": convert_content(&message.content, name_map, id_map),
        })),
        Role::Assistant => Some(json!({
            "role": "assistant",
            "content": convert_content(&message.content, name_map, id_map),
        })),
        Role::Tool => {
            let content: Vec<Value> = message
                .content
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::ToolResult(result) => Some(convert_tool_result(result, id_map)),
                    _ => None,
                })
                .collect();
            Some(json!({ "role": "user", "content": content }))
        }
    }
}

fn convert_content(
    blocks: &[ContentBlock],
    name_map: &crate::tool_name_alias::ToolNameMap,
    id_map: &crate::tool_id_alias::ToolIdMap,
) -> Vec<Value> {
    blocks
        .iter()
        .filter_map(|block| match block {
            // Drop an empty text part (Bedrock rejects `text:""`). Defensive against foreign input; the owned normalizers never emit empty text.
            ContentBlock::Text { text } if text.is_empty() => None,
            ContentBlock::Text { text } => Some(json!({ "text": text })),
            ContentBlock::ToolCall(call) => Some(json!({
                "toolUse": {
                    "toolUseId": id_map.wire_id(&call.tool_call_id.0),
                    "name": name_map.wire_name(&call.tool_name),
                    "input": call.input,
                }
            })),
            ContentBlock::ToolResult(result) => Some(convert_tool_result(result, id_map)),
            ContentBlock::Reasoning { .. } | ContentBlock::RedactedReasoning { .. } => None,
        })
        .collect()
}

fn convert_tool_result(
    result: &broca_protocol::content::ToolResult,
    id_map: &crate::tool_id_alias::ToolIdMap,
) -> Value {
    json!({
        "toolResult": {
            "toolUseId": id_map.wire_id(&result.tool_call_id.0),
            "content": convert_tool_output(&result.output),
            "status": if result.is_error { "error" } else { "success" },
        }
    })
}

fn convert_tool_output(output: &ToolOutput) -> Vec<Value> {
    match output {
        // Bedrock rejects blank text blocks. A JSON empty string preserves an empty tool result
        // without inventing prose for the model; `toolResult.status` still carries error state.
        ToolOutput::Text { text } if text.is_empty() => vec![json!({ "json": text })],
        ToolOutput::Text { text } => vec![json!({ "text": text })],
        ToolOutput::Json { value } => vec![json!({ "json": value })],
        ToolOutput::Denied { reason } => vec![json!({ "text": format!("denied: {reason}") })],
        ToolOutput::Error { message } if message.is_empty() => vec![json!({ "json": message })],
        ToolOutput::Error { message } => vec![json!({ "text": message })],
    }
}

fn convert_inference_config(frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
    let mut config = Map::new();
    config.insert(
        "maxTokens".to_string(),
        json!(opts
            .generation
            .max_output_tokens
            .unwrap_or(frozen.max_output_tokens)),
    );
    if let Some(temperature) = opts.generation.temperature {
        config.insert("temperature".to_string(), json!(temperature));
    }
    if let Some(top_p) = opts.generation.top_p {
        config.insert("topP".to_string(), json!(top_p));
    }
    if !opts.generation.stop_sequences.is_empty() {
        config.insert(
            "stopSequences".to_string(),
            json!(opts.generation.stop_sequences),
        );
    }
    Value::Object(config)
}

fn convert_tool_config(
    tools: &[Tool],
    choice: &ToolChoice,
    name_map: &crate::tool_name_alias::ToolNameMap,
) -> Option<Value> {
    if tools.is_empty() || matches!(choice, ToolChoice::None) {
        return None;
    }
    Some(json!({
        "tools": tools.iter().map(|tool| convert_tool(tool, name_map)).collect::<Vec<_>>(),
        "toolChoice": convert_tool_choice(choice, name_map),
    }))
}

fn convert_tool(tool: &Tool, name_map: &crate::tool_name_alias::ToolNameMap) -> Value {
    json!({
        "toolSpec": {
            "name": name_map.wire_name(&tool.name),
            "description": tool.description,
            "inputSchema": { "json": tool.input_schema },
        }
    })
}

fn convert_tool_choice(
    choice: &ToolChoice,
    name_map: &crate::tool_name_alias::ToolNameMap,
) -> Value {
    match choice {
        ToolChoice::Auto | ToolChoice::None => json!({ "auto": {} }),
        ToolChoice::Required => json!({ "any": {} }),
        ToolChoice::Tool { name } => {
            json!({ "tool": { "name": name_map.wire_name(name) } })
        }
    }
}

fn merge_top_level_options(body: &mut Map<String, Value>, options: &Value) {
    if let Some(options) = options.as_object() {
        for (key, value) in options {
            body.insert(key.clone(), value.clone());
        }
    }
}

fn base_url_authority(base: &str) -> &str {
    let remainder = base
        .split_once("://")
        .map(|(_, remainder)| remainder)
        .unwrap_or(base);
    remainder
        .split_once('/')
        .map(|(authority, _)| authority)
        .unwrap_or(remainder)
}

fn percent_encode_path_label(input: &str) -> String {
    let mut encoded = String::new();
    for byte in input.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            _ => {
                encoded.push('%');
                encoded.push(hex_digit(byte >> 4));
                encoded.push(hex_digit(byte & 0x0f));
            }
        }
    }
    encoded
}

fn hex_digit(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        _ => (b'A' + (n - 10)) as char,
    }
}

// ───────────────────────── event-stream decoder ─────────────────────────

struct BedrockEventStreamDecoder {
    buf: BytesMut,
}

impl BedrockEventStreamDecoder {
    fn new() -> Self {
        Self {
            buf: BytesMut::new(),
        }
    }

    fn push(&mut self, chunk: &[u8]) -> Result<Vec<BedrockEventFrame>, NormalizeError> {
        self.buf.extend_from_slice(chunk);
        let mut frames = Vec::new();
        loop {
            if self.buf.len() < 12 {
                break;
            }
            let prelude = &self.buf[..12];
            let total_len = read_u32(&prelude[0..4]) as usize;
            let headers_len = read_u32(&prelude[4..8]) as usize;
            if total_len < 16 {
                return Err(NormalizeError::Invalid(
                    "event-stream message shorter than minimum".into(),
                ));
            }
            if total_len > MAX_EVENT_STREAM_MESSAGE_BYTES {
                return Err(NormalizeError::Invalid(
                    "event-stream message exceeds maximum size".into(),
                ));
            }
            if 12usize.saturating_add(headers_len).saturating_add(4) > total_len {
                return Err(NormalizeError::Invalid(
                    "event-stream headers exceed message length".into(),
                ));
            }
            if self.buf.len() < total_len {
                break;
            }

            let message = self.buf.split_to(total_len).freeze();
            validate_event_stream_crc(&message)?;
            let headers_start = 12;
            let headers_end = headers_start + headers_len;
            let payload_end = total_len - 4;
            let headers = parse_headers(&message[headers_start..headers_end])?;
            let payload = message.slice(headers_end..payload_end);
            frames.push(BedrockEventFrame { headers, payload });
        }
        Ok(frames)
    }
}

#[derive(Debug)]
struct BedrockEventFrame {
    headers: BTreeMap<String, HeaderValue>,
    payload: Bytes,
}

#[derive(Debug, Clone, Copy)]
enum BedrockFrameKind<'a> {
    Event(Option<&'a str>),
    Error {
        code: &'a str,
        message: Option<&'a str>,
    },
}

impl BedrockEventFrame {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).and_then(HeaderValue::as_str)
    }

    fn kind(&self) -> BedrockFrameKind<'_> {
        let event = || BedrockFrameKind::Event(self.header(":event-type"));
        match self.header(":message-type") {
            Some("exception") => self
                .header(":exception-type")
                .map(|code| BedrockFrameKind::Error {
                    code,
                    message: None,
                })
                .unwrap_or_else(event),
            Some("error") => self
                .header(":error-code")
                .map(|code| BedrockFrameKind::Error {
                    code,
                    message: self.header(":error-message"),
                })
                .unwrap_or_else(event),
            // Missing and unknown message types retain the old event interpretation so a
            // provider that omits this routing header does not acquire a new failure mode.
            Some("event") | Some(_) | None => event(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum HeaderValue {
    String(String),
    Other,
}

impl HeaderValue {
    fn as_str(&self) -> Option<&str> {
        match self {
            HeaderValue::String(value) => Some(value.as_str()),
            _ => None,
        }
    }
}

fn validate_event_stream_crc(message: &[u8]) -> Result<(), NormalizeError> {
    let expected_prelude_crc = read_u32(&message[8..12]);
    let actual_prelude_crc = crc32(&message[0..8]);
    if expected_prelude_crc != actual_prelude_crc {
        return Err(NormalizeError::Invalid(
            "event-stream prelude CRC mismatch".into(),
        ));
    }

    let expected_message_crc = read_u32(&message[message.len() - 4..]);
    let actual_message_crc = crc32(&message[..message.len() - 4]);
    if expected_message_crc != actual_message_crc {
        return Err(NormalizeError::Invalid(
            "event-stream message CRC mismatch".into(),
        ));
    }
    Ok(())
}

fn parse_headers(bytes: &[u8]) -> Result<BTreeMap<String, HeaderValue>, NormalizeError> {
    let mut headers = BTreeMap::new();
    let mut pos = 0usize;
    while pos < bytes.len() {
        let name_len = *bytes.get(pos).ok_or_else(|| {
            NormalizeError::Invalid("event-stream header missing name length".into())
        })? as usize;
        pos += 1;
        let name_bytes = take(bytes, &mut pos, name_len)?;
        let name = std::str::from_utf8(name_bytes)
            .map_err(|e| {
                NormalizeError::Invalid(format!("event-stream header name is not UTF-8: {e}"))
            })?
            .to_owned();
        let value_type = *bytes.get(pos).ok_or_else(|| {
            NormalizeError::Invalid("event-stream header missing value type".into())
        })?;
        pos += 1;
        let value = parse_header_value(value_type, bytes, &mut pos)?;
        headers.insert(name, value);
    }
    Ok(headers)
}

fn parse_header_value(
    value_type: u8,
    bytes: &[u8],
    pos: &mut usize,
) -> Result<HeaderValue, NormalizeError> {
    match value_type {
        0 | 1 => Ok(HeaderValue::Other),
        2 => {
            let _ = take(bytes, pos, 1)?;
            Ok(HeaderValue::Other)
        }
        3 => {
            let _ = take(bytes, pos, 2)?;
            Ok(HeaderValue::Other)
        }
        4 => {
            let _ = take(bytes, pos, 4)?;
            Ok(HeaderValue::Other)
        }
        5 | 8 => {
            let _ = take(bytes, pos, 8)?;
            Ok(HeaderValue::Other)
        }
        6 => {
            let len = u16::from_be_bytes(read_array::<2>(take(bytes, pos, 2)?)?) as usize;
            let _ = take(bytes, pos, len)?;
            Ok(HeaderValue::Other)
        }
        7 => {
            let len = u16::from_be_bytes(read_array::<2>(take(bytes, pos, 2)?)?) as usize;
            let value = std::str::from_utf8(take(bytes, pos, len)?)
                .map_err(|e| {
                    NormalizeError::Invalid(format!("event-stream string header is not UTF-8: {e}"))
                })?
                .to_owned();
            Ok(HeaderValue::String(value))
        }
        9 => {
            let _ = take(bytes, pos, 16)?;
            Ok(HeaderValue::Other)
        }
        _ => Err(NormalizeError::Invalid(format!(
            "unsupported event-stream header value type {value_type}"
        ))),
    }
}

fn take<'a>(bytes: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8], NormalizeError> {
    let end = pos
        .checked_add(len)
        .ok_or_else(|| NormalizeError::Invalid("event-stream header offset overflow".into()))?;
    let slice = bytes
        .get(*pos..end)
        .ok_or_else(|| NormalizeError::Invalid("event-stream header truncated".into()))?;
    *pos = end;
    Ok(slice)
}

fn read_array<const N: usize>(bytes: &[u8]) -> Result<[u8; N], NormalizeError> {
    bytes
        .try_into()
        .map_err(|_| NormalizeError::Invalid("event-stream integer header truncated".into()))
}

fn read_u32(bytes: &[u8]) -> u32 {
    let mut out = [0u8; 4];
    if bytes.len() == 4 {
        out.copy_from_slice(bytes);
    }
    u32::from_be_bytes(out)
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = 0xffff_ffffu32;
    for byte in bytes {
        crc ^= u32::from(*byte);
        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ 0xedb8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

// ───────────────────────── stream normalizer ─────────────────────────

#[derive(Debug)]
enum BlockBuf {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input_json: String,
    },
}

pub struct BedrockStreamNormalizer {
    arguments_guard: crate::wire::ToolArgsGuard,
    completed_blocks: std::collections::HashSet<usize>,
    step_id: StepId,
    message_id: MessageId,
    decoder: BedrockEventStreamDecoder,
    content_observer: crate::content_progress::ContentObserver,
    blocks: BTreeMap<usize, BlockBuf>,
    usage: Usage,
    finish_reason: FinishReason,
    /// The provider's `stopReason`, verbatim, for naming an output-ceiling cut.
    stop_reason: Option<String>,
    /// The output cap the request was rendered with, when known (see `normalizer`).
    max_output_tokens: Option<u32>,
    started: bool,
    message_stopped: bool,
    saw_metadata: bool,
    emitted_final: bool,
    /// The `metadata` event's `serviceTier.type`, verbatim (the ConverseStream API reference
    /// lists `priority | default | flex | reserved`). Absent when the event carries no
    /// `serviceTier`, which is what the recorded Nova streams show.
    service_tier_used: Option<String>,
}

impl BedrockStreamNormalizer {
    fn new(step_id: StepId) -> Self {
        Self {
            arguments_guard: Default::default(),
            completed_blocks: Default::default(),
            step_id,
            message_id: MessageId(format!("bedrock-step-{}", step_id.0)),
            decoder: BedrockEventStreamDecoder::new(),
            content_observer: crate::content_progress::ContentObserver::new(
                crate::content_progress::ContentFamily::Bedrock,
            ),
            blocks: BTreeMap::new(),
            usage: Usage::default(),
            finish_reason: FinishReason::Other,
            stop_reason: None,
            max_output_tokens: None,
            started: false,
            message_stopped: false,
            saw_metadata: false,
            emitted_final: false,
            service_tier_used: None,
        }
    }

    fn handle_frame(
        &mut self,
        frame: BedrockEventFrame,
        out: &mut Vec<NormalizedEvent>,
    ) -> Result<(), NormalizeError> {
        if self.emitted_final {
            return Ok(());
        }
        let frame_kind = frame.kind();
        let payload = if frame.payload.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&frame.payload)
                .map_err(|e| NormalizeError::Invalid(format!("decode bedrock event JSON: {e}")))?
        };
        let event_type = match frame_kind {
            BedrockFrameKind::Event(event_type) => event_type,
            BedrockFrameKind::Error { code, message } => {
                self.content_observer.observe(
                    "error",
                    Some(&payload.to_string()),
                    frame.payload.len(),
                );
                out.push(NormalizedEvent::Control(ProviderControl::Error {
                    error: bedrock_error(code, &payload, message),
                    retries_used: 0,
                }));
                return Ok(());
            }
        };
        let (event_type, event_payload) = event_type_and_payload(event_type, &payload);
        self.content_observer.observe(
            event_type.as_deref().unwrap_or("unknown"),
            Some(&event_payload.to_string()),
            frame.payload.len(),
        );
        match event_type.as_deref().unwrap_or("") {
            "messageStart" => self.handle_message_start(out),
            "contentBlockStart" => self.handle_content_block_start(event_payload),
            "contentBlockDelta" => self.handle_content_block_delta(event_payload, out)?,
            "contentBlockStop" => {
                self.completed_blocks.insert(block_index(event_payload));
            }
            "messageStop" => {
                if let Some(stop_reason) =
                    string_field(event_payload, &["stopReason", "stop_reason"])
                {
                    self.finish_reason = map_stop_reason(stop_reason);
                    self.stop_reason = Some(stop_reason.to_owned());
                }
                self.message_stopped = true;
                self.emit_final_if_ready(out)?;
            }
            "metadata" => {
                if let Some(usage) = event_payload.get("usage") {
                    self.merge_usage(usage);
                }
                if let Some(tier) = event_payload
                    .get("serviceTier")
                    .and_then(|tier| tier.get("type"))
                    .and_then(Value::as_str)
                {
                    self.service_tier_used = Some(tier.to_owned());
                }
                self.saw_metadata = true;
                self.emit_final_if_ready(out)?;
            }
            event if event.ends_with("Exception") || event.ends_with("Error") => {
                out.push(NormalizedEvent::Control(ProviderControl::Error {
                    error: bedrock_error(event, event_payload, None),
                    retries_used: 0,
                }));
            }
            _ => {}
        }
        if self.arguments_guard.fired() && !self.emitted_final {
            self.finish_reason = FinishReason::ToolCalls;
            out.extend(self.final_events()?);
        }
        Ok(())
    }

    fn handle_message_start(&mut self, out: &mut Vec<NormalizedEvent>) {
        if self.started {
            return;
        }
        self.started = true;
        out.push(NormalizedEvent::Control(ProviderControl::StreamStart {
            warnings: Vec::new(),
        }));
        out.push(NormalizedEvent::Control(ProviderControl::StepStarted {
            step_id: self.step_id,
        }));
    }

    fn handle_content_block_start(&mut self, event_payload: &Value) {
        let idx = block_index(event_payload);
        let start = event_payload.get("start").unwrap_or(event_payload);
        if let Some(tool_use) = start.get("toolUse").or_else(|| start.get("tool_use")) {
            let id = string_field(tool_use, &["toolUseId", "tool_use_id"])
                .unwrap_or("")
                .to_owned();
            let name = string_field(tool_use, &["name"]).unwrap_or("").to_owned();
            self.blocks.insert(
                idx,
                BlockBuf::ToolUse {
                    id,
                    name,
                    input_json: String::new(),
                },
            );
        } else if start.get("text").is_some() {
            self.blocks.entry(idx).or_insert_with(|| BlockBuf::Text {
                text: String::new(),
            });
        }
    }

    fn handle_content_block_delta(
        &mut self,
        event_payload: &Value,
        out: &mut Vec<NormalizedEvent>,
    ) -> Result<(), NormalizeError> {
        if !self.started {
            return Err(NormalizeError::Invalid(
                "bedrock contentBlockDelta before messageStart".into(),
            ));
        }
        let idx = block_index(event_payload);
        let delta = event_payload.get("delta").unwrap_or(event_payload);
        if let Some(text) = string_field(delta, &["text"]) {
            self.push_text_delta(idx, text, out);
        }
        if let Some(tool_use) = delta.get("toolUse").or_else(|| delta.get("tool_use")) {
            if let Some(input) = string_field(tool_use, &["input"]) {
                self.push_tool_input_delta(idx, input, out);
            }
        }
        Ok(())
    }

    fn push_text_delta(&mut self, idx: usize, text_delta: &str, out: &mut Vec<NormalizedEvent>) {
        let entry = self.blocks.entry(idx).or_insert_with(|| BlockBuf::Text {
            text: String::new(),
        });
        if let BlockBuf::Text { text } = entry {
            text.push_str(text_delta);
        }
        if !text_delta.is_empty() {
            out.push(NormalizedEvent::Display(DisplayEvent::TextDelta {
                message_id: self.message_id.clone(),
                tool_call_id: None,
                delta: text_delta.to_owned(),
            }));
        }
    }

    fn push_tool_input_delta(
        &mut self,
        idx: usize,
        input_delta: &str,
        out: &mut Vec<NormalizedEvent>,
    ) {
        let entry = self.blocks.entry(idx).or_insert_with(|| BlockBuf::ToolUse {
            id: String::new(),
            name: String::new(),
            input_json: String::new(),
        });
        if let BlockBuf::ToolUse {
            id,
            name,
            input_json,
        } = entry
        {
            input_json.push_str(input_delta);
            self.arguments_guard
                .stream(idx as u64, id, name, input_json, input_delta);
            if !input_delta.is_empty() {
                out.push(NormalizedEvent::Display(DisplayEvent::ToolInputDelta {
                    tool_call_id: ToolCallId(id.clone()),
                    delta: input_delta.to_owned(),
                }));
            }
        }
    }

    fn merge_usage(&mut self, usage: &Value) {
        if let Some(value) = number_field(usage, &["inputTokens", "input_tokens"]) {
            self.usage.input_tokens = Some(value);
        }
        if let Some(value) = number_field(usage, &["outputTokens", "output_tokens"]) {
            self.usage.output_tokens = Some(value);
        }
        if let Some(value) = number_field(
            usage,
            &[
                "cacheReadInputTokens",
                "cache_read_input_tokens",
                "cachedInputTokens",
                "cached_input_tokens",
            ],
        ) {
            self.usage.cached_input_tokens = Some(value);
        }
        if let Some(value) = number_field(
            usage,
            &[
                "cacheWriteInputTokens",
                "cache_write_input_tokens",
                "cacheCreationInputTokens",
                "cache_creation_input_tokens",
            ],
        ) {
            self.usage.cache_write_tokens = Some(value);
        }
    }

    fn emit_final_if_ready(
        &mut self,
        out: &mut Vec<NormalizedEvent>,
    ) -> Result<(), NormalizeError> {
        if self.message_stopped && self.saw_metadata && !self.emitted_final {
            out.extend(self.final_events()?);
        }
        Ok(())
    }

    fn final_events(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        if self.emitted_final || !self.started {
            return Ok(Vec::new());
        }
        self.emitted_final = true;
        let message = self.assemble_message()?;
        let mut events: Vec<_> = self.arguments_guard.event().into_iter().collect();
        events.extend([
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }),
            NormalizedEvent::Control(ProviderControl::StepFinished {
                step_id: self.step_id,
                usage: self.usage,
                finish_reason: self.finish_reason,
                service_tier_used: self.service_tier_used.clone(),
                speed_used: None,
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: self.usage,
                finish_reason: self.finish_reason,
                retries_used: 0,
            }),
        ]);
        Ok(events)
    }

    fn assemble_message(&mut self) -> Result<AssistantMessage, NormalizeError> {
        for block in self.blocks.values() {
            if let BlockBuf::ToolUse {
                id,
                name,
                input_json,
            } = block
            {
                self.arguments_guard.terminal(id, name, input_json);
            }
        }
        if self.arguments_guard.fired() {
            self.finish_reason = FinishReason::ToolCalls;
        }
        // Tool arguments are parsed here, once both `messageStop` (which carries `stopReason`)
        // and the metadata trailer have arrived. So a `max_tokens` stop is already known when a
        // buffer fails to parse, and it proves the ceiling cut the call.
        let ceiling = (self.finish_reason == FinishReason::Length)
            .then_some(self.stop_reason.as_deref())
            .flatten()
            .map(|provider_stop_reason| crate::wire::CeilingStop {
                provider_stop_reason,
                max_output_tokens: self.max_output_tokens,
                output_tokens: self.usage.output_tokens,
            });
        let mut content = Vec::new();
        for (idx, block) in std::mem::take(&mut self.blocks) {
            if self.arguments_guard.fired()
                && !self.completed_blocks.contains(&idx)
                && matches!(block, BlockBuf::Text { .. })
            {
                continue;
            }
            match block {
                BlockBuf::Text { text } => content.push(ContentBlock::Text { text }),
                BlockBuf::ToolUse {
                    id,
                    name,
                    input_json,
                } => {
                    let Some(input) = self.arguments_guard.decode(
                        &input_json,
                        &name,
                        ceiling,
                        self.completed_blocks.contains(&idx),
                    )?
                    else {
                        continue;
                    };
                    content.push(ContentBlock::ToolCall(ToolCall {
                        tool_call_id: ToolCallId(id),
                        tool_name: name,
                        input,
                        thought_signature: None,
                    }));
                }
            }
        }
        Ok(AssistantMessage {
            message_id: self.message_id.clone(),
            content,
        })
    }
}

impl StreamNormalizer for BedrockStreamNormalizer {
    fn content_progress(&self) -> broca_protocol::provider::ContentProgress {
        self.content_observer.snapshot()
    }
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let frames = self.decoder.push(chunk)?;
        let mut out = Vec::new();
        for frame in frames {
            self.handle_frame(frame, &mut out)?;
        }
        Ok(out)
    }

    fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        if self.emitted_final || !self.started {
            return Ok(Vec::new());
        }
        if self.message_stopped && self.saw_metadata {
            return self.final_events();
        }

        let missing = match (self.message_stopped, self.saw_metadata) {
            (true, false) => "the metadata trailer",
            (false, true) => "messageStop",
            (false, false) => "messageStop and the metadata trailer",
            (true, true) => unreachable!("complete stream returned above"),
        };
        Err(NormalizeError::Invalid(format!(
            "Bedrock stream truncated: ended before {missing}"
        )))
    }
}

fn event_type_and_payload<'a>(
    header_event_type: Option<&str>,
    payload: &'a Value,
) -> (Option<String>, &'a Value) {
    if let Some(event_type) = header_event_type {
        if let Some(wrapped) = payload.get(event_type) {
            return (Some(event_type.to_owned()), wrapped);
        }
        return (Some(event_type.to_owned()), payload);
    }
    if let Some(object) = payload.as_object() {
        if object.len() == 1 {
            if let Some((key, value)) = object.iter().next() {
                return (Some(key.clone()), value);
            }
        }
    }
    (None, payload)
}

fn block_index(value: &Value) -> usize {
    number_field(
        value,
        &["contentBlockIndex", "content_block_index", "index"],
    )
    .unwrap_or(0) as usize
}

fn string_field<'a>(value: &'a Value, keys: &[&str]) -> Option<&'a str> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str))
}

fn number_field(value: &Value, keys: &[&str]) -> Option<u64> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_u64))
}

fn map_stop_reason(reason: &str) -> FinishReason {
    match reason {
        "end_turn" | "stop_sequence" => FinishReason::Stop,
        "max_tokens" => FinishReason::Length,
        "tool_use" => FinishReason::ToolCalls,
        "guardrail_intervened" | "content_filtered" => FinishReason::ContentFilter,
        _ => FinishReason::Other,
    }
}

fn bedrock_error(event_type: &str, payload: &Value, header_message: Option<&str>) -> ProviderError {
    let message = string_field(payload, &["message", "errorMessage", "error_message"])
        .or(header_message)
        .unwrap_or(event_type)
        .to_owned();
    ProviderError {
        provider_subcode: None,
        refusal: None,
        class: if crate::normalize::context_overflow_code(
            crate::spec::WireFamilyId::BedrockConverse,
            Some(event_type),
            &message,
            None,
        )
        .is_some()
        {
            ErrorClass::ContextOverflow
        } else {
            classify_bedrock_error(event_type)
        },
        message,
        status: None,
        retry_after_secs: None,
        provider_code: Some(event_type.to_owned()),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
    }
}

fn classify_bedrock_error(event_type: &str) -> ErrorClass {
    match event_type {
        "throttlingException"
        | "serviceUnavailableException"
        | "internalServerException"
        | "modelStreamErrorException" => ErrorClass::Transient,
        "accessDeniedException" => ErrorClass::Permanent,
        "authenticationError" => ErrorClass::Auth,
        "validationException" => ErrorClass::Permanent,
        _ => ErrorClass::Permanent,
    }
}

/// Encode one AWS event-stream frame, for tests that feed a Bedrock normalizer or serve a
/// Bedrock body from a scripted server.
#[cfg(test)]
pub(crate) fn test_event_stream_message(event_type: &str, payload: &[u8]) -> Vec<u8> {
    test_event_stream_message_with_headers(
        &[
            (":message-type", "event"),
            (":event-type", event_type),
            (":content-type", "application/json"),
        ],
        payload,
    )
}

#[cfg(test)]
pub(crate) fn test_event_stream_message_with_headers(
    headers: &[(&str, &str)],
    payload: &[u8],
) -> Vec<u8> {
    let mut encoded_headers = Vec::new();
    for (name, value) in headers {
        test_string_header(&mut encoded_headers, name, value);
    }

    let total_len = 12 + encoded_headers.len() + payload.len() + 4;
    let mut message = Vec::new();
    message.extend((total_len as u32).to_be_bytes());
    message.extend((encoded_headers.len() as u32).to_be_bytes());
    let prelude_crc = crc32(&message);
    message.extend(prelude_crc.to_be_bytes());
    message.extend(encoded_headers);
    message.extend(payload);
    let message_crc = crc32(&message);
    message.extend(message_crc.to_be_bytes());
    message
}

#[cfg(test)]
fn test_string_header(out: &mut Vec<u8>, name: &str, value: &str) {
    out.push(name.len() as u8);
    out.extend(name.as_bytes());
    out.push(7);
    out.extend((value.len() as u16).to_be_bytes());
    out.extend(value.as_bytes());
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::families::bedrock_frozen_config_with_region;

    fn host_header(request: &HttpRequest) -> Option<&str> {
        request
            .headers
            .iter()
            .find(|(name, _)| name == "host")
            .map(|(_, value)| value.as_str())
    }

    #[test]
    fn access_denial_is_permanent_but_authentication_is_auth() {
        assert_eq!(
            classify_bedrock_error("accessDeniedException"),
            ErrorClass::Permanent,
            "revert-check: authenticated Bedrock denial must not trigger re-authentication"
        );
        assert_eq!(
            classify_bedrock_error("authenticationError"),
            ErrorClass::Auth,
            "revert-check: Bedrock authentication failure must remain auth-classified"
        );
    }

    #[test]
    fn endpoint_uses_frozen_region_for_bedrock_host_when_sigv4_region_differs() {
        let frozen = bedrock_frozen_config_with_region(
            "anthropic.claude-3-5-sonnet-20240620-v1:0",
            "us-west-2",
        );
        let request = BedrockConverse.endpoint(
            &frozen,
            &AuthHandle::SigV4 {
                region: "us-east-1".into(),
                service: BEDROCK_RUNTIME_SERVICE.into(),
                signed_headers: vec![("x-amz-date".into(), "20260102T030405Z".into())],
            },
            b"{}".to_vec(),
        );

        assert_eq!(
            request.url,
            "https://bedrock-runtime.us-west-2.amazonaws.com/model/anthropic.claude-3-5-sonnet-20240620-v1%3A0/converse-stream"
        );
        assert_eq!(
            host_header(&request),
            Some("bedrock-runtime.us-west-2.amazonaws.com")
        );
        assert!(request
            .headers
            .iter()
            .any(|(name, value)| name == "x-amz-date" && value == "20260102T030405Z"));
    }

    fn legacy_convert_tool_output(output: &ToolOutput) -> Vec<Value> {
        match output {
            ToolOutput::Text { text } => vec![json!({ "text": text })],
            ToolOutput::Json { value } => vec![json!({ "json": value })],
            ToolOutput::Denied { reason } => vec![json!({ "text": format!("denied: {reason}") })],
            ToolOutput::Error { message } => vec![json!({ "text": message })],
        }
    }

    fn tool_result_request(output: ToolOutput, is_error: bool) -> Value {
        let tool_call_id = ToolCallId("call_1".into());
        let opts = CallOptions::new(vec![
            Message::new(
                Role::Assistant,
                vec![ContentBlock::ToolCall(ToolCall {
                    tool_call_id: tool_call_id.clone(),
                    tool_name: "lookup".into(),
                    input: json!({}),
                    thought_signature: None,
                })],
            ),
            Message::new(
                Role::Tool,
                vec![ContentBlock::ToolResult(
                    broca_protocol::content::ToolResult {
                        tool_call_id,
                        output,
                        is_error,
                    },
                )],
            ),
        ]);
        serde_json::from_slice(&BedrockConverse.render(
            &crate::families::bedrock_frozen_config("amazon.nova-lite-v1:0"),
            &opts,
        ))
        .unwrap()
    }

    #[test]
    fn empty_text_tool_result_uses_json_string_and_keeps_success_status() {
        let output = ToolOutput::Text {
            text: String::new(),
        };
        let body = tool_result_request(output.clone(), false);
        let tool_result = &body["messages"][1]["content"][0]["toolResult"];

        assert_eq!(tool_result["content"], json!([{ "json": "" }]));
        assert_eq!(tool_result["status"], "success");
        assert_eq!(
            legacy_convert_tool_output(&output),
            vec![json!({ "text": "" })],
            "legacy contrast must prove the old renderer emitted a rejected blank text block"
        );
    }

    #[test]
    fn empty_error_tool_result_uses_json_string_and_keeps_error_status() {
        let body = tool_result_request(
            ToolOutput::Error {
                message: String::new(),
            },
            true,
        );
        let tool_result = &body["messages"][1]["content"][0]["toolResult"];

        assert_eq!(tool_result["content"], json!([{ "json": "" }]));
        assert_eq!(tool_result["status"], "error");
    }

    #[test]
    fn nonempty_tool_outputs_match_legacy_bytes() {
        for output in [
            ToolOutput::Text {
                text: "result".into(),
            },
            ToolOutput::Json {
                value: json!({ "answer": 42 }),
            },
            ToolOutput::Denied {
                reason: "policy".into(),
            },
            ToolOutput::Error {
                message: "failed".into(),
            },
        ] {
            assert_eq!(
                serde_json::to_vec(&convert_tool_output(&output)).unwrap(),
                serde_json::to_vec(&legacy_convert_tool_output(&output)).unwrap(),
                "nonempty tool output changed bytes: {output:?}"
            );
        }
    }

    #[test]
    fn denied_tool_output_is_never_empty_text() {
        let output = convert_tool_output(&ToolOutput::Denied {
            reason: String::new(),
        });
        let text = output[0]["text"].as_str().unwrap();

        assert_eq!(text, "denied: ");
        assert!(!text.is_empty());
    }

    #[test]
    fn event_stream_decoder_rejects_corrupt_crcs() {
        let message = test_event_stream_message("contentBlockDelta", br#"{"delta":{"text":"x"}}"#);

        let mut corrupt_prelude = message.clone();
        corrupt_prelude[8] ^= 1;
        let error = BedrockEventStreamDecoder::new()
            .push(&corrupt_prelude)
            .expect_err("a corrupt prelude CRC must be rejected");
        assert!(matches!(
            error,
            NormalizeError::Invalid(message) if message == "event-stream prelude CRC mismatch"
        ));

        let mut corrupt_message = message;
        let payload_end = corrupt_message.len() - 4;
        corrupt_message[payload_end - 1] ^= 1;
        let error = BedrockEventStreamDecoder::new()
            .push(&corrupt_message)
            .expect_err("a corrupt message CRC must be rejected");
        assert!(matches!(
            error,
            NormalizeError::Invalid(message) if message == "event-stream message CRC mismatch"
        ));
    }

    #[test]
    fn event_stream_decoder_preserves_frame_across_every_split_boundary() {
        let payload = br#"{"delta":{"text":"fragmented"}}"#;
        let message = test_event_stream_message("contentBlockDelta", payload);
        let mut baseline = BedrockEventStreamDecoder::new()
            .push(&message)
            .expect("complete frame decodes");
        let expected = baseline.pop().expect("complete frame is emitted");
        assert!(baseline.is_empty());

        for split in 0..=message.len() {
            let mut decoder = BedrockEventStreamDecoder::new();
            let mut frames = decoder
                .push(&message[..split])
                .expect("frame prefix buffers");
            frames.extend(
                decoder
                    .push(&message[split..])
                    .expect("frame suffix decodes"),
            );
            assert_eq!(frames.len(), 1, "split at byte {split}");
            let actual = frames.pop().expect("one frame was emitted");
            assert_eq!(actual.headers, expected.headers, "split at byte {split}");
            assert_eq!(actual.payload, expected.payload, "split at byte {split}");
        }
    }

    #[test]
    fn modeled_exception_headers_surface_typed_error_instead_of_legacy_truncation() {
        let message_start = test_event_stream_message("messageStart", br#"{"role":"assistant"}"#);
        let exception = test_event_stream_message_with_headers(
            &[
                (":message-type", "exception"),
                (":exception-type", "modelStreamErrorException"),
                (":content-type", "application/json"),
            ],
            br#"{"message":"upstream model stream failed"}"#,
        );

        // Reproduce the old discriminator: it looked only at :event-type, so the exception
        // disappeared and the already-started stream later failed as generic truncation.
        let mut legacy_frames = BedrockEventStreamDecoder::new()
            .push(&exception)
            .expect("valid exception frame");
        let legacy_frame = legacy_frames.pop().expect("one exception frame");
        assert!(legacy_frame
            .headers
            .get(":event-type")
            .and_then(HeaderValue::as_str)
            .is_none());
        let legacy_payload: Value = serde_json::from_slice(&legacy_frame.payload).unwrap();
        let (legacy_type, _) = event_type_and_payload(None, &legacy_payload);
        assert_eq!(legacy_type.as_deref(), Some("message"));
        let mut legacy_normalizer = BedrockStreamNormalizer::new(StepId(7));
        legacy_normalizer
            .push(&message_start)
            .expect("message start decodes");
        let legacy_error = legacy_normalizer
            .finish()
            .expect_err("the ignored exception leaves a truncated stream");
        assert!(legacy_error
            .to_string()
            .contains("Bedrock stream truncated"));

        let mut normalizer = BedrockStreamNormalizer::new(StepId(7));
        let mut events = normalizer
            .push(&message_start)
            .expect("message start decodes");
        events.extend(normalizer.push(&exception).expect("exception decodes"));
        let error = events
            .iter()
            .find_map(|event| match event {
                NormalizedEvent::Control(ProviderControl::Error { error, .. }) => Some(error),
                _ => None,
            })
            .expect("modeled exception becomes a typed provider error");
        assert_eq!(
            error.provider_code.as_deref(),
            Some("modelStreamErrorException")
        );
        assert_eq!(error.message, "upstream model stream failed");
        assert_eq!(
            error.class,
            classify_bedrock_error("modelStreamErrorException")
        );
    }

    #[test]
    fn in_stream_throttling_exception_is_transient() {
        let exception = test_event_stream_message_with_headers(
            &[
                (":message-type", "exception"),
                (":exception-type", "throttlingException"),
                (":content-type", "application/json"),
            ],
            br#"{"message":"too many requests"}"#,
        );
        let events = BedrockStreamNormalizer::new(StepId(8))
            .push(&exception)
            .expect("throttling exception decodes");
        assert!(matches!(
            events.as_slice(),
            [NormalizedEvent::Control(ProviderControl::Error { error, .. })]
                if error.class == ErrorClass::Transient
                    && error.provider_code.as_deref() == Some("throttlingException")
                    && error.message == "too many requests"
        ));
    }

    #[test]
    fn unmodeled_error_uses_header_code_and_message() {
        let error_frame = test_event_stream_message_with_headers(
            &[
                (":message-type", "error"),
                (":error-code", "InternalError"),
                (":error-message", "event stream transport failed"),
            ],
            b"",
        );
        let events = BedrockStreamNormalizer::new(StepId(9))
            .push(&error_frame)
            .expect("unmodeled error decodes");
        assert!(matches!(
            events.as_slice(),
            [NormalizedEvent::Control(ProviderControl::Error { error, .. })]
                if error.provider_code.as_deref() == Some("InternalError")
                    && error.message == "event stream transport failed"
                    && error.class == classify_bedrock_error("InternalError")
        ));
    }

    #[test]
    fn missing_or_unknown_message_type_keeps_event_interpretation() {
        for message_type in [None, Some("future-message-kind")] {
            let mut headers = Vec::new();
            if let Some(message_type) = message_type {
                headers.push((":message-type", message_type));
            }
            headers.push((":event-type", "messageStart"));
            headers.push((":content-type", "application/json"));
            let frame =
                test_event_stream_message_with_headers(&headers, br#"{"role":"assistant"}"#);
            let events = BedrockStreamNormalizer::new(StepId(10))
                .push(&frame)
                .expect("legacy event interpretation remains valid");
            assert!(events.iter().any(|event| matches!(
                event,
                NormalizedEvent::Control(ProviderControl::StepStarted { step_id })
                    if *step_id == StepId(10)
            )));
        }
    }
}

#[cfg(test)]
mod usage_presence_tests {
    use super::*;

    #[test]
    fn bedrock_reports_zero_and_positive_cache_write_but_no_reasoning_split() {
        for n in [0, 7] {
            let mut normalizer = BedrockStreamNormalizer::new(StepId(1));
            normalizer.merge_usage(
                &serde_json::json!({"inputTokens":0,"outputTokens":0,"cacheWriteInputTokens":n}),
            );
            assert_eq!(normalizer.usage.input_tokens, Some(0));
            assert_eq!(normalizer.usage.output_tokens, Some(0));
            assert_eq!(normalizer.usage.cache_write_tokens, Some(n));
            assert_eq!(normalizer.usage.cached_input_tokens, None);
            assert_eq!(normalizer.usage.reasoning_tokens, None);
        }
    }

    /// The ConverseStream `metadata` event reports the tier that served the request as
    /// `serviceTier.type`; a metadata event without it records no tier. No recorded Bedrock
    /// stream carries `serviceTier`, so both streams follow the API reference's shape.
    #[test]
    fn metadata_service_tier_is_recorded_verbatim_and_absent_stays_absent() {
        fn served_tier(metadata: &[u8]) -> Option<String> {
            let mut body = Vec::new();
            body.extend(test_event_stream_message(
                "messageStart",
                br#"{"role":"assistant"}"#,
            ));
            body.extend(test_event_stream_message(
                "contentBlockDelta",
                br#"{"contentBlockIndex":0,"delta":{"text":"hi"}}"#,
            ));
            body.extend(test_event_stream_message(
                "contentBlockStop",
                br#"{"contentBlockIndex":0}"#,
            ));
            body.extend(test_event_stream_message(
                "messageStop",
                br#"{"stopReason":"end_turn"}"#,
            ));
            body.extend(test_event_stream_message("metadata", metadata));
            let mut normalizer = BedrockStreamNormalizer::new(StepId(1));
            let mut events = normalizer.push(&body).expect("stream decodes");
            events.extend(normalizer.finish().expect("stream ends cleanly"));
            let tiers = events
                .iter()
                .filter_map(|event| match event {
                    NormalizedEvent::Control(ProviderControl::StepFinished {
                        service_tier_used,
                        ..
                    }) => Some(service_tier_used.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            assert_eq!(tiers.len(), 1, "one StepFinished: {events:?}");
            tiers.into_iter().next().unwrap()
        }

        assert_eq!(
            served_tier(
                br#"{"usage":{"inputTokens":3,"outputTokens":1,"totalTokens":4},"metrics":{"latencyMs":5},"serviceTier":{"type":"default"}}"#
            )
            .as_deref(),
            Some("default")
        );
        assert_eq!(
            served_tier(
                br#"{"usage":{"inputTokens":3,"outputTokens":1,"totalTokens":4},"metrics":{"latencyMs":5}}"#
            ),
            None
        );
    }
}
