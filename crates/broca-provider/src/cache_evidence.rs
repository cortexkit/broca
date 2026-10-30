//! Pure request-prefix evidence. Attribution belongs to the consumer's shared classifier;
//! this module neither changes rendered bytes nor guesses provider token boundaries.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::spec::WireFamilyId;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Region {
    System,
    Tools,
    Messages,
    ProviderState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Breakpoint {
    pub region: Region,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub part_index: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Divergence {
    pub region: Region,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub part_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix_tokens_before: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TtlKind {
    Explicit,
    Implicit,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct View {
    pub applied: Option<u64>,
    pub previous: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct CacheEvidence {
    pub compared_to: Option<String>,
    pub extends_previous: bool,
    pub first_divergence: Option<Divergence>,
    pub previous_last_breakpoint: Option<Breakpoint>,
    pub idle_gap_ms: u64,
    pub cache_ttl_ms: Option<u64>,
    pub ttl_kind: TtlKind,
    pub model_changed: bool,
    pub account_changed: bool,
    pub view: Option<View>,
}

#[derive(Debug, PartialEq)]
struct Segment {
    location: Divergence,
    value: Value,
}

#[derive(Default)]
struct Request {
    segments: Vec<Segment>,
    breakpoint: Option<Breakpoint>,
}

impl Request {
    fn push(&mut self, region: Region, message: Option<usize>, part: Option<usize>, value: Value) {
        self.segments.push(Segment {
            location: Divergence {
                region,
                message_index: message.map(|n| n as u32),
                part_index: part.map(|n| n as u32),
                field: None,
                prefix_tokens_before: None,
            },
            value,
        });
    }

    fn parts(&mut self, region: Region, message: Option<usize>, value: &Value, bedrock: bool) {
        let parts: Vec<&Value> = match value.as_array() {
            Some(parts) => parts.iter().collect(),
            None => vec![value],
        };
        for (index, part) in parts.iter().enumerate() {
            if part.get("cache_control").is_some() || (bedrock && part.get("cachePoint").is_some())
            {
                self.breakpoint = Some(Breakpoint {
                    region: region.clone(),
                    message_index: message.map(|n| n as u32),
                    part_index: Some(index as u32),
                });
            }
            // Bedrock's cachePoint is a standalone marker, not a content segment.
            if bedrock && part.get("cachePoint").is_some() {
                continue;
            }
            let mut part = (*part).clone();
            // cache_control changes placement and TTL without changing the cached content.
            if let Some(object) = part.as_object_mut() {
                object.remove("cache_control");
            }
            if region == Region::System {
                normalize_billing_text(&mut part);
            }
            self.push(region.clone(), message, Some(index), part);
        }
    }
}

fn segments(family: WireFamilyId, body: &[u8]) -> Result<Request, String> {
    let mut body: Value =
        serde_json::from_slice(body).map_err(|_| "invalid rendered request JSON")?;
    let mut request = Request::default();
    if family == WireFamilyId::GoogleCodeAssist {
        let envelope = body.as_object_mut().ok_or("invalid Code Assist envelope")?;
        let inner = envelope
            .remove("request")
            .ok_or("missing Code Assist request")?;
        // requestId is a per-step tracing id, not model input. project/model stay evidence.
        envelope.remove("requestId");
        request.push(
            Region::ProviderState,
            None,
            None,
            Value::Object(envelope.clone()),
        );
        body = inner;
    }
    let object = body
        .as_object_mut()
        .ok_or("rendered request is not an object")?;
    let (system, tools, messages, content, bedrock) = match family {
        WireFamilyId::AnthropicMessages => ("system", "tools", "messages", "content", false),
        WireFamilyId::BedrockConverse => ("system", "toolConfig", "messages", "content", true),
        WireFamilyId::Gemini | WireFamilyId::GoogleCodeAssist => {
            ("systemInstruction", "tools", "contents", "parts", false)
        }
        WireFamilyId::OpenAiChat => ("__system", "tools", "messages", "content", false),
        WireFamilyId::OpenAiResponses | WireFamilyId::OpenAiResponsesChatgpt => {
            ("instructions", "tools", "input", "content", false)
        }
    };
    if let Some(mut value) = object.remove(tools) {
        if bedrock {
            if let Some(config) = value.as_object_mut() {
                let tools = config.remove("tools");
                request.push(
                    Region::ProviderState,
                    None,
                    None,
                    json_field("toolConfig", Value::Object(config.clone())),
                );
                value = tools.unwrap_or(Value::Null);
            }
        }
        request.parts(Region::Tools, None, &value, bedrock);
    }
    if let Some(mut value) = object.remove(system) {
        if matches!(
            family,
            WireFamilyId::Gemini | WireFamilyId::GoogleCodeAssist
        ) {
            if let Some(parts) = value.get_mut("parts") {
                value = parts.take();
            }
        }
        request.parts(Region::System, None, &value, bedrock);
    }
    if let Some(value) = object.remove(messages) {
        let messages = value
            .as_array()
            .ok_or("rendered messages are not an array")?;
        for (index, message) in messages.iter().enumerate() {
            let mut message = message.clone();
            let region = if matches!(message["role"].as_str(), Some("system" | "developer")) {
                Region::System
            } else {
                Region::Messages
            };
            if let Some(object) = message.as_object_mut() {
                let parts = object.remove(content);
                request.push(
                    region.clone(),
                    Some(index),
                    None,
                    Value::Object(object.clone()),
                );
                if let Some(parts) = parts {
                    request.parts(region, Some(index), &parts, bedrock);
                }
            } else {
                request.push(region, Some(index), None, message);
            }
        }
    }
    normalize_settings(family, object);
    request.push(
        Region::ProviderState,
        None,
        None,
        Value::Object(object.clone()),
    );
    Ok(request)
}

fn normalize_billing_text(value: &mut Value) {
    match value {
        Value::String(text) => {
            // The persona's billing header rotates cch (body signature), cc_prev_req (receipt),
            // cc_prompt_id and turn indexes (episode correlation), not model instructions.
            *text = text
                .split('\n')
                .map(|line| {
                    if line.trim_start().starts_with("x-anthropic-billing-header:") {
                        "x-anthropic-billing-header: <rotating>"
                    } else {
                        line
                    }
                })
                .collect::<Vec<_>>()
                .join("\n");
        }
        Value::Object(object) => {
            if let Some(text) = object.get_mut("text") {
                normalize_billing_text(text);
            }
        }
        _ => {}
    }
}

fn normalize_settings(family: WireFamilyId, object: &mut serde_json::Map<String, Value>) {
    // Output reservation clamps rotate max_tokens/max_completion_tokens/max_output_tokens,
    // inferenceConfig.maxTokens and generationConfig.maxOutputTokens near the context limit.
    // Output caps are not cached prompt input; other generation/reasoning settings stay raw.
    for key in ["max_tokens", "max_completion_tokens", "max_output_tokens"] {
        object.remove(key);
    }
    for (parent, key) in [
        ("inferenceConfig", "maxTokens"),
        ("generationConfig", "maxOutputTokens"),
    ] {
        if let Some(Value::Object(config)) = object.get_mut(parent) {
            config.remove(key);
        }
    }
    if family == WireFamilyId::AnthropicMessages {
        if let Some(user) = object
            .get_mut("metadata")
            .and_then(|m| m.get_mut("user_id"))
        {
            if let Some(mut identity) = user
                .as_str()
                .and_then(|s| serde_json::from_str::<Value>(s).ok())
            {
                if let Some(identity_object) = identity.as_object_mut() {
                    // metadata.user_id's device_id and session_id identify an install/episode,
                    // not the prompt. account_uuid stays raw because accounts scope caches.
                    identity_object.remove("device_id");
                    identity_object.remove("session_id");
                    *user = Value::String(identity.to_string());
                }
            }
        }
    }
    if family == WireFamilyId::OpenAiResponsesChatgpt {
        if let Some(Value::Object(metadata)) = object.get_mut("client_metadata") {
            // These client_metadata keys rotate with the install, connection or turn. They
            // route/trace the request rather than contributing to its model-visible prefix.
            for key in [
                "x-codex-installation-id",
                "session_id",
                "thread_id",
                "x-codex-window-id",
                "turn_id",
                "x-codex-turn-metadata",
                "x-codex-turn-state",
                "x-codex-ws-stream-request-start-ms",
            ] {
                metadata.remove(key);
            }
        }
    }
}

fn json_field(field: &str, value: Value) -> Value {
    let mut object = serde_json::Map::new();
    object.insert(field.into(), value);
    Value::Object(object)
}

fn changed_field(previous: &Value, current: &Value) -> Option<String> {
    let (Some(previous), Some(current)) = (previous.as_object(), current.as_object()) else {
        return None;
    };
    for field in previous.keys().chain(current.keys()) {
        if previous.get(field) != current.get(field) {
            // Signed reasoning is prefix-bound input, not a rotating authentication field.
            return Some(
                if field == "signature"
                    && previous.get("type").and_then(Value::as_str) == Some("thinking")
                {
                    "thinking.signature".into()
                } else {
                    field.clone()
                },
            );
        }
    }
    None
}

/// Compare logical rendered requests, not transport frames. Only a clean message-tail append
/// extends the previous request. Breakpoints locate the protected prefix but do not hide edits
/// outside it; the dashboard decides which edits could account for a provider cache miss.
pub fn compare_rendered_requests(
    family: WireFamilyId,
    previous: &[u8],
    current: &[u8],
) -> Result<CacheEvidence, String> {
    let previous = segments(family, previous)?;
    let current = segments(family, current)?;
    let mut evidence = CacheEvidence {
        extends_previous: true,
        previous_last_breakpoint: previous.breakpoint,
        ttl_kind: if matches!(
            family,
            WireFamilyId::AnthropicMessages | WireFamilyId::BedrockConverse
        ) {
            TtlKind::Explicit
        } else {
            TtlKind::Implicit
        },
        ..Default::default()
    };
    for region in [
        Region::Tools,
        Region::System,
        Region::Messages,
        Region::ProviderState,
    ] {
        let old: Vec<_> = previous
            .segments
            .iter()
            .filter(|s| s.location.region == region)
            .collect();
        let new: Vec<_> = current
            .segments
            .iter()
            .filter(|s| s.location.region == region)
            .collect();
        for index in 0..old.len().max(new.len()) {
            match (old.get(index), new.get(index)) {
                (Some(a), Some(b)) if a.value == b.value => {}
                (None, Some(_)) if region == Region::Messages => break,
                (a, b) => {
                    let mut location = b.or(a).expect("one segment exists").location.clone();
                    location.field = a
                        .zip(b)
                        .and_then(|(a, b)| changed_field(&a.value, &b.value));
                    evidence.extends_previous = false;
                    evidence.first_divergence = Some(location);
                    return Ok(evidence);
                }
            }
        }
    }
    Ok(evidence)
}

#[cfg(test)]
#[path = "cache_evidence_tests.rs"]
mod tests;
