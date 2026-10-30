//! Request-prefix evidence without retaining prompt bytes. Consumers attribute cache misses.

use std::{borrow::Cow, fmt};

use serde::{de, Deserialize, Serialize};
use serde_json::{value::RawValue, Value};
use sha2::{Digest, Sha256};

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
    /// Durable transcript ordinal, not the rendered array position. System/tool evidence
    /// and messages without a durable ordinal leave this absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message_ordinal: Option<u64>,
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

/// Owned evidence contains only coordinates and hashes, never request or segment bytes.
#[derive(Debug, Clone)]
pub struct RequestFingerprint {
    family: WireFamilyId,
    segments: Vec<(Divergence, [u8; 32])>,
    breakpoint: Option<Breakpoint>,
}

impl RequestFingerprint {
    /// Attach only identities reported by the renderer for this exact wire layout.
    /// A custom renderer with no matching provenance leaves identities absent.
    pub fn with_provenance(
        mut self,
        provenance: &broca_protocol::render_provenance::RenderProvenance,
    ) -> Self {
        let count = self
            .segments
            .iter()
            .filter_map(|(location, _)| location.message_index)
            .max()
            .map_or(0, |n| n as usize + 1);
        if count != provenance.message_count() {
            return self;
        }
        for (location, _) in &mut self.segments {
            if location.region == Region::Messages {
                location.message_ordinal = location.message_index.and_then(|message| {
                    provenance
                        .message_ordinal(message as usize, location.part_index.map(|n| n as usize))
                });
            }
        }
        self
    }
}

#[derive(Deserialize)]
struct Key<'a>(#[serde(borrow)] Cow<'a, str>);

// Visitors borrow raw values and unescaped keys. They do not allocate a container per message
// or content part; the fingerprint's single vector grows geometrically for the whole request.
fn object<'a>(
    raw: &'a RawValue,
    mut field: impl FnMut(&str, &'a RawValue) -> Result<(), String>,
) -> Result<(), String> {
    struct Fields<F>(F);
    impl<'de, F> de::Visitor<'de> for Fields<F>
    where
        F: FnMut(&str, &'de RawValue) -> Result<(), String>,
    {
        type Value = ();
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a rendered object")
        }
        fn visit_map<M: de::MapAccess<'de>>(mut self, mut map: M) -> Result<(), M::Error> {
            while let Some(Key(key)) = map.next_key::<Key<'de>>()? {
                (self.0)(&key, map.next_value()?).map_err(de::Error::custom)?;
            }
            Ok(())
        }
    }
    let mut decoder = serde_json::Deserializer::from_str(raw.get());
    de::Deserializer::deserialize_map(&mut decoder, Fields(&mut field)).map_err(|e| e.to_string())
}

fn array<'a>(
    raw: &'a RawValue,
    mut item: impl FnMut(usize, &'a RawValue) -> Result<(), String>,
) -> Result<(), String> {
    struct Items<F>(F);
    impl<'de, F> de::Visitor<'de> for Items<F>
    where
        F: FnMut(usize, &'de RawValue) -> Result<(), String>,
    {
        type Value = ();
        fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
            f.write_str("a rendered array")
        }
        fn visit_seq<S: de::SeqAccess<'de>>(mut self, mut seq: S) -> Result<(), S::Error> {
            let mut index = 0;
            while let Some(raw) = seq.next_element::<&RawValue>()? {
                (self.0)(index, raw).map_err(de::Error::custom)?;
                index += 1;
            }
            Ok(())
        }
    }
    let mut decoder = serde_json::Deserializer::from_str(raw.get());
    de::Deserializer::deserialize_seq(&mut decoder, Items(&mut item)).map_err(|e| e.to_string())
}

fn get<'a>(raw: &'a RawValue, name: &str) -> Result<Option<&'a RawValue>, String> {
    let mut found = None;
    object(raw, |key, value| {
        if key == name {
            found = Some(value);
        }
        Ok(())
    })?;
    Ok(found)
}

#[derive(Clone, Copy)]
enum Source<'a> {
    Raw(&'a RawValue),
    Without(&'a RawValue, &'a [&'a str]),
    Part(&'a RawValue, bool),
    Settings(&'a RawValue, WireFamilyId, &'a [&'a str]),
}

impl Source<'_> {
    fn value(self) -> Result<Value, String> {
        if let Self::Without(raw, names) | Self::Settings(raw, _, names) = self {
            let mut bytes = Vec::new();
            object_without(raw, names, &mut bytes)?;
            let mut value: Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
            if let Self::Settings(_, family, _) = self {
                normalize_settings(family, value.as_object_mut().ok_or("invalid settings")?);
            }
            return Ok(value);
        }
        let raw = match self {
            Self::Raw(raw)
            | Self::Without(raw, _)
            | Self::Part(raw, _)
            | Self::Settings(raw, _, _) => raw,
        };
        let mut value: Value = serde_json::from_str(raw.get()).map_err(|e| e.to_string())?;
        match self {
            Self::Without(..) | Self::Settings(..) => unreachable!("handled above"),
            Self::Part(_, system) => {
                if let Some(map) = value.as_object_mut() {
                    map.remove("cache_control");
                }
                if system {
                    normalize_billing_text(&mut value);
                }
            }
            Self::Raw(_) => {}
        }
        Ok(value)
    }

    fn hash(self, scratch: &mut Vec<u8>) -> Result<[u8; 32], String> {
        match self {
            Self::Raw(raw) => Ok(Sha256::digest(raw.get().as_bytes()).into()),
            Self::Part(raw, system) => {
                let marker = raw.get().starts_with('{') && get(raw, "cache_control")?.is_some();
                if !marker && !(system && raw.get().contains("x-anthropic-billing-header:")) {
                    return Self::Raw(raw).hash(scratch);
                }
                scratch.clear();
                serde_json::to_writer(&mut *scratch, &self.value()?).map_err(|e| e.to_string())?;
                Ok(Sha256::digest(&*scratch).into())
            }
            Self::Without(raw, names) => {
                object_without(raw, names, scratch)?;
                Ok(Sha256::digest(&*scratch).into())
            }
            Self::Settings(raw, family, names) => {
                // Remove the potentially huge arrays before parsing the few remaining settings.
                object_without(raw, names, scratch)?;
                let mut value: Value =
                    serde_json::from_slice(scratch).map_err(|e| e.to_string())?;
                normalize_settings(family, value.as_object_mut().ok_or("invalid settings")?);
                scratch.clear();
                serde_json::to_writer(&mut *scratch, &value).map_err(|e| e.to_string())?;
                Ok(Sha256::digest(&*scratch).into())
            }
        }
    }
}

fn object_without(raw: &RawValue, names: &[&str], bytes: &mut Vec<u8>) -> Result<(), String> {
    bytes.clear();
    bytes.push(b'{');
    object(raw, |key, value| {
        if !names.contains(&key) {
            if bytes.len() > 1 {
                bytes.push(b',');
            }
            serde_json::to_writer(&mut *bytes, key).map_err(|e| e.to_string())?;
            bytes.push(b':');
            bytes.extend_from_slice(value.get().as_bytes());
        }
        Ok(())
    })?;
    bytes.push(b'}');
    Ok(())
}

fn location(region: Region, message: Option<usize>, part: Option<usize>) -> Divergence {
    Divergence {
        region,
        message_index: message.map(|n| n as u32),
        message_ordinal: None,
        part_index: part.map(|n| n as u32),
        field: None,
        prefix_tokens_before: None,
    }
}

struct Scanner<F> {
    emit: F,
    breakpoint: Option<Breakpoint>,
}

impl<F> Scanner<F>
where
    F: FnMut(Divergence, Source<'_>) -> Result<(), String>,
{
    fn parts(
        &mut self,
        region: Region,
        message: Option<usize>,
        raw: &RawValue,
        bedrock: bool,
    ) -> Result<(), String> {
        let mut part = |index, raw: &RawValue| {
            let is_object = raw.get().starts_with('{');
            let cache_point = bedrock && is_object && get(raw, "cachePoint")?.is_some();
            if cache_point || (is_object && get(raw, "cache_control")?.is_some()) {
                self.breakpoint = Some(Breakpoint {
                    region: region.clone(),
                    message_index: message.map(|n| n as u32),
                    part_index: Some(index as u32),
                });
            }
            if !cache_point {
                (self.emit)(
                    location(region.clone(), message, Some(index)),
                    Source::Part(raw, region == Region::System),
                )?;
            }
            Ok(())
        };
        if raw.get().starts_with('[') {
            array(raw, part)
        } else {
            part(0, raw)
        }
    }
}

fn scan(
    family: WireFamilyId,
    body: &[u8],
    emit: impl FnMut(Divergence, Source<'_>) -> Result<(), String>,
) -> Result<Option<Breakpoint>, String> {
    let mut body: &RawValue =
        serde_json::from_slice(body).map_err(|_| "invalid rendered request JSON")?;
    let mut scanner = Scanner {
        emit,
        breakpoint: None,
    };
    if family == WireFamilyId::GoogleCodeAssist {
        (scanner.emit)(
            location(Region::ProviderState, None, None),
            Source::Without(body, &["request", "requestId"]),
        )?;
        body = get(body, "request")?.ok_or("missing Code Assist request")?;
    }
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
    if let Some(mut raw) = get(body, tools)? {
        if bedrock && raw.get().starts_with('{') {
            // toolConfig's non-tool settings occupy a separate provider-state segment.
            let mut value: Value = serde_json::from_str("{}").expect("empty object");
            value["toolConfig"] = Source::Without(raw, &["tools"]).value()?;
            let bytes = serde_json::to_vec(&value).expect("settings serialize");
            let wrapped: &RawValue = serde_json::from_slice(&bytes).expect("serialized settings");
            (scanner.emit)(
                location(Region::ProviderState, None, None),
                Source::Raw(wrapped),
            )?;
            raw = get(raw, "tools")?.unwrap_or(RawValue::NULL);
        }
        scanner.parts(Region::Tools, None, raw, bedrock)?;
    }
    if let Some(mut raw) = get(body, system)? {
        if matches!(
            family,
            WireFamilyId::Gemini | WireFamilyId::GoogleCodeAssist
        ) {
            raw = get(raw, "parts")?.unwrap_or(raw);
        }
        scanner.parts(Region::System, None, raw, bedrock)?;
    }
    if let Some(raw) = get(body, messages)? {
        array(raw, |index, raw| {
            if raw.get().starts_with('{') {
                let system = get(raw, "role")?
                    .is_some_and(|v| matches!(v.get(), "\"system\"" | "\"developer\""));
                let region = if system {
                    Region::System
                } else {
                    Region::Messages
                };
                (scanner.emit)(
                    location(region.clone(), Some(index), None),
                    Source::Without(raw, &[content]),
                )?;
                if let Some(raw) = get(raw, content)? {
                    scanner.parts(region, Some(index), raw, bedrock)?;
                }
            } else {
                (scanner.emit)(
                    location(Region::Messages, Some(index), None),
                    Source::Raw(raw),
                )?;
            }
            Ok(())
        })?;
    }
    (scanner.emit)(
        location(Region::ProviderState, None, None),
        Source::Settings(body, family, &[tools, system, messages]),
    )?;
    Ok(scanner.breakpoint)
}

/// Hash deterministic renderer output. Only cache-marker and named rotating-field segments
/// are normalized; ordinary text, signed reasoning and unknown settings stay byte-sensitive.
pub fn fingerprint(family: WireFamilyId, body: &[u8]) -> Result<RequestFingerprint, String> {
    let mut segments = Vec::new();
    let mut scratch = Vec::new();
    let breakpoint = scan(family, body, |location, source| {
        segments.push((location, source.hash(&mut scratch)?));
        Ok(())
    })?;
    Ok(RequestFingerprint {
        family,
        segments,
        breakpoint,
    })
}

/// Compare owned hashes. Live evidence deliberately omits object-field detail because the
/// previous request bytes are not retained; offline readers can resolve the divergent segment.
pub fn compare_fingerprints(
    previous: &RequestFingerprint,
    current: &RequestFingerprint,
) -> CacheEvidence {
    comparison(previous, current).0
}

fn comparison(
    previous: &RequestFingerprint,
    current: &RequestFingerprint,
) -> (CacheEvidence, Option<usize>) {
    let mut evidence = CacheEvidence {
        extends_previous: true,
        previous_last_breakpoint: previous.breakpoint.clone(),
        ttl_kind: if matches!(
            previous.family,
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
        let mut old = previous.segments.iter().filter(|(l, _)| l.region == region);
        let mut new = current.segments.iter().filter(|(l, _)| l.region == region);
        for index in 0.. {
            match (old.next(), new.next()) {
                (None, None) => break,
                (Some((_, a)), Some((_, b))) if a == b => {}
                (None, Some(_)) if region == Region::Messages => break,
                (a, b) => {
                    evidence.extends_previous = false;
                    evidence.first_divergence =
                        Some(b.or(a).expect("one segment exists").0.clone());
                    return (evidence, Some(index));
                }
            }
        }
    }
    (evidence, None)
}

fn divergent_value(
    family: WireFamilyId,
    body: &[u8],
    region: &Region,
    index: usize,
) -> Result<Option<Value>, String> {
    let mut value = None;
    let mut ordinal = 0;
    scan(family, body, |location, source| {
        if location.region == *region {
            if ordinal == index {
                value = Some(source.value()?);
            }
            ordinal += 1;
        }
        Ok(())
    })?;
    Ok(value)
}

/// Compare canonical logical requests, not transport frames. Only the first divergent segment
/// is parsed for field-level detail; the prefix comparison itself uses the shared fingerprints.
pub fn compare_rendered_requests(
    family: WireFamilyId,
    previous: &[u8],
    current: &[u8],
) -> Result<CacheEvidence, String> {
    compare_rendered_requests_with_provenance(family, previous, current, None, None)
}

pub fn compare_rendered_requests_with_provenance(
    family: WireFamilyId,
    previous: &[u8],
    current: &[u8],
    previous_provenance: Option<&broca_protocol::render_provenance::RenderProvenance>,
    current_provenance: Option<&broca_protocol::render_provenance::RenderProvenance>,
) -> Result<CacheEvidence, String> {
    let mut old = fingerprint(family, previous)?;
    let mut new = fingerprint(family, current)?;
    if let Some(provenance) = previous_provenance {
        old = old.with_provenance(provenance);
    }
    if let Some(provenance) = current_provenance {
        new = new.with_provenance(provenance);
    }
    let (mut evidence, index) = comparison(&old, &new);
    if let Some((at, index)) = evidence.first_divergence.as_mut().zip(index) {
        if let Some((old, new)) = divergent_value(family, previous, &at.region, index)?
            .zip(divergent_value(family, current, &at.region, index)?)
        {
            at.field = changed_field(&old, &new);
        }
    }
    #[cfg(test)]
    {
        let reference = reference::compare_rendered_requests(family, previous, current)?;
        let mut body_only = evidence.clone();
        if let Some(divergence) = body_only.first_divergence.as_mut() {
            divergence.message_ordinal = None;
        }
        assert_eq!(
            serde_json::to_value(&body_only).unwrap(),
            serde_json::to_value(reference).unwrap(),
            "borrowed fingerprints must agree with the full-parse oracle"
        );
    }
    Ok(evidence)
}

fn normalize_billing_text(value: &mut Value) {
    match value {
        Value::String(text) => {
            // The billing header identifies a request/episode, not model instructions.
            if text
                .lines()
                .any(|line| line.trim_start().starts_with("x-anthropic-billing-header:"))
            {
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
    // Output reservation clamps change output caps, not cached prompt input.
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
                    // Device/session IDs rotate per install/episode; account_uuid scopes caches.
                    identity_object.remove("device_id");
                    identity_object.remove("session_id");
                    *user = Value::String(identity.to_string());
                }
            }
        }
    }
    if family == WireFamilyId::OpenAiResponsesChatgpt {
        if let Some(Value::Object(metadata)) = object.get_mut("client_metadata") {
            // These client_metadata fields route/trace a connection or turn, not prompt input.
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

fn changed_field(previous: &Value, current: &Value) -> Option<String> {
    let (Some(previous), Some(current)) = (previous.as_object(), current.as_object()) else {
        return None;
    };
    previous
        .keys()
        .chain(current.keys())
        .find(|key| previous.get(*key) != current.get(*key))
        .map(|field| {
            if field == "signature"
                && previous.get("type").and_then(Value::as_str) == Some("thinking")
            {
                "thinking.signature".into()
            } else {
                field.clone()
            }
        })
}

#[cfg(test)]
#[path = "cache_evidence_reference.rs"]
mod reference;
#[cfg(test)]
#[path = "cache_evidence_tests.rs"]
mod tests;
