//! The CK-Message canonical type — the harness-agnostic message an external reduction
//! transform (MC) operates on. This is the normative CK#1 shape (see the CK-Message spec):
//! a SMALL FIXED typed core of seven content kinds (exactly the transform's branch surface)
//! plus one universal `Opaque` carrier for every provider- or harness-native block with no
//! typed-core equivalent. The enum does NOT grow when a provider adds a feature — new
//! blocks become `Opaque`.
//!
//! broca is the PRODUCER side: it lifts its own canonical `Message` into `CkMessage`
//! per provider call, an external transform reduces the array, and broca projects the
//! result back and renders it. The owned canonical has no `Opaque`/`Media` blocks today, so
//! the producer leg never emits those variants (they exist for round-trip completeness and
//! the future MITM leg); projecting one fails loud rather than carrying a lossy shape.

use std::{
    collections::BTreeMap,
    ops::{Deref, DerefMut, Index, IndexMut},
};

use serde::{de::Error as _, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{value::RawValue, Value};

pub use broca_protocol::content::Role;
use broca_protocol::request::Tool;

/// A provider-keyed extras bag: `provider -> key -> value`, in deterministic canonical
/// (sorted) order. The transform never interprets it; it is carried value-preserved.
///
/// In addition to the semantic [`Value`] view, this bag keeps the source [`RawValue`] for
/// decoded wire fields. That lets a provider encoder splice an extension value back verbatim
/// without forcing object keys through `serde_json::Value`'s normalizing map. The outer and
/// inner maps are B-trees, so extras-map serialization itself remains deterministically sorted;
/// this ordering is identity-bearing for provider round trips.
#[derive(Debug, Clone, Default)]
pub struct ProviderExtras {
    values: BTreeMap<String, BTreeMap<String, Value>>,
    raw_values: BTreeMap<String, BTreeMap<String, Box<RawValue>>>,
}

impl ProviderExtras {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get(&self, provider: &str) -> Option<&BTreeMap<String, Value>> {
        self.values.get(provider)
    }

    pub fn get_mut(&mut self, provider: &str) -> Option<&mut BTreeMap<String, Value>> {
        self.raw_values.remove(provider);
        self.values.get_mut(provider)
    }

    pub fn insert(
        &mut self,
        provider: String,
        family: BTreeMap<String, Value>,
    ) -> Option<BTreeMap<String, Value>> {
        self.raw_values.remove(&provider);
        self.values.insert(provider, family)
    }

    /// Insert a decoded wire value while retaining its source spelling for serialization.
    pub fn insert_raw(&mut self, provider: String, key: String, raw: Box<RawValue>) {
        let value: Value = serde_json::from_str(raw.get()).expect("RawValue contains valid JSON");
        self.values
            .entry(provider.clone())
            .or_default()
            .insert(key.clone(), value);
        self.raw_values
            .entry(provider)
            .or_default()
            .insert(key, raw);
    }

    /// Return the retained source spelling for one value, if it came from a decoded wire field.
    pub fn raw_value(&self, provider: &str, key: &str) -> Option<&RawValue> {
        self.raw_values
            .get(provider)
            .and_then(|family| family.get(key))
            .map(Box::as_ref)
    }

    /// Merge another extras bag, preserving any retained raw values it carries.
    pub fn merge_from(&mut self, other: Self) {
        for (provider, family) in &other.values {
            for (key, value) in family {
                if let Some(raw) = other.raw_value(provider, key) {
                    self.insert_raw(provider.clone(), key.clone(), raw.to_owned());
                } else {
                    if let Some(raw_family) = self.raw_values.get_mut(provider) {
                        raw_family.remove(key);
                    }
                    self.values
                        .entry(provider.clone())
                        .or_default()
                        .insert(key.clone(), value.clone());
                }
            }
        }
    }
}

impl Deref for ProviderExtras {
    type Target = BTreeMap<String, BTreeMap<String, Value>>;

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}

impl DerefMut for ProviderExtras {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.raw_values.clear();
        &mut self.values
    }
}

impl Index<&str> for ProviderExtras {
    type Output = BTreeMap<String, Value>;

    fn index(&self, provider: &str) -> &Self::Output {
        &self.values[provider]
    }
}

impl IndexMut<&str> for ProviderExtras {
    fn index_mut(&mut self, provider: &str) -> &mut Self::Output {
        self.raw_values.remove(provider);
        self.values
            .get_mut(provider)
            .expect("indexed provider extras entry must exist")
    }
}

impl PartialEq for ProviderExtras {
    fn eq(&self, other: &Self) -> bool {
        self.values == other.values
    }
}

impl Serialize for ProviderExtras {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;

        let mut outer = serializer.serialize_map(Some(self.values.len()))?;
        for (provider, family) in &self.values {
            outer.serialize_entry(
                provider,
                &SerializedFamily {
                    family,
                    raw_values: self.raw_values.get(provider),
                },
            )?;
        }
        outer.end()
    }
}

struct SerializedFamily<'a> {
    family: &'a BTreeMap<String, Value>,
    raw_values: Option<&'a BTreeMap<String, Box<RawValue>>>,
}

impl Serialize for SerializedFamily<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeMap;

        let mut map = serializer.serialize_map(Some(self.family.len()))?;
        for (key, value) in self.family {
            if let Some(raw) = self.raw_values.and_then(|family| family.get(key)) {
                map.serialize_entry(key, raw)?;
            } else {
                map.serialize_entry(key, value)?;
            }
        }
        map.end()
    }
}

impl<'de> Deserialize<'de> for ProviderExtras {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw_values =
            BTreeMap::<String, BTreeMap<String, Box<RawValue>>>::deserialize(deserializer)?;
        let mut extras = Self::new();
        for (provider, family) in raw_values {
            for (key, raw) in family {
                let value = serde_json::from_str(raw.get()).map_err(D::Error::custom)?;
                extras
                    .values
                    .entry(provider.clone())
                    .or_default()
                    .insert(key.clone(), value);
                extras
                    .raw_values
                    .entry(provider.clone())
                    .or_default()
                    .insert(key, raw);
            }
        }
        Ok(extras)
    }
}

/// A decoded request from a provider JSON API. It contains canonical CK messages,
/// recovered tool declarations, the provider model id, and any extra top-level fields needed
/// to reconstruct the original request. System bytes are represented as leading
/// [`Role::System`] messages, not as a separate field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DecodedRequest {
    pub messages: Vec<CkMessage>,
    pub tools: Vec<Tool>,
    pub wire_model_id: String,
    pub residual: Value,
}

/// The canonical, harness-agnostic message the transform operates on.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CkMessage {
    pub role: Role,
    /// Content parts in order (order is byte-affecting — preserved verbatim).
    pub content: Vec<ContentBlock>,
    /// The `{provider, model, api}` that produced an assistant turn; drives the codec's
    /// same-model reasoning-signature keep and tool-id normalization. `None` for
    /// origin-less messages (user/tool/synthetic).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub origin: Option<MessageOrigin>,
    /// Message-level provider extras the transform never interprets.
    #[serde(default, skip_serializing_if = "ProviderExtras::is_empty")]
    pub provider_extras: ProviderExtras,
    /// Harness-only metadata. `project` strips all of it; it must never affect rendered
    /// bytes.
    #[serde(default)]
    pub meta: HarnessMeta,
}

/// The model that produced an assistant turn. `model` is the native same-model key (the
/// requested model id), not a served substitution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageOrigin {
    pub provider: String,
    pub model: String,
    pub api: String,
}

/// A content part: its typed kind + a part-level provider-extras bag.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContentBlock {
    pub kind: ContentKind,
    #[serde(default, skip_serializing_if = "ProviderExtras::is_empty")]
    pub provider_extras: ProviderExtras,
}

impl ContentBlock {
    /// A content block with the given kind and no provider extras (the common case for the
    /// owned producer leg, whose canonical carries no per-block extras).
    pub fn bare(kind: ContentKind) -> Self {
        Self {
            kind,
            provider_extras: ProviderExtras::new(),
        }
    }
}

/// The small fixed typed core: seven kinds = exactly the transform's branch surface.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentKind {
    /// Plain text. `§N§` tag prefixes live as literal text inside `text`, never as a field.
    Text { text: String },
    /// Reasoning. `signature` is typed because the transform branches on it (nulls on
    /// clear); byte-verbatim.
    Reasoning {
        text: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        signature: Option<String>,
    },
    /// Encrypted reasoning with no plaintext. `data` is opaque and byte-verbatim. The optional
    /// provider format remains attached when converting between protocol messages and this
    /// transform-facing representation, preventing another provider format from emitting it.
    RedactedReasoning {
        data: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_format: Option<broca_protocol::EncryptedReasoningFormat>,
    },
    /// A standard-shape tool call. `id` is the CK-canonical id. `provider_executed` is
    /// codec-derived (whether the provider ran the tool server-side).
    ToolCall {
        id: String,
        name: String,
        input: Value,
        /// Opaque provider signature carried through transforms so a same-model replay can
        /// attach it to the function call again.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        thought_signature: Option<String>,
        #[serde(default)]
        provider_executed: bool,
    },
    /// A standard-shape tool result. `id` pairs with the `ToolCall.id`. `tool_name` is
    /// preserved; `output` is the typed output shape.
    ToolResult {
        id: String,
        tool_name: String,
        output: ToolResultOutput,
        #[serde(default)]
        provider_executed: bool,
    },
    /// Multimodal input.
    Media(MediaBlock),
    /// The universal opaque carrier: any block with no typed-core equivalent. `raw` is the
    /// native block's faithful bytes; the transform never interprets it. `arc` is
    /// codec-derived pairing metadata so a tool/approval arc stays grouped without parsing
    /// `raw`.
    Opaque(OpaqueBlock),
}

/// A tool result's output (mirrors the @ai-sdk V4 tool-result output shape). Errors are
/// output variants here (not a separate flag), and an execution-denied result stays inside
/// the typed `ToolResult` for pairing.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolResultOutput {
    pub kind: ToolResultOutputKind,
    #[serde(default, skip_serializing_if = "ProviderExtras::is_empty")]
    pub provider_extras: ProviderExtras,
}

impl ToolResultOutput {
    /// An output of the given kind with no provider extras.
    pub fn bare(kind: ToolResultOutputKind) -> Self {
        Self {
            kind,
            provider_extras: ProviderExtras::new(),
        }
    }
}

/// The typed output kinds. Internally tagged by `type` (like [`ContentKind`], so a nested
/// output reads `{"type":"text","text":"…"}` — not a stuttering `kind.kind` under the
/// enclosing `ToolResultOutput.kind` field). Scalar variants match the owned
/// `broca_protocol::ToolOutput` convention; nested content variants preserve richer provider
/// inputs that the owned path cannot represent. A cross-language parser (magic-context's ingress)
/// pins its own golden against this shape.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ToolResultOutputKind {
    Text {
        text: String,
    },
    Json {
        value: Value,
    },
    ErrorText {
        text: String,
    },
    ErrorJson {
        value: Value,
    },
    /// A human-in-the-loop denied tool output.
    ExecutionDenied {
        reason: Option<String>,
    },
    /// A nested sequence of successful result blocks (text/media/opaque).
    Content {
        blocks: Vec<ResultBlock>,
    },
    /// A nested sequence of result blocks reported as a tool execution error.
    ErrorContent {
        blocks: Vec<ResultBlock>,
    },
}

/// A nested result block: text, media, or a provider-native nested block carried as opaque.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResultBlock {
    pub kind: ResultBlockKind,
    #[serde(default, skip_serializing_if = "ProviderExtras::is_empty")]
    pub provider_extras: ProviderExtras,
}

/// A nested result block's kind, internally tagged by `type` with struct variants (so the
/// wire is `{"type":"text","text":"…"}`), consistent with `ContentKind`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResultBlockKind {
    Text { text: String },
    Media { media: MediaBlock },
    Opaque { opaque: OpaqueBlock },
}

/// A provider- or harness-native block with no typed-core equivalent, carried verbatim.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpaqueBlock {
    /// Which edge produced the block, and the round-trip target (an opaque block is
    /// family-bound — e.g. a `server_tool_use` only round-trips to Anthropic).
    pub source: OpaqueSource,
    /// The native type tag (e.g. `server_tool_use`, `opencode:step-start`), for
    /// logging/summary and the codec's re-render dispatch. The transform does not branch on
    /// it.
    pub kind: String,
    /// The native block's faithful bytes. For a wire source these are verbatim wire bytes
    /// (so re-emit is byte-identity for free); for a harness source, a lossless
    /// serialization of the harness part.
    pub raw: Vec<u8>,
    /// Codec-derived arc metadata when this block is half of a tool or approval arc, so
    /// pairing groups the arc without parsing `raw`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arc: Option<OpaqueArc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum OpaqueSource {
    /// A provider wire block, tagged with the family it round-trips to (e.g. `anthropic`).
    Wire { wire: String },
    /// A harness-native part, tagged with the harness (e.g. `opencode`).
    Harness { harness: String },
}

/// Codec-derived arc metadata for an opaque block that is half of a paired group. `kind`
/// separates arc families (tool use/result vs approval request/response) so reclaim never
/// groups across families; `id` is the shared correlation id; `role` is which half.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpaqueArc {
    pub kind: OpaqueArcKind,
    pub id: String,
    pub role: OpaqueArcRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpaqueArcKind {
    Tool,
    Approval,
    /// OpenAI Responses encrypted reasoning (`reasoning` items) that must be echoed back
    /// byte-verbatim and kept paired with the following `function_call` in the same assistant
    /// turn.
    Reasoning,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpaqueArcRole {
    /// The initiating half (a `server_tool_use` / `mcp_approval_request`).
    Request,
    /// The answering half (a `*_tool_result` / `mcp_approval_response`).
    Response,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MediaBlock {
    pub kind: MediaKind,
    /// The raw provider media type, verbatim (may be `image/png`, top-level `image`, or
    /// `image/*`).
    pub media_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    pub source: MediaSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MediaKind {
    Image,
    Audio,
    Video,
    File,
    Document,
}

/// Mirrors the @ai-sdk V4 file-data shape. The `data` variant keeps the raw-bytes vs
/// base64-string distinction so a harness round-trip reproduces the same value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case")]
pub enum MediaSource {
    DataBytes { bytes: Vec<u8> },
    DataBase64 { base64: String },
    Url { url: String },
    ProviderReference { reference: Value },
    InlineText { text: String },
}

/// Harness-only metadata. `project` strips all of it; none of it may affect rendered bytes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessMeta {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub harness_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ordinal: Option<u64>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub synthetic: bool,
}

#[cfg(test)]
mod tests {
    use super::{
        MediaSource, ProviderExtras, ResultBlock, ResultBlockKind, ToolResultOutput,
        ToolResultOutputKind,
    };
    use serde_json::{json, Value};

    fn assert_media_source_round_trip(source: MediaSource, expected: Value) {
        let encoded = serde_json::to_value(&source).expect("serialize media source");
        assert_eq!(encoded, expected);
        let decoded: MediaSource =
            serde_json::from_value(encoded).expect("deserialize media source");
        assert_eq!(decoded, source);
    }

    #[test]
    fn data_bytes_media_source_round_trips() {
        assert_media_source_round_trip(
            MediaSource::DataBytes {
                bytes: vec![0, 1, 255],
            },
            json!({"source": "data_bytes", "bytes": [0, 1, 255]}),
        );
    }

    #[test]
    fn data_base64_media_source_round_trips() {
        assert_media_source_round_trip(
            MediaSource::DataBase64 {
                base64: "aGVsbG8=".into(),
            },
            json!({"source": "data_base64", "base64": "aGVsbG8="}),
        );
    }

    #[test]
    fn url_media_source_round_trips() {
        assert_media_source_round_trip(
            MediaSource::Url {
                url: "https://example.invalid/image.png".into(),
            },
            json!({"source": "url", "url": "https://example.invalid/image.png"}),
        );
    }

    #[test]
    fn provider_reference_media_source_round_trips() {
        assert_media_source_round_trip(
            MediaSource::ProviderReference {
                reference: json!({"file_id": "file-1"}),
            },
            json!({"source": "provider_reference", "reference": {"file_id": "file-1"}}),
        );
    }

    #[test]
    fn inline_text_media_source_round_trips() {
        assert_media_source_round_trip(
            MediaSource::InlineText {
                text: "caption".into(),
            },
            json!({"source": "inline_text", "text": "caption"}),
        );
    }

    #[test]
    fn error_content_tool_output_round_trips_with_its_wire_tag() {
        let output = ToolResultOutput::bare(ToolResultOutputKind::ErrorContent {
            blocks: vec![ResultBlock {
                kind: ResultBlockKind::Text {
                    text: "boom".into(),
                },
                provider_extras: ProviderExtras::new(),
            }],
        });
        let encoded = serde_json::to_string(&output).expect("serialize error content output");

        assert_eq!(
            encoded,
            r#"{"kind":{"type":"error_content","blocks":[{"kind":{"type":"text","text":"boom"}}]}}"#
        );
        let decoded: ToolResultOutput =
            serde_json::from_str(&encoded).expect("deserialize error content output");
        assert_eq!(decoded, output);
    }
}
