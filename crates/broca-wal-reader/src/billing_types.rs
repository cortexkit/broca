//! Shared decoding shapes for billing evidence. Unknown fields remain available as extensions;
//! decoding a shape alone does not enforce the reader's cross-field or attempt lifecycle checks.

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Immutable request facts. Attribution objects are opaque, forward-compatible billing facts.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reservation {
    pub run_id: String,
    pub step_id: u64,
    pub attempt: u64,
    pub send_index: u64,
    pub warm: bool,
    pub reserved_at_ms: u64,
    pub provider: String,
    pub endpoint_host: String,
    pub model_sent: String,
    pub credential: Map<String, Value>,
    pub session: Map<String, Value>,
    pub run: Map<String, Value>,
    pub request_body_bytes: u64,
    pub compressed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens_estimate: Option<u64>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

impl Reservation {
    /// Decode the optional admission snapshot without inventing facts for older reservations.
    pub fn vault_billing_facts(
        &self,
    ) -> Result<Option<crate::vault_facts::VaultBillingFacts>, serde_json::Error> {
        self.credential
            .get("auth_selection")
            .and_then(|selection| selection.get("billing_vault_facts"))
            .map(|facts| serde_json::from_value(facts.clone()))
            .transpose()
    }
}

/// Provider counters, preserving the difference between absent evidence and measured zero.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Usage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cached_input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_tokens: Option<u64>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Usage attributed to one serving model in a multi-model response.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageByModel {
    pub model_used: String,
    pub usage: Usage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_5m_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_1h_tokens: Option<u64>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}

/// Wire values are `true`, `false`, and `"unknown"`, not three string variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "Value", into = "Value")]
pub enum Sent {
    Yes,
    No,
    Unknown,
}

impl TryFrom<Value> for Sent {
    type Error = &'static str;

    fn try_from(value: Value) -> Result<Self, Self::Error> {
        match value {
            Value::Bool(true) => Ok(Self::Yes),
            Value::Bool(false) => Ok(Self::No),
            Value::String(s) if s == "unknown" => Ok(Self::Unknown),
            _ => Err("sent must be true, false, or unknown"),
        }
    }
}

impl From<Sent> for Value {
    fn from(sent: Sent) -> Self {
        match sent {
            Sent::Yes => Self::Bool(true),
            Sent::No => Self::Bool(false),
            Sent::Unknown => Self::String("unknown".into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Complete,
    ToolArgsTrailingGarbage,
    EofBeforeCompletion,
    ChunkError,
    IdleTimeout,
    ContentTimeout,
    ByteLimit,
    DecodeError,
    RateFloor,
    Cancelled,
    SealedInFlight,
    HttpError,
    CompressionRefused,
    ProviderError,
    ConnectFailed,
    HeaderTimeout,
    NoStatus,
    Unknown,
}

/// Terminal or correction evidence, with optional provider facts left absent when unobserved.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub sent: Sent,
    pub response_bytes: u64,
    pub outcome: Outcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_used: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_by_model: Option<Vec<UsageByModel>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_5m_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_1h_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_included_in_output: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_raw: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vendor_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier_used: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed_used: Option<String>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
