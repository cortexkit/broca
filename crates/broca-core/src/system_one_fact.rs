//! Sessionless provider measurements in the immutable usage export stream. Pricing belongs
//! to the consumer; batch estimates and frozen ceiling rates never become measurements.

use serde::{Deserialize, Serialize};

use crate::{ChargeBasis, SpendSegment};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename = "system_one")]
pub struct SystemOneFact {
    pub fact_id: String,
    pub batch_id: String,
    pub item_index: u64,
    pub provider: String,
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_selection: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_id: Option<String>,
    pub charge_basis: ChargeBasis,
    pub charge_basis_mapping_version: u32,
    pub charge_basis_overrides: String,
    pub usage: SystemOneUsage,
    pub occurred_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SystemOneUsage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
}

/// The on-disk shape of session facts is unchanged: only sessionless facts carry `kind`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum UsageExportSegment {
    Session(Box<SpendSegment>),
    SystemOne(Box<SystemOneFact>),
}

impl UsageExportSegment {
    pub fn as_session(&self) -> Option<&SpendSegment> {
        match self {
            Self::Session(segment) => Some(segment),
            Self::SystemOne(_) => None,
        }
    }
}

impl<'de> Deserialize<'de> for UsageExportSegment {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = serde_json::Value::deserialize(deserializer)?;
        match value.get("kind").and_then(serde_json::Value::as_str) {
            Some("system_one") => {
                serde_json::from_value(value).map(|fact| Self::SystemOne(Box::new(fact)))
            }
            None => serde_json::from_value(value).map(|segment| Self::Session(Box::new(segment))),
            Some(kind) => {
                return Err(serde::de::Error::custom(format!(
                    "unknown usage fact kind {kind}"
                )))
            }
        }
        .map_err(serde::de::Error::custom)
    }
}
