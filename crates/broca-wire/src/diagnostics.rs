//! The read-only `diagnostics.findings` v1 frame result.
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;

pub const OP: &str = "diagnostics.findings";
pub const DEFAULT_WINDOW_MS: i64 = 21_600_000;
pub const MAX_WINDOW_MS: i64 = 604_800_000;

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub window_ms: Option<i64>,
}

impl Request {
    pub fn window(params: Value) -> Result<i64, String> {
        let request: Self = if params.is_null() {
            Self::default()
        } else {
            serde_json::from_value(params).map_err(|e| e.to_string())?
        };
        let window = request.window_ms.unwrap_or(DEFAULT_WINDOW_MS);
        if !(1..=MAX_WINDOW_MS).contains(&window) {
            return Err(format!(
                "diagnostics.findings window_ms must be between 1 and {MAX_WINDOW_MS}"
            ));
        }
        Ok(window)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Severity {
    Warn,
    Error,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Finding {
    pub check: String,
    pub key: String,
    pub severity: Severity,
    pub count: u64,
    pub first_seen_ms: i64,
    pub last_seen_ms: i64,
    pub summary: String,
    pub samples: Vec<Value>,
    pub hint: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Coverage {
    pub since_ms: i64,
    pub truncated: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Reply {
    pub generated_at_ms: i64,
    pub window_ms: i64,
    pub findings: Vec<Finding>,
    pub coverage: BTreeMap<String, Coverage>,
}
