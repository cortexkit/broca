//! Content-free run notifications. Subjects and payload bytes are independently stable.

use cortexkit_bus_naming::{AccountNames, NamingError};
use serde::{Deserialize, Serialize, Serializer};

use crate::{ErrorClass, PauseReason, RunFinishReason};

/// The limit is exclusive: a 512-byte event is refused, not shortened.
pub const MAX_EVENT_BYTES: usize = 512;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct RunEvent {
    pub v: u8,
    pub session_ref: String,
    pub run_id: String,
    pub seq: u64,
    pub at_ms: u64,
    #[serde(flatten)]
    pub detail: RunEventDetail,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
pub enum RunEventDetail {
    Started {},
    Paused {
        pause_reason: PauseReason,
    },
    Ended {
        terminal: RunFinishReason,
        #[serde(
            skip_serializing_if = "Option::is_none",
            serialize_with = "shared_error_class"
        )]
        error_class: Option<ErrorClass>,
        #[serde(skip_serializing_if = "Option::is_none")]
        structured_output: Option<StructuredOutputStatus>,
    },
    SteerConsumed {
        step: u64,
        consumed: u64,
        send_ids: Vec<String>,
        truncated: bool,
    },
    Progress {
        step: u64,
        #[serde(skip_serializing_if = "Option::is_none")]
        last_content_at_ms: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_content_gap_ms: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StructuredOutputStatus {
    Passed,
    Failed,
    Absent,
}

fn shared_error_class<S: Serializer>(class: &Option<ErrorClass>, s: S) -> Result<S::Ok, S::Error> {
    class.map(ErrorClass::shared_class).serialize(s)
}

#[derive(Debug)]
pub enum EncodeError {
    InvalidIdentity,
    Json(serde_json::Error),
    Oversize(usize),
}

impl std::fmt::Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidIdentity => f.write_str("run event requires v1 and a 32-hex session_ref"),
            Self::Json(error) => write!(f, "run event JCS encoding: {error}"),
            Self::Oversize(bytes) => write!(
                f,
                "run event is {bytes} bytes; must be under {MAX_EVENT_BYTES}"
            ),
        }
    }
}

impl std::error::Error for EncodeError {}

impl RunEvent {
    pub fn subject(&self, account: &AccountNames) -> Result<String, NamingError> {
        // The naming constructor validates each event name, including its dot tokens.
        account.event_subject("broca", self.detail.name(), 1)
    }

    pub fn jcs(&self) -> Result<Vec<u8>, EncodeError> {
        if self.v != 1
            || self.session_ref.len() != 32
            || !self
                .session_ref
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return Err(EncodeError::InvalidIdentity);
        }
        let bytes = serde_jcs::to_vec(self).map_err(EncodeError::Json)?;
        if bytes.len() >= MAX_EVENT_BYTES {
            return Err(EncodeError::Oversize(bytes.len()));
        }
        Ok(bytes)
    }

    /// Keep a prefix of the caller's send ids, explicitly marking any omitted ids.
    /// All other fields retain their full values, even when those alone exceed the limit.
    pub fn fit_send_ids(&mut self) -> Result<Vec<u8>, EncodeError> {
        loop {
            match self.jcs() {
                Err(EncodeError::Oversize(_)) => match &mut self.detail {
                    RunEventDetail::SteerConsumed {
                        send_ids,
                        truncated,
                        ..
                    } if !send_ids.is_empty() => {
                        send_ids.pop();
                        *truncated = true;
                    }
                    _ => return self.jcs(),
                },
                result => return result,
            }
        }
    }
}

impl RunEventDetail {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Started {} => "run.started",
            Self::Paused { .. } => "run.paused",
            Self::Ended { .. } => "run.ended",
            Self::SteerConsumed { .. } => "run.steer_consumed",
            Self::Progress { .. } => "run.progress",
        }
    }
}
