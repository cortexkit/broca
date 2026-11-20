//! Host creation facts use recorded acceptance and request-send stamps, never row clocks.

use std::collections::{BTreeMap, VecDeque};

use broca_protocol::StepId;

/// Only unfinished placement needs a continuation. No message bodies are cached here.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreationTimeSeed {
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    requests: BTreeMap<StepId, u64>,
    #[serde(default, skip_serializing_if = "VecDeque::is_empty")]
    steers: VecDeque<Option<u64>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    assistant: Option<u64>,
}

impl CreationTimeSeed {}
