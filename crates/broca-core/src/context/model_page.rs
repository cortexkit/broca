//! Model pages reflect the same full-prompt guidance fold and compaction slice as a send.
use super::*;
use serde::Serialize;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ModelEntry {
    /// Built from the pinned role's model_view_source for replacements.
    pub source: Value,
    pub message: Message,
}
