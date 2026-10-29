//! Admission checkpoint decoder from broca 0.3.284 (75dbd49fd87e5edd4b91d122a9494b2decb9b756).
//! Only the type name and crate qualifiers differ; its field order and serde rules are frozen.
use broca_core::admission::*;
use broca_core::PendingSubmission;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LegacyAdmissionCheckpoint {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub head: Option<broca_core::head::HeadProjection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub has_deferred_calls: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_budget: Option<broca_core::input_budget::InputTokenBudget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_class: Option<String>,
    pub at_seq: u64,
    pub last_episode: Option<LastEpisodeV1>,
    #[serde(
        serialize_with = "serialize_pending_fifo",
        deserialize_with = "deserialize_pending_fifo"
    )]
    pub pending_fifo: Arc<Vec<PendingSubmission>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ownership: Option<SessionOwnershipV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub effort_history: Option<EffortHistoryV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lineage_retention: Option<broca_core::LineageRetention>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compaction: Option<broca_core::compaction::CompactionCheckpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<broca_core::tags::LineageTags>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending_order: Option<StagedOrder>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_reservation: Option<broca_core::output_reservation::OutputReservationHistory>,
}

fn serialize_pending_fifo<S>(
    pending: &Arc<Vec<PendingSubmission>>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    pending.as_ref().serialize(serializer)
}

fn deserialize_pending_fifo<'de, D>(
    deserializer: D,
) -> Result<Arc<Vec<PendingSubmission>>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<PendingSubmission>::deserialize(deserializer).map(Arc::new)
}
