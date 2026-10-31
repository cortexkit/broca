//! Frozen admission-checkpoint decoder before the optional attempt-presence summary.
//! Keep the old field list so rollback compatibility is tested by an actual old schema.

use super::*;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct AdmissionCheckpointV1 {
    #[cfg(feature = "test-attachments")]
    #[serde(default, skip_serializing_if = "Option::is_none")]
    media: Option<crate::media_request::DurableMediaSeed>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    head: Option<crate::head::HeadProjection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    has_deferred_calls: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    input_budget: Option<crate::input_budget::InputTokenBudget>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    session_class: Option<String>,
    at_seq: u64,
    last_episode: Option<LastEpisodeV1>,
    #[serde(
        serialize_with = "serialize_pending_fifo",
        deserialize_with = "deserialize_pending_fifo"
    )]
    pending_fifo: Arc<Vec<PendingSubmission>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    ownership: Option<SessionOwnershipV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    effort_history: Option<EffortHistoryV1>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    lineage_retention: Option<crate::LineageRetention>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    compaction: Option<crate::compaction::CompactionCheckpoint>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    tags: Option<crate::tags::LineageTags>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pending_order: Option<StagedOrder>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    output_reservation: Option<crate::output_reservation::OutputReservationHistory>,
}

#[derive(Serialize)]
struct AdmissionCheckpointEnvelopeRef<'a> {
    version: u32,
    checkpoint: &'a AdmissionCheckpointV1,
}

#[derive(Deserialize)]
struct AdmissionCheckpointEnvelope {
    version: u32,
    checkpoint: serde_json::Value,
}

fn encode_admission_checkpoint(
    checkpoint: &AdmissionCheckpointV1,
) -> Result<String, serde_json::Error> {
    serde_json::to_string(&AdmissionCheckpointEnvelopeRef {
        version: 1,
        checkpoint,
    })
}

fn decode_admission_checkpoint(encoded: &str) -> Option<AdmissionCheckpointV1> {
    let envelope: AdmissionCheckpointEnvelope = serde_json::from_str(encoded).ok()?;
    if envelope.version != 1 {
        return None;
    }
    serde_json::from_value(envelope.checkpoint).ok()
}

pub(super) fn round_trip(encoded: &str) -> Option<String> {
    encode_admission_checkpoint(&decode_admission_checkpoint(encoded)?).ok()
}
