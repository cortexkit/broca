use super::*;
use broca_core::AdmissionCheckpointV1;
use broca_wal::FramedRecord;
mod surrogate;
use surrogate::SurrogateAdmissionCheckpoint;

fn decode_surrogate_envelope(value: serde_json::Value) -> SurrogateAdmissionCheckpoint {
    #[derive(serde::Deserialize)]
    struct Envelope {
        version: u32,
        checkpoint: SurrogateAdmissionCheckpoint,
    }
    let envelope: Envelope = serde_json::from_value(value).unwrap();
    assert_eq!(envelope.version, 1);
    envelope.checkpoint
}

#[test]
fn surrogate_0_3_284_envelope_ignores_seed_with_current_render_parity() {
    let mut seeded: serde_json::Value = serde_json::from_str(include_str!(
        "../../../tests/fixtures/media-checkpoint-v1.json"
    ))
    .unwrap();
    let seed = seeded
        .as_object_mut()
        .unwrap()
        .remove("media")
        .expect("fixture carries the versioned media seed");
    assert_eq!(seed["version"], 1);
    #[cfg(feature = "test-attachments")]
    broca_core::media_request::MediaCheckpoint::decode(&seed)
        .expect("fixture is a current valid media seed");
    // The fixture is a bare payload; the public checkpoint decoder takes an envelope.
    let legacy = decode_surrogate_envelope(json!({"version":1,"checkpoint":seeded.clone()}));
    seeded["media"] = seed;
    let decoded = decode_surrogate_envelope(json!({"version":1,"checkpoint":seeded}));
    let episode = decoded.last_episode.as_ref().unwrap();
    let original = legacy.last_episode.as_ref().unwrap();
    let frozen: crate::wire::FrozenRenderConfig =
        serde_json::from_value(episode.config.frozen_render_config.clone()).unwrap();
    let family = crate::registry::wire_family(frozen.family_id).unwrap();
    assert_eq!(
        family.render(
            &frozen,
            &broca_protocol::CallOptions::new(episode.input.clone())
        ),
        family.render(
            &frozen,
            &broca_protocol::CallOptions::new(original.input.clone())
        )
    );
    assert_eq!(decoded.at_seq, legacy.at_seq);
    assert_eq!(decoded.compaction, legacy.compaction);
    assert_eq!(decoded.last_episode, legacy.last_episode);
}

#[test]
fn media_free_checkpoint_field_order_matches_surrogate_0_3_284_schema() {
    let mut input = Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "legacy text-only checkpoint".into(),
        }],
    );
    input.ordinal = Some(0);
    let records = [start(&[input]), step(1)];
    let frames: Vec<_> = records
        .into_iter()
        .enumerate()
        .map(|(index, record)| FramedRecord {
            seq: index as u64 + 1,
            fence: 1,
            record,
        })
        .collect();
    let mut checkpoint = AdmissionCheckpointV1::from_frames(&frames).unwrap();
    let bytes = serde_json::to_vec(&checkpoint).unwrap();
    let legacy: SurrogateAdmissionCheckpoint = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(checkpoint.has_model_attempt, Some(false));
    // The old decoder ignores the additive summary. Its known fields must still serialize
    // in exactly their old order after removing only the member it does not know.
    checkpoint.has_model_attempt = None;
    assert_eq!(
        serde_json::to_vec(&checkpoint).unwrap(),
        serde_json::to_vec(&legacy).unwrap()
    );
}
