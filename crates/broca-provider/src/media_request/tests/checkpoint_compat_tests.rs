use super::*;
use broca_core::AdmissionCheckpointV1;
use broca_wal::FramedRecord;
mod legacy;
use legacy::LegacyAdmissionCheckpoint;

#[test]
fn media_checkpoint_rollback_decoder_ignores_seed_without_changing_render() {
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
    let legacy: LegacyAdmissionCheckpoint = serde_json::from_value(seeded.clone()).unwrap();
    seeded["media"] = seed;
    let decoded: LegacyAdmissionCheckpoint = serde_json::from_value(seeded).unwrap();
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
fn media_free_checkpoint_bytes_equal_0_3_284_decoder() {
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
    let checkpoint = AdmissionCheckpointV1::from_frames(&frames).unwrap();
    let bytes = serde_json::to_vec(&checkpoint).unwrap();
    let legacy: LegacyAdmissionCheckpoint = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(bytes, serde_json::to_vec(&legacy).unwrap());
}
