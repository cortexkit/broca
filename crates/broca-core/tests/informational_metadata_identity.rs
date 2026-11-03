use broca_core::{billing, render, replay, AdmissionCheckpointV1};
use broca_protocol::{BindPrincipal, CallOptions};
use broca_wal::{FramedRecord, WalRecord};

#[test]
fn informational_finish_and_sender_members_do_not_enter_derived_identity_domains() {
    let mut old: Vec<WalRecord> = [
        include_str!("../../broca-wal/tests/golden/pre_step_transforms/run_started.json"),
        include_str!("../../broca-wal/tests/golden/pre_step_transforms/queued.json"),
        include_str!("../../broca-wal/tests/golden/pre_step_transforms/steered.json"),
        include_str!("../../broca-wal/tests/golden/pre_step_transforms/model_step_finished.json"),
    ]
    .into_iter()
    .map(|json| serde_json::from_str(json).unwrap())
    .collect();
    old.push(serde_json::from_value(serde_json::json!({"type":"run_finished","run_id":"run-7","reason":"completed","usage":{},"retries_used":0})).unwrap());
    let mut decorated = old.clone();
    for record in &mut decorated {
        match record {
            WalRecord::RunStarted { sent_by, .. }
            | WalRecord::Queued { sent_by, .. }
            | WalRecord::Steered { sent_by, .. } => {
                *sent_by = Some(BindPrincipal::Reserved {
                    module_id: "display-only-not-the-owner".into(),
                })
            }
            WalRecord::ModelStepFinished {
                raw_finish_reason, ..
            } => *raw_finish_reason = Some("native-token-not-a-canonical-reason".into()),
            _ => {}
        }
    }
    assert_ne!(
        serde_json::to_vec(&old).unwrap(),
        serde_json::to_vec(&decorated).unwrap()
    );
    let before = replay::replay(&old).unwrap();
    let after = replay::replay(&decorated).unwrap();
    assert_eq!(before.prompt, after.prompt);
    assert_eq!(before.run_config, after.run_config);
    let rendered_before = render::canonical_request_bytes(&CallOptions::new(before.prompt.clone()));
    let rendered_after = render::canonical_request_bytes(&CallOptions::new(after.prompt.clone()));
    assert_eq!(rendered_before, rendered_after);
    assert_eq!(
        render::request_digest(&rendered_before),
        render::request_digest(&rendered_after)
    );
    assert_eq!(
        broca_wal::compaction_replacement_sha256(&before.prompt).unwrap(),
        broca_wal::compaction_replacement_sha256(&after.prompt).unwrap()
    );
    for (before, after) in before.prompt.iter().zip(&after.prompt) {
        assert_eq!(
            broca_protocol::caller_message::message_content_identity(before).digest,
            broca_protocol::caller_message::message_content_identity(after).digest
        );
    }
    let frames = |records: Vec<WalRecord>| {
        records
            .into_iter()
            .enumerate()
            .map(|(index, record)| FramedRecord {
                seq: index as u64 + 1,
                fence: 1,
                record,
            })
            .collect::<Vec<_>>()
    };
    let old = frames(old);
    let decorated = frames(decorated);
    let before = AdmissionCheckpointV1::from_frames(&old).unwrap();
    let after = AdmissionCheckpointV1::from_frames(&decorated).unwrap();
    assert_eq!(
        broca_core::encode_admission_checkpoint(&before).unwrap(),
        broca_core::encode_admission_checkpoint(&after).unwrap()
    );
    let resolver = |_: &str, _: &str| (billing::ChargeBasis::MeteredApi, 1);
    let before = billing::compute_segments(&old, "stable-source", &resolver);
    let after = billing::compute_segments(&decorated, "stable-source", &resolver);
    assert!(!before.is_empty());
    assert_eq!(
        serde_json::to_vec(&before).unwrap(),
        serde_json::to_vec(&after).unwrap()
    );
}
