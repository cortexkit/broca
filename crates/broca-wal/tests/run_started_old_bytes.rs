use broca_wal::framing;
use broca_wal::WalRecord;
use sha2::{Digest, Sha256};

#[test]
fn flow_run_started_bytes_before_agent_id_freeze_are_pinned() {
    let payload = include_str!("fixtures/pre_agent_id_frozen_flow_run_started.json").trim_end();
    let envelope: serde_json::Value = serde_json::from_str(payload).unwrap();
    let record: WalRecord = serde_json::from_value(envelope["record"].clone()).unwrap();
    let bytes = framing::encode(1, 1, &record).unwrap();
    assert_eq!(bytes[4], 3);
    assert_eq!(&bytes[53..], payload.as_bytes());
    assert_eq!(
        format!("{:x}", Sha256::digest(payload.as_bytes())),
        "aff6e09262c555b3c9d46d2cd69511bf56d501641d16dbaa376eb92fe23746ca"
    );
}

#[test]
fn flow_discriminator_changes_only_scope_policy_not_replay_inputs() {
    let mut envelope: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/pre_agent_id_frozen_flow_run_started.json"
    ))
    .unwrap();
    let old: WalRecord = serde_json::from_value(envelope["record"].clone()).unwrap();
    envelope["record"]["scope"]["agent_id_frozen"] = serde_json::json!(true);
    let future: WalRecord = serde_json::from_value(envelope["record"].clone()).unwrap();
    let WalRecord::RunStarted { scope, .. } = &future else {
        panic!("expected run start");
    };
    assert!(scope.as_ref().unwrap().agent_id_frozen);
    let mut restored = future.clone();
    if let WalRecord::RunStarted { scope, .. } = &mut restored {
        scope.as_mut().unwrap().agent_id_frozen = false;
    }
    // The policy flag changes no replay input or frozen render configuration.
    assert_eq!(restored, old);
}
