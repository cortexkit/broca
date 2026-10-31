use super::*;
use crate::record::{AttemptReservation, AuthSelection, RunConfig};
use serde_json::Value;

const START: &str = r#"{"type":"run_started","run_id":"r","session":{"project_root":"/project","harness":"runner","session":"s"},"config":{"model":{"provider_module_id":"anthropic","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"oauth:anthropic","resolved_credential_id":"vault-work"}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
const RESERVATION: &str = r#"{"run_id":"r","step_id":1,"attempt":0,"send_index":1,"warm":false,"reserved_at_ms":100,"provider":"anthropic","endpoint_host":"example.com","model_sent":"m","credential":{"credential_ref":"vault-work","auth_selection":{"credential_id":"oauth:anthropic","resolved_credential_id":"vault-work"}},"session":{"project_root":"/project","harness":"runner","session":"s","session_ref":"ref"},"run":{},"request_body_bytes":123,"compressed":false}"#;

#[test]
fn future_vault_snapshots_roundtrip_and_refuse_pre_snapshot_readers() {
    let snapshots: Vec<Value> = serde_json::from_str(include_str!(
        "../../tests/fixtures/vault_billing_snapshots.json"
    ))
    .unwrap();
    let older: Vec<_> = crate::framing::KNOWN_FEATURES
        .iter()
        .copied()
        .filter(|f| *f != crate::framing::BILL_VAULT_FACTS_FEATURE)
        .collect();
    let mut samples = Vec::new();
    for snapshot in snapshots {
        let mut start: Value = serde_json::from_str(START).unwrap();
        start["config"]["auth_selection"]["billing_vault_facts"] = snapshot.clone();
        let mut reservation: Value = serde_json::from_str(RESERVATION).unwrap();
        reservation["credential"]["auth_selection"]["billing_vault_facts"] = snapshot.clone();
        if snapshot["state"] == "observed" {
            for field in ["provider_ids", "plan_tier", "plan_tier_source"] {
                reservation["credential"][field] = snapshot[field].clone();
            }
        }
        let refrozen = serde_json::json!({"type":"run_auth_refrozen", "auth_selection":start["config"]["auth_selection"]});
        let reserved = serde_json::json!({"type":"attempt_reserved", "attempt_id":"00112233445566778899aabbccddeeff", "reservation":reservation});
        for value in [start.clone(), refrozen, reserved] {
            let record: WalRecord = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(serde_json::to_value(&record).unwrap(), value);
            assert!(record
                .required_features()
                .contains(&crate::framing::BILL_VAULT_FACTS_FEATURE));
            for frame in [
                crate::framing::encode(1, 1, &record).unwrap(),
                crate::framing::encode_v4(
                    broca_wal_reader::frame::Metadata {
                        seq: 1,
                        fence: 1,
                        appended_ms: 100,
                        writer_id: [1; 16],
                    },
                    &[],
                    &record,
                )
                .unwrap(),
            ] {
                assert!(
                    matches!(crate::framing::decode_next(&frame), crate::framing::DecodeStep::Record { framed, .. } if framed.record == record)
                );
                assert!(
                    matches!(crate::framing::decode_next_with(&frame, &older), crate::framing::DecodeStep::NewerWriter { missing, .. } if missing == [crate::framing::BILL_VAULT_FACTS_FEATURE])
                );
            }
        }
        // Export actual serializer output so downstream decoders can check compatibility.
        let start_record: WalRecord = serde_json::from_value(start).unwrap();
        let attempt_reservation: AttemptReservation = serde_json::from_value(reservation).unwrap();
        samples.push(serde_json::json!({
            "state":snapshot["state"],
            "start_record":&start_record,
            "attempt_reservation":&attempt_reservation,
            "start_record_json":serde_json::to_string(&start_record).unwrap(),
            "attempt_reservation_json":serde_json::to_string(&attempt_reservation).unwrap()
        }));
    }
    let old: WalRecord = serde_json::from_str(START).unwrap();
    assert_eq!(serde_json::to_string(&old).unwrap(), START);
    assert!(old.required_features().is_empty());
    let WalRecord::RunStarted { config, .. } = &old else {
        unreachable!()
    };
    let selection = config.auth_selection.as_ref().unwrap();
    assert!(selection.billing_vault_facts.is_none());
    let snapshot_field_bytes = std::mem::size_of_val(&selection.billing_vault_facts);
    assert_eq!(snapshot_field_bytes, std::mem::size_of::<usize>());
    let layout = serde_json::json!({
        "architecture":std::env::consts::ARCH,
        "AuthSelection":std::mem::size_of::<AuthSelection>(),
        "RunConfig":std::mem::size_of::<RunConfig>(),
        "WalRecord":std::mem::size_of::<WalRecord>(),
        "FramedRecord":std::mem::size_of::<crate::FramedRecord>(),
        "snapshot_field_bytes":snapshot_field_bytes
    });
    let old_reservation: AttemptReservation = serde_json::from_str(RESERVATION).unwrap();
    samples.push(serde_json::json!({
        "state":"not_captured", "start_record":&old, "attempt_reservation":&old_reservation,
        "start_record_json":serde_json::to_string(&old).unwrap(),
        "attempt_reservation_json":serde_json::to_string(&old_reservation).unwrap()
    }));
    if let Some(path) = std::env::var_os("BROCA_VAULT_FACTS_SAMPLES") {
        let path = std::path::Path::new(&path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, serde_json::to_vec_pretty(&samples).unwrap()).unwrap();
        std::fs::write(
            path.with_file_name("reader-type-layout.json"),
            serde_json::to_vec_pretty(&layout).unwrap(),
        )
        .unwrap();
    }
}
