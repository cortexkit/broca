use super::*;
use broca_protocol::{ControlUnit, RunId};
use cortexkit_role_llm_runner::{awaits::RunAwaits, live::LiveEvent, run::RunStatus};
use serde_json::{json, Value};

#[test]
fn run_awaits_capability_is_withheld_until_the_storage_cutover() {
    for retention in [false, true] {
        for hooks in [false, true] {
            let body = serde_json::to_string(&crate::role::broca_role_describe_with_writers(
                env!("CARGO_PKG_VERSION"),
                true,
                retention,
                hooks,
            ))
            .unwrap();
            assert!(body.contains("\"run_status\""));
            assert!(!body.contains("\"run_awaits\""));
        }
    }
}

#[test]
fn run_awaits_contract_cases_decode_through_broca_bodies_and_frames() {
    let vectors: Value =
        serde_json::from_str(include_str!("../tests/golden/run-awaits.contract.json")).unwrap();
    let cases = vectors["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 10);
    for (index, case) in cases.iter().enumerate() {
        let name = case["name"].as_str().unwrap();
        let status: RunStatusResponse = serde_json::from_value(case["status"].clone()).unwrap();
        let status = serde_json::to_value(status).unwrap();
        assert_eq!(status["awaits"], case["status"]["awaits"], "{name}");
        let head: LastRunState = serde_json::from_value(case["head"].clone()).unwrap();
        assert_eq!(
            serde_json::to_value(head).unwrap()["awaits"],
            status["awaits"],
            "{name}"
        );
        if let Some(result) = case.get("result") {
            let result: RunResultResponse = serde_json::from_value(result.clone()).unwrap();
            assert_eq!(
                serde_json::to_value(result).unwrap()["awaits"],
                status["awaits"],
                "{name}"
            );
        }
        let shared: RunStatus = serde_json::from_value(case["status"].clone()).unwrap();
        let unit = ControlUnit::Status {
            run_id: RunId(format!("r{index}")),
            status: Box::new(shared),
        };
        let bytes = encode_live_subscribe_control(
            7,
            index as u32,
            &unit,
            index as u64,
            Some(&LiveEvent::DisplayReset),
        )
        .unwrap();
        let frame: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(frame["awaits"], status["awaits"], "{name}");
        let decoded = decode_subscribe_event(&bytes).unwrap();
        assert_eq!(
            decoded,
            SubscribeEvent::Control {
                cursor: CursorWire::new(7, index as u32),
                unit
            }
        );
        if name == "unknown_party_action" {
            sample("subscribe.unknown-party-action.json", &bytes);
        }
        if let Some(paused) = case.get("run_paused") {
            let unit: ControlUnit = serde_json::from_value({
                let mut value = paused.clone();
                value["type"] = json!("paused");
                value
            })
            .unwrap();
            let bytes =
                encode_live_subscribe_control(7, 0, &unit, 1, Some(&LiveEvent::DisplayReset))
                    .unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&bytes).unwrap()["unit"]["awaits"],
                status["awaits"]
            );
            assert!(decode_subscribe_event(&bytes).is_ok());
        }
    }
    for case in vectors["malformed"].as_array().unwrap() {
        let wait = case["awaits"].clone();
        assert!(serde_json::from_value::<RunAwaits>(wait.clone()).is_err());
        assert!(serde_json::from_value::<RunStatusResponse>(
            json!({"state":"active","awaits":wait})
        )
        .is_err());
    }
}

fn sample(name: &str, bytes: &[u8]) {
    if let Some(directory) = std::env::var_os("BROCA_RUN_AWAITS_SAMPLE_DIR") {
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(std::path::PathBuf::from(directory).join(name), bytes).unwrap();
    }
}

#[test]
fn run_awaits_display_frame_keeps_open_party_and_action() {
    let awaits: RunAwaits = serde_json::from_value(
        json!({"party":"future_party","since_ms":42,"person":{"action":"future_action"}}),
    )
    .unwrap();
    let event = DisplayEvent::RunStatus {
        run_id: RunId("run".into()),
        state: "active".into(),
        reason: None,
        deadline_ms: None,
        awaits: Some(Box::new(awaits)),
    };
    let bytes = encode_live_subscribe_display(&event, 4).unwrap();
    assert_eq!(
        decode_subscribe_event(&bytes).unwrap(),
        SubscribeEvent::Display(event)
    );
    sample("subscribe.display.unknown-party-action.json", &bytes);
}

#[test]
fn run_awaits_legacy_status_bytes_are_unchanged() {
    let legacy = br#"{"state":"paused","retries_used":0}"#;
    let status: RunStatusResponse = serde_json::from_slice(legacy).unwrap();
    assert_eq!(serde_json::to_vec(&status).unwrap(), legacy);
    let legacy = br#"{"run_id":"r1","state":"paused","reason":"auth_required"}"#;
    let head: LastRunState = serde_json::from_slice(legacy).unwrap();
    assert_eq!(serde_json::to_vec(&head).unwrap(), legacy);
}
