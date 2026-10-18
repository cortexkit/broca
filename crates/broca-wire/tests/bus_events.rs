use broca_protocol::{ErrorClass, PauseReason, RunFinishReason};
use broca_wire::run_events::{EncodeError, RunEvent, RunEventDetail, StructuredOutputStatus};
use sha2::{Digest, Sha256};

fn event(detail: RunEventDetail) -> RunEvent {
    RunEvent {
        v: 1,
        session_ref: "0123456789abcdef0123456789abcdef".into(),
        run_id: "run-golden".into(),
        seq: 42,
        at_ms: 1700000000000,
        detail,
    }
}

#[test]
fn bus_event_golden_jcs() {
    let account = cortexkit_bus_naming::AccountNames::derive("box_goldenfixture").unwrap();
    let vectors = [
        ("started", RunEventDetail::Started {}),
        (
            "paused",
            RunEventDetail::Paused {
                pause_reason: PauseReason::AuthRequired,
            },
        ),
        (
            "ended.completed",
            RunEventDetail::Ended {
                terminal: RunFinishReason::Completed,
                error_class: None,
                structured_output: Some(StructuredOutputStatus::Passed),
            },
        ),
        (
            "ended.interrupted",
            RunEventDetail::Ended {
                terminal: RunFinishReason::Interrupted,
                error_class: None,
                structured_output: Some(StructuredOutputStatus::Absent),
            },
        ),
        (
            "steer_consumed",
            RunEventDetail::SteerConsumed {
                step: 3,
                consumed: 2,
                send_ids: vec!["send-a".into(), "send-b".into()],
                truncated: false,
            },
        ),
        (
            "progress",
            RunEventDetail::Progress {
                step: 3,
                last_content_at_ms: Some(1699999999000),
                max_content_gap_ms: Some(800),
            },
        ),
    ];
    for (name, detail) in vectors {
        let event = event(detail);
        let bytes = event.jcs().unwrap();
        let file = format!(
            "{}/tests/golden/bus_event.{name}.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let golden = std::fs::read(file).unwrap();
        assert_eq!(bytes, golden, "{name}");
        assert_eq!(
            event.subject(&account).unwrap(),
            format!(
                "ck.box_goldenfixture.event.broca.{}.v1",
                event.detail.name()
            )
        );
        println!(
            "bus_event.{name}.json {} {:x}",
            bytes.len(),
            Sha256::digest(&bytes)
        );
    }
}

#[test]
fn bus_event_refuses_512_bytes_and_truncates_only_send_ids() {
    let mut value = event(RunEventDetail::Started {});
    let base = value.jcs().unwrap().len();
    value.run_id.push_str(&"x".repeat(511 - base));
    assert_eq!(value.jcs().unwrap().len(), 511);
    value.run_id.push('x');
    assert!(matches!(value.jcs(), Err(EncodeError::Oversize(512))));

    let mut steers = event(RunEventDetail::SteerConsumed {
        step: 3,
        consumed: 3,
        send_ids: vec!["short".into(), "x".repeat(1000), "last".into()],
        truncated: false,
    });
    let bytes = steers.fit_send_ids().unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["send_ids"], serde_json::json!(["short"]));
    assert_eq!(json["consumed"], 3);
    assert_eq!(json["truncated"], true);
    steers.run_id = "x".repeat(512);
    assert!(matches!(
        steers.fit_send_ids(),
        Err(EncodeError::Oversize(_))
    ));
}

#[test]
fn bus_event_error_class_uses_only_shared_wire_vocabulary() {
    let bytes = event(RunEventDetail::Ended {
        terminal: RunFinishReason::Error,
        error_class: Some(ErrorClass::ProviderUnavailable),
        structured_output: None,
    })
    .jcs()
    .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["error_class"], "transient");
    assert!(json.get("structured_output").is_none());
}

#[test]
fn bus_event_get_reply_golden_preserves_exact_jcs_string() {
    let body = include_str!("golden/bus_event.started.json");
    let reply = broca_wire::RunEventGetResponse::Found {
        event_key: "a".repeat(64),
        jcs: body.into(),
    };
    let golden = include_bytes!("golden/bus_event.get.reply.json");
    assert_eq!(serde_jcs::to_vec(&reply).unwrap(), golden);
    let parsed: broca_wire::RunEventGetResponse = serde_json::from_slice(golden).unwrap();
    assert_eq!(parsed, reply);
}
