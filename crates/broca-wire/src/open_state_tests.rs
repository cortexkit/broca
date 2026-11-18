use super::{
    decode_subscribe_event, AccountSelectionRefusedDetail, DisplayEvent, FlowPrincipalRefusal,
    QueuedSendState, RunPaused, RunPausedDetail, RunStatusResponse, SendResult, SubscribeEvent,
    UnrecognizedRunState,
};
use broca_protocol::{ControlUnit, OpenState, PauseReason, RunFinishReason, RunId};

#[test]
fn unknown_run_status_state_decodes_to_a_nonterminal_fallback() {
    let status: RunStatusResponse =
        serde_json::from_str(r#"{"state":"future_runner_state","extra":"future field"}"#).unwrap();
    assert!(matches!(status, RunStatusResponse::Unknown));
}

#[test]
fn run_status_auth_selection_is_optional_and_round_trips() {
    for state in [
        "active",
        "paused",
        "interrupted",
        "completed",
        "max_steps",
        "cancelled",
        "error",
        "transform_unavailable",
    ] {
        for selection in [None, Some("apikey:cline-pass")] {
            let mut wire = serde_json::json!({"state": state});
            if let Some(selection) = selection {
                wire["auth_selection"] = selection.into();
            }
            let status: RunStatusResponse = serde_json::from_value(wire).unwrap();
            let encoded = serde_json::to_value(&status).unwrap();
            assert_eq!(
                encoded.get("auth_selection").and_then(|v| v.as_str()),
                selection
            );
            assert_eq!(encoded.get("auth_selection").is_some(), selection.is_some());
            assert_eq!(
                serde_json::from_value::<RunStatusResponse>(encoded).unwrap(),
                status
            );
        }
    }
}

#[test]
fn run_result_auth_selection_is_optional_and_round_trips() {
    for selection in [None, Some("apikey:cline-pass")] {
        let mut wire = serde_json::json!({"run_id": "run-1", "state": "error"});
        if let Some(selection) = selection {
            wire["auth_selection"] = selection.into();
        }
        let result: super::RunResultResponse = serde_json::from_value(wire).unwrap();
        assert_eq!(result.auth_selection.as_deref(), selection);
        let encoded = serde_json::to_value(&result).unwrap();
        assert_eq!(encoded.get("auth_selection").is_some(), selection.is_some());
        assert_eq!(
            serde_json::from_value::<super::RunResultResponse>(encoded).unwrap(),
            result
        );
    }
}

#[test]
fn unknown_send_state_decodes_without_closing_the_wire_vocabulary() {
    let result: SendResult = serde_json::from_str(r#"{"state":"future_send_state"}"#).unwrap();
    assert!(matches!(result, SendResult::Unknown));
}

#[test]
fn unknown_queued_send_state_decodes_to_a_non_actionable_fallback() {
    let state: QueuedSendState = serde_json::from_str(r#"{"state":"future_queue_state"}"#).unwrap();
    assert_eq!(state, QueuedSendState::Unknown);
}

#[test]
fn unknown_pause_and_finish_reasons_decode_as_explicit_unknown_states() {
    let status: RunStatusResponse =
        serde_json::from_str(r#"{"state":"paused","pause_reason":"future_pause"}"#).unwrap();
    let RunStatusResponse::Paused { pause_reason, .. } = &status else {
        panic!("expected a paused status, got {status:?}");
    };
    assert_eq!(
        pause_reason,
        &Some(OpenState::Unknown("future_pause".into()))
    );

    let paused: SendResult = serde_json::from_str(
        r#"{"state":"paused","run_id":"run-1","pause_reason":"future_pause"}"#,
    )
    .unwrap();
    assert_eq!(
        paused,
        SendResult::Paused {
            run_id: "run-1".into(),
            pause_reason: OpenState::Unknown("future_pause".into()),
        }
    );

    let finished: SendResult =
        serde_json::from_str(r#"{"state":"finished","run_id":"run-1","reason":"future_finish"}"#)
            .unwrap();
    assert_eq!(
        finished,
        SendResult::Finished {
            run_id: "run-1".into(),
            reason: OpenState::Unknown("future_finish".into()),
        }
    );

    let refusal: RunPaused = serde_json::from_value(serde_json::json!({
        "code": "run_paused",
        "message": "paused",
        "detail": {"kind": "run_paused", "run_id": "run-1", "reason": "future_pause"}
    }))
    .unwrap();
    assert_eq!(
        refusal.detail,
        RunPausedDetail::RunPaused {
            run_id: "run-1".into(),
            reason: OpenState::Unknown("future_pause".into()),
        }
    );
}

#[test]
fn known_pause_and_finish_reasons_keep_their_wire_bytes() {
    let paused = SendResult::Paused {
        run_id: "run-1".into(),
        pause_reason: PauseReason::AuthRequired.into(),
    };
    let bytes = r#"{"state":"paused","run_id":"run-1","pause_reason":"auth_required"}"#;
    assert_eq!(serde_json::to_string(&paused).unwrap(), bytes);
    assert_eq!(serde_json::from_str::<SendResult>(bytes).unwrap(), paused);

    let finished = SendResult::Finished {
        run_id: "run-1".into(),
        reason: RunFinishReason::Completed.into(),
    };
    let bytes = r#"{"state":"finished","run_id":"run-1","reason":"completed"}"#;
    assert_eq!(serde_json::to_string(&finished).unwrap(), bytes);
    assert_eq!(serde_json::from_str::<SendResult>(bytes).unwrap(), finished);

    let refusal = RunPaused::new("run-1".into(), PauseReason::Restart);
    let value = serde_json::to_value(&refusal).unwrap();
    assert_eq!(
        value["detail"],
        serde_json::json!({"kind": "run_paused", "run_id": "run-1", "reason": "restart"})
    );
}

#[test]
fn subscribe_units_with_unknown_run_reasons_become_unrecognized_run_states() {
    let paused: SubscribeEvent = serde_json::from_value(serde_json::json!({
        "kind": "control",
        "cursor": {"wal_seq": 7, "sub_index": 1},
        "unit": {"type": "paused", "run_id": "run-1", "reason": "future_pause"}
    }))
    .unwrap();
    assert_eq!(
        paused,
        SubscribeEvent::UnrecognizedRunState {
            wal_seq: 7,
            sub_index: 1,
            state: UnrecognizedRunState::Paused {
                run_id: RunId("run-1".into()),
                reason: "future_pause".into(),
            },
        }
    );

    let body = br#"{"kind":"control","cursor":{"wal_seq":8,"sub_index":0},"unit":{"type":"run_finished","run_id":"run-1","reason":"future_finish","retries_used":0}}"#;
    let finished = decode_subscribe_event(body).unwrap();
    let SubscribeEvent::UnrecognizedRunState { state, .. } = &finished else {
        panic!("expected an unrecognized run state, got {finished:?}");
    };
    assert_eq!(state.run_id(), Some(&RunId("run-1".into())));
    assert_eq!(state.reason(), "future_finish");
    assert!(matches!(state, UnrecognizedRunState::Finished { .. }));
    assert_eq!(
        serde_json::to_value(&finished).unwrap(),
        serde_json::json!({
            "kind": "control",
            "cursor": {"wal_seq": 8, "sub_index": 0},
            "unit": {"type": "run_finished", "run_id": "run-1", "reason": "future_finish"}
        })
    );

    // A known reason still decodes as the ordinary control unit.
    let known = decode_subscribe_event(
        br#"{"kind":"control","cursor":{"wal_seq":9,"sub_index":0},"unit":{"type":"run_finished","run_id":"run-1","reason":"completed"}}"#,
    )
    .unwrap();
    assert!(matches!(
        known,
        SubscribeEvent::Control {
            unit: ControlUnit::RunFinished {
                reason: RunFinishReason::Completed,
                ..
            },
            ..
        }
    ));

    // A pause without the run id it must carry is refused even with an unknown reason, and
    // a unit with a known reason that fails to decode for another cause is still refused.
    let malformed = decode_subscribe_event(
        br#"{"kind":"control","cursor":{"wal_seq":10,"sub_index":0},"unit":{"type":"paused","reason":"future_pause"}}"#,
    );
    assert!(malformed.is_err(), "{malformed:?}");
    let malformed_known = decode_subscribe_event(
        br#"{"kind":"control","cursor":{"wal_seq":11,"sub_index":0},"unit":{"type":"run_finished","reason":"completed","retries_used":"x"}}"#,
    );
    assert!(malformed_known.is_err(), "{malformed_known:?}");
}

#[test]
fn unknown_lanes_and_display_reset_while_unknown_control_units_are_named() {
    let envelope: SubscribeEvent = serde_json::from_str(r#"{"kind":"future_lane_event"}"#).unwrap();
    assert_eq!(
        envelope,
        SubscribeEvent::Display(DisplayEvent::DisplayReset)
    );

    let display: SubscribeEvent = serde_json::from_value(serde_json::json!({
        "kind": "display",
        "event": {"type": "future_display_event", "payload": "ignored"}
    }))
    .unwrap();
    assert_eq!(display, SubscribeEvent::Display(DisplayEvent::DisplayReset));

    let control: SubscribeEvent = serde_json::from_value(serde_json::json!({
        "kind": "control",
        "cursor": {"wal_seq": 1, "sub_index": 0},
        "unit": {"type": "future_control_unit", "payload": "ignored"}
    }))
    .unwrap();
    assert!(matches!(
        control,
        SubscribeEvent::UnknownControlUnit {
            wal_seq: 1,
            sub_index: 0,
            unit_type,
            unit,
        } if unit_type == "future_control_unit" && unit["payload"] == "ignored"
    ));
}

#[test]
fn refusal_classes_are_open_when_consumers_decode_them() {
    let mut principal =
        serde_json::to_value(FlowPrincipalRefusal::new(true, "reserved:agent")).unwrap();
    principal["class"] = serde_json::json!("future_class");
    let principal: FlowPrincipalRefusal = serde_json::from_value(principal).unwrap();
    assert_eq!(
        serde_json::to_value(&principal).unwrap()["class"],
        "future_class"
    );

    let account: AccountSelectionRefusedDetail = serde_json::from_value(serde_json::json!({
        "routing_code": "account_pool_exhausted",
        "class": "future_class"
    }))
    .unwrap();
    assert_eq!(account.class.as_str(), "future_class");
}

#[test]
fn run_status_error_keeps_an_unrecognized_provider_class() {
    let status: RunStatusResponse = serde_json::from_value(serde_json::json!({
        "state": "error",
        "error": {"class": "future_class", "message": "new provider outcome"}
    }))
    .unwrap();
    let RunStatusResponse::Error {
        error: Some(error), ..
    } = status
    else {
        panic!("expected the error status")
    };
    assert_eq!(error.class_wire(), "future_class");
    assert!(error.has_unknown_class());
}
