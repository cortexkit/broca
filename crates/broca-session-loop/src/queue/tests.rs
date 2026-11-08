use broca_protocol::request::SubmissionRequest;
use broca_protocol::{BindPrincipal, SubmissionId};
use broca_session_store::record::Record;
use broca_wal::{FramedRecord, WalRecord};

use super::{queued_send, request_of, steer, Queued, Receipt, SteerReceipt};
use crate::parts::MemoryParts;

fn request() -> SubmissionRequest {
    serde_json::from_value(serde_json::json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [{"role": "user", "content": [{"type": "text", "text": "queued"}]}],
        "tools": [{"name": "read", "description": "reads a file", "input_schema": {"type": "object"}}],
        "policy_allowed": ["read"],
        "stop_when": [{"type": "step_count", "n": 3}],
        "route_decision_id": "decision-1",
    }))
    .unwrap()
}

fn receipt() -> Receipt {
    Receipt {
        sent_by: Some(BindPrincipal::Reserved {
            module_id: "prefrontal-core".into(),
        }),
        request_identity_digest: Some("digest".into()),
        principal: Some(BindPrincipal::Direct),
        title: Some("a title".into()),
        ..Receipt::default()
    }
}

/// The record the store actor writes for a queued send is the record the converter makes of
/// today's `Queued` record for the same send, tool part included.
#[test]
fn a_queued_send_is_the_record_the_converter_makes_of_todays() {
    let mut parts = MemoryParts::default();
    let at = Queued {
        written_at_ms: Some(1_000),
        ts_ms: Some(2_000),
        delivery: Some(broca_protocol::Delivery::Interrupt),
    };
    let built = queued_send("sub-r1", Some("k1"), &request(), &receipt(), at, &mut parts).unwrap();
    let receipt = receipt();
    let today = WalRecord::Queued {
        sent_by: receipt.sent_by,
        written_at_ms: at.written_at_ms,
        retention: None,
        ts_ms: at.ts_ms,
        keep_warm: None,
        conversation_key: None,
        submission_id: SubmissionId("sub-r1".into()),
        request: Box::new(request()),
        request_identity_digest: receipt.request_identity_digest,
        principal: receipt.principal,
        send_id: Some("k1".into()),
        title: receipt.title,
        delivery: at.delivery,
        hooks: None,
        message_marks: None,
    };
    let converted = broca_session_convert::convert(
        &[FramedRecord {
            seq: 1,
            fence: 0,
            record: today,
        }],
        None,
    )
    .unwrap();
    let converted_send = converted
        .records
        .iter()
        .find_map(|record| match record {
            Record::QueuedSend(send) => Some(send.clone()),
            _ => None,
        })
        .expect("the converter makes a queued send");
    assert_eq!(built, converted_send);
    assert!(converted.lost.is_empty(), "{:?}", converted.lost);
    assert_eq!(
        parts.parts.keys().collect::<Vec<_>>(),
        converted.parts.keys().collect::<Vec<_>>(),
        "both store the same tool part"
    );
}

/// A queued send's request comes back as it was sent.
#[test]
fn a_queued_send_rebuilds_its_request() {
    let mut parts = MemoryParts::default();
    let send = queued_send(
        "sub-r1",
        None,
        &request(),
        &Receipt::default(),
        Queued::default(),
        &mut parts,
    )
    .unwrap();
    assert_eq!(send.receipt, None, "an empty receipt is left out");
    assert_eq!(send.delivery, None, "a plain queued send names no mode");
    assert_eq!(
        serde_json::to_value(request_of(&send, &parts).unwrap()).unwrap(),
        serde_json::to_value(request()).unwrap()
    );
}

/// The record the store actor writes for a steer is the record the converter makes of today's
/// `Steered` record for the same steer.
#[test]
fn a_steer_is_the_record_the_converter_makes_of_todays() {
    let input = request().input;
    let receipt = SteerReceipt {
        sent_by: Some(BindPrincipal::Reserved {
            module_id: "prefrontal-core".into(),
        }),
        send_id: Some("k1".into()),
        request_identity_digest: Some("digest".into()),
        title: Some("a title".into()),
        delivery: Some(broca_protocol::Delivery::Steer),
        ..SteerReceipt::default()
    };
    let built = steer(input.clone(), Some(1_000), &receipt).unwrap();
    let today = WalRecord::Steered {
        sent_by: receipt.sent_by.clone(),
        written_at_ms: Some(1_000),
        tags: None,
        retention: None,
        ts_ms: None,
        send_id: receipt.send_id.clone(),
        request_identity_digest: receipt.request_identity_digest.clone(),
        title: receipt.title.clone(),
        delivery: receipt.delivery,
        input: std::sync::Arc::new(input),
        hooks: None,
        message_marks: None,
    };
    let converted = broca_session_convert::convert(
        &[FramedRecord {
            seq: 1,
            fence: 0,
            record: today,
        }],
        None,
    )
    .unwrap();
    let converted_steer = converted
        .records
        .iter()
        .find_map(|record| match record {
            Record::Steer(steer) => Some(steer.clone()),
            _ => None,
        })
        .expect("the converter makes a steer");
    assert_eq!(built, converted_steer);
    assert!(converted.lost.is_empty(), "{:?}", converted.lost);
}
