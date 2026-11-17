use broca_protocol::request::SubmissionRequest;
use broca_protocol::BindPrincipal;

use super::{queued_send, request_of, Queued, Receipt};
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

/// Display marks as a send's admission keeps them: the agent its module speaks for, and a
/// notice.
fn marks() -> broca_protocol::MessageMarks {
    let mut marks = broca_protocol::MessageMarks::default();
    marks.speaks_for = Some(broca_protocol::SpeaksFor::new("agent-1"));
    marks.notice = true;
    marks
}

/// [`marks`], and the same marks with the speaker's display name, each with whether the record
/// must set its must-understand `named_speaker` field.
fn both_marks() -> [(broca_protocol::MessageMarks, Option<bool>); 2] {
    let mut named = marks();
    named.speaks_for = Some(broca_protocol::SpeaksFor::new("agent-1").with_name("Ada"));
    [(marks(), None), (named, Some(true))]
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
        None,
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

/// A queued send keeps its receipt and its display marks, and sets the must-understand
/// `named_speaker` exactly when its marks name the speaker.
#[test]
fn a_queued_send_keeps_its_receipt_and_marks() {
    for (marks, named_speaker) in both_marks() {
        let mut parts = MemoryParts::default();
        let send = queued_send(
            "sub-r1",
            Some("k1"),
            &request(),
            &receipt(),
            Queued::default(),
            Some(&marks),
            &mut parts,
        )
        .unwrap();
        let kept = send.receipt.as_ref().expect("a receipt is kept").parse();
        assert_eq!(kept["title"], "a title", "{kept}");
        assert_eq!(kept["request_identity_digest"], "digest", "{kept}");
        assert_eq!(
            send.message_marks,
            Some(serde_json::to_value(&marks).unwrap()),
            "the marks are kept as the send's admission gave them"
        );
        assert_eq!(send.named_speaker, named_speaker);
    }
}
