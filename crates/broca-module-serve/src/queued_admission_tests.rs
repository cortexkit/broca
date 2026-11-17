use super::*;

#[tokio::test]
async fn routing_admission_refusal_keeps_direct_send_reply_bytes() {
    let account = broca_core::AccountSelectionRefusal {
        routing_code: "account_hold".into(),
        retry_after_s: Some(17),
        resets_at_ms: Some(123_456),
        routing_detail: Some(Box::new(serde_json::json!({"future": ["diagnostic"]}))),
        decision_id: Some("decision".into()),
        class: broca_protocol::ErrorClass::Transient,
    };
    let expected_account = broca_wire::AccountSelectionRefused::new(
        "account_hold".into(),
        broca_protocol::ErrorClass::Transient,
        Some(17),
        Some("decision".into()),
    )
    .with_routing_detail(account.routing_detail.clone(), Some(123_456));
    for (error, expected) in [
        (
            ServeError::AccountSelectionRefused(account),
            serde_json::to_vec(&expected_account).unwrap(),
        ),
        (
            ServeError::RoutingUnavailable(broca_core::RoutingUnavailable::unreachable()),
            serde_json::to_vec(&broca_wire::RoutingUnavailable::new("unreachable", None)).unwrap(),
        ),
        (
            ServeError::RoutingUnavailable(
                broca_core::RoutingUnavailable::no_decision_within_budget(1500),
            ),
            serde_json::to_vec(&broca_wire::RoutingUnavailable::new(
                "no_decision_within_budget",
                Some(1500),
            ))
            .unwrap(),
        ),
    ] {
        let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
        send_serve_error(&tx, PROTOCOL_VERSION, 7, 3, 11, &error)
            .await
            .unwrap();
        let frame = rx.recv().await.unwrap();
        assert_eq!(frame.header.ty, FrameType::Error);
        assert_eq!(frame.body.as_slice(), expected);
    }
}
