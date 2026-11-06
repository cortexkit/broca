use super::*;
use broca_module::executor::RunKind;
use broca_module::serve::ServeHandler;
use broca_protocol::{DeliveryMode, DeliveryOrder};

#[path = "../../broca-module/tests/it/common/mod.rs"]
mod actor_rig;
use actor_rig::{req, FakeExecutor, Rig};
use std::time::Duration;

async fn status_frame<T: serde::Serialize>(value: &T) -> Frame {
    let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
    send_json_response(&tx, PROTOCOL_VERSION, 7, 3, 11, value)
        .await
        .unwrap();
    rx.recv().await.unwrap()
}

#[tokio::test]
async fn idle_start_routing_refusal_is_failed_on_both_status_surfaces_without_another_send() {
    let refusal = crate::account_routing::tests::unavailable_admission_error().await;
    let exec = FakeExecutor::refusing_admission(refusal);
    let rig = Rig::new(
        "idle-routing-refusal",
        exec.clone(),
        Duration::from_secs(60),
    );
    let key = rig.key("s1");
    let run_id = run_id_for_send(&key, Some("refused"));
    let request = req(&key, &run_id.0);
    let response = rig
        .registry
        .send(broca_module::serve::SendRequest {
            session_class: None,
            retention: None,
            stop_sequences_named: false,
            title: None,
            conversation_key: None,
            delivery: Default::default(),
            session: key.clone(),
            run_id: run_id.clone(),
            send_id: Some("refused".into()),
            request: Box::new(request.to_submission()),
            skip_materialization: false,
            kind: RunKind::FreshOrResume,
            plan: None,
            order: Some(DeliveryOrder {
                modes: vec![DeliveryMode::IdleStart],
                dedupe_key: None,
                expires_at_ms: None,
                template: None,
            }),
        })
        .await
        .unwrap();
    assert!(matches!(
        response,
        broca_module::serve::SendResponse::Pending { .. }
    ));

    // Status reads must resolve the refusal without another send waking the queue.
    let status = rig
        .registry
        .run_status_reply(&key, run_id.clone())
        .await
        .unwrap();
    let send_status = rig
        .registry
        .send_status(&key, run_id.clone(), "refused", None)
        .await
        .unwrap();
    let mut replies = Vec::new();
    for (op, frame) in [
        ("run.status", status_frame(&status).await),
        ("send.status", status_frame(&send_status).await),
    ] {
        assert_eq!(frame.header.ty, FrameType::Response);
        if let Some(dir) = std::env::var_os("BROCA_QUEUED_REFUSAL_SAMPLES") {
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(
                std::path::Path::new(&dir).join(format!("{op}.json")),
                &frame.body,
            )
            .unwrap();
        }
        replies.push(serde_json::from_slice::<serde_json::Value>(&frame.body).unwrap());
    }
    assert_eq!(exec.started_count(), 1, "broca must not retry admission");
    assert!(rig.registry.stop_and_seal().await);
    let closed = rig.registry.run_status_reply(&key, run_id).await.unwrap();
    assert_eq!(
        serde_json::to_value(&closed).unwrap(),
        serde_json::to_value(&status).unwrap()
    );
    assert_eq!(replies[0]["result"]["state"], "error", "{}", replies[0]);
    assert_eq!(
        replies[0]["result"]["error"]["class"], "transient",
        "{}",
        replies[0]
    );
    assert_eq!(
        replies[0]["result"]["error"]["provider_code"], "routing_unavailable",
        "{}",
        replies[0]
    );
    assert_eq!(replies[1]["result"]["state"], "finished", "{}", replies[1]);
    assert_eq!(replies[1]["result"]["reason"], "error", "{}", replies[1]);
}

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
