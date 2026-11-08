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

#[tokio::test]
async fn head_send_and_send_status_capture_serve_reply_bytes() {
    async fn dispatch(
        handler: &Arc<LineageRegistry>,
        session: &BrocaBindIdentity,
        bind: &BindObservation,
        method: &str,
        params: &serde_json::Value,
    ) -> Frame {
        broca_test_support::wait_for(
            method,
            async {
                let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
                // Dispatch through the real handlers so send conversion, result envelopes
                // and error bodies cannot drift from the served bytes.
                execute_route_request_inner(
                    RouteRequest {
                        method: method.into(),
                        params: params.clone(),
                    },
                    &tx,
                    PROTOCOL_VERSION,
                    7,
                    3,
                    11,
                    session,
                    bind,
                    handler,
                )
                .await
                .unwrap();
                rx.recv().await.unwrap()
            },
            || format!("params: {params}"),
        )
        .await
    }

    fn capture(step: &str, params: &serde_json::Value, frame: &Frame) -> serde_json::Value {
        if let Some(dir) = std::env::var_os("BROCA_HEAD_SEND_SAMPLES") {
            let dir = std::path::Path::new(&dir);
            std::fs::create_dir_all(dir).unwrap();
            std::fs::write(
                dir.join(format!("{step}.request.json")),
                serde_json::to_vec_pretty(params).unwrap(),
            )
            .unwrap();
            // Preserve the frame body, not a parsed and reserialized approximation.
            std::fs::write(dir.join(format!("{step}.reply.json")), &frame.body).unwrap();
        }
        serde_json::from_slice(&frame.body).unwrap()
    }

    async fn status_in_state(
        handler: &Arc<LineageRegistry>,
        session: &BrocaBindIdentity,
        bind: &BindObservation,
        params: &serde_json::Value,
        state: &str,
    ) -> Frame {
        broca_test_support::wait_until(&format!("send status {state}"), || async {
            let frame = dispatch(handler, session, bind, OP_SESSION_SEND_STATUS, params).await;
            assert_eq!(frame.header.ty, FrameType::Response);
            let reply: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
            if reply["result"]["state"] == state {
                Ok(frame)
            } else {
                Err(reply.to_string())
            }
        })
        .await
    }

    let (exec, gate) = FakeExecutor::gated();
    let rig = Rig::new("head-send-samples", exec.clone(), Duration::from_secs(60));
    let session = rig.key("alfonso:head:agent_sample");
    let handler = Arc::new(rig.registry.clone());
    let head = broca_protocol::BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    };
    let bind = BindObservation {
        principal: Some(head.clone()),
        scope: broca_protocol::SendScope::Unstamped,
        ..Default::default()
    };
    let send_a = serde_json::json!({
        "model": {"provider": "fake", "model": "fake"},
        "prompt": "Synthetic head inbox message A",
        "delivery": "queue",
        "send_id": "inbox-send_sample_a"
    });
    let status_a = serde_json::json!({"send_id": "inbox-send_sample_a"});
    let send_b = serde_json::json!({
        "model": {"provider": "fake", "model": "fake"},
        "prompt": "Synthetic head inbox message B",
        "delivery": "queue",
        "send_id": "inbox-send_sample_b"
    });
    let status_b = serde_json::json!({"send_id": "inbox-send_sample_b"});

    let frame = dispatch(&handler, &session, &bind, OP_SEND, &send_a).await;
    assert_eq!(frame.header.ty, FrameType::Response);
    let accepted_a = capture("01-send-a", &send_a, &frame);
    assert_eq!(accepted_a["result"]["state"], "active", "{accepted_a}");
    let run_a = accepted_a["result"]["run_id"].as_str().unwrap();
    assert!(!run_a.is_empty());

    let frame = dispatch(&handler, &session, &bind, OP_SESSION_SEND_STATUS, &status_a).await;
    assert_eq!(frame.header.ty, FrameType::Response);
    let active_a = capture("02-status-a-active", &status_a, &frame);
    assert_eq!(active_a["result"]["state"], "active", "{active_a}");
    assert_eq!(active_a["result"]["run_id"], run_a);

    let frame = dispatch(&handler, &session, &bind, OP_SEND, &send_b).await;
    assert_eq!(frame.header.ty, FrameType::Response);
    let accepted_b = capture("03-send-b", &send_b, &frame);
    assert_eq!(accepted_b["result"]["state"], "pending", "{accepted_b}");
    let submission_b = accepted_b["result"]["submission_id"].as_str().unwrap();
    assert!(!submission_b.is_empty());

    let frame = dispatch(&handler, &session, &bind, OP_SESSION_SEND_STATUS, &status_b).await;
    assert_eq!(frame.header.ty, FrameType::Response);
    let pending_b = capture("04-status-b-pending", &status_b, &frame);
    assert_eq!(pending_b["result"]["state"], "pending", "{pending_b}");
    assert_eq!(pending_b["result"]["submission_id"], submission_b);
    assert_eq!(exec.started_count(), 1);
    assert_eq!(exec.finished_count(), 0);

    gate.release().await;
    let frame = status_in_state(&handler, &session, &bind, &status_a, "finished").await;
    let finished_a = capture("05-status-a-completed", &status_a, &frame);
    assert_eq!(finished_a["result"]["state"], "finished", "{finished_a}");
    assert_eq!(finished_a["result"]["reason"], "completed", "{finished_a}");
    assert_eq!(finished_a["result"]["run_id"], run_a);

    let frame = status_in_state(&handler, &session, &bind, &status_b, "active").await;
    let active_b = capture("06-status-b-active", &status_b, &frame);
    assert_eq!(active_b["result"]["state"], "active", "{active_b}");
    let run_b = active_b["result"]["run_id"].as_str().unwrap();
    assert!(!run_b.is_empty());
    assert_ne!(run_b, run_a);
    assert_eq!(exec.started_count(), 2);
    assert_eq!(exec.finished_count(), 1);
    assert!(exec
        .seen
        .lock()
        .unwrap()
        .iter()
        .all(|run| run.principal.as_ref() == Some(&head)));

    gate.release().await;
    let frame = status_in_state(&handler, &session, &bind, &status_b, "finished").await;
    let finished_b = capture("07-status-b-finished", &status_b, &frame);
    assert_eq!(finished_b["result"]["state"], "finished", "{finished_b}");
    assert_eq!(finished_b["result"]["reason"], "completed", "{finished_b}");
    assert_eq!(finished_b["result"]["run_id"], run_b);

    let other_bind = BindObservation {
        principal: Some(broca_protocol::BindPrincipal::Reserved {
            module_id: "occipital".into(),
        }),
        ..bind
    };
    let frame = dispatch(
        &handler,
        &session,
        &other_bind,
        OP_SESSION_SEND_STATUS,
        &status_a,
    )
    .await;
    assert_eq!(frame.header.ty, FrameType::Error);
    let unknown = capture("08-status-a-occipital", &status_a, &frame);
    assert_eq!(unknown["code"], "unknown_send", "{unknown}");
    assert!(rig.registry.stop_and_seal().await);
}
