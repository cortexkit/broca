//! ServeHandler dispatch conformance — the transport-agnostic serve surface mapping.
//!
//! Drives the `session.*`/`run.*` ops through the ServeHandler (implemented by the
//! registry) with a fake executor + real file stores — NO subc daemon. Proves each op maps
//! the actor outcome to the wire response type per docs/module-surface.md.

use crate::common;

use std::time::Duration;

use broca_core::RunIndex;
use broca_module::executor::RunKind;
use broca_module::serve::{
    CancelResponse, RetractResponse, SendRequest, SendResponse, SessionImportRequest,
    SessionReadRequest, StatusResponse, SubscribeFrom,
};
use broca_module::{Delivery, ServeHandler};
use common::{req, FakeExecutor, Rig};

const LONG_TTL: Duration = Duration::from_secs(3600);

fn send_req(key: &broca_protocol::BindIdentity, run: &str) -> SendRequest {
    let req = req(key, run);
    SendRequest {
        session_class: None,
        retention: None,
        stop_sequences_named: false,
        title: None,
        conversation_key: None,
        delivery: Default::default(),
        session: key.clone(),
        run_id: req.run_id.clone(),
        send_id: Some(format!("send-{run}")),
        request: Box::new(req.to_submission()),
        skip_materialization: false,
        kind: RunKind::FreshOrResume,
        plan: None,
        order: None,
    }
}

#[tokio::test]
async fn import_then_read_returns_the_canonical_transcript() {
    let rig = Rig::new("serve-import", FakeExecutor::auto(), LONG_TTL);
    let key = rig.key("s-import");
    let messages = vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: "imported".into(),
        }],
    )];
    let response = rig
        .registry
        .import(SessionImportRequest {
            project_id: None,
            principal: None,
            session: key.clone(),
            run_id: broca_protocol::RunId("run-import".into()),
            messages: messages.clone(),
            source_ref: "cc:source-1".into(),
        })
        .await
        .expect("import succeeds");
    assert_eq!(response.synthesized_tool_results, 0);
    assert!(!response.already_imported);

    let page = rig
        .registry
        .read(SessionReadRequest {
            late_after: None,
            session: key,
            from_ordinal: Some(0),
            limit: Some(500),
            include_tools: false,
            after_mid: None,
            lineage_id: None,
            max_bytes: None,
        })
        .await
        .expect("read import");
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].message.content, messages[0].content);
    assert_eq!(page.lineage_state.state, "completed");
}

/// session.send on an idle lineage → Active{run_id}; a second send while active → Pending.
#[tokio::test]
async fn send_maps_active_then_pending() {
    let (exec, _gate) = FakeExecutor::gated();
    let rig = Rig::new("serve-send", exec.clone(), LONG_TTL);
    let key = rig.key("s1");

    let r1 = rig.registry.send(send_req(&key, "r1")).await.unwrap();
    assert!(matches!(r1, SendResponse::Active { .. }));

    let r2 = rig.registry.send(send_req(&key, "r2")).await.unwrap();
    assert!(matches!(r2, SendResponse::Pending { .. }));
}

/// Server-minted nonce ids skip deterministic-send identity storage. Their uniqueness makes a
/// historical lookup unnecessary.
#[tokio::test]
async fn nonce_send_does_not_compute_or_store_request_identity() {
    let (exec, _gate) = FakeExecutor::gated();
    let rig = Rig::new("serve-nonce-send", exec, LONG_TTL);
    let key = rig.key("s1");
    let mut request = send_req(&key, "nonce-r1");
    request.send_id = None;

    assert!(matches!(
        rig.registry.send(request).await.unwrap(),
        SendResponse::Active { .. }
    ));
    assert_eq!(
        rig.run_index
            .lookup_run("nonce-r1")
            .unwrap()
            .unwrap()
            .request_identity_digest,
        None
    );
}

/// run.status reflects Idle → Active.
#[tokio::test]
async fn status_maps_idle_then_active() {
    let (exec, gate) = FakeExecutor::gated();
    let rig = Rig::new("serve-status", exec.clone(), LONG_TTL);
    let key = rig.key("s1");

    assert_eq!(
        rig.registry.status(&key).await.unwrap(),
        StatusResponse::Idle
    );
    rig.registry.send(send_req(&key, "r1")).await.unwrap();
    assert!(matches!(
        rig.registry.status(&key).await.unwrap(),
        StatusResponse::Active { .. }
    ));
    gate.release().await;
}

/// run.cancel on the active run → Ack; on a non-active id → NotActive.
#[tokio::test]
async fn cancel_maps_ack_then_not_active() {
    let (exec, _gate) = FakeExecutor::gated();
    let rig = Rig::new("serve-cancel", exec.clone(), LONG_TTL);
    let key = rig.key("s1");

    rig.registry.send(send_req(&key, "r1")).await.unwrap();
    assert_eq!(
        rig.registry
            .cancel(&key, broca_protocol::RunId("r1".into()))
            .await
            .unwrap(),
        CancelResponse::Ack
    );
    assert_eq!(
        rig.registry
            .cancel(&key, broca_protocol::RunId("nope".into()))
            .await
            .unwrap(),
        CancelResponse::NotActive
    );
}

/// session.retract on a pending submission → Retracted; on an unknown id → NotPending.
#[tokio::test]
async fn retract_maps_retracted_then_not_pending() {
    let (exec, _gate) = FakeExecutor::gated();
    let rig = Rig::new("serve-retract", exec.clone(), LONG_TTL);
    let key = rig.key("s1");

    // r1 active, r2 queued.
    rig.registry.send(send_req(&key, "r1")).await.unwrap();
    let r2 = rig.registry.send(send_req(&key, "r2")).await.unwrap();
    let pending_sub = match r2 {
        SendResponse::Pending { submission_id } => broca_protocol::SubmissionId(submission_id),
        other => panic!("expected Pending, got {other:?}"),
    };
    assert_eq!(
        rig.registry.retract(&key, pending_sub).await.unwrap(),
        RetractResponse::Retracted
    );
    // Retracting again (now gone) → NotPending.
    assert_eq!(
        rig.registry
            .retract(&key, broca_protocol::SubmissionId("sub-r2".into()))
            .await
            .unwrap(),
        RetractResponse::NotPending
    );
    // An unknown id → NotPending.
    assert_eq!(
        rig.registry
            .retract(&key, broca_protocol::SubmissionId("unknown".into()))
            .await
            .unwrap(),
        RetractResponse::NotPending
    );
}

/// session.subscribe attaches and delivers the control stream (Start).
#[tokio::test]
async fn subscribe_attaches_and_delivers() {
    let exec = FakeExecutor::auto();
    let rig = Rig::new("serve-subscribe", exec.clone(), LONG_TTL);
    let key = rig.key("s1");

    rig.registry.send(send_req(&key, "r1")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await; // let the run emit + finish

    let (sink, mut rx) = tokio::sync::mpsc::channel::<Delivery>(64);
    rig.registry
        .subscribe(&key, SubscribeFrom::Start, sink)
        .await
        .unwrap();

    // The subscriber receives at least the RunStarted control unit (the fake writes one).
    let got =
        broca_test_support::event_timeout("subscribe_attaches_and_delivers", rx.recv(), || {
            format!("slot={:?}", rig.registry.slot_state(&key))
        })
        .await
        .expect("a delivery must arrive")
        .expect("the sink is open");
    assert!(
        matches!(got, Delivery::Control(_)),
        "attached + delivered a control event"
    );
}
