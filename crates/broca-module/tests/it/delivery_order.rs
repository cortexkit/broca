//! Sends whose `delivery` is an ordered list (the `delivery_order` group), driven through the
//! ServeHandler with a fake executor and real file stores.
//!
//! These prove the actor's half of the group: a list without `idle_start` never starts a run, a
//! list naming it starts one on an idle session, a newer send under the same dedupe key from the
//! same sender replaces a queued one (and only a queued one), an expired send is dropped durably
//! and never delivered, and a send accepted before the actor restarts is delivered exactly once.
//! The run loop's half (`steer` at a step boundary, `tool_result` with a batch) is proven in
//! broca-core's `run/delivery_order_tests.rs`.

use crate::common;

use std::time::Duration;

use broca_core::WalStore;
use broca_module::executor::RunKind;
use broca_module::serve::{SendRequest, SendResponse, ServeError};
use broca_module::ServeHandler;
use broca_protocol::{BindPrincipal, DeliveryMode, DeliveryOrder};
use broca_wal::{record::SendEndReason, WalRecord};
use common::{req, FakeExecutor, Rig};

const LONG_TTL: Duration = Duration::from_secs(3600);
const SHORT_TTL: Duration = Duration::from_millis(100);

fn send(
    key: &broca_protocol::BindIdentity,
    run: &str,
    order: Option<DeliveryOrder>,
) -> SendRequest {
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
        order,
    }
}

fn order(modes: &[DeliveryMode], dedupe_key: Option<&str>) -> DeliveryOrder {
    DeliveryOrder {
        modes: modes.to_vec(),
        dedupe_key: dedupe_key.map(str::to_owned),
        expires_at_ms: None,
    }
}

fn records(rig: &Rig, key: &broca_protocol::BindIdentity) -> Vec<WalRecord> {
    broca_core::FileWalStore::new(rig.dir.join("wal"))
        .read_records(key)
        .expect("read the WAL")
        .into_iter()
        .map(|frame| frame.record)
        .collect()
}

fn started_runs(records: &[WalRecord]) -> Vec<String> {
    records
        .iter()
        .filter_map(|record| match record {
            WalRecord::RunStarted { run_id, .. } => Some(run_id.0.clone()),
            _ => None,
        })
        .collect()
}

async fn wait_idle(rig: &Rig, key: &broca_protocol::BindIdentity) {
    broca_test_support::wait_for(
        "the session goes idle",
        async {
            loop {
                if matches!(
                    rig.registry.route_status(key).await.unwrap(),
                    broca_module::actor::LineageStatus::Idle
                ) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        },
        || "status never became idle".into(),
    )
    .await;
}

/// A list without `idle_start` is queued, durably, and never starts a run: not when sent to an
/// idle session, and not when that session goes idle again after a run something else started.
#[tokio::test]
async fn an_ordered_list_without_idle_start_never_starts_a_run() {
    let rig = Rig::new("ordered-no-idle-start", FakeExecutor::auto(), LONG_TTL);
    let key = rig.key("s");
    let reply = rig
        .registry
        .send(send(
            &key,
            "waits",
            Some(order(
                &[DeliveryMode::ToolResult, DeliveryMode::Steer],
                None,
            )),
        ))
        .await
        .unwrap();
    assert!(matches!(reply, SendResponse::Pending { .. }), "{reply:?}");
    let wal = records(&rig, &key);
    assert!(started_runs(&wal).is_empty(), "no run started: {wal:?}");
    let ordered_at = wal
        .iter()
        .position(|record| matches!(record, WalRecord::DeliveryOrdered { .. }))
        .expect("the order is durable");
    assert!(
        matches!(&wal[ordered_at + 1], WalRecord::Queued { .. }),
        "the Queued record follows its order"
    );

    // A plain send to the idle session starts its own run at once: the waiting ordered send does
    // not count as queued work it must wait behind, and is never drained into a run.
    let plain = rig.registry.send(send(&key, "plain", None)).await.unwrap();
    assert!(
        matches!(plain, SendResponse::Active { ref run_id, .. } if run_id == "plain"),
        "{plain:?}"
    );
    wait_idle(&rig, &key).await;
    assert_eq!(started_runs(&records(&rig, &key)), vec!["plain".to_owned()]);
    assert_eq!(rig.exec.started_count(), 1);
}

/// On an idle session `idle_start` is the first possible mode of a list naming it, so the send
/// starts its run at once, consuming its own queued record.
#[tokio::test]
async fn idle_start_starts_a_run_on_an_idle_session() {
    let rig = Rig::new("ordered-idle-start", FakeExecutor::auto(), LONG_TTL);
    let key = rig.key("s");
    let reply = rig
        .registry
        .send(send(
            &key,
            "now",
            Some(order(&[DeliveryMode::Steer, DeliveryMode::IdleStart], None)),
        ))
        .await
        .unwrap();
    assert!(
        matches!(reply, SendResponse::Active { ref run_id, .. } if run_id == "now"),
        "{reply:?}"
    );
    wait_idle(&rig, &key).await;
    let wal = records(&rig, &key);
    assert!(wal.iter().any(|record| matches!(
        record,
        WalRecord::RunStarted { submission_id: Some(id), .. } if id.0 == "sub-now"
    )));
}

/// A newer send from the same sender under the same dedupe key replaces the queued one, which
/// ends `superseded` durably and is reported so on a retry; a send from another sender under the
/// same key replaces nothing.
#[tokio::test]
async fn dedupe_replaces_a_queued_send_from_the_same_sender() {
    let rig = Rig::new("ordered-dedupe", FakeExecutor::auto(), LONG_TTL);
    let key = rig.key("s");
    let keyed = || Some(order(&[DeliveryMode::ToolResult], Some("ctx")));
    assert!(matches!(
        rig.registry
            .send(send(&key, "first", keyed()))
            .await
            .unwrap(),
        SendResponse::Pending { .. }
    ));
    assert!(matches!(
        rig.registry
            .send(send(&key, "second", keyed()))
            .await
            .unwrap(),
        SendResponse::Pending { .. }
    ));
    let mut other = send(&key, "other", keyed());
    other.request.principal = Some(BindPrincipal::Direct);
    assert!(matches!(
        rig.registry.send(other).await.unwrap(),
        SendResponse::Pending { .. }
    ));

    let ended: Vec<_> = records(&rig, &key)
        .into_iter()
        .filter_map(|record| match record {
            WalRecord::SendEnded {
                submission_id,
                reason,
            } => Some((submission_id.0, reason)),
            _ => None,
        })
        .collect();
    assert_eq!(
        ended,
        vec![(
            "sub-first".to_owned(),
            SendEndReason::Superseded {
                by: broca_protocol::SubmissionId("sub-second".into())
            }
        )],
        "only the same sender's queued send is replaced"
    );
    match rig.registry.send(send(&key, "first", keyed())).await {
        Err(ServeError::SendEnded {
            submission_id,
            outcome,
        }) => {
            assert_eq!(submission_id, "sub-first");
            assert_eq!(outcome, "superseded");
        }
        other => panic!("a retry of the replaced send reports it: {other:?}"),
    }
    // The replacement itself is still queued.
    assert!(matches!(
        rig.registry
            .send(send(&key, "second", keyed()))
            .await
            .unwrap(),
        SendResponse::Pending { .. }
    ));
}

/// A send delivered into a run is a transcript entry: a newer send under its dedupe key queues
/// beside it and replaces nothing.
#[tokio::test]
async fn dedupe_never_replaces_a_delivered_send() {
    let (exec, gate) = FakeExecutor::gated_with_step_boundary();
    let rig = Rig::new("ordered-dedupe-delivered", exec, LONG_TTL);
    let key = rig.key("s");
    let keyed = || Some(order(&[DeliveryMode::Steer], Some("ctx")));
    assert!(matches!(
        rig.registry.send(send(&key, "run", None)).await.unwrap(),
        SendResponse::Active { .. }
    ));
    assert!(matches!(
        rig.registry
            .send(send(&key, "first", keyed()))
            .await
            .unwrap(),
        SendResponse::Pending { .. }
    ));
    gate.release().await;
    wait_idle(&rig, &key).await;
    assert!(matches!(
        rig.registry
            .send(send(&key, "second", keyed()))
            .await
            .unwrap(),
        SendResponse::Pending { .. }
    ));
    let wal = records(&rig, &key);
    assert!(wal.iter().any(|record| matches!(
        record,
        WalRecord::SendDelivered { submission_id, mode: DeliveryMode::Steer, .. } if submission_id.0 == "sub-first"
    )));
    assert!(
        !wal.iter()
            .any(|record| matches!(record, WalRecord::SendEnded { .. })),
        "a delivered send is never replaced: {wal:?}"
    );
}

/// A send whose expiry passes before any boundary could deliver it is dropped with a durable
/// `SendEnded {expired}` and never delivered; a retry reports it expired.
#[tokio::test]
async fn an_expired_send_is_dropped_durably_and_never_delivered() {
    let (exec, gate) = FakeExecutor::gated_with_step_boundary();
    let rig = Rig::new("ordered-expiry", exec, LONG_TTL);
    let key = rig.key("s");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let expiring = || {
        Some(DeliveryOrder {
            expires_at_ms: Some(now + 300),
            ..order(&[DeliveryMode::Steer], None)
        })
    };
    assert!(matches!(
        rig.registry
            .send(send(&key, "late", expiring()))
            .await
            .unwrap(),
        SendResponse::Pending { .. }
    ));
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(matches!(
        rig.registry.send(send(&key, "run", None)).await.unwrap(),
        SendResponse::Active { .. }
    ));
    gate.release().await;
    wait_idle(&rig, &key).await;
    let wal = records(&rig, &key);
    assert!(wal.iter().any(|record| matches!(
        record,
        WalRecord::SendEnded { submission_id, reason: SendEndReason::Expired } if submission_id.0 == "sub-late"
    )));
    assert!(!wal
        .iter()
        .any(|record| matches!(record, WalRecord::SendDelivered { .. })));
    match rig.registry.send(send(&key, "late", expiring())).await {
        Err(ServeError::SendEnded { outcome, .. }) => assert_eq!(outcome, "expired"),
        other => panic!("a retry of the expired send reports it: {other:?}"),
    }
}

/// A send accepted before the session's actor restarts is rebuilt from the WAL with its order
/// and delivered exactly once, at the first step boundary of the next run.
#[tokio::test]
async fn a_restart_between_acceptance_and_delivery_delivers_once() {
    let (exec, gate) = FakeExecutor::gated_with_step_boundary();
    let rig = Rig::new("ordered-restart", exec, SHORT_TTL);
    let key = rig.key("s");
    assert!(matches!(
        rig.registry
            .send(send(
                &key,
                "note",
                Some(order(&[DeliveryMode::Steer], None))
            ))
            .await
            .unwrap(),
        SendResponse::Pending { .. }
    ));
    broca_test_support::wait_for(
        "the idle actor closes",
        async {
            while rig.registry.slot_state(&key).is_some() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
        || format!("slot={:?}", rig.registry.slot_state(&key)),
    )
    .await;
    for run in ["first", "second"] {
        assert!(matches!(
            rig.registry.send(send(&key, run, None)).await.unwrap(),
            SendResponse::Active { .. }
        ));
        gate.release().await;
        wait_idle(&rig, &key).await;
    }
    let wal = records(&rig, &key);
    let delivered: Vec<_> = wal
        .iter()
        .filter_map(|record| match record {
            WalRecord::SendDelivered { run_id, .. } => Some(run_id.0.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        delivered,
        vec!["first".to_owned()],
        "delivered once, into the first run"
    );
    let at = wal
        .iter()
        .position(|record| matches!(record, WalRecord::SendDelivered { .. }))
        .unwrap();
    assert!(
        matches!(wal[at + 1], WalRecord::StepStarted { .. }),
        "placed at the step boundary"
    );
}

/// On a session whose owner is a module, a send with an ordered list from another module is
/// refused unless the session's plan names that module as a sender, matched on the principal the
/// daemon stamped on the route. Nothing is written for the refused send.
#[tokio::test]
async fn an_unnamed_sender_is_refused() {
    let rig = Rig::new("ordered-sender", FakeExecutor::auto(), LONG_TTL);
    let key = rig.key("s");
    let mut first = send(&key, "owner", None);
    first.request.principal = Some(BindPrincipal::Reserved {
        module_id: "owner".into(),
    });
    assert!(matches!(
        rig.registry.send(first).await.unwrap(),
        SendResponse::Active { .. }
    ));
    wait_idle(&rig, &key).await;
    let before = records(&rig, &key).len();
    let mut stranger = send(&key, "stranger", Some(order(&[DeliveryMode::Steer], None)));
    stranger.request.principal = Some(BindPrincipal::Reserved {
        module_id: "ck-mc".into(),
    });
    match rig.registry.send(stranger).await {
        Err(ServeError::DeliveryRefused { code, .. }) => assert_eq!(code, "sender_not_permitted"),
        other => panic!("an unnamed sender is refused: {other:?}"),
    }
    assert_eq!(records(&rig, &key).len(), before, "nothing written");
    // The owner itself needs no plan entry.
    let mut owner = send(
        &key,
        "owner-note",
        Some(order(&[DeliveryMode::Steer], None)),
    );
    owner.request.principal = Some(BindPrincipal::Reserved {
        module_id: "owner".into(),
    });
    assert!(matches!(
        rig.registry.send(owner).await.unwrap(),
        SendResponse::Pending { .. }
    ));
}
