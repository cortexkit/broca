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
        template: None,
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

#[tokio::test]
async fn already_expired_send_is_durable_and_never_starts() {
    let rig = Rig::new("ordered-expired-admission", FakeExecutor::auto(), LONG_TTL);
    let key = rig.key("s");
    let expired = || {
        Some(DeliveryOrder {
            expires_at_ms: Some(1),
            ..order(&[DeliveryMode::IdleStart], None)
        })
    };
    for _ in 0..2 {
        assert!(matches!(
            rig.registry.send(send(&key, "late", expired())).await,
            Err(ServeError::SendEnded {
                outcome: "expired",
                ..
            })
        ));
    }
    let wal = records(&rig, &key);
    assert_eq!(
        wal.iter()
            .filter(|record| matches!(record, WalRecord::Queued { .. }))
            .count(),
        1
    );
    assert_eq!(
        wal.iter()
            .filter(|record| matches!(
                record,
                WalRecord::SendEnded {
                    reason: SendEndReason::Expired,
                    ..
                }
            ))
            .count(),
        1
    );
    assert!(started_runs(&wal).is_empty());
    assert_eq!(rig.exec.started_count(), 0);
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
        Err(ServeError::DeliveryRefused { code, .. }) => assert_eq!(code, "scope_owner_mismatch"),
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

fn plan_naming(sender: &str, idle_start: bool) -> broca_module::actor::SendPlan {
    let plan = serde_json::json!({
        "senders": [{"module": sender, "idle_start": idle_start}],
        "composition": {}
    });
    broca_module::actor::SendPlan {
        identity: format!("plan-{sender}-{idle_start}"),
        manifest: Some(std::sync::Arc::new(broca_wal::PlanManifest {
            plan: broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap(),
            plan_identity: format!("plan-{sender}-{idle_start}"),
            composition_digest: "digest".into(),
            tools: std::sync::Arc::new(vec![]),
            system_text: String::new(),
            system_digest: String::new(),
            join_version: 1,
            role_versions: Default::default(),
            fetches: vec![],
            declarations: vec![],
        })),
    }
}

fn reserved(module: &str) -> Option<BindPrincipal> {
    Some(BindPrincipal::Reserved {
        module_id: module.into(),
    })
}

fn template_fixture() -> (
    broca_module::actor::SendPlan,
    cortexkit_role_llm_runner::delivery_order::TemplateUse,
) {
    use cortexkit_role_llm_runner::delivery_order::{template_digest, Slot, Template, TemplateUse};
    let template = Template::new(
        "context",
        "Context is {pct}% full.",
        std::collections::BTreeMap::from([("pct".into(), Slot::Percentage)]),
    );
    let use_ = TemplateUse::new(
        "context",
        template_digest(&template),
        serde_json::json!({"pct":82}).as_object().unwrap().clone(),
    );
    let mut plan = plan_naming("ck-mc", false);
    let manifest = std::sync::Arc::make_mut(plan.manifest.as_mut().unwrap());
    let mut value: serde_json::Value = serde_json::from_str(manifest.plan.as_json_text()).unwrap();
    value["senders"][0]["templates"] = serde_json::json!([template]);
    manifest.plan = broca_wal::OpaqueJson::from_json_text(value.to_string()).unwrap();
    (plan, use_)
}

#[tokio::test]
async fn template_invalid_slot_is_refused_without_writes() {
    let rig = Rig::new("ordered-template-slot", FakeExecutor::auto(), LONG_TTL);
    let key = rig.key("s");
    let (plan, mut template) = template_fixture();
    let mut first = send(&key, "owner", None);
    first.request.principal = reserved("owner");
    first.plan = Some(plan);
    rig.registry.send(first).await.unwrap();
    wait_idle(&rig, &key).await;
    template.slots.insert("pct".into(), 101.into());
    let mut order = order(&[DeliveryMode::TurnSystem, DeliveryMode::Steer], None);
    order.template = Some(template);
    let mut bad = send(&key, "bad-slot", Some(order));
    bad.request.principal = reserved("ck-mc");
    let before = records(&rig, &key).len();
    assert!(matches!(rig.registry.send(bad).await,
        Err(ServeError::InvalidParams { field, .. }) if field == "template"));
    assert_eq!(records(&rig, &key).len(), before);
}

#[tokio::test]
async fn template_text_is_recorded_once_and_falls_through_to_steer_after_restart() {
    let (exec, gate) = FakeExecutor::gated_with_step_boundary();
    let rig = Rig::new("ordered-template-restart", exec, SHORT_TTL);
    let key = rig.key("s");
    let (plan, template) = template_fixture();
    let mut first = send(&key, "owner", None);
    first.request.principal = reserved("owner");
    first.plan = Some(plan);
    rig.registry.send(first).await.unwrap();
    gate.release().await;
    wait_idle(&rig, &key).await;
    let mut order = order(&[DeliveryMode::TurnSystem, DeliveryMode::Steer], None);
    order.template = Some(template);
    let mut note = send(&key, "template", Some(order));
    note.request.principal = reserved("ck-mc");
    assert!(matches!(
        rig.registry.send(note).await.unwrap(),
        SendResponse::Pending { .. }
    ));
    let queued = records(&rig, &key)
        .into_iter()
        .find_map(|record| match record {
            WalRecord::Queued { request, .. } => Some(request.input),
            _ => None,
        })
        .unwrap();
    assert_eq!(
        serde_json::to_value(queued).unwrap(),
        serde_json::json!([
            {"role":"user","content":[{"type":"text","text":"Context is 82% full."}]}
        ])
    );
    broca_test_support::wait_for(
        "the template's actor closes",
        async {
            while rig.registry.slot_state(&key).is_some() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
        || "the actor did not close".into(),
    )
    .await;
    let mut owner = send(&key, "next", None);
    owner.request.principal = reserved("owner");
    rig.registry.send(owner).await.unwrap();
    gate.release().await;
    wait_idle(&rig, &key).await;
    let wal = records(&rig, &key);
    assert_eq!(
        wal.iter()
            .filter(|record| matches!(
                record,
                WalRecord::SendDelivered {
                    mode: DeliveryMode::Steer,
                    ..
                }
            ))
            .count(),
        1
    );
    assert!(!wal.iter().any(|record| matches!(
        record,
        WalRecord::SendDelivered {
            mode: DeliveryMode::TurnSystem,
            ..
        }
    )));
    let page = rig
        .registry
        .read(broca_module::serve::SessionReadRequest::new(
            key,
            Some(0),
            Some(200),
        ))
        .await
        .unwrap();
    let note = page.messages.iter().find(|entry| entry.message.content.iter().any(|block|
        matches!(block, broca_protocol::ContentBlock::Text { text } if text == "Context is 82% full."))).unwrap();
    assert_eq!(note.delivered_as.as_ref().unwrap().mode, "steer");
}

/// Open a session whose owner is the module `owner` and whose frozen plan names `ck-mc` as a
/// sender, with or without the `idle_start` grant.
async fn session_naming_ck_mc(
    rig: &Rig,
    name: &str,
    idle_start: bool,
) -> broca_protocol::BindIdentity {
    let key = rig.key(name);
    let mut first = send(&key, &format!("{name}-owner"), None);
    first.request.principal = reserved("owner");
    first.plan = Some(plan_naming("ck-mc", idle_start));
    let reply = rig.registry.send(first).await.unwrap();
    assert!(matches!(reply, SendResponse::Active { .. }), "{reply:?}");
    wait_idle(rig, &key).await;
    key
}

/// A plan-named sender's send runs under the session's frozen principal: the run it starts
/// records the owner, never the sender, and its request carries no scope of the sender's (its
/// tool routes are opened as the owner's). The sender is recorded only on `DeliveryOrdered`,
/// dedupe keys are per sender, and the owner's next send is still admitted.
#[tokio::test]
async fn a_named_sender_never_becomes_the_sessions_principal() {
    let rig = Rig::new("ordered-named-sender", FakeExecutor::auto(), LONG_TTL);
    let key = session_naming_ck_mc(&rig, "named", true).await;

    let mut mc = send(
        &key,
        "mc-start",
        Some(order(&[DeliveryMode::IdleStart], Some("ctx"))),
    );
    mc.request.principal = reserved("ck-mc");
    let reply = rig.registry.send(mc).await.unwrap();
    assert!(
        matches!(reply, SendResponse::Active { ref run_id, .. } if run_id == "mc-start"),
        "{reply:?}"
    );
    wait_idle(&rig, &key).await;

    let seen = rig.exec.seen.lock().unwrap().clone();
    let run = seen
        .iter()
        .find(|run| run.run_id == "mc-start")
        .expect("the sender's run started");
    assert_eq!(
        run.principal,
        reserved("owner"),
        "the run's principal stays the owner's"
    );
    assert!(
        run.scope
            .as_ref()
            .is_none_or(|scope| Some(&scope.owner) == reserved("owner").as_ref()),
        "the run's tool routes carry no scope but the owner's: {:?}",
        run.scope
    );
    let wal = records(&rig, &key);
    for record in &wal {
        match record {
            WalRecord::RunStarted { principal, .. } | WalRecord::Queued { principal, .. } => {
                assert_eq!(principal, &reserved("owner"), "{record:?}")
            }
            WalRecord::DeliveryOrdered { sent_by, .. } => assert_eq!(sent_by, &reserved("ck-mc")),
            _ => {}
        }
    }

    // Dedupe keys are per sender: the owner's send under the same key replaces nothing of
    // ck-mc's, and ck-mc's next send replaces only its own.
    let mut mc_wait = send(
        &key,
        "mc-wait",
        Some(order(&[DeliveryMode::ToolResult], Some("k"))),
    );
    mc_wait.request.principal = reserved("ck-mc");
    assert!(matches!(
        rig.registry.send(mc_wait).await.unwrap(),
        SendResponse::Pending { .. }
    ));
    let mut owner_wait = send(
        &key,
        "owner-wait",
        Some(order(&[DeliveryMode::ToolResult], Some("k"))),
    );
    owner_wait.request.principal = reserved("owner");
    assert!(matches!(
        rig.registry.send(owner_wait).await.unwrap(),
        SendResponse::Pending { .. }
    ));
    let mut mc_again = send(
        &key,
        "mc-again",
        Some(order(&[DeliveryMode::ToolResult], Some("k"))),
    );
    mc_again.request.principal = reserved("ck-mc");
    assert!(matches!(
        rig.registry.send(mc_again).await.unwrap(),
        SendResponse::Pending { .. }
    ));
    let ended: Vec<_> = records(&rig, &key)
        .into_iter()
        .filter_map(|record| match record {
            WalRecord::SendEnded { submission_id, .. } => Some(submission_id.0),
            _ => None,
        })
        .collect();
    assert_eq!(ended, vec!["sub-mc-wait".to_owned()]);

    // The owner's next ordinary send is admitted as before.
    let mut owner = send(&key, "owner-next", None);
    owner.request.principal = reserved("owner");
    let reply = rig.registry.send(owner).await.unwrap();
    assert!(matches!(reply, SendResponse::Active { .. }), "{reply:?}");
}

/// A named sender without the plan's `idle_start` grant is refused a list naming `idle_start`
/// before anything is written, and may still send a list that only joins runs.
#[tokio::test]
async fn idle_start_without_the_grant_is_refused() {
    let rig = Rig::new("ordered-no-grant", FakeExecutor::auto(), LONG_TTL);
    let key = session_naming_ck_mc(&rig, "no-grant", false).await;
    let before = records(&rig, &key).len();
    let mut start = send(
        &key,
        "mc-start",
        Some(order(&[DeliveryMode::Steer, DeliveryMode::IdleStart], None)),
    );
    start.request.principal = reserved("ck-mc");
    match rig.registry.send(start).await {
        Err(ServeError::InvalidParams { field, .. }) => assert_eq!(field, "delivery"),
        other => panic!("idle_start without the grant is refused: {other:?}"),
    }
    assert_eq!(records(&rig, &key).len(), before, "nothing written");
    let mut join = send(&key, "mc-join", Some(order(&[DeliveryMode::Steer], None)));
    join.request.principal = reserved("ck-mc");
    assert!(matches!(
        rig.registry.send(join).await.unwrap(),
        SendResponse::Pending { .. }
    ));
    let mut owner = send(&key, "owner-next", None);
    owner.request.principal = reserved("owner");
    assert!(matches!(
        rig.registry.send(owner).await.unwrap(),
        SendResponse::Active { .. }
    ));
}

/// A keyed retry of an ordered send delivered into a run is answered with that run's state now
/// (finished once it ended, not active), and a retry under the same `send_id` asking for another
/// delivery (a legacy send, or another list) is refused naming `delivery`.
#[tokio::test]
async fn a_keyed_retry_reports_the_delivered_runs_state_and_keeps_its_order() {
    let (exec, gate) = FakeExecutor::gated_with_step_boundary();
    let rig = Rig::new("ordered-retry", exec, LONG_TTL);
    let key = rig.key("s");
    let steer = || Some(order(&[DeliveryMode::Steer], None));
    assert!(matches!(
        rig.registry.send(send(&key, "run", None)).await.unwrap(),
        SendResponse::Active { .. }
    ));
    assert!(matches!(
        rig.registry
            .send(send(&key, "note", steer()))
            .await
            .unwrap(),
        SendResponse::Pending { .. }
    ));
    gate.release().await;
    wait_idle(&rig, &key).await;
    match rig
        .registry
        .send(send(&key, "note", steer()))
        .await
        .unwrap()
    {
        SendResponse::Finished { run_id, .. } => assert_eq!(run_id, "run"),
        other => panic!("the delivered send's run has finished: {other:?}"),
    }
    for differing in [
        None,
        Some(order(&[DeliveryMode::Steer, DeliveryMode::IdleStart], None)),
    ] {
        match rig
            .registry
            .send(send(&key, "note", differing.clone()))
            .await
        {
            Err(ServeError::SendIdReuse { differing }) => assert_eq!(differing, "delivery"),
            other => panic!("another delivery under the same send_id is refused: {other:?}"),
        }
    }
    // A queued (undelivered) ordered send and a legacy retry of its send_id.
    assert!(matches!(
        rig.registry
            .send(send(
                &key,
                "waiting",
                Some(order(&[DeliveryMode::ToolResult], None))
            ))
            .await
            .unwrap(),
        SendResponse::Pending { .. }
    ));
    match rig.registry.send(send(&key, "waiting", None)).await {
        Err(ServeError::SendIdReuse { differing }) => assert_eq!(differing, "delivery"),
        other => panic!("a legacy retry of a queued ordered send is refused: {other:?}"),
    }
}
