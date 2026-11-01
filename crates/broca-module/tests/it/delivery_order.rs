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

#[tokio::test]
async fn keyed_retry_ignores_deadlines_for_owner_and_named_senders() {
    for sender in ["owner", "ck-mc"] {
        let mut rig = Rig::new("retry-deadline", FakeExecutor::auto(), LONG_TTL);
        let key = session_naming_ck_mc(&rig, "deadline", false).await;
        let original = DeliveryOrder {
            expires_at_ms: Some(u64::MAX - 100),
            ..order(&[DeliveryMode::Steer], Some("deadline-key"))
        };
        let mut request = send(&key, "deadline-note", Some(original.clone()));
        request.request.principal = reserved(sender);
        let receipt = rig.registry.send(request.clone()).await.unwrap();
        let before = records(&rig, &key);
        for restarted in [false, true] {
            if restarted {
                restart_registry(&mut rig).await;
            }
            for deadline in [Some(u64::MAX), None, Some(1)] {
                request.order.as_mut().unwrap().expires_at_ms = deadline;
                let retry = rig.registry.send(request.clone()).await.unwrap();
                assert_eq!(
                    serde_json::to_value(retry).unwrap(),
                    serde_json::to_value(&receipt).unwrap()
                );
                assert_eq!(
                    records(&rig, &key),
                    before,
                    "a retry must not rewrite its deadline"
                );
            }
            for changed in [
                DeliveryOrder {
                    modes: vec![DeliveryMode::ToolResult],
                    ..original.clone()
                },
                DeliveryOrder {
                    dedupe_key: Some("other".into()),
                    ..original.clone()
                },
            ] {
                request.order = Some(changed);
                assert!(
                    matches!(rig.registry.send(request.clone()).await, Err(ServeError::SendIdReuse { differing }) if differing == "delivery")
                );
            }
            request.order = Some(original.clone());
        }
    }
}

#[tokio::test]
async fn successor_prefers_removed_predecessor_receipts_after_refresh_and_restart() {
    let mut rig = Rig::new("succession", FakeExecutor::auto(), LONG_TTL);
    let key = session_naming_ck_mc(&rig, "succession", false).await;
    let predecessor = pending_steer(&rig, &key, "ck-mc", "shared").await;
    refresh_senders(
        &rig,
        &key,
        1,
        "on_prefix_rebuild",
        plan_naming("phone", false),
    )
    .await;
    let mut independent = send(&key, "shared", Some(order(&[DeliveryMode::Steer], None)));
    independent.request.principal = reserved("phone");
    independent.request.input[0].content = vec![broca_protocol::ContentBlock::Text {
        text: "different".into(),
    }];
    let own_receipt = rig.registry.send(independent.clone()).await.unwrap();
    assert_eq!(
        serde_json::to_value(rig.registry.send(independent).await.unwrap()).unwrap(),
        serde_json::to_value(&own_receipt).unwrap()
    );
    let mut plan = plan_naming("phone", false);
    let manifest = std::sync::Arc::make_mut(plan.manifest.as_mut().unwrap());
    let mut value: serde_json::Value = serde_json::from_str(manifest.plan.as_json_text()).unwrap();
    value["senders"][0]["succeeds"] = serde_json::json!("ck-mc");
    manifest.plan = broca_wal::OpaqueJson::from_json_text(value.to_string()).unwrap();
    refresh_senders(&rig, &key, 2, "on_prefix_rebuild", plan).await;
    for restarted in [false, true] {
        if restarted {
            restart_registry(&mut rig).await;
        }
        let before = records(&rig, &key);
        assert_eq!(
            pending_steer(&rig, &key, "phone", "shared").await,
            predecessor
        );
        let status = rig
            .registry
            .send_status(
                &key,
                broca_protocol::RunId("shared".into()),
                "send-shared",
                reserved("phone").as_ref(),
            )
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(status).unwrap()["submission_id"],
            predecessor.0
        );
        let mut changed = send(&key, "shared", Some(order(&[DeliveryMode::Steer], None)));
        changed.request.principal = reserved("phone");
        changed.request.input[0].content = vec![broca_protocol::ContentBlock::Text {
            text: "different".into(),
        }];
        assert!(matches!(
            rig.registry.send(changed).await,
            Err(ServeError::SendIdReuse { .. })
        ));
        assert_eq!(records(&rig, &key), before);
        assert!(matches!(
            retract_as(&rig, &key, reserved("phone"), &predecessor).await,
            Err(ServeError::ScopeRefused(_))
        ));
    }
    let own = pending_steer(&rig, &key, "owner", "shared").await;
    assert_ne!(
        own, predecessor,
        "the owner's send namespace stays independent"
    );
    let fresh = pending_steer(&rig, &key, "phone", "fresh").await;
    assert_ne!(fresh, predecessor);
}

#[tokio::test]
async fn successor_retries_legacy_owner_steers_with_the_original_receipt_without_delivery() {
    let (exec, gate) = FakeExecutor::gated_with_step_boundary();
    let mut rig = Rig::new("owner-succession", exec, LONG_TTL);
    let key = rig.key("owner-steer");
    let mut first = send(&key, "owner-run", None);
    first.request.principal = reserved("owner");
    first.plan = Some(plan_naming("old-module", false));
    rig.registry.send(first).await.unwrap();
    let mut legacy = send(&key, "legacy-note", None);
    legacy.delivery = broca_protocol::Delivery::Steer;
    legacy.request.principal = reserved("owner");
    assert!(
        matches!(rig.registry.send(legacy.clone()).await.unwrap(), SendResponse::Active { ref run_id, .. } if run_id == "owner-run")
    );
    gate.release().await;
    wait_idle(&rig, &key).await;
    let receipt = serde_json::to_value(rig.registry.send(legacy.clone()).await.unwrap()).unwrap();
    let mut plan = plan_naming("phone", true);
    let manifest = std::sync::Arc::make_mut(plan.manifest.as_mut().unwrap());
    let mut value: serde_json::Value = serde_json::from_str(manifest.plan.as_json_text()).unwrap();
    value["senders"][0]["succeeds"] = serde_json::json!("owner");
    manifest.plan = broca_wal::OpaqueJson::from_json_text(value.to_string()).unwrap();
    refresh_senders(&rig, &key, 1, "on_prefix_rebuild", plan).await;
    legacy.request.principal = reserved("phone");
    for restarted in [false, true] {
        if restarted {
            restart_registry(&mut rig).await;
        }
        let before = records(&rig, &key);
        assert_eq!(
            serde_json::to_value(rig.registry.send(legacy.clone()).await.unwrap()).unwrap(),
            receipt
        );
        let status = rig
            .registry
            .send_status(
                &key,
                broca_protocol::RunId("legacy-note".into()),
                "send-legacy-note",
                reserved("phone").as_ref(),
            )
            .await
            .unwrap();
        assert_eq!(serde_json::to_value(status).unwrap(), receipt);
        let mut conflicting = legacy.clone();
        conflicting.request.input[0].content = vec![broca_protocol::ContentBlock::Text {
            text: "different".into(),
        }];
        assert!(matches!(
            rig.registry.send(conflicting).await,
            Err(ServeError::SendIdReuse { .. })
        ));
        assert_eq!(
            records(&rig, &key),
            before,
            "a predecessor receipt never delivers another message"
        );
    }
    assert!(matches!(
        rig.registry
            .send_status(
                &key,
                broca_protocol::RunId("legacy-note".into()),
                "send-legacy-note",
                reserved("stranger").as_ref()
            )
            .await,
        Err(ServeError::UnknownSend)
    ));
    assert!(rig
        .registry
        .authorize_session_owner(
            &key,
            reserved("phone").as_ref(),
            &broca_protocol::SendScope::Unstamped
        )
        .await
        .is_err());
}

#[tokio::test]
async fn successor_retries_legacy_owner_started_sends_with_unchanged_receipts() {
    let rig = Rig::new("owner-start-succession", FakeExecutor::auto(), LONG_TTL);
    let key = rig.key("owner-start");
    let mut original = send(&key, "owner-turn", None);
    original.request.principal = reserved("owner");
    original.plan = Some(plan_naming("old-module", false));
    rig.registry.send(original.clone()).await.unwrap();
    wait_idle(&rig, &key).await;
    original.plan = None;
    let receipt = serde_json::to_value(rig.registry.send(original.clone()).await.unwrap()).unwrap();
    let mut plan = plan_naming("phone", true);
    let manifest = std::sync::Arc::make_mut(plan.manifest.as_mut().unwrap());
    let mut value: serde_json::Value = serde_json::from_str(manifest.plan.as_json_text()).unwrap();
    value["senders"][0]["succeeds"] = serde_json::json!("owner");
    manifest.plan = broca_wal::OpaqueJson::from_json_text(value.to_string()).unwrap();
    refresh_senders(&rig, &key, 1, "on_prefix_rebuild", plan).await;
    original.request.principal = reserved("phone");
    let before = records(&rig, &key);
    assert_eq!(
        serde_json::to_value(rig.registry.send(original).await.unwrap()).unwrap(),
        receipt
    );
    assert_eq!(records(&rig, &key), before);
}

async fn retract_as(
    rig: &Rig,
    key: &broca_protocol::BindIdentity,
    sender: Option<BindPrincipal>,
    id: &broca_protocol::SubmissionId,
) -> Result<broca_module::serve::RetractResponse, ServeError> {
    rig.registry
        .authorize_session_retract(
            key,
            sender.as_ref(),
            &broca_protocol::SendScope::Unstamped,
            Some(&id.0),
        )
        .await?;
    rig.registry.retract(key, id.clone()).await
}

async fn pending_steer(
    rig: &Rig,
    key: &broca_protocol::BindIdentity,
    sender: &str,
    run: &str,
) -> broca_protocol::SubmissionId {
    let mut request = send(key, run, Some(order(&[DeliveryMode::Steer], None)));
    request.request.principal = reserved(sender);
    match rig.registry.send(request).await.unwrap() {
        SendResponse::Pending { submission_id } => broca_protocol::SubmissionId(submission_id),
        other => panic!("expected a pending steer: {other:?}"),
    }
}

async fn restart_registry(rig: &mut Rig) {
    use std::sync::Arc;
    assert!(rig.registry.stop_and_seal().await);
    let wal = Arc::new(broca_core::FileWalStore::new(rig.dir.join("wal")));
    let store = Arc::new(broca_store::SqliteStore::open(rig.dir.join("store.db")).unwrap());
    let opener = Arc::new(
        broca_module::DefaultOpener::new(
            Arc::new(broca_core::FileLeaseStore::new(rig.dir.join("leases"))),
            wal.clone(),
            rig.exec.clone(),
        )
        .with_idle_ttl(LONG_TTL)
        .with_run_index(store.clone()),
    );
    rig.registry = broca_module::LineageRegistry::new_with_reader(opener, wal, Some(store));
}

#[tokio::test]
async fn retract_own_send_matches_owner_and_refuses_other_principals_without_writes() {
    use broca_module::serve::RetractResponse;
    let rig = Rig::new("retract-own", FakeExecutor::auto(), LONG_TTL);
    let key = session_naming_ck_mc(&rig, "own", false).await;
    let own = pending_steer(&rig, &key, "ck-mc", "own-note").await;
    let owner = pending_steer(&rig, &key, "ck-mc", "owner-note").await;
    let before = records(&rig, &key);
    for principal in [
        reserved("stranger"),
        None,
        Some(BindPrincipal::Direct),
        Some(BindPrincipal::Unverified),
    ] {
        assert!(matches!(
            retract_as(&rig, &key, principal, &own).await,
            Err(ServeError::ScopeRefused(
                broca_core::ScopeRefusal::OwnerMismatch(_)
            ))
        ));
        assert_eq!(records(&rig, &key), before, "refusal writes nothing");
    }
    let own_reply = retract_as(&rig, &key, reserved("ck-mc"), &own)
        .await
        .unwrap();
    let owner_reply = retract_as(&rig, &key, reserved("owner"), &owner)
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_vec(&own_reply).unwrap(),
        serde_json::to_vec(&owner_reply).unwrap()
    );
    assert_eq!(own_reply, RetractResponse::Retracted);
    assert_eq!(
        retract_as(&rig, &key, reserved("ck-mc"), &own)
            .await
            .unwrap(),
        RetractResponse::NotPending
    );
    assert_eq!(
        records(&rig, &key)
            .iter()
            .filter(|r| matches!(r, WalRecord::Retracted { .. }))
            .count(),
        2
    );
    assert!(
        rig.registry
            .authorize_session_owner(
                &key,
                reserved("ck-mc").as_ref(),
                &broca_protocol::SendScope::Unstamped
            )
            .await
            .is_err(),
        "run.cancel stays owner-only"
    );
}

#[tokio::test]
async fn retract_own_send_shared_send_id_retries_and_status_are_isolated_after_restart() {
    use broca_module::serve::RetractResponse;
    for senders in [["ck-mc", "phone"], ["phone", "ck-mc"]] {
        let mut rig = Rig::new("retract-shared", FakeExecutor::auto(), LONG_TTL);
        let key = rig.key("shared");
        let mut first = send(&key, "owner", None);
        first.request.principal = reserved("owner");
        let mut plan = plan_naming("ck-mc", true);
        let manifest = std::sync::Arc::make_mut(plan.manifest.as_mut().unwrap());
        let mut value: serde_json::Value =
            serde_json::from_str(manifest.plan.as_json_text()).unwrap();
        value["senders"]
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({"module":"phone","idle_start":true}));
        manifest.plan = broca_wal::OpaqueJson::from_json_text(value.to_string()).unwrap();
        first.plan = Some(plan);
        let owner_reply = rig.registry.send(first.clone()).await.unwrap();
        assert_eq!(
            serde_json::to_vec(&owner_reply).unwrap(),
            br#"{"state":"active","run_id":"owner"}"#
        );
        wait_idle(&rig, &key).await;
        let frozen = records(&rig, &key)
            .into_iter()
            .find(|r| matches!(r, WalRecord::RunStarted { .. }))
            .unwrap();
        let a = pending_steer(&rig, &key, senders[0], "same").await;
        let b = pending_steer(&rig, &key, senders[1], "same").await;
        assert_eq!(a.0, "sub-same", "the first sender keeps its legacy ID");
        assert_ne!(a, b);
        assert!(b.0.starts_with("sub-same-sender-"));
        for restarted in [false, true] {
            if restarted {
                restart_registry(&mut rig).await;
            }
            for (sender, id) in [(senders[0], &a), (senders[1], &b)] {
                assert_eq!(pending_steer(&rig, &key, sender, "same").await, *id);
                let status = rig
                    .registry
                    .send_status(
                        &key,
                        broca_protocol::RunId("same".into()),
                        "send-same",
                        reserved(sender).as_ref(),
                    )
                    .await
                    .unwrap();
                assert_eq!(serde_json::to_value(status).unwrap()["submission_id"], id.0);
            }
            assert!(matches!(
                rig.registry
                    .send_status(
                        &key,
                        broca_protocol::RunId("same".into()),
                        "send-same",
                        reserved("stranger").as_ref()
                    )
                    .await,
                Err(ServeError::UnknownSend)
            ));
        }
        assert_eq!(
            records(&rig, &key)
                .into_iter()
                .find(|r| matches!(r, WalRecord::RunStarted { .. }))
                .unwrap(),
            frozen,
            "qualification never changes the frozen first episode"
        );
        let owner_retry = rig.registry.send(first).await.unwrap();
        assert!(
            matches!(owner_reply, SendResponse::Active { ref run_id, .. } if run_id == "owner")
        );
        assert!(
            matches!(owner_retry, SendResponse::Finished { ref run_id, .. } if run_id == "owner")
        );
        assert!(matches!(
            retract_as(&rig, &key, reserved(senders[0]), &b).await,
            Err(ServeError::ScopeRefused(_))
        ));
        assert_eq!(
            retract_as(&rig, &key, reserved(senders[0]), &a)
                .await
                .unwrap(),
            RetractResponse::Retracted
        );
        assert_eq!(
            retract_as(&rig, &key, reserved(senders[0]), &a)
                .await
                .unwrap(),
            RetractResponse::NotPending
        );
        assert!(broca_core::pending_submissions(&records(&rig, &key))
            .iter()
            .any(|pending| pending.submission_id == b));
        assert_eq!(
            retract_as(&rig, &key, reserved(senders[1]), &b)
                .await
                .unwrap(),
            RetractResponse::Retracted
        );
        let owner_pending = pending_steer(&rig, &key, "owner", "owner-pending").await;
        assert_eq!(owner_pending.0, "sub-owner-pending");
    }
}

#[tokio::test]
async fn retract_own_send_delivered_steer_is_not_pending_and_started_run_is_already_started() {
    use broca_module::serve::RetractResponse;
    let (exec, gate) = FakeExecutor::gated_with_step_boundary();
    let rig = Rig::new("retract-delivered", exec, LONG_TTL);
    let key = rig.key("delivered");
    let mut first = send(&key, "owner", None);
    first.request.principal = reserved("owner");
    first.plan = Some(plan_naming("ck-mc", true));
    rig.registry.send(first).await.unwrap();
    gate.release().await;
    wait_idle(&rig, &key).await;
    let id = pending_steer(&rig, &key, "ck-mc", "note").await;
    let mut next = send(&key, "owner-next", None);
    next.request.principal = reserved("owner");
    rig.registry.send(next).await.unwrap();
    gate.release().await;
    wait_idle(&rig, &key).await;
    let before = records(&rig, &key);
    assert!(before.iter().any(
        |r| matches!(r, WalRecord::SendDelivered { submission_id, .. } if submission_id == &id)
    ));
    assert_eq!(
        retract_as(&rig, &key, reserved("ck-mc"), &id)
            .await
            .unwrap(),
        RetractResponse::NotPending
    );
    assert_eq!(
        records(&rig, &key),
        before,
        "a delivered steer writes no tombstone"
    );

    let mut starts = send(
        &key,
        "sender-run",
        Some(order(&[DeliveryMode::IdleStart], None)),
    );
    starts.request.principal = reserved("ck-mc");
    rig.registry.send(starts).await.unwrap();
    assert_eq!(
        retract_as(
            &rig,
            &key,
            reserved("ck-mc"),
            &broca_protocol::SubmissionId("sub-sender-run".into())
        )
        .await
        .unwrap(),
        RetractResponse::AlreadyStarted
    );
    gate.release().await;
    wait_idle(&rig, &key).await;
}

#[tokio::test]
async fn retract_own_send_without_recorded_sender_stays_owner_only_after_restart() {
    let mut rig = Rig::new("retract-legacy", FakeExecutor::auto(), LONG_TTL);
    let key = session_naming_ck_mc(&rig, "legacy", false).await;
    assert!(rig.registry.stop_and_seal().await);
    let wal = broca_core::FileWalStore::new(rig.dir.join("wal"));
    let id = broca_protocol::SubmissionId("legacy-no-sender".into());
    let mut writer = wal.open(&key).unwrap();
    writer
        .append(
            1,
            &WalRecord::Queued {
                written_at_ms: None,
                retention: None,
                ts_ms: None,
                hooks: None,
                keep_warm: None,
                conversation_key: None,
                delivery: None,
                send_id: None,
                title: None,
                submission_id: id.clone(),
                request: send(&key, "legacy-note", None).request,
                request_identity_digest: None,
                principal: None,
            },
            false,
        )
        .unwrap();
    writer.sync().unwrap();
    drop(writer);
    restart_registry(&mut rig).await;
    let before = records(&rig, &key);
    assert!(matches!(
        retract_as(&rig, &key, reserved("ck-mc"), &id).await,
        Err(ServeError::ScopeRefused(_))
    ));
    assert!(rig
        .registry
        .authorize_session_retract(
            &key,
            reserved("owner").as_ref(),
            &broca_protocol::SendScope::Unstamped,
            Some(&id.0)
        )
        .await
        .is_ok());
    assert_eq!(records(&rig, &key), before);
    assert_eq!(
        rig.registry.slot_state(&key),
        None,
        "authorization never opens an actor"
    );
    let mut duplicate = before
        .into_iter()
        .find(|r| matches!(r, WalRecord::Queued { .. }))
        .unwrap();
    if let WalRecord::Queued { principal, .. } = &mut duplicate {
        *principal = reserved("ck-mc");
    }
    let mut writer = wal.open(&key).unwrap();
    writer.append(1, &duplicate, false).unwrap();
    writer.sync().unwrap();
    drop(writer);
    let before = records(&rig, &key);
    assert!(
        matches!(
            retract_as(&rig, &key, reserved("ck-mc"), &id).await,
            Err(ServeError::ScopeRefused(_))
        ),
        "conflicting attribution is not a sender grant"
    );
    assert_eq!(records(&rig, &key), before);
}

#[tokio::test]
async fn retract_own_send_shared_started_runs_resolve_status_and_index_per_sender() {
    let mut rig = Rig::new("retract-shared-runs", FakeExecutor::auto(), LONG_TTL);
    let key = session_naming_ck_mc(&rig, "runs", true).await;
    // The owner occupies the legacy ID; the delegated sender must not reuse its run index row.
    let mut owner = send(
        &key,
        "collision",
        Some(order(&[DeliveryMode::IdleStart], None)),
    );
    owner.request.principal = reserved("owner");
    owner.title = Some("owner".into());
    let owner_reply = rig.registry.send(owner).await.unwrap();
    assert!(
        matches!(owner_reply, SendResponse::Active { ref run_id, .. } if run_id == "collision")
    );
    wait_idle(&rig, &key).await;
    let mut sender = send(
        &key,
        "collision",
        Some(order(&[DeliveryMode::IdleStart], None)),
    );
    sender.request.principal = reserved("ck-mc");
    sender.title = Some("ck-mc".into());
    let reply = rig.registry.send(sender.clone()).await.unwrap();
    let SendResponse::Active { run_id, .. } = reply else {
        panic!("{reply:?}")
    };
    assert!(run_id.starts_with("collision-sender-"));
    wait_idle(&rig, &key).await;
    for restarted in [false, true] {
        if restarted {
            restart_registry(&mut rig).await;
        }
        let retry = rig.registry.send(sender.clone()).await.unwrap();
        assert!(matches!(retry, SendResponse::Finished { run_id: ref id, .. } if id == &run_id));
        for (principal, expected) in [("owner", "collision"), ("ck-mc", run_id.as_str())] {
            let status = rig
                .registry
                .send_status(
                    &key,
                    broca_protocol::RunId("collision".into()),
                    "send-collision",
                    reserved(principal).as_ref(),
                )
                .await
                .unwrap();
            assert_eq!(serde_json::to_value(status).unwrap()["run_id"], expected);
            let status = rig
                .registry
                .run_status(&key, broca_protocol::RunId(expected.into()))
                .await
                .unwrap();
            assert_eq!(serde_json::to_value(status).unwrap()["title"], principal);
            let indexed = broca_core::RunIndex::lookup_run(rig.run_index.as_ref(), expected)
                .unwrap()
                .unwrap();
            assert_eq!(indexed.run_id, expected);
            assert_eq!(indexed.session, key);
        }
    }
    assert_eq!(
        rig.exec.started_count(),
        3,
        "two distinct indexed runs, no retry executes again"
    );
}

fn send(
    key: &broca_protocol::BindIdentity,
    run: &str,
    order: Option<DeliveryOrder>,
) -> SendRequest {
    let req = req(key, run);
    let mut request = req.to_submission();
    // Plan sessions freeze tools at admission; later sends must inherit them by omission.
    request.tools = None;
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
        request: Box::new(request),
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
#[cfg(feature = "test-clock")]
#[tokio::test]
async fn an_expired_send_is_dropped_durably_and_never_delivered() {
    use std::sync::Arc;

    use broca_core::{FileLeaseStore, FileWalStore};
    use broca_module::retention::RetentionService;

    let (exec, gate) = FakeExecutor::gated_with_step_boundary();
    let rig = Rig::new("ordered-expiry", exec, LONG_TTL);
    let key = rig.key("s");
    let now = 1_000_000;
    std::fs::write(
        rig.dir.join("test-clock"),
        serde_json::json!({"now_ms": now}).to_string(),
    )
    .unwrap();
    // Ordered-send expiry uses the retention clock; freeze it before opening the actor so
    // admission cannot overtake the deadline while the machine is busy.
    let retention = Arc::new(
        RetentionService::new(
            Arc::new(FileWalStore::new(rig.dir.join("wal"))),
            rig.run_index.clone(),
            Arc::new(FileLeaseStore::new(rig.dir.join("leases"))),
            rig.run_index.clone(),
            Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1)),
        )
        .unwrap(),
    );
    rig.registry.set_retention_service(retention.clone());
    assert_eq!(retention.clock.now_ms(), now);
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
    std::fs::write(
        rig.dir.join("test-clock"),
        serde_json::json!({"now_ms": now + 400}).to_string(),
    )
    .unwrap();
    retention.clock.refresh().unwrap();
    assert_eq!(retention.clock.now_ms(), now + 400);
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
            rich_ui: None,
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
        model_check: None,
    }
}

fn reserved(module: &str) -> Option<BindPrincipal> {
    Some(BindPrincipal::Reserved {
        module_id: module.into(),
    })
}

async fn refresh_senders(
    rig: &Rig,
    key: &broca_protocol::BindIdentity,
    generation: u64,
    policy: &str,
    plan: broca_module::actor::SendPlan,
) {
    let mut manifest = (*plan.manifest.unwrap()).clone();
    let mut value: serde_json::Value = serde_json::from_str(manifest.plan.as_json_text()).unwrap();
    value["tool_items"] = serde_json::json!([]);
    value["system_text_items"] = serde_json::json!([]);
    value["step_transform_items"] = serde_json::json!([]);
    value["composition"] = serde_json::json!({"providers":[],"step_transform_items":[]});
    value["composition_digest"] = serde_json::json!(
        cortexkit_role_tool_provider::catalog::composition_digest(&value["composition"]).unwrap()
    );
    manifest.plan_identity = broca_fetch_plan::plan_identity(&value).unwrap();
    manifest.plan = broca_wal::OpaqueJson::from_json_text(value.to_string()).unwrap();
    let reply = rig
        .registry
        .route_session_refresh(key, generation, policy.into(), Box::new(manifest))
        .await
        .unwrap();
    assert!(
        matches!(
            reply,
            broca_module::actor::SessionRefreshOutcome::Accepted { .. }
        ),
        "{reply:?}"
    );
}

fn owner_template_plan(
    text: &str,
) -> (
    broca_module::actor::SendPlan,
    cortexkit_role_llm_runner::delivery_order::TemplateUse,
) {
    use cortexkit_role_llm_runner::delivery_order::{template_digest, Template, TemplateUse};
    let template = Template::new("notice", text, Default::default());
    let use_ = TemplateUse::new("notice", template_digest(&template), Default::default());
    let mut plan = plan_naming("owner", true);
    let manifest = std::sync::Arc::make_mut(plan.manifest.as_mut().unwrap());
    let mut value: serde_json::Value = serde_json::from_str(manifest.plan.as_json_text()).unwrap();
    value["senders"][0]["templates"] = serde_json::json!([template]);
    manifest.plan = broca_wal::OpaqueJson::from_json_text(value.to_string()).unwrap();
    (plan, use_)
}

fn prefield_sender_session(
    fail_repair: bool,
    refreshed: bool,
) -> (
    Rig,
    broca_protocol::BindIdentity,
    cortexkit_role_llm_runner::delivery_order::TemplateUse,
) {
    use broca_core::{Persister, WalStore};
    let mut rig = Rig::new("prefield-sender", FakeExecutor::auto(), LONG_TTL);
    let key = rig.key("prefield");
    let (mut old_plan, _) = owner_template_plan("OLD OWNER TEMPLATE");
    let old_manifest = std::sync::Arc::make_mut(old_plan.manifest.as_mut().unwrap());
    let mut old_value: serde_json::Value =
        serde_json::from_str(old_manifest.plan.as_json_text()).unwrap();
    old_value["senders"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({"module":"occipital","idle_start":true}));
    old_manifest.plan = broca_wal::OpaqueJson::from_json_text(old_value.to_string()).unwrap();
    let (next_plan, latest_template) = owner_template_plan("LATEST OWNER TEMPLATE");
    let started: WalRecord = serde_json::from_value(serde_json::json!({
        "type":"run_started", "run_id":"seed", "session":key,
        "principal":reserved("owner"), "manifest":old_plan.manifest.unwrap(),
        "config":{"model":{"provider_module_id":"fake","model_id":"fake"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},
        "input":[], "origin":{"kind":"fresh"}
    })).unwrap();
    let step: WalRecord = serde_json::from_value(serde_json::json!({
        "type":"model_step_finished", "step_id":1,
        "assistant_message":{"message_id":"seed-message","content":[{"type":"text","text":"seed"}]},
        "usage":{},"retries_used":0
    }))
    .unwrap();
    let finished: WalRecord = serde_json::from_value(
        serde_json::json!({"type":"run_finished","reason":"completed","usage":{},"retries_used":0}),
    )
    .unwrap();
    let wal_store = std::sync::Arc::new(broca_core::FileWalStore::new(rig.dir.join("wal")));
    let mut wal = wal_store.open(&key).unwrap();
    wal.append(1, &started, true).unwrap();
    if refreshed {
        wal.append(
            1,
            &WalRecord::SessionRefreshAccepted {
                generation: 1,
                policy: "on_prefix_rebuild".into(),
                manifest: Box::new((*next_plan.manifest.unwrap()).clone()),
                run_in_progress: true,
                applies_from_next_run: false,
            },
            true,
        )
        .unwrap();
    }
    wal.append(1, &step, true).unwrap();
    wal.append(
        1,
        &WalRecord::TurnFinished {
            step_id: broca_protocol::StepId(1),
        },
        true,
    )
    .unwrap();
    wal.append(1, &finished, true).unwrap();
    let safe_head = wal.replay().unwrap().last().unwrap().seq;
    drop(wal);
    let range = wal_store
        .read_records_range(&key, 0, Some(safe_head), None)
        .unwrap();
    Persister::project_range(rig.run_index.as_ref(), &key, &range).unwrap();
    let conn = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
    let encoded: String = conn
        .query_row("SELECT admission_checkpoint_json FROM meta", [], |row| {
            row.get(0)
        })
        .unwrap();
    let mut checkpoint: serde_json::Value = serde_json::from_str(&encoded).unwrap();
    checkpoint["checkpoint"]["ownership"]
        .as_object_mut()
        .unwrap()
        .remove("sender_admission");
    conn.execute(
        "UPDATE meta SET admission_checkpoint_json=?1",
        [checkpoint.to_string()],
    )
    .unwrap();
    if fail_repair {
        conn.execute_batch("CREATE TRIGGER fail_sender_repair BEFORE UPDATE ON meta BEGIN SELECT RAISE(FAIL, 'sender policy repair unavailable'); END;").unwrap();
    }
    let opener = std::sync::Arc::new(
        broca_module::DefaultOpener::new(
            std::sync::Arc::new(broca_core::FileLeaseStore::new(rig.dir.join("leases"))),
            wal_store.clone(),
            rig.exec.clone(),
        )
        .with_idle_ttl(LONG_TTL)
        .with_run_index(rig.run_index.clone())
        .with_materialized_store(rig.run_index.clone()),
    );
    rig.registry = broca_module::LineageRegistry::new_with_reader(
        opener,
        wal_store,
        Some(rig.run_index.clone()),
    );
    (rig, key, latest_template)
}

#[tokio::test]
async fn sender_policy_repair_failure_does_not_block_ordinary_owner() {
    let (rig, key, template) = prefield_sender_session(true, true);
    let mut owner = send(&key, "ordinary-owner", None);
    owner.request.principal = reserved("owner");
    assert!(matches!(
        rig.registry.send(owner).await.unwrap(),
        SendResponse::Active { .. }
    ));
    wait_idle(&rig, &key).await;
    for (module, template) in [("occipital", None), ("owner", Some(template))] {
        let mut delivery = order(&[DeliveryMode::IdleStart], None);
        delivery.template = template;
        let mut next = send(&key, &format!("needs-policy-{module}"), Some(delivery));
        next.request.principal = reserved(module);
        let before = records(&rig, &key).len();
        match rig.registry.send(next).await {
            Err(ServeError::Transient(message)) => {
                assert!(message.contains(module), "{message}");
                assert!(
                    message.contains("sender policy repair unavailable"),
                    "{message}"
                );
            }
            other => panic!("unknown sender policy must fail retryably: {other:?}"),
        }
        assert_eq!(records(&rig, &key).len(), before);
    }
    let mut owner = send(&key, "ordinary-owner-after-failure", None);
    owner.request.principal = reserved("owner");
    assert!(matches!(
        rig.registry.send(owner).await.unwrap(),
        SendResponse::Active { .. }
    ));
    wait_idle(&rig, &key).await;
}

#[tokio::test]
async fn no_refresh_prefield_policy_admits_named_sender_without_reprojection() {
    let (rig, key, _) = prefield_sender_session(true, false);
    let mut next = send(
        &key,
        "implicit-baseline-sender",
        Some(order(&[DeliveryMode::Steer], None)),
    );
    next.request.principal = reserved("occipital");
    assert!(matches!(
        rig.registry.send(next).await.unwrap(),
        SendResponse::Pending { .. }
    ));
}

#[tokio::test]
async fn owner_template_repairs_prefield_policy_once_and_uses_latest_text() {
    let (rig, key, template) = prefield_sender_session(false, true);
    for n in 0..2 {
        let mut delivery = order(&[DeliveryMode::IdleStart], None);
        delivery.template = Some(template.clone());
        let mut next = send(&key, &format!("owner-template-{n}"), Some(delivery));
        next.request.principal = reserved("owner");
        assert!(matches!(
            rig.registry.send(next).await.unwrap(),
            SendResponse::Active { .. }
        ));
        wait_idle(&rig, &key).await;
        if n == 0 {
            rusqlite::Connection::open(rig.dir.join("store.db")).unwrap().execute_batch("CREATE TRIGGER reject_second_repair BEFORE UPDATE ON meta BEGIN SELECT RAISE(FAIL, 'sender policy repaired twice'); END;").unwrap();
        }
    }
    let runs = records(&rig, &key)
        .into_iter()
        .filter_map(|record| match record {
            WalRecord::RunStarted { run_id, input, .. }
                if run_id.0.starts_with("owner-template-") =>
            {
                Some(serde_json::to_string(&input).unwrap())
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(runs.len(), 2);
    assert!(
        runs.iter()
            .all(|text| text.contains("LATEST OWNER TEMPLATE")
                && !text.contains("OLD OWNER TEMPLATE"))
    );
}

#[tokio::test]
async fn latest_accepted_senders_start_idle_immediately_and_after_restart() {
    let mut rig = Rig::new("refresh-sender-idle", FakeExecutor::auto(), LONG_TTL);
    let key = session_naming_ck_mc(&rig, "refresh-idle", false).await;
    for (generation, policy) in [
        (1, "on_prefix_rebuild"),
        (2, "on_system_rebuild"),
        (3, "on_history_rebuild"),
        (4, "immediately"),
    ] {
        refresh_senders(
            &rig,
            &key,
            generation,
            policy,
            plan_naming("occipital", true),
        )
        .await;
        if generation == 1 {
            restart_registry(&mut rig).await;
        }
        let page = rig
            .registry
            .route_read_page(&key, Some(0), Some(100), false)
            .await
            .unwrap();
        assert!(page
            .head_facts
            .sender(
                reserved("occipital").as_ref(),
                &broca_protocol::SendScope::Internal,
                &[DeliveryMode::IdleStart]
            )
            .is_some());
        let mut start = send(
            &key,
            &format!("refreshed-{generation}"),
            Some(order(&[DeliveryMode::IdleStart], None)),
        );
        start.request.principal = reserved("occipital");
        let reply = rig.registry.send(start).await.unwrap();
        assert!(matches!(reply, SendResponse::Active { .. }), "{reply:?}");
        wait_idle(&rig, &key).await;
        assert_eq!(
            rig.exec.seen.lock().unwrap().last().unwrap().principal,
            reserved("owner")
        );
    }
}

#[tokio::test]
async fn latest_accepted_senders_replace_pending_grants_and_preserve_admitted_queue() {
    let (exec, gate) = FakeExecutor::gated_with_step_boundary();
    let rig = Rig::new("refresh-sender-replace", exec, LONG_TTL);
    let key = rig.key("replace");
    let mut first = send(&key, "owner-first", None);
    first.request.principal = reserved("owner");
    first.plan = Some(plan_naming("ck-mc", false));
    rig.registry.send(first).await.unwrap();
    gate.release().await;
    wait_idle(&rig, &key).await;
    refresh_senders(
        &rig,
        &key,
        1,
        "on_prefix_rebuild",
        plan_naming("occipital", true),
    )
    .await;
    let mut queued = send(
        &key,
        "accepted-before-removal",
        Some(order(&[DeliveryMode::Steer], None)),
    );
    queued.request.principal = reserved("occipital");
    assert!(matches!(
        rig.registry.send(queued).await.unwrap(),
        SendResponse::Pending { .. }
    ));
    refresh_senders(
        &rig,
        &key,
        2,
        "on_prefix_rebuild",
        plan_naming("replacement", true),
    )
    .await;
    let before = records(&rig, &key).len();
    for ordered in [false, true] {
        let mut next = send(
            &key,
            &format!("removed-{ordered}"),
            ordered.then(|| order(&[DeliveryMode::IdleStart], None)),
        );
        next.request.principal = reserved("occipital");
        assert!(rig.registry.send(next).await.is_err());
    }
    assert_eq!(records(&rig, &key).len(), before);
    let page = rig
        .registry
        .route_read_page(&key, Some(0), Some(100), false)
        .await
        .unwrap();
    assert!(page
        .head_facts
        .sender(
            reserved("occipital").as_ref(),
            &broca_protocol::SendScope::Internal,
            &[DeliveryMode::IdleStart]
        )
        .is_none());
    let mut start = send(&key, "replacement-start", None);
    start.request.principal = reserved("replacement");
    assert!(matches!(
        rig.registry.send(start).await.unwrap(),
        SendResponse::Active { .. }
    ));
    gate.release().await;
    wait_idle(&rig, &key).await;
    assert!(records(&rig, &key).iter().any(|record| matches!(record, WalRecord::SendDelivered { submission_id, .. } if submission_id.0 == "sub-accepted-before-removal")));
}

#[tokio::test]
async fn latest_accepted_senders_without_idle_start_join_running_and_use_new_templates() {
    let (exec, gate) = FakeExecutor::gated_with_step_boundary();
    let rig = Rig::new("refresh-sender-join", exec, LONG_TTL);
    let key = rig.key("join");
    let mut first = send(&key, "owner-first", None);
    first.request.principal = reserved("owner");
    first.plan = Some(plan_naming("old", false));
    rig.registry.send(first).await.unwrap();
    gate.release().await;
    wait_idle(&rig, &key).await;
    let (plan, template) = template_fixture();
    refresh_senders(&rig, &key, 1, "on_prefix_rebuild", plan).await;
    for ordered in [false, true] {
        let mut start = send(
            &key,
            &format!("no-grant-{ordered}"),
            ordered.then(|| order(&[DeliveryMode::IdleStart], None)),
        );
        start.request.principal = reserved("ck-mc");
        assert!(matches!(
            rig.registry.send(start).await,
            Err(ServeError::InvalidParams { .. })
        ));
    }
    let mut owner = send(&key, "owner-running", None);
    owner.request.principal = reserved("owner");
    rig.registry.send(owner).await.unwrap();
    let mut delivery = order(&[DeliveryMode::Steer], None);
    delivery.template = Some(template);
    let mut join = send(&key, "new-template", Some(delivery));
    join.request.principal = reserved("ck-mc");
    let reply = rig.registry.send(join).await.unwrap();
    assert!(matches!(reply, SendResponse::Pending { .. }), "{reply:?}");
    gate.release().await;
    wait_idle(&rig, &key).await;
    assert!(records(&rig, &key).iter().any(|record| matches!(record, WalRecord::Queued { request, .. } if serde_json::to_string(&request.input).unwrap().contains("Context is 82% full."))));
}

#[tokio::test]
async fn latest_accepted_senders_control_string_interrupt_grants() {
    let rig = Rig::new("refresh-sender-interrupt", FakeExecutor::auto(), LONG_TTL);
    let key = session_naming_ck_mc(&rig, "interrupt", false).await;
    for (generation, granted) in [(1, true), (2, false)] {
        refresh_senders(
            &rig,
            &key,
            generation,
            "on_prefix_rebuild",
            plan_naming_with_interrupt("occipital", true, granted),
        )
        .await;
        let mut next = send(&key, &format!("interrupt-{generation}"), None);
        next.delivery = broca_protocol::Delivery::Interrupt;
        next.request.principal = reserved("occipital");
        let before = records(&rig, &key).len();
        let reply = rig.registry.send(next).await;
        if granted {
            assert!(matches!(reply.unwrap(), SendResponse::Active { .. }));
            wait_idle(&rig, &key).await;
        } else {
            assert!(matches!(reply, Err(ServeError::InvalidParams { .. })));
            assert_eq!(records(&rig, &key).len(), before);
        }
    }
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

/// [`plan_naming`] with the sender's `interrupt` grant set as given (llm-runner 0.5.2 §9.7).
fn plan_naming_with_interrupt(
    sender: &str,
    idle_start: bool,
    interrupt: bool,
) -> broca_module::actor::SendPlan {
    let mut plan = plan_naming(sender, idle_start);
    let manifest = std::sync::Arc::make_mut(plan.manifest.as_mut().unwrap());
    let mut value: serde_json::Value = serde_json::from_str(manifest.plan.as_json_text()).unwrap();
    value["senders"][0]["interrupt"] = serde_json::json!(interrupt);
    manifest.plan = broca_wal::OpaqueJson::from_json_text(value.to_string()).unwrap();
    manifest.plan_identity = format!("{}-interrupt-{interrupt}", manifest.plan_identity);
    plan.identity = manifest.plan_identity.clone();
    plan
}

/// The run `run_id` started: what its request carried, and the principal its `RunStarted`
/// recorded.
fn run_principals(
    rig: &Rig,
    key: &broca_protocol::BindIdentity,
    run_id: &str,
) -> (Option<BindPrincipal>, Option<BindPrincipal>) {
    let seen = rig.exec.seen.lock().unwrap().clone();
    let run = seen
        .iter()
        .find(|run| run.run_id == run_id)
        .unwrap_or_else(|| panic!("run {run_id} started: {seen:?}"));
    let recorded = records(rig, key)
        .into_iter()
        .find_map(|record| match record {
            WalRecord::RunStarted {
                run_id: started,
                principal,
                ..
            } if started.0 == run_id => Some(principal),
            _ => None,
        })
        .expect("the run's RunStarted");
    (run.principal.clone(), recorded)
}

/// A plan-named sender may send the legacy strings `queue` and `steer` (llm-runner 0.5.2 §9.7):
/// with the `idle_start` grant they are admitted as the lists `[idle_start]` and
/// `[steer, idle_start]` and start a run on an idle session, recorded with the sender on
/// `DeliveryOrdered`; without it they are refused naming `delivery` and write nothing. The run
/// keeps the session owner's principal, never the sender's.
#[tokio::test]
async fn a_named_senders_string_delivery_is_its_list_and_runs_under_the_owner() {
    let rig = Rig::new("named-string", FakeExecutor::auto(), LONG_TTL);
    for (delivery, run, modes) in [
        (
            broca_protocol::Delivery::Queue,
            "mc-queue",
            vec![DeliveryMode::IdleStart],
        ),
        (
            broca_protocol::Delivery::Steer,
            "mc-steer",
            vec![DeliveryMode::Steer, DeliveryMode::IdleStart],
        ),
    ] {
        let key = session_naming_ck_mc(&rig, run, true).await;
        let mut mc = send(&key, run, None);
        mc.delivery = delivery;
        mc.request.principal = reserved("ck-mc");
        let reply = rig.registry.send(mc).await.unwrap();
        assert!(
            matches!(reply, SendResponse::Active { ref run_id, .. } if run_id == run),
            "{reply:?}"
        );
        wait_idle(&rig, &key).await;
        assert_eq!(
            run_principals(&rig, &key, run),
            (reserved("owner"), reserved("owner")),
            "the sender's run keeps the owner's principal"
        );
        let ordered = records(&rig, &key)
            .into_iter()
            .find_map(|record| match record {
                WalRecord::DeliveryOrdered { order, sent_by, .. } => Some((order.modes, sent_by)),
                _ => None,
            })
            .expect("admitted as its list");
        assert_eq!(ordered, (modes, reserved("ck-mc")));

        let key = session_naming_ck_mc(&rig, &format!("{run}-no-grant"), false).await;
        let before = records(&rig, &key).len();
        let mut refused = send(&key, &format!("{run}-refused"), None);
        refused.delivery = delivery;
        refused.request.principal = reserved("ck-mc");
        match rig.registry.send(refused).await {
            Err(ServeError::InvalidParams { field, .. }) => assert_eq!(field, "delivery"),
            other => panic!("{delivery:?} without idle_start is refused: {other:?}"),
        }
        assert_eq!(records(&rig, &key).len(), before, "nothing written");
    }
}

/// A plan-named sender may send `interrupt` only with its entry's `interrupt` grant
/// (llm-runner 0.5.2 §9.7); without it the send is refused naming `delivery` and writes
/// nothing, even with the `idle_start` grant. A granted interrupt's run keeps the owner's
/// principal.
#[tokio::test]
async fn a_named_senders_interrupt_needs_the_grant_and_runs_under_the_owner() {
    let rig = Rig::new("named-interrupt", FakeExecutor::auto(), LONG_TTL);
    for granted in [false, true] {
        let key = rig.key(&format!("interrupt-{granted}"));
        let mut first = send(&key, &format!("owner-{granted}"), None);
        first.request.principal = reserved("owner");
        first.plan = Some(plan_naming_with_interrupt("ck-mc", true, granted));
        rig.registry.send(first).await.unwrap();
        wait_idle(&rig, &key).await;
        let before = records(&rig, &key).len();
        let run = format!("mc-interrupt-{granted}");
        let mut mc = send(&key, &run, None);
        mc.delivery = broca_protocol::Delivery::Interrupt;
        mc.request.principal = reserved("ck-mc");
        let reply = rig.registry.send(mc).await;
        if !granted {
            match reply {
                Err(ServeError::InvalidParams { field, .. }) => assert_eq!(field, "delivery"),
                other => panic!("an interrupt without the grant is refused: {other:?}"),
            }
            assert_eq!(records(&rig, &key).len(), before, "nothing written");
            continue;
        }
        let reply = reply.unwrap();
        assert!(
            matches!(reply, SendResponse::Active { ref run_id, .. } if *run_id == run),
            "{reply:?}"
        );
        wait_idle(&rig, &key).await;
        assert_eq!(
            run_principals(&rig, &key, &run),
            (reserved("owner"), reserved("owner")),
            "the granted interrupt's run keeps the owner's principal"
        );
    }
}
