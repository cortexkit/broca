//! Idle maintenance uses the recorded real request and durable warm spend across actor eviction.

use super::*;
use broca_session_loop::writer::Writer as _;
use std::sync::atomic::{AtomicU64, Ordering};

mod account_move;
mod billing_recovery;

cipher_tests!(
    idle_head_uses_hour_lifetime_and_stops_at_durable_cap,
    idle_warm_rebuilds_after_restart_without_changing_prompt_state,
    idle_warm_failure_and_no_cache_read_stop,
    idle_warm_never_arms_off_non_head_or_one_shot,
    idle_real_run_cancels_timer_and_end_rearms,
    idle_warm_recognises_an_adopted_head,
);

async fn idle_warm_recognises_an_adopted_head(cipher: Cipher) {
    let side = super::super::side(false, cipher).await;
    let mut actor = head(
        &side,
        cipher,
        Arc::new(Warming::default()),
        converted("finished-head-unclassed"),
        Arc::new(AtomicU64::new(crate::actor::epoch_ms())),
    )
    .await
    .with_class_placement(broca_wal::StorageClass::Durable, vec!["head".into()]);
    assert_eq!(actor.idle_warm_due().await.unwrap_err(), "not_auto_head");
    let mut start = actor
        .session
        .operations()
        .owner
        .first_start
        .clone()
        .unwrap();
    start.run_id = "adopting".into();
    start.session_class = actor.session_class(Some("head")).unwrap();
    let end = broca_session_store::record::RunEnd {
        run_id: "adopting".into(),
        reason: broca_protocol::RunFinishReason::Completed,
        usage: None,
        ts_ms: None,
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    };
    actor
        .session
        .append(
            &[Record::RunStart(Box::new(start)), Record::RunEnd(end)],
            true,
        )
        .await
        .unwrap();
    assert_eq!(actor.frozen_session_class().as_deref(), Some("head"));
    // The adopted head and its route's declared cache lifetime pass eligibility. Only the
    // new run's missing prefix-token usage keeps idle warming from arming.
    assert_eq!(
        actor.idle_warm_due().await.unwrap_err(),
        "prefix_tokens_unavailable"
    );
    side.registry.stop_and_seal().await;
}

async fn head(
    side: &super::super::Side,
    cipher: Cipher,
    executor: Arc<Warming>,
    history: Converted,
    clock: Arc<AtomicU64>,
) -> StoreActor<LocalBackend> {
    reopened(side, history, cipher, executor)
        .await
        .with_clock(Arc::new(move || clock.load(Ordering::SeqCst)))
}

async fn warm(actor: &mut StoreActor<LocalBackend>) -> crate::actor::WarmOutcome {
    let (reply, result) = tokio::sync::oneshot::channel();
    actor.warm(None, reply, true).await;
    finish_media(actor).await;
    if let Some(pending) = actor.warm.as_mut() {
        let sent = (&mut pending.task).await;
        actor.finish_warm(sent).await;
    }
    result.await.unwrap()
}

async fn idle_head_uses_hour_lifetime_and_stops_at_durable_cap(cipher: Cipher) {
    let side = super::super::side(false, cipher).await;
    let history = converted("finished-head");
    let clock = Arc::new(AtomicU64::new(crate::actor::epoch_ms()));
    let executor = Arc::new(Warming {
        clock: Some(clock.clone()),
        ..Default::default()
    });
    let mut actor = head(&side, cipher, executor.clone(), history, clock.clone()).await;
    let sent = actor
        .session
        .operations()
        .last_cache_send
        .as_ref()
        .unwrap()
        .sent_at_ms;
    assert_eq!(actor.idle_warm_due().await.unwrap(), sent + 3_570_000);
    actor.arm_idle_warm().await;
    assert!(side.registry.handle().has_idle_warm(&bind()));
    for n in 1..=2 {
        clock.store(sent + n * 3_570_000, Ordering::SeqCst);
        assert!(matches!(
            warm(&mut actor).await,
            crate::actor::WarmOutcome::Warmed(_)
        ));
    }
    assert_eq!(actor.idle_warm_due().await.unwrap_err(), "cap");
    assert!(!side.registry.handle().has_idle_warm(&bind()));
    assert_eq!(executor.sent.lock().unwrap().len(), 2);
    // A skipped warm cannot date the cache: expiry still follows the last successful send.
    let last = actor.session.operations().last_cache_send.as_ref().unwrap();
    assert!(broca_core::compaction_v2::conditions::cache_proven_expired(
        last.requested_ttl_ms,
        Some(last.sent_at_ms),
        sent + 2 * 3_570_000 + 3_600_000
    ));
    side.registry.stop_and_seal().await;
}

async fn idle_warm_rebuilds_after_restart_without_changing_prompt_state(cipher: Cipher) {
    let side = super::super::side(false, cipher).await;
    let history = converted("finished-head");
    let clock = Arc::new(AtomicU64::new(crate::actor::epoch_ms()));
    let executor = Arc::new(Warming {
        clock: Some(clock.clone()),
        ..Default::default()
    });
    let mut actor = head(&side, cipher, executor.clone(), history, clock.clone()).await;
    let export = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
    let billing = crate::store_billing::StoreBilling {
        index: export.clone(),
        resolver: Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::MeteredApi, 4)),
    };
    actor.billing = Some(billing.clone());
    actor.bill_owed().await;
    assert!(
        actor.session.operations().billing_owed.is_empty(),
        "the real run was already settled"
    );
    // Converted history already carries Billed; seed the export as the original process did.
    billing
        .insert(&actor.session, &bind(), &actor.parts, "r1")
        .await
        .unwrap();
    let real_facts = export.read_export_page(None, 1000).unwrap().facts;
    let original = actor.recorded_warm_request().await.unwrap();
    let before = actor.session.operations().clone();
    let conversation = format!("{:?}", actor.session.conversation());
    clock.store(clock.load(Ordering::SeqCst) + 3_570_000, Ordering::SeqCst);
    assert!(matches!(
        warm(&mut actor).await,
        crate::actor::WarmOutcome::Warmed(_)
    ));
    assert_eq!(
        broca_core::render::request_digest_hex(&executor.sent.lock().unwrap()[0]),
        original.request_body_sha256
    );
    assert_eq!(actor.session.operations().estimate, before.estimate);
    assert_eq!(actor.session.operations().view_applied, before.view_applied);
    assert_eq!(format!("{:?}", actor.session.conversation()), conversation);
    let last = actor.session.operations().last_cache_send.as_ref().unwrap();
    assert!(
        !broca_core::compaction_v2::conditions::cache_proven_expired(
            last.requested_ttl_ms,
            Some(last.sent_at_ms),
            clock.load(Ordering::SeqCst) + 30_001
        )
    );
    let warms: Vec<_> = actor
        .session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .filter_map(|(_, r)| match r {
            Record::Warmed(w) => Some(w),
            _ => None,
        })
        .collect();
    let w = &warms[0];
    assert_eq!(w.trigger.as_deref(), Some("keep_warm"));
    assert_eq!(w.step_id, 2);
    assert_eq!(w.requested_cache_ttl_ms, Some(3_600_000));
    assert!(w.attempt_id.is_some());
    assert!(w.billing.as_ref().unwrap().parse()["finished_at_ms"].is_u64());
    let first_facts = export.read_export_page(None, 1000).unwrap().facts;
    assert_eq!(
        first_facts.len(),
        real_facts.len() + 1,
        "a late warm inserts its own spend fact"
    );
    let warm_fact = first_facts
        .iter()
        .find(|fact| {
            serde_json::to_value(fact.segment.as_session().unwrap()).unwrap()["usage"]
                ["cached_input_tokens"]
                == 1000
        })
        .expect("the warm's measured cache reads are billed");
    assert_eq!(
        serde_json::to_value(warm_fact.segment.as_session().unwrap()).unwrap()["run_id"],
        "r1"
    );
    let parts = actor.parts.clone();
    drop(actor);
    side.registry.handle().stop_idle_warm(&bind(), "shutdown");
    let root = side.dir.join("resumed-store");
    let session = Session::load(
        LocalBackend::new(&root),
        SessionAddr(broca_core::session_addr(&bind())),
        &SessionTriple {
            project_root: "/project".into(),
            harness: "store-actor".into(),
            session: "s1".into(),
        },
        &DeploymentKey::new(1, &[7; 32]).creating(cipher),
    )
    .await
    .unwrap();
    let mut restarted = StoreActor::new(bind(), session, executor, Box::new(parts))
        .with_readers(Arc::new(move || LocalBackend::new(&root)));
    let reboot = super::super::side(false, cipher).await;
    restarted.registry = Some(reboot.registry.handle());
    restarted = restarted.with_clock(Arc::new(move || clock.load(Ordering::SeqCst)));
    restarted.billing = Some(billing.clone());
    restarted.bill_recorded_idle_warms().await;
    assert_eq!(
        export.read_export_page(None, 1000).unwrap().facts.len(),
        first_facts.len(),
        "restart insertion is idempotent"
    );
    restarted.arm_idle_warm().await;
    assert!(reboot.registry.handle().has_idle_warm(&bind()));
    // Model a crash after the next warm's durable record but before its export insert.
    restarted.billing = None;
    assert!(matches!(
        warm(&mut restarted).await,
        crate::actor::WarmOutcome::Warmed(_)
    ));
    assert_eq!(
        export.read_export_page(None, 1000).unwrap().facts.len(),
        first_facts.len()
    );
    restarted.billing = Some(billing);
    restarted.bill_recorded_idle_warms().await;
    assert_eq!(
        export.read_export_page(None, 1000).unwrap().facts.len(),
        real_facts.len() + 2,
        "startup recovery inserts a missing late-warm fact without new billing debt"
    );
    assert_eq!(restarted.idle_warm_due().await.unwrap_err(), "cap");
    reboot.registry.stop_and_seal().await;
    side.registry.stop_and_seal().await;
}

async fn idle_real_run_cancels_timer_and_end_rearms(cipher: Cipher) {
    let side = super::super::side(false, cipher).await;
    let history = converted("finished-head");
    let clock = Arc::new(AtomicU64::new(crate::actor::epoch_ms()));
    let mut store = super::super::super::executor(
        vec![crate::store_actor::tests::text_step("next", "done")],
        None,
    );
    store.provider.render_config = Some(serde_json::to_value(warmable()).unwrap());
    store.provider.notes_sends = true;
    let executor = Arc::new(Warming {
        store: Some(store),
        ..Default::default()
    });
    let mut actor = head(&side, cipher, executor, history, clock).await;
    actor.arm_idle_warm().await;
    assert!(side.registry.handle().has_idle_warm(&bind()));
    let mut start = actor
        .recovery_start(actor.session.operations().run.as_ref().unwrap())
        .unwrap();
    start.run_id = RunId("r2".into());
    start.request = super::super::super::request("again");
    start.request.model = broca_protocol::ModelHandle::new("anthropic", "claude-opus-4-8");
    start.request.keep_warm = Some(broca_protocol::request::KeepWarm::Auto);
    start.session_class = Some("head".into());
    actor
        .start_run(start, None, None, StoreRunMode::Fresh)
        .await;
    assert!(!side.registry.handle().has_idle_warm(&bind()));
    actor.run_current_to_end().await;
    assert_eq!(
        actor.session.operations().run.as_ref().unwrap().run_id,
        "r2"
    );
    assert!(side.registry.handle().has_idle_warm(&bind()));
    side.registry.stop_and_seal().await;
}

async fn idle_warm_failure_and_no_cache_read_stop(cipher: Cipher) {
    for no_cache in [false, true] {
        let side = super::super::side(false, cipher).await;
        let history = converted("finished-head");
        let clock = Arc::new(AtomicU64::new(crate::actor::epoch_ms()));
        let executor = Arc::new(Warming {
            clock: Some(clock.clone()),
            no_cache,
            fail: !no_cache,
            ..Default::default()
        });
        let mut actor = head(&side, cipher, executor, history, clock.clone()).await;
        actor.arm_idle_warm().await;
        assert!(side.registry.handle().has_idle_warm(&bind()));
        let sent = actor
            .session
            .operations()
            .last_cache_send
            .as_ref()
            .unwrap()
            .sent_at_ms;
        clock.store(sent + 3_570_000, Ordering::SeqCst);
        let result = warm(&mut actor).await;
        if no_cache {
            assert!(matches!(result, crate::actor::WarmOutcome::Warmed(_)));
            assert_eq!(actor.idle_warm_due().await.unwrap_err(), "no_cache_read");
        } else {
            assert!(!matches!(result, crate::actor::WarmOutcome::Warmed(_)));
            let last = actor.session.operations().last_cache_send.as_ref().unwrap();
            assert!(broca_core::compaction_v2::conditions::cache_proven_expired(
                last.requested_ttl_ms,
                Some(last.sent_at_ms),
                sent + 3_600_000
            ));
        }
        assert!(!side.registry.handle().has_idle_warm(&bind()));
        side.registry.stop_and_seal().await;
    }
}

async fn idle_warm_never_arms_off_non_head_or_one_shot(cipher: Cipher) {
    for case in ["off", "non_head", "one_shot"] {
        let side = super::super::side(false, cipher).await;
        let history = converted(match case {
            "off" => "finished-head-keep-warm-off",
            "non_head" => "finished-head-unclassed",
            _ => "finished-head",
        });
        let mut actor = head(
            &side,
            cipher,
            Arc::new(Warming::default()),
            history,
            Arc::new(AtomicU64::new(crate::actor::epoch_ms())),
        )
        .await;
        if case == "one_shot" {
            actor.one_shot_runs.insert(RunId("r1".into()));
        }
        actor.arm_idle_warm().await;
        assert!(!side.registry.handle().has_idle_warm(&bind()), "{case}");
        side.registry.stop_and_seal().await;
    }
}
