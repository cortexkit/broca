//! One-shot actor lifetime is the same through both real registries, including idle holders.

use super::*;

const LONG_IDLE: Duration = Duration::from_secs(3_600);

async fn opened(steps: Vec<Vec<NormalizedEvent>>, gate: Option<Arc<Notify>>) -> Module {
    module_options(steps, None, None, LONG_IDLE, gate).await
}

async fn send(module: &Module, run_id: &str, text: &str, one_shot: bool) -> SendOutcome {
    module
        .registry
        .route_submission_with_materialization(
            &module.key,
            RunId(run_id.into()),
            module.caller.shape(run_id, &SendKey::RunId, request(text)),
            RunKind::FreshOrResume,
            SendKey::RunId,
            one_shot,
        )
        .await
        .expect("the send routes")
}

async fn idle(module: &Module) {
    broca_test_support::wait_until("the run to finish or pause", || async {
        let status = module.registry.route_status(&module.key).await.unwrap();
        if status == LineageStatus::Idle {
            Ok(())
        } else {
            Err(format!("{status:?}"))
        }
    })
    .await;
}

async fn closed(module: &Module) {
    broca_test_support::wait_until("the actor to close", || {
        std::future::ready(match module.registry.slot_state(&module.key) {
            None => Ok(()),
            slot => Err(format!("slot={slot:?}")),
        })
    })
    .await;
}

#[tokio::test]
async fn a_one_shot_closes_at_its_terminal_and_reads_back_alike() {
    let mut pages = Vec::new();
    {
        let module = opened(hello(), None).await;
        assert!(matches!(
            send(&module, "r1", "first", true).await,
            SendOutcome::Active { .. }
        ));
        closed(&module).await;
        {
            let session = module.load().await;
            assert!(session.operations.run.as_ref().unwrap().finished.is_some());
            assert_eq!(module.store_texts().await, ["first", "hello"]);
        }
        let (page, _) = read_json(&module, None, None, None).await;
        assert_eq!(page["messages"].as_array().unwrap().len(), 2);
        pages.push(page);
        assert!(module.registry.stop_and_seal().await);
    }
    broca_test_support::pin!("closed sessions still read alike", pages[0]);
}

#[tokio::test]
async fn an_ordinary_run_waits_for_idle_close_alike() {
    {
        let module = module_options(hello(), None, None, Duration::from_secs(1), None).await;
        assert!(matches!(
            send(&module, "r1", "first", false).await,
            SendOutcome::Active { .. }
        ));
        idle(&module).await;
        assert_eq!(module.registry.slot_state(&module.key), Some("open"));
        closed(&module).await;
        assert!(module.registry.stop_and_seal().await);
    }
}

#[tokio::test]
async fn a_live_subscriber_holds_a_one_shot_open_alike() {
    {
        let gate = Arc::new(tokio::sync::Notify::new());
        let module = module_options(
            hello(),
            None,
            None,
            Duration::from_secs(1),
            Some(Arc::clone(&gate)),
        )
        .await;
        assert!(matches!(
            send(&module, "r1", "first", true).await,
            SendOutcome::Active { .. }
        ));
        // The subscriber attaches while the run's model step waits.
        let (sink, deliveries) = mpsc::channel(64);
        module
            .registry
            .route_subscribe(&module.key, crate::fanout::FromCursor::Start, sink)
            .await
            .expect("a subscriber attaches to the session");
        gate.notify_one();
        idle(&module).await;
        assert_eq!(module.registry.slot_state(&module.key), Some("open"));
        assert_eq!(module.registry.idle_holds(&module.key), ["live_subscriber"]);
        drop(deliveries);
        // A blocked one-shot close is not armed again when the subscriber leaves.
        idle(&module).await;
        assert_eq!(module.registry.slot_state(&module.key), Some("open"));
        closed(&module).await;
        assert!(module.registry.stop_and_seal().await);
    }
}

#[tokio::test]
async fn queued_work_holds_a_one_shot_open_and_keeps_its_own_policy_alike() {
    {
        for queued_one_shot in [false, true] {
            let gate = Arc::new(Notify::new());
            let module = opened(hello(), Some(gate.clone())).await;
            assert!(matches!(
                send(&module, "r1", "first", true).await,
                SendOutcome::Active { .. }
            ));
            assert!(matches!(
                send(&module, "r2", "second", queued_one_shot).await,
                SendOutcome::Pending { .. }
            ));
            gate.notify_one();
            broca_test_support::wait_until("the queued run to start before close", || async {
                let status = module.registry.route_status(&module.key).await.unwrap();
                if matches!(&status, LineageStatus::Active { run_id, .. } if run_id.0 == "r2") {
                    Ok(())
                } else {
                    Err(format!("{status:?}"))
                }
            })
            .await;
            assert_eq!(module.registry.slot_state(&module.key), Some("open"));
            gate.notify_one();
            if queued_one_shot {
                closed(&module).await;
            } else {
                idle(&module).await;
                assert_eq!(module.registry.slot_state(&module.key), Some("open"));
            }
            assert!(module.registry.stop_and_seal().await);
            {
                assert_eq!(
                    module.store_texts().await,
                    ["first", "hello", "second", "again"]
                );
            }
        }
    }
}

#[tokio::test]
async fn a_one_shot_pause_stays_open_and_its_resume_keeps_the_hint_alike() {
    {
        let mut steps = vec![auth_failure()];
        steps.extend(hello());
        let module = opened(steps, None).await;
        assert!(matches!(
            send(&module, "r1", "first", true).await,
            SendOutcome::Active { .. }
        ));
        idle(&module).await;
        assert_eq!(module.registry.slot_state(&module.key), Some("open"));
        // The one-shot policy survives a same-process retry even when it omits the hint.
        assert!(matches!(
            send(&module, "r1", "first", false).await,
            SendOutcome::Active { .. }
        ));
        closed(&module).await;
        {
            assert_eq!(module.store_texts().await, ["first", "hello"]);
        }
        assert!(module.registry.stop_and_seal().await);
    }
}
