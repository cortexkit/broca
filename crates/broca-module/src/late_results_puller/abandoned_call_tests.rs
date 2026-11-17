mod abandoned_call {
    use super::*;
    use crate::held_slots::{HeldRunResumer, HeldSlots, SlotDelivery};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct WakeProbe(AtomicUsize);

    #[async_trait]
    impl HeldRunResumer for WakeProbe {
        async fn resume_for_late_result(&self, _: &BindIdentity) -> Result<(), String> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err("an abandoned call must not wake its run".into())
        }
    }

    #[tokio::test]
    async fn late_settlement_of_abandoned_calls_changes_neither_the_session_nor_run_wakes() {
        for ended in [false, true] {
            let bind = session(if ended {
                "abandoned-ended"
            } else {
                "abandoned-open"
            });
            let key = "abandoned:4";
            let mut rig = Rig::new(
                if ended {
                    "abandoned-ended"
                } else {
                    "abandoned-open"
                },
                None,
                vec![registration(key, "tools", &bind)],
            )
            .await;
            // The call was dispatched and its answer is unknown: it was never held.
            let mut records = held_records(key, "tools");
            records.truncate(3);
            records.push(outcome("run", "b", "a", CallOutcome::Unknown));
            if ended {
                records.push(run_end("run", RunFinishReason::Interrupted));
            }
            rig.seed(&bind, &records).await;
            let before = rig.records(&bind).await;
            let slots = HeldSlots::new();
            let wake = Arc::new(WakeProbe::default());
            let mappings = Arc::new(AtomicUsize::new(0));
            let seen = mappings.clone();
            let delivery = Arc::new(SlotDelivery::new(
                slots.clone(),
                wake.clone(),
                Arc::new(move |id, _| {
                    seen.fetch_add(1, Ordering::SeqCst);
                    Some(broca_core::outcome_unknown_result(id))
                }),
            ));
            rig.intake =
                LateResultsIntake::new(rig.store.clone(), "broca", delivery, rig.feed.clone());
            rig.intake.set_session_store(Arc::clone(&rig.runtime));
            let _puller = rig.puller("tools");
            let mut first =
                entry(kinds::NOT_STARTED, key, "scope-ended-1").with_reason("scope_ended");
            first.result = None;
            let registered = rig.store.registrations().unwrap();
            assert_eq!(registered.len(), 1);
            assert_eq!(registered[0].module, "tools");
            assert_eq!(registered[0].call_key, key);
            assert_eq!(first.custodian, rig.intake.custodian());
            assert!(scope_matches(&registered[0], &first));
            assert!(settles_a_call(&first));
            assert!(rig
                .store
                .receipt(&first.intake_key("tools"))
                .unwrap()
                .is_none());
            let mut second = first.clone();
            second.event_id = "scope-ended-2".into();
            rig.feed.fail_ack_once();
            rig.feed.replies(vec![
                reply(vec![first.clone(), second.clone()], 2, false),
                reply(vec![first.clone(), second], 2, false),
            ]);
            let _lost_ack = rig.intake.tick("tools").await;
            assert_eq!(
                rig.store.receipt(&first.intake_key("tools")).unwrap(),
                Some(Disposition::NoOp)
            );
            rig.intake.tick("tools").await.unwrap();
            assert_eq!(rig.feed.acks(), vec![cursor(2)]);
            assert_eq!(mappings.load(Ordering::SeqCst), 0);
            assert_eq!(wake.0.load(Ordering::SeqCst), 0);
            assert!(slots.is_empty());
            // Nothing was written: the call keeps its one intent and its unknown terminal.
            assert_eq!(rig.records(&bind).await, before);
        }
    }
}
