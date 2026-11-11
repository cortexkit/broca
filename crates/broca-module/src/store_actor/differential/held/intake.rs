//! Exercise the provider feed all the way through classification, slots and the store actor.

use super::cancel::{cancelled, position, terminal, Withdrawals};
use super::*;
use crate::actor::CancelOutcome;
use crate::held_slots::SlotDelivery;
use crate::held_withdrawals::HeldWithdrawals;
use crate::late_results_puller::{
    FeedError, FeedReply, HeldDelivery, LateResultsFeed, LateResultsIntake,
};
use crate::late_results_store::LateResultsStore;
use broca_core::tool_plane::{LateResultRegistration, LateResultsIntake as _};
use cortexkit_role_tool_provider::late_results::{Cursor, LateEntry};
use std::sync::Mutex;
use tokio_util::sync::CancellationToken;

struct Feed {
    entries: Mutex<Vec<LateEntry>>,
    acks: Mutex<Vec<Cursor>>,
}

#[async_trait]
impl LateResultsFeed for Feed {
    async fn pull(
        &self,
        _: &str,
        _: Option<&Cursor>,
        _: Option<u32>,
    ) -> Result<FeedReply, FeedError> {
        Ok(FeedReply {
            entries: self.entries.lock().unwrap().clone(),
            cursor: Cursor {
                provider_incarnation: "provider".into(),
                seq: 1,
            },
            more: false,
        })
    }

    async fn ack(&self, _: &str, through: &Cursor) -> Result<(), FeedError> {
        self.acks.lock().unwrap().push(through.clone());
        Ok(())
    }
}

struct Delivery {
    inner: SlotDelivery,
    calls: AtomicUsize,
}

#[async_trait]
impl HeldDelivery for Delivery {
    async fn deliver(
        &self,
        registration: &LateResultRegistration,
        id: &ToolCallId,
        entry: &LateEntry,
    ) -> Result<(), String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.inner.deliver(registration, id, entry).await
    }
}

struct Intake {
    intake: Arc<LateResultsIntake>,
    feed: Arc<Feed>,
    delivery: Arc<Delivery>,
    store: Arc<LateResultsStore>,
    key: String,
}

impl Intake {
    async fn new(side: &Side) -> Self {
        let status = side.status().await;
        let key = status["held_calls"][0]["call_key"]
            .as_str()
            .unwrap()
            .to_owned();
        Self::for_key(side, key).await
    }

    async fn for_key(side: &Side, key: String) -> Self {
        let store = Arc::new(LateResultsStore::open(&side.dir.join("late-results.db")).unwrap());
        store
            .register_group(&[LateResultRegistration {
                lineage: key.rsplit_once(':').unwrap().0.into(),
                call_key: key.clone(),
                module: "tools".into(),
                bind: side.key.clone(),
                scope: None,
            }])
            .await
            .unwrap();
        let feed = Arc::new(Feed {
            entries: Mutex::new(vec![
                LateEntry::new(
                    "result",
                    "reserved:broca",
                    "ref",
                    1,
                    "reserved:broca",
                    &key,
                    "event-1",
                    1,
                ),
                LateEntry::new(
                    "result",
                    "reserved:broca",
                    "ref",
                    1,
                    "reserved:broca",
                    &key,
                    "event-2",
                    1,
                ),
            ]),
            acks: Mutex::new(Vec::new()),
        });
        let delivery = Arc::new(Delivery {
            inner: SlotDelivery::new(
                side.executor.slots.clone(),
                Arc::new(side.registry.clone()),
                Arc::new(|id, _| {
                    Some(ToolResult {
                        tool_call_id: id.clone(),
                        output: ToolOutput::Text {
                            text: "late answer".into(),
                        },
                        is_error: false,
                    })
                }),
            ),
            calls: AtomicUsize::new(0),
        });
        let intake = LateResultsIntake::new(
            store.clone(),
            side.executor.wal.clone(),
            "broca",
            delivery.clone(),
            feed.clone(),
        );
        intake.set_session_store(side.registry.store_runtime().unwrap());
        Self {
            intake,
            feed,
            delivery,
            store,
            key,
        }
    }

    async fn tick(&self) {
        self.intake.tick_for_test("tools").await.unwrap();
    }
}

cipher_tests!(
    new_storage_feed_resumes_held_run_and_duplicate_is_noop,
    legacy_storage_feed_resumes_held_run,
    new_storage_ended_run_does_not_deliver_into_later_run
);

async fn resumes(store: bool, cipher: Cipher) {
    let initial = Side::new(
        store,
        cipher,
        vec![super::super::super::tests::read_step("m1")],
    )
    .await;
    initial.send().await;
    initial.idle().await;
    let side = initial.restarted().await;
    side.registry.route_status(&side.key).await.unwrap();
    side.idle().await;
    let rig = Intake::new(&side).await;
    assert_eq!(
        rig.intake.sweep_registrations().await.unwrap(),
        0,
        "an open held registration must survive retirement"
    );
    rig.tick().await;
    assert_eq!(
        rig.delivery.calls.load(Ordering::SeqCst),
        1,
        "the feed must deliver the held answer"
    );
    side.idle().await;
    assert_eq!(side.result().await["state"], "completed");
    assert_eq!(
        side.executor.tools.0.load(Ordering::SeqCst),
        1,
        "the held tool must never be dispatched again"
    );
    assert_eq!(
        side.executor.provider.inner.calls().len(),
        1,
        "the model continues once after the answer"
    );
    assert_eq!(rig.feed.acks.lock().unwrap().len(), 1);
    // Acked receipts are pruned. A re-served event must be rejected by durable call facts,
    // including a different event id for the same call key in the same feed page.
    rig.feed.entries.lock().unwrap().push(LateEntry::new(
        "result",
        "reserved:broca",
        "ref",
        1,
        "reserved:broca",
        &rig.key,
        "event-3",
        1,
    ));
    rig.tick().await;
    assert_eq!(rig.delivery.calls.load(Ordering::SeqCst), 1);
    assert_eq!(side.executor.provider.inner.calls().len(), 1);
    assert_eq!(rig.feed.acks.lock().unwrap().len(), 2);
    assert_eq!(
        rig.intake.sweep_registrations().await.unwrap(),
        usize::from(store)
    );
    assert!(rig.store.registration_rows().unwrap().is_empty());
    assert!(initial.registry.stop_and_seal().await);
    assert!(side.registry.stop_and_seal().await);
}

async fn new_storage_feed_resumes_held_run_and_duplicate_is_noop(cipher: Cipher) {
    resumes(true, cipher).await;
}

async fn legacy_storage_feed_resumes_held_run(cipher: Cipher) {
    resumes(false, cipher).await;
}

#[tokio::test]
async fn encrypted_cancelled_held_run_settles_late_feed_without_delivery_alike() {
    use broca_session_store::record::{Record, Stream};
    use broca_wal::WalRecord;

    let mut observations = Vec::new();
    for store in [false, true] {
        let side = Side::new(
            store,
            Cipher::XChaCha20Poly1305,
            vec![super::super::super::tests::read_step("m1")],
        )
        .await;
        side.send().await;
        side.idle().await;
        assert_eq!(side.status().await["state"], "active");
        let rig = Intake::new(&side).await;
        let entries = std::mem::take(&mut *rig.feed.entries.lock().unwrap());
        let entry = entries.into_iter().next().unwrap();
        let transport = Arc::new(Withdrawals::default());
        let withdrawals = HeldWithdrawals::new(
            rig.store.clone(),
            side.executor.wal.clone(),
            transport.clone(),
        );
        withdrawals.bind_stopping_signal(CancellationToken::new());
        withdrawals.with_store_runtime(side.registry.store_runtime().unwrap());
        side.executor.slots.with_withdrawals(withdrawals.clone());

        assert_eq!(
            side.registry
                .route_cancel(&side.key, RunId("r1".into()))
                .await
                .unwrap(),
            CancelOutcome::Ack
        );
        cancelled(&side).await;
        broca_test_support::wait_until(
            "held withdrawal completes before the late answer",
            || async {
                let state = rig.store.withdrawal_state("tools", &rig.key).unwrap();
                if state == Some(("done".into(), 1)) {
                    Ok(())
                } else {
                    Err(format!("{state:?}: {}", withdrawals.health()))
                }
            },
        )
        .await;
        assert_eq!(
            transport.0.lock().unwrap().as_slice(),
            [(rig.key.clone(), false)]
        );
        assert!(rig.store.registration("tools", &rig.key).unwrap().is_some());
        let before = (
            side.status().await,
            side.result().await,
            terminal(&side).await,
        );
        assert_eq!(before.0["state"], "cancelled");
        assert_eq!(before.1["state"], "cancelled");
        assert_eq!(before.2 .0, "cancelled");
        assert_eq!(
            before.2 .1,
            [json!({"outcome": "interrupted", "reason": "provider_held_cancelled"})]
        );
        if store {
            let view = side
                .registry
                .store_runtime()
                .unwrap()
                .current_view(&side.key)
                .await
                .unwrap()
                .unwrap();
            let records = view.records_from(0).await.unwrap();
            assert_eq!(
                records
                    .iter()
                    .filter(|(_, record)| matches!(record, Record::BatchEnd(_)))
                    .count(),
                1
            );
            let end = records
                .iter()
                .rposition(
                    |(_, record)| matches!(record, Record::RunEnd(end) if end.run_id == "r1"),
                )
                .expect("cancelled run has a durable end");
            let tail: Vec<_> = records[end + 1..]
                .iter()
                .map(|(seq, record)| (*seq, record.name(), record.stream()))
                .collect();
            eprintln!("new-storage records after RunEnd for r1: {tail:?}");
            // Entry and turn facts summarize the transcript, not new run events.
            assert!(
                records[end + 1..].iter().all(|(_, record)| matches!(
                    record,
                    Record::EntryFacts(_) | Record::TurnFacts(_)
                )),
                "only derived transcript facts may follow RunEnd for r1; tail={tail:?}"
            );
            let last_ledger = records
                .iter()
                .rfind(|(_, record)| record.stream() == Stream::Ledger);
            assert!(
                matches!(last_ledger, Some((_, Record::RunEnd(end))) if end.run_id == "r1"),
                "last ledger record must be RunEnd for r1; tail={tail:?}"
            );
        } else {
            let records = side.executor.wal.read_records(&side.key).unwrap();
            assert_eq!(
                records
                    .iter()
                    .filter(|frame| matches!(frame.record, WalRecord::ToolBatchFinished { .. }))
                    .count(),
                1
            );
            assert!(matches!(
                records.last().map(|frame| &frame.record),
                Some(WalRecord::RunFinished { .. })
            ));
        }
        let before_feed = position(&side).await;

        // Publish the late answer only after the run is sealed and the provider has
        // acknowledged tool.withdraw for the held call.
        rig.feed.entries.lock().unwrap().push(entry);
        let ((), logs) = broca_test_support::capture_logs(rig.tick()).await;
        if store {
            assert!(
                logs.iter().any(|event| {
                    event.target == "late_results"
                        && event.fields.get("call_key") == Some(&rig.key)
                        && matches!(
                            event.message(),
                            "late-results entry settled without delivery: run_cancelling"
                                | "late-results entry settled without delivery: already_terminal"
                        )
                }),
                "{logs:?}"
            );
        }
        side.idle().await;
        assert_eq!(rig.delivery.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            rig.feed.acks.lock().unwrap().as_slice(),
            [Cursor {
                provider_incarnation: "provider".into(),
                seq: 1,
            }]
        );
        assert_eq!(
            position(&side).await,
            before_feed,
            "no record after the run end"
        );
        assert_eq!(
            (
                side.status().await,
                side.result().await,
                terminal(&side).await,
            ),
            before
        );
        assert!(side.executor.slots.is_empty());
        assert_eq!(side.executor.tools.0.load(Ordering::SeqCst), 1);
        assert_eq!(side.executor.provider.inner.calls().len(), 1);
        rig.intake.sweep_registrations().await.unwrap();
        assert!(rig.store.registration_rows().unwrap().is_empty());
        assert!(!rig.intake.module_needs_feed("tools").await.unwrap());
        observations.push((before, rig.delivery.calls.load(Ordering::SeqCst)));
        assert!(side.registry.stop_and_seal().await);
    }
    assert_eq!(observations[1], observations[0]);
}

async fn new_storage_ended_run_does_not_deliver_into_later_run(cipher: Cipher) {
    use broca_session_store::backend::{LocalBackend, SessionAddr};
    use broca_session_store::record::{Record, RunEnd};
    let initial = Side::new(
        true,
        cipher,
        vec![super::super::super::tests::read_step("m1")],
    )
    .await;
    initial.send().await;
    initial.idle().await;
    let key = initial.status().await["held_calls"][0]["call_key"]
        .as_str()
        .unwrap()
        .to_owned();
    let side = initial
        .restarted_with_steps(vec![super::super::super::tests::read_step("m2")])
        .await;
    // Seed an ended run with an unanswered call on the restart copy before any actor opens it.
    // No terminal can hide the run-ended check, and the later run reuses the tool call id.
    let runtime = side.registry.store_runtime().unwrap();
    let class = runtime.class_of(&side.key).unwrap().unwrap();
    let mut session = Session::load(
        LocalBackend::new(class.store_root(&side.dir)),
        SessionAddr(broca_core::session_addr(&side.key)),
        &crate::store_open::triple(&side.key),
        runtime.deployment(),
    )
    .await
    .unwrap();
    session
        .append(
            &[Record::RunEnd(RunEnd {
                run_id: "r1".into(),
                reason: broca_protocol::RunFinishReason::Cancelled,
                usage: None,
                ts_ms: None,
                retries_used: 0,
                interrupt_reason: None,
                error: None,
            })],
            true,
        )
        .await
        .unwrap();
    drop(session);
    let rig = Intake::for_key(&side, key).await;
    assert_eq!(side.result().await["state"], "cancelled");
    let mut next = request("next");
    next.tools = None;
    let outcome = side
        .registry
        .route_submission_with_order(
            &side.key,
            RunId("r2".into()),
            next,
            RunKind::AppendEpisode,
            SendKey::Minted,
            false,
            broca_protocol::Delivery::Queue,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
        )
        .await
        .unwrap();
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    side.idle().await;
    rig.tick().await;
    assert_eq!(rig.delivery.calls.load(Ordering::SeqCst), 0);
    assert_eq!(rig.feed.acks.lock().unwrap().len(), 1);
    let status = side
        .registry
        .run_status(&side.key, RunId("r2".into()))
        .await
        .unwrap();
    let status = serde_json::to_value(status).unwrap();
    assert_eq!(
        status["held_calls"].as_array().unwrap().len(),
        1,
        "the later run stays held on its own key"
    );
    assert_ne!(
        status["held_calls"][0]["call_key"].as_str().unwrap(),
        rig.key
    );
    assert_eq!(rig.intake.sweep_registrations().await.unwrap(), 1);
    assert!(initial.registry.stop_and_seal().await);
    assert!(side.registry.stop_and_seal().await);
}

#[tokio::test]
async fn unreadable_new_storage_blocks_feed_and_retirement() {
    let side = Side::new(
        true,
        Cipher::XChaCha20Poly1305,
        vec![super::super::super::tests::read_step("m1")],
    )
    .await;
    side.send().await;
    side.idle().await;
    let rig = Intake::new(&side).await;
    let unloaded = crate::store_open::StoreRuntime::open_without_key(
        &side.dir,
        1,
        Cipher::XChaCha20Poly1305,
        side.executor.clone(),
        IDLE,
    )
    .await
    .unwrap();
    rig.intake.set_session_store(Arc::new(unloaded));
    rig.tick().await;
    assert_eq!(rig.delivery.calls.load(Ordering::SeqCst), 0);
    assert!(rig.feed.acks.lock().unwrap().is_empty());
    assert!(rig
        .store
        .receipt(&rig.feed.entries.lock().unwrap()[0].intake_key("tools"))
        .unwrap()
        .is_none());
    assert_eq!(rig.intake.sweep_registrations().await.unwrap(), 0);
    assert_eq!(rig.store.registration_rows().unwrap().len(), 1);
    assert!(rig.intake.module_needs_feed("tools").await.unwrap());
    assert!(side.registry.stop_and_seal().await);
}
