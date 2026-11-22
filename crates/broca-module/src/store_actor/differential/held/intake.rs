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
        Self::with_store(side, key, store)
    }

    fn with_store(side: &Side, key: String, store: Arc<LateResultsStore>) -> Self {
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
        let intake = LateResultsIntake::new(store.clone(), "broca", delivery.clone(), feed.clone());
        intake.set_session_store(side.registry.store_runtime().unwrap());
        intake.set_registry(Arc::new(side.registry.clone()));
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

#[tokio::test]
async fn encrypted_store_hold_survives_lost_late_results_database() {
    let initial = Side::new(
        true,
        Cipher::XChaCha20Poly1305,
        vec![super::super::super::tests::read_step("m1")],
    )
    .await;
    initial.send().await;
    initial.idle().await;
    let original = Intake::new(&initial).await;
    let registrations = original.store.registrations().unwrap();
    let key = original.key.clone();
    drop(original);
    std::fs::remove_file(initial.dir.join("late-results.db")).unwrap();

    let side = initial.restarted().await;
    let store = Arc::new(LateResultsStore::open(&side.dir.join("late-results.db")).unwrap());
    let rig = Intake::with_store(&side, key, store);
    rig.intake.set_rebuilding(true);
    rig.tick().await;
    assert_eq!(rig.delivery.calls.load(Ordering::SeqCst), 0);
    assert!(rig.feed.acks.lock().unwrap().is_empty());
    let mut report = crate::late_results_puller::RebuildReport::default();
    crate::late_results_puller::rebuild_store_from_sessions_with_progress(
        &rig.store,
        &side.dir,
        Some(side.registry.store_runtime().unwrap().as_ref()),
        &mut report,
        |_| {},
    )
    .await
    .unwrap();
    assert!(report.unreadable.is_empty());
    assert_eq!(rig.store.registrations().unwrap(), registrations);
    rig.tick().await;
    assert_eq!(rig.delivery.calls.load(Ordering::SeqCst), 0);
    assert!(rig.feed.acks.lock().unwrap().is_empty());
    rig.store.finish_rebuild().unwrap();
    rig.intake.set_rebuilding(false);
    rig.tick().await;
    side.idle().await;
    assert_eq!(rig.delivery.calls.load(Ordering::SeqCst), 1);
    assert_eq!(side.result().await["state"], "completed");
    assert_eq!(side.executor.tools.0.load(Ordering::SeqCst), 1);
    assert_eq!(side.executor.provider.inner.calls().len(), 1);
    assert_eq!(rig.feed.acks.lock().unwrap().len(), 1);
    assert!(initial.registry.stop_and_seal().await);
    assert!(side.registry.stop_and_seal().await);
}

#[tokio::test]
async fn store_rebuild_omits_completed_and_cancelled_runs() {
    for cancel in [false, true] {
        let side = Side::new(
            true,
            Cipher::XChaCha20Poly1305,
            vec![
                super::super::super::tests::read_step("m1"),
                super::super::super::tests::text_step("m2", "done"),
            ],
        )
        .await;
        side.send().await;
        side.idle().await;
        if cancel {
            assert_eq!(
                side.registry
                    .route_cancel(&side.key, RunId("r1".into()))
                    .await
                    .unwrap(),
                CancelOutcome::Ack
            );
            cancelled(&side).await;
        } else {
            let rig = Intake::new(&side).await;
            rig.tick().await;
            side.idle().await;
            assert_eq!(side.result().await["state"], "completed");
        }
        let store = LateResultsStore::open(&side.dir.join("rebuilt.db")).unwrap();
        let mut report = crate::late_results_puller::RebuildReport::default();
        crate::late_results_puller::rebuild_store_from_sessions_with_progress(
            &store,
            &side.dir,
            Some(side.registry.store_runtime().unwrap().as_ref()),
            &mut report,
            |_| {},
        )
        .await
        .unwrap();
        assert_eq!(report.sessions, 1);
        assert!(report.unreadable.is_empty());
        assert!(store.registrations().unwrap().is_empty());
        assert!(side.registry.stop_and_seal().await);
    }
}

async fn seed_rebuild_inventory(side: &Side, count: usize) -> serde_json::Value {
    use broca_session_store::{
        backend::{LocalBackend, SessionAddr},
        record::Record,
        session::Session,
    };
    let runtime = side.registry.store_runtime().unwrap();
    let view = runtime.current_view(&side.key).await.unwrap().unwrap();
    let mut template = view
        .ledger_tail_until(|record| matches!(record, Record::RunStart(_)))
        .await
        .unwrap();
    template.reverse();
    let scope = json!({
        "owner": { "kind": "reserved", "module_id": "owner" },
        "ref": "original-scope", "scope_epoch": 42,
        "flow_id": "flow", "agent_id": "agent", "run_id": "agent-run",
        "sender": { "kind": "reserved", "module_id": "carrier" }, "agent_id_frozen": true
    });
    for index in 0..count {
        let bind = BindIdentity::new(
            &side.key.project_root,
            &side.key.harness,
            format!("synthetic-{index}"),
        );
        let address = broca_core::session_addr(&bind);
        let key = format!("{address}:11");
        let class = broca_wal::StorageClass::ALL[index % 3];
        let mut session = Session::create(
            LocalBackend::new(class.store_root(&side.dir)),
            SessionAddr(address),
            crate::store_open::triple(&bind),
            runtime.deployment(),
            false,
        )
        .await
        .unwrap();
        let records: Vec<_> = template
            .iter()
            .filter_map(|(_, record)| match record {
                Record::RunStart(start) => {
                    let mut start = start.clone();
                    start.scope = Some(scope.clone());
                    Some(Record::RunStart(start))
                }
                Record::BatchStart(batch) => {
                    let mut batch = batch.clone();
                    batch.mid = None;
                    Some(Record::BatchStart(batch))
                }
                Record::ToolIntent(intent) => {
                    let mut intent = intent.clone();
                    intent.call_key = Some(key.clone());
                    Some(Record::ToolIntent(intent))
                }
                Record::ToolHeld(held) if index % 2 == 0 => {
                    let mut held = held.clone();
                    held.call_key = key.clone();
                    Some(Record::ToolHeld(held))
                }
                _ => None,
            })
            .collect();
        session.append(&records, true).await.unwrap();
        session.seal(None, 1).await.unwrap();
        session.close().await.unwrap();
    }
    scope
}

#[tokio::test]
async fn store_rebuild_covers_every_class_intents_and_frozen_scope() {
    let side = Side::new(
        true,
        Cipher::XChaCha20Poly1305,
        vec![super::super::super::tests::read_step("m1")],
    )
    .await;
    side.send().await;
    side.idle().await;
    let scope = seed_rebuild_inventory(&side, 6).await;
    let store = LateResultsStore::open(&side.dir.join("rebuilt.db")).unwrap();
    let mut report = crate::late_results_puller::RebuildReport::default();
    crate::late_results_puller::rebuild_store_from_sessions_with_progress(
        &store,
        &side.dir,
        Some(side.registry.store_runtime().unwrap().as_ref()),
        &mut report,
        |_| {},
    )
    .await
    .unwrap();
    assert!(report.unreadable.is_empty());
    assert_eq!(report.sessions, 7);
    assert_eq!(report.registered, 7);
    let calls = store.registrations().unwrap();
    for index in 0..6 {
        let call = calls
            .iter()
            .find(|call| call.bind.session == format!("synthetic-{index}"))
            .unwrap();
        assert_eq!(
            call.call_key,
            format!("{}:11", broca_core::session_addr(&call.bind))
        );
        assert_eq!(call.lineage, broca_core::session_addr(&call.bind));
        assert_eq!(call.module, "tools");
        assert_eq!(serde_json::to_value(&call.scope).unwrap(), scope);
    }
    assert!(store.feed("tools").unwrap().unwrap().ack_owed);
    assert!(side.registry.stop_and_seal().await);
}

#[tokio::test]
async fn store_rebuild_unreadable_head_or_missing_runtime_keeps_scan_incomplete() {
    let side = Side::new(
        true,
        Cipher::XChaCha20Poly1305,
        vec![super::super::super::tests::read_step("m1")],
    )
    .await;
    side.send().await;
    side.idle().await;
    let store = LateResultsStore::open(&side.dir.join("rebuilt.db")).unwrap();
    let mut missing_runtime = crate::late_results_puller::RebuildReport::default();
    crate::late_results_puller::rebuild_store_from_sessions_with_progress(
        &store,
        &side.dir,
        None,
        &mut missing_runtime,
        |_| {},
    )
    .await
    .unwrap();
    assert_eq!(missing_runtime.unreadable.len(), 1);
    assert!(store.registrations().unwrap().is_empty());
    let runtime = side.registry.store_runtime().unwrap();
    let head = broca_wal::session_storage_head(
        &side.dir,
        runtime.class_of(&side.key).unwrap().unwrap(),
        &broca_core::session_addr(&side.key),
    );
    std::fs::write(&head, b"unreadable head").unwrap();
    let mut unreadable = crate::late_results_puller::RebuildReport::default();
    crate::late_results_puller::rebuild_store_from_sessions_with_progress(
        &store,
        &side.dir,
        Some(&runtime),
        &mut unreadable,
        |_| {},
    )
    .await
    .unwrap();
    assert_eq!(unreadable.unreadable, [head]);
    assert!(store.registrations().unwrap().is_empty());
    assert!(side.registry.stop_and_seal().await);
}

#[tokio::test]
#[ignore = "synthetic rebuild cost measurement"]
async fn store_rebuild_cost_on_three_hundred_encrypted_sessions() {
    let side = Side::new(
        true,
        Cipher::XChaCha20Poly1305,
        vec![super::super::super::tests::read_step("m1")],
    )
    .await;
    side.send().await;
    side.idle().await;
    seed_rebuild_inventory(&side, 300).await;
    let store = LateResultsStore::open(&side.dir.join("rebuilt.db")).unwrap();
    let mut report = crate::late_results_puller::RebuildReport::default();
    let started = std::time::Instant::now();
    crate::late_results_puller::rebuild_store_from_sessions_with_progress(
        &store,
        &side.dir,
        Some(side.registry.store_runtime().unwrap().as_ref()),
        &mut report,
        |_| {},
    )
    .await
    .unwrap();
    eprintln!(
        "store rebuild: sessions={} registered={} unreadable={} elapsed_ms={}",
        report.sessions,
        report.registered,
        report.unreadable.len(),
        started.elapsed().as_millis()
    );
    assert_eq!(report.sessions, 301);
    assert_eq!(report.registered, 301);
    assert!(report.unreadable.is_empty());
    assert!(side.registry.stop_and_seal().await);
}

cipher_tests!(
    new_storage_feed_resumes_held_run_and_duplicate_is_noop,
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

/// While a session's actor is open, the intake reads its held calls from the actor's memory and
/// loads no view of the session: a running session moves between pulls, so a view loaded for one
/// pull would be stale by the next and load again.
#[tokio::test]
async fn open_session_held_calls_are_read_from_its_actor() {
    let side = Side::new(
        true,
        Cipher::XChaCha20Poly1305,
        vec![
            super::super::super::tests::read_step("m1"),
            super::super::super::tests::text_step("m2", "done"),
        ],
    )
    .await;
    side.send().await;
    side.idle().await;
    let rig = Intake::new(&side).await;
    assert!(
        side.registry.open_handle(&side.key).is_some(),
        "the held run's actor stays open"
    );
    let runtime = side.registry.store_runtime().unwrap();
    let loads = || runtime.views().memory(|_| 0).loads.unwrap();
    let before = loads();
    assert!(
        rig.intake.module_holds_calls("tools").await.unwrap(),
        "the held call keeps the feed"
    );
    let request = crate::store_reads::StoreReadRequest::HeldCalls {
        call_keys: vec![rig.key.clone()],
    };
    let held = |reply| match reply {
        Some(crate::store_reads::StoreReadReply::HeldCalls(Ok(held))) => format!("{held:?}"),
        _ => panic!("the session's held calls must read"),
    };
    let from_actor = held(
        runtime
            .read_own(Some(&side.registry), &side.key, &request)
            .await
            .unwrap(),
    );
    assert_eq!(
        loads(),
        before,
        "the open actor answers, so no view of the session loads"
    );
    // The actor's answer is the one the session as stored gives.
    let as_stored = held(runtime.read_own(None, &side.key, &request).await.unwrap());
    assert_eq!(from_actor, as_stored);
    let before = loads();
    rig.tick().await;
    assert_eq!(
        rig.delivery.calls.load(Ordering::SeqCst),
        1,
        "the feed delivers the held answer"
    );
    assert_eq!(
        loads(),
        before,
        "the pull classifies the entry from the open actor too"
    );
    side.idle().await;
    assert_eq!(side.result().await["state"], "completed");
    assert!(side.registry.stop_and_seal().await);
}

#[tokio::test]
async fn encrypted_cancelled_held_run_settles_late_feed_without_delivery_alike() {
    use broca_session_store::record::{Record, Stream};

    let mut observations = Vec::new();
    for store in [true] {
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
        let withdrawals = HeldWithdrawals::new(rig.store.clone(), transport.clone());
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
        {
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
        }
        let before_feed = position(&side).await;

        // Publish the late answer only after the run is sealed and the provider has
        // acknowledged tool.withdraw for the held call.
        rig.feed.entries.lock().unwrap().push(entry);
        let ((), logs) = broca_test_support::capture_logs(rig.tick()).await;
        {
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
    broca_test_support::pin!("alike", observations[0]);
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
    // The side's registry still holds the session open under a store that reads it. A store
    // that can't read the session never has an actor open on it, so the intake reads it as
    // stored, through the store that can't.
    rig.intake.forget_registry();
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
