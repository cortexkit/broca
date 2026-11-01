use super::*;
use broca_core::{MaterializedStore, Persister, RunIndex, RunIndexRow, RunIndexState, WalStore};
use broca_module::registry::{ActorHandle, LineageOpener, OpenError, RegistryHandle};
use broca_wal::access_counter::{ReadClass, WalAccessCounter};
use broca_wal::{FramedRecord, Wal, WalByteAnchor, WalError, WalRangeRead};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

#[derive(Clone, Copy, Debug)]
enum Policy {
    NeverRefreshed,
    Current,
    Old,
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Cost {
    reads: usize,
    bytes: u64,
    decodes: u64,
    origins: usize,
}

struct CountedWal {
    inner: broca_core::FileWalStore,
    counter: Mutex<WalAccessCounter>,
    opens: AtomicUsize,
}
impl CountedWal {
    fn reset(&self) {
        *self.counter.lock().unwrap() = WalAccessCounter::default();
    }
    fn measure<T>(&self, name: &str, read: impl FnOnce() -> T) -> T {
        self.counter
            .lock()
            .unwrap()
            .clone()
            .operation(name)
            .run(read)
    }
    fn cost(&self) -> Cost {
        let mut cost = Cost::default();
        for operation in self.counter.lock().unwrap().snapshot() {
            cost.decodes += operation.frames_decoded;
            for read in operation.reads {
                if read.class != ReadClass::LineageHeader && read.length > 0 {
                    cost.reads += 1;
                    cost.bytes += read.length;
                    cost.origins += usize::from(read.class == ReadClass::Origin);
                }
            }
        }
        cost
    }
}
impl WalStore for CountedWal {
    fn open(&self, key: &broca_protocol::BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.inner.open(key)
    }
    fn read_records(
        &self,
        key: &broca_protocol::BindIdentity,
    ) -> Result<Vec<FramedRecord>, WalError> {
        self.measure("full read", || self.inner.read_records(key))
    }
    fn read_records_range(
        &self,
        key: &broca_protocol::BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        self.measure("range", || {
            self.inner.read_records_range(key, after, through, anchor)
        })
    }
    fn read_records_range_for_authorization(
        &self,
        key: &broca_protocol::BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: Option<&WalByteAnchor>,
    ) -> Result<WalRangeRead, WalError> {
        self.measure("authorization", || {
            self.inner
                .read_records_range_for_authorization(key, after, through, anchor)
        })
    }
    fn file_identity(&self, key: &broca_protocol::BindIdentity) -> Result<Option<u64>, WalError> {
        self.inner.file_identity(key)
    }
    fn file_identity_for_authorization(
        &self,
        key: &broca_protocol::BindIdentity,
    ) -> Result<Option<u64>, WalError> {
        self.inner.file_identity_for_authorization(key)
    }
    fn flush_for_read(&self, key: &broca_protocol::BindIdentity) -> Result<Option<u64>, WalError> {
        self.inner.flush_for_read(key)
    }
    fn flush_for_authorization(
        &self,
        key: &broca_protocol::BindIdentity,
    ) -> Result<Option<u64>, WalError> {
        self.inner.flush_for_authorization(key)
    }
}

struct CountedOpener {
    inner: broca_module::DefaultOpener<broca_core::FileLeaseStore, CountedWal>,
    opens: AtomicUsize,
}
#[async_trait::async_trait]
impl LineageOpener for CountedOpener {
    async fn open(
        &self,
        key: &broca_protocol::BindIdentity,
        generation: u64,
        registry: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        self.opens.fetch_add(1, Ordering::SeqCst);
        self.inner.open(key, generation, registry).await
    }
}

struct Fixture {
    rig: Rig,
    key: broca_protocol::BindIdentity,
    wal: Arc<CountedWal>,
    opener: Arc<CountedOpener>,
    gate: common::RunGate,
    refusal: Mutex<Option<String>>,
}
impl Fixture {
    fn new(messages: usize, policy: Policy) -> Self {
        let (executor, gate) = FakeExecutor::gated();
        let mut rig = Rig::new(
            &format!("cancel-cost-{messages}-{policy:?}"),
            executor,
            LONG_TTL,
        );
        let key = rig.key("sender-cost");
        let wal = Arc::new(CountedWal {
            inner: broca_core::FileWalStore::new(rig.dir.join("wal")),
            counter: Mutex::new(WalAccessCounter::default()),
            opens: AtomicUsize::new(0),
        });
        let initial_grant = matches!(policy, Policy::NeverRefreshed);
        let manifest = |granted| {
            plan_naming_with_interrupt("phone", true, granted)
                .manifest
                .unwrap()
        };
        let initial = manifest(initial_grant);
        let start = |id: &str, input: Vec<broca_protocol::Message>| -> WalRecord {
            serde_json::from_value(serde_json::json!({
                "type":"run_started", "run_id":id, "session":key, "principal":reserved("owner"),
                "manifest":initial,
                "config":{"model":{"provider_module_id":"fake","model_id":"fake"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},
                "input":input, "origin":{"kind":"fresh"}
            })).unwrap()
        };
        let finish: WalRecord = serde_json::from_value(serde_json::json!({"type":"run_finished","reason":"completed","usage":{},"retries_used":0})).unwrap();
        let mut writer = wal.inner.open(&key).unwrap();
        writer
            .append(
                1,
                &start(
                    "history",
                    vec![
                        broca_protocol::Message::new(
                            broca_protocol::Role::User,
                            vec![broca_protocol::ContentBlock::Text {
                                text: "discarded historical message".into()
                            }]
                        );
                        messages
                    ],
                ),
                false,
            )
            .unwrap();
        writer.append(1, &finish, false).unwrap();
        writer.append(1, &start("seed", vec![]), false).unwrap();
        if !matches!(policy, Policy::NeverRefreshed) {
            writer
                .append(
                    1,
                    &WalRecord::SessionRefreshAccepted {
                        generation: 1,
                        policy: "on_prefix_rebuild".into(),
                        manifest: Box::new((*manifest(matches!(policy, Policy::Old))).clone()),
                        run_in_progress: true,
                        applies_from_next_run: false,
                    },
                    false,
                )
                .unwrap();
        }
        let step: WalRecord = serde_json::from_value(serde_json::json!({
            "type":"model_step_finished", "step_id":1,
            "assistant_message":{"message_id":"seed-message","content":[{"type":"text","text":"seed"}]},
            "usage":{},"retries_used":0
        })).unwrap();
        writer.append(1, &step, false).unwrap();
        writer
            .append(
                1,
                &WalRecord::TurnFinished {
                    step_id: broca_protocol::StepId(1),
                },
                false,
            )
            .unwrap();
        writer.append(1, &finish, true).unwrap();
        let head = writer.last_seq();
        drop(writer);
        let range = wal
            .inner
            .read_records_range(&key, 0, Some(head), None)
            .unwrap();
        Persister::project_range(rig.run_index.as_ref(), &key, &range).unwrap();
        // The coordinator owns lifecycle rows separately from transcript projection.
        rig.run_index
            .index_run_active(&RunIndexRow {
                run_id: "seed".into(),
                session: key.clone(),
                submission_id: None,
                request_identity_digest: None,
                state: RunIndexState::Active,
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
                title: None,
                withdrawals_owed: false,
            })
            .unwrap();
        rig.run_index
            .index_run_terminal(
                "seed",
                RunIndexState::Completed,
                Some(broca_protocol::Usage::default()),
                None,
                0,
            )
            .unwrap();
        if matches!(policy, Policy::Old) {
            let connection = rusqlite::Connection::open(rig.dir.join("store.db")).unwrap();
            let encoded: String = connection
                .query_row("SELECT admission_checkpoint_json FROM meta", [], |row| {
                    row.get(0)
                })
                .unwrap();
            let mut value: serde_json::Value = serde_json::from_str(&encoded).unwrap();
            value["checkpoint"]["ownership"]
                .as_object_mut()
                .unwrap()
                .remove("sender_admission");
            connection
                .execute(
                    "UPDATE meta SET admission_checkpoint_json=?1",
                    [value.to_string()],
                )
                .unwrap();
        }
        if matches!(policy, Policy::Current) {
            // The projected checkpoint is complete, but the newer accepted grant exists
            // only in the WAL record appended after that checkpoint.
            let mut writer = wal.inner.open(&key).unwrap();
            writer
                .append(
                    1,
                    &WalRecord::SessionRefreshAccepted {
                        generation: 2,
                        policy: "on_prefix_rebuild".into(),
                        manifest: Box::new((*manifest(true)).clone()),
                        run_in_progress: false,
                        applies_from_next_run: true,
                    },
                    true,
                )
                .unwrap();
        }
        let opener = Arc::new(CountedOpener {
            inner: broca_module::DefaultOpener::new(
                Arc::new(broca_core::FileLeaseStore::new(rig.dir.join("leases"))),
                wal.clone(),
                rig.exec.clone(),
            )
            .with_idle_ttl(LONG_TTL)
            .with_run_index(rig.run_index.clone())
            .with_materialized_store(rig.run_index.clone()),
            opens: AtomicUsize::new(0),
        });
        rig.registry = broca_module::LineageRegistry::new_with_reader(
            opener.clone(),
            wal.clone(),
            Some(rig.run_index.clone()),
        );
        Self {
            rig,
            key,
            wal,
            opener,
            gate,
            refusal: Mutex::new(None),
        }
    }
    fn store_bytes(&self) -> Vec<(String, Vec<u8>)> {
        ["store.db", "store.db-wal"]
            .into_iter()
            .map(|name| {
                (
                    name.into(),
                    std::fs::read(self.rig.dir.join(name)).unwrap_or_default(),
                )
            })
            .collect()
    }
    fn lease_bytes(&self) -> Vec<(std::path::PathBuf, Vec<u8>)> {
        fn visit(path: &std::path::Path, files: &mut Vec<(std::path::PathBuf, Vec<u8>)>) {
            let Ok(entries) = std::fs::read_dir(path) else {
                return;
            };
            for entry in entries {
                let entry = entry.unwrap();
                if entry.file_type().unwrap().is_dir() {
                    visit(&entry.path(), files);
                } else {
                    files.push((entry.path(), std::fs::read(entry.path()).unwrap()));
                }
            }
        }
        let mut files = Vec::new();
        visit(&self.rig.dir.join("leases"), &mut files);
        files.sort();
        files
    }
    async fn cancel(
        &self,
        sender: &str,
        run: &str,
    ) -> Result<broca_module::CancelResponse, ServeError> {
        self.rig
            .registry
            .cancel_from(
                &self.key,
                broca_protocol::RunId(run.into()),
                broca_wal::record::RunCancelAt::Now,
                reserved(sender).as_ref(),
                &broca_protocol::SendScope::Unstamped,
            )
            .await
    }
    async fn refused(&self, run: &str) -> Cost {
        let before = std::fs::read(self.wal.inner.wal_path(&self.key)).unwrap();
        let store = self.store_bytes();
        let leases = self.lease_bytes();
        let opens = self.opener.opens.load(Ordering::SeqCst);
        let wal_opens = self.wal.opens.load(Ordering::SeqCst);
        self.wal.reset();
        let error = self.cancel("outsider", run).await.unwrap_err();
        assert!(matches!(error, ServeError::ScopeRefused(_)));
        let rendered = format!("{error:?}");
        let mut refusal = self.refusal.lock().unwrap();
        if let Some(expected) = refusal.as_ref() {
            assert_eq!(
                &rendered, expected,
                "run state must not change an unauthorized caller's answer"
            );
        } else {
            *refusal = Some(rendered);
        }
        let cost = self.wal.cost();
        assert_eq!(
            self.opener.opens.load(Ordering::SeqCst),
            opens,
            "refused stop opened/recovered an actor"
        );
        assert_eq!(
            self.wal.opens.load(Ordering::SeqCst),
            wal_opens,
            "refused stop opened a writer"
        );
        assert_eq!(
            std::fs::read(self.wal.inner.wal_path(&self.key)).unwrap(),
            before
        );
        assert_eq!(self.store_bytes(), store, "refused stop changed store rows");
        assert_eq!(
            self.lease_bytes(),
            leases,
            "refused stop acquired a new lease epoch"
        );
        cost
    }
}

fn no_more_than(cancel: &Cost, send: &Cost) {
    assert!(
        cancel.reads <= send.reads && cancel.bytes <= send.bytes && cancel.decodes <= send.decodes,
        "cancel {cancel:?} exceeded sender admission {send:?}"
    );
}

#[tokio::test]
async fn interrupt_cancel_read_cost_matches_send_at_1k_and_150k_messages() {
    let mut costs = Vec::new();
    for messages in [1_000, 150_000] {
        for policy in [Policy::NeverRefreshed, Policy::Current] {
            let fixture = Fixture::new(messages, policy);
            let cold = fixture.refused("seed").await;
            assert_eq!(fixture.opener.opens.load(Ordering::SeqCst), 0);
            assert_eq!(cold.origins, 0, "cold {messages}/{policy:?}: {cold:?}");
            assert!(
                cold.bytes < 16_384,
                "cold sender lookup read history: {cold:?}"
            );
            fixture.wal.reset();
            assert_eq!(
                fixture.cancel("phone", "seed").await.unwrap(),
                broca_module::CancelResponse::NotActive
            );
            let cold_granted = fixture.wal.cost();
            assert_eq!(
                fixture.opener.opens.load(Ordering::SeqCst),
                1,
                "authorized cold stop uses the existing opener"
            );
            let mut owner = send(&fixture.key, "held", None);
            owner.request.principal = reserved("owner");
            assert!(matches!(
                fixture.rig.registry.send(owner).await.unwrap(),
                SendResponse::Active { .. }
            ));
            let refused = fixture.refused("held").await;
            let mut outsider = send(
                &fixture.key,
                "refused-send",
                Some(order(&[DeliveryMode::Steer], None)),
            );
            outsider.request.principal = reserved("outsider");
            fixture.wal.reset();
            assert!(matches!(
                fixture.rig.registry.send(outsider).await,
                Err(ServeError::DeliveryRefused { .. })
            ));
            let refused_send = fixture.wal.cost();
            no_more_than(&refused, &refused_send);
            let mut sender = send(
                &fixture.key,
                "granted-send",
                Some(order(&[DeliveryMode::Steer], None)),
            );
            sender.request.principal = reserved("phone");
            fixture.wal.reset();
            assert!(matches!(
                fixture.rig.registry.send(sender).await.unwrap(),
                SendResponse::Pending { .. }
            ));
            let granted_send = fixture.wal.cost();
            fixture.wal.reset();
            assert_eq!(
                fixture.cancel("phone", "held").await.unwrap(),
                broca_module::CancelResponse::Ack
            );
            let granted = fixture.wal.cost();
            no_more_than(&granted, &granted_send);
            assert_eq!(granted.origins, 0, "{granted:?}");
            assert!(
                granted.bytes < 16_384 && granted.decodes < 64,
                "granted stop read history: {granted:?}"
            );
            costs.push((
                cold.reads,
                cold.decodes,
                refused.reads,
                refused.decodes,
                granted.reads,
                granted.decodes,
            ));
            println!("messages={messages} policy={policy:?} cold={cold:?} cold_granted={cold_granted:?} refused={refused:?} refused_send={refused_send:?} granted={granted:?} granted_send={granted_send:?}");
            fixture.gate.release().await;
            wait_idle(&fixture.rig, &fixture.key).await;
            fixture.refused("missing-run").await;
            assert_eq!(
                fixture.cancel("phone", "missing-run").await.unwrap(),
                broca_module::CancelResponse::NotActive
            );
        }
    }
    assert_eq!(
        &costs[..2],
        &costs[2..],
        "discarded history must not affect stop reads or decodes"
    );
}

#[tokio::test]
async fn interrupt_cancel_old_fold_repair_is_read_only_and_cached_warm_and_cold() {
    for messages in [1_000, 150_000] {
        let fixture = Fixture::new(messages, Policy::Old);
        let cold = fixture.refused("seed").await;
        assert_eq!(
            cold.origins, 1,
            "only an old sender fold may require a full prefix: {cold:?}"
        );
        let cold_repeat = fixture.refused("unknown-run").await;
        assert_eq!(
            cold_repeat.origins, 0,
            "cold repair must remain in memory: {cold_repeat:?}"
        );
        assert_eq!(fixture.opener.opens.load(Ordering::SeqCst), 0);
        fixture.rig.registry.actor_for(&fixture.key).await.unwrap();
        let warm = fixture.refused("seed").await;
        assert_eq!(
            warm.origins, 1,
            "old warm policy repairs without publishing: {warm:?}"
        );
        let warm_repeat = fixture.refused("unknown-run").await;
        assert_eq!(
            warm_repeat.origins, 0,
            "warm repair must remain in memory: {warm_repeat:?}"
        );
        // Prime the owner's ordinary terminal reconciliation so the next measurement
        // isolates sender-policy reuse rather than the independent lifecycle lookup.
        assert_eq!(
            fixture.cancel("owner", "seed").await.unwrap(),
            broca_module::CancelResponse::NotActive
        );
        let before = std::fs::read(fixture.wal.inner.wal_path(&fixture.key)).unwrap();
        let store = fixture.store_bytes();
        fixture.wal.reset();
        assert_eq!(
            fixture.cancel("phone", "seed").await.unwrap(),
            broca_module::CancelResponse::NotActive
        );
        let granted = fixture.wal.cost();
        assert_eq!(
            granted.origins, 0,
            "the granted sender must reuse the read-only repair: {granted:?}"
        );
        assert_eq!(
            std::fs::read(fixture.wal.inner.wal_path(&fixture.key)).unwrap(),
            before
        );
        assert_eq!(fixture.store_bytes(), store);
        let snapshot = fixture
            .rig
            .run_index
            .read_admission_snapshot(&fixture.key)
            .unwrap()
            .unwrap();
        assert!(
            !snapshot
                .checkpoint
                .unwrap()
                .ownership
                .unwrap()
                .sender_admission_complete(),
            "a refused stop published its repair"
        );
        fixture.wal.reset();
        let mut outsider = send(
            &fixture.key,
            "old-fold-send",
            Some(order(&[DeliveryMode::Steer], None)),
        );
        outsider.request.principal = reserved("outsider");
        assert!(matches!(
            fixture.rig.registry.send(outsider).await,
            Err(ServeError::DeliveryRefused { .. })
        ));
        let send = fixture.wal.cost();
        no_more_than(&warm, &send);
        no_more_than(&granted, &send);
        println!("messages={messages} old cold={cold:?} cold_repeat={cold_repeat:?} warm={warm:?} warm_repeat={warm_repeat:?} granted={granted:?} send={send:?}");
    }
}
