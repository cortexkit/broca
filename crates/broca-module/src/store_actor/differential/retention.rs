//! Retained creating sends, read refusals, recreation and actor deadlines through both modules.

use super::*;
use crate::retention::RetentionService;
use crate::serve::{ServeError, ServeHandler, SessionReadRequest};
use broca_session_store::keys::{Cipher, DeploymentKey};

const NOW: u64 = 1_700_000_000_000;
const SECONDS: u64 = 60;
const TTL: Duration = Duration::from_secs(600);

struct Retained {
    module: Module,
    service: Arc<RetentionService>,
    runtime: Arc<crate::store_open::StoreRuntime>,
    executor: Arc<dyn RunExecutor>,
}

async fn retained(on: bool, cipher: Cipher, idle: Duration, gate: Option<Arc<Notify>>) -> Retained {
    let dir = scratch_dir();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("test-clock"),
        serde_json::json!({"now_ms": NOW}).to_string(),
    )
    .unwrap();
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let leases = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let index = Arc::new(broca_store::SqliteStore::open(dir.join("run-index.db")).unwrap());
    let service = Arc::new(
        RetentionService::new(
            wal.clone(),
            index.clone(),
            leases.clone(),
            index,
            Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1)),
        )
        .unwrap(),
    );
    let steps = [hello(), hello()].concat();
    let executor: Arc<dyn RunExecutor> = Arc::new(BothLoops {
        today: TodayLoop {
            provider: Gated {
                inner: broca_core::provider::StubProvider::new(steps.clone()),
                gate: gate.clone(),
                render_config: None,
                notes_sends: false,
                hold: None,
            },
            wal: wal.clone(),
            planes: Default::default(),
        },
        store: executor(steps, gate),
    });
    let runtime = Arc::new(
        crate::store_open::StoreRuntime::open(
            &dir,
            DeploymentKey::new(1, &[7; 32]).creating(cipher),
            executor.clone(),
            idle,
        )
        .await
        .unwrap(),
    );
    let registry = LineageRegistry::new_with_reader(
        Arc::new(
            crate::opener::DefaultOpener::new(leases, wal.clone(), executor.clone())
                .with_idle_ttl(idle)
                .with_store_runtime(runtime.clone()),
        ),
        wal.clone(),
        None,
    );
    registry.set_retention_service(service.clone());
    let registry = if on {
        registry.with_store_new_sessions()
    } else {
        registry
    };
    let key = BindIdentity::new(dir.join("project"), "retention-differential", "s1");
    Retained {
        module: Module {
            dir,
            registry,
            wal,
            key,
            caller: Default::default(),
        },
        service,
        runtime,
        executor,
    }
}

impl Retained {
    fn advance(&self, now: u64) {
        std::fs::write(
            self.module.dir.join("test-clock"),
            serde_json::json!({"now_ms": now}).to_string(),
        )
        .unwrap();
        assert!(self.service.clock.refresh().unwrap());
    }

    async fn send(
        &self,
        run: &str,
        retention: Option<u64>,
    ) -> Result<SendOutcome, crate::registry::OpenError> {
        self.send_with_plan(run, retention, None).await
    }

    async fn send_with_plan(
        &self,
        run: &str,
        retention: Option<u64>,
        plan: Option<crate::actor::SendPlan>,
    ) -> Result<SendOutcome, crate::registry::OpenError> {
        self.module
            .registry
            .route_submission_with_retention(
                &self.module.key,
                RunId(run.into()),
                self.module
                    .caller
                    .shape(run, &SendKey::Minted, request("hello")),
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                plan,
                None,
                None,
                retention,
                false,
            )
            .await
    }

    async fn idle(&self) {
        broca_test_support::wait_until("retained run finished", || async {
            match self.module.registry.route_status(&self.module.key).await {
                Ok(LineageStatus::Idle) => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
    }

    async fn deleted(&self) {
        broca_test_support::wait_until("retained actor deleted its session", || async {
            let held = self.runtime.holds(&self.module.key).unwrap();
            let wal = self
                .module
                .wal
                .ephemeral_wal_path(&self.module.key)
                .exists();
            if self.module.registry.slot_state(&self.module.key).is_none() && !held && !wal {
                Ok(())
            } else {
                Err(format!("head={held}, wal={wal}"))
            }
        })
        .await;
    }

    async fn expired_read(&self, deadline: u64) {
        assert_eq!(crate::retention::EXPIRED, "expired");
        let error = self
            .module
            .registry
            .read(SessionReadRequest::new(self.module.key.clone(), None, None))
            .await
            .expect_err("a deleted retained session remains expired");
        assert!(
            matches!(error, ServeError::Expired { expired_at_ms } if expired_at_ms == deadline),
            "{error:?}"
        );
        let error = self
            .module
            .registry
            .check_expired(&self.module.key, Some("r1"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, crate::registry::OpenError::Expired { expired_at_ms } if expired_at_ms == deadline)
        );
    }

    async fn stop(&self) {
        assert!(self.module.registry.stop_and_seal().await);
    }
}

async fn retained_creating_sends_validate_alike(cipher: Cipher) {
    let old = retained(false, cipher, TTL, None).await;
    let new = retained(true, cipher, TTL, None).await;
    let old_invalid = old.send("invalid", Some(0)).await.unwrap_err();
    let new_invalid = new.send("invalid", Some(0)).await.unwrap_err();
    assert_eq!(new_invalid.to_string(), old_invalid.to_string());
    assert!(!new.runtime.holds(&new.module.key).unwrap());
    assert_eq!(
        new.send("r1", Some(SECONDS)).await.unwrap(),
        old.send("r1", Some(SECONDS)).await.unwrap()
    );
    assert!(
        new.runtime.holds(&new.module.key).unwrap(),
        "a retained creating send must use the new storage"
    );
    old.idle().await;
    new.idle().await;
    assert_eq!(
        new.send("changed", Some(SECONDS + 1)).await.unwrap(),
        old.send("changed", Some(SECONDS + 1)).await.unwrap()
    );
    assert_eq!(
        new.send("r2", Some(SECONDS)).await.unwrap(),
        old.send("r2", Some(SECONDS)).await.unwrap()
    );
    old.idle().await;
    new.idle().await;
    old.stop().await;
    new.stop().await;
}

async fn swept_retained_reads_and_fresh_sends_match(cipher: Cipher) {
    let old = retained(false, cipher, Duration::from_millis(1), None).await;
    let new = retained(true, cipher, Duration::from_millis(1), None).await;
    assert_eq!(
        new.send("r1", Some(SECONDS)).await.unwrap(),
        old.send("r1", Some(SECONDS)).await.unwrap()
    );
    old.module.closed().await;
    new.module.closed().await;
    let deadline = NOW + SECONDS * 1000;
    for rig in [&old, &new] {
        rig.advance(deadline);
        assert_eq!(rig.module.registry.sweep_once(deadline).await.unwrap(), 1);
        rig.expired_read(deadline).await;
    }
    assert_eq!(
        new.send("r2", Some(SECONDS)).await.unwrap(),
        old.send("r2", Some(SECONDS)).await.unwrap()
    );
    assert!(new.runtime.holds(&new.module.key).unwrap());
    old.module.closed().await;
    new.module.closed().await;
    for rig in [&old, &new] {
        let error = rig
            .module
            .registry
            .check_expired(&rig.module.key, Some("r1"))
            .await
            .unwrap_err();
        assert!(
            matches!(error, crate::registry::OpenError::Expired { expired_at_ms } if expired_at_ms == deadline)
        );
        assert!(rig
            .module
            .registry
            .read(SessionReadRequest::new(rig.module.key.clone(), None, None))
            .await
            .is_ok());
    }
    old.stop().await;
    new.stop().await;
}

async fn closed_retained_sessions_expire_before_any_sweep_alike(cipher: Cipher) {
    let old = retained(false, cipher, Duration::from_millis(1), None).await;
    let new = retained(true, cipher, Duration::from_millis(1), None).await;
    assert_eq!(
        new.send("r1", Some(SECONDS)).await.unwrap(),
        old.send("r1", Some(SECONDS)).await.unwrap()
    );
    old.module.closed().await;
    new.module.closed().await;
    let deadline = NOW + SECONDS * 1000;
    old.advance(deadline);
    new.advance(deadline);
    assert!(new.runtime.holds(&new.module.key).unwrap());
    old.expired_read(deadline).await;
    new.expired_read(deadline).await;
    assert_eq!(
        new.send("r2", Some(SECONDS)).await.unwrap(),
        old.send("r2", Some(SECONDS)).await.unwrap()
    );
    old.module.closed().await;
    new.module.closed().await;
    assert!(new.runtime.holds(&new.module.key).unwrap());
    assert_eq!(
        new.runtime.expiry_markers(&new.module.key).unwrap()[0].expired_at_ms,
        deadline
    );
    old.stop().await;
    new.stop().await;
}

async fn an_open_retained_session_expires_at_its_deadline(cipher: Cipher) {
    let old = retained(false, cipher, TTL, None).await;
    let new = retained(true, cipher, TTL, None).await;
    assert_eq!(
        new.send("r1", Some(SECONDS)).await.unwrap(),
        old.send("r1", Some(SECONDS)).await.unwrap()
    );
    old.idle().await;
    new.idle().await;
    assert!(new.module.registry.slot_state(&new.module.key).is_some());
    let (old_sink, mut old_stream) = mpsc::channel(64);
    let (new_sink, mut new_stream) = mpsc::channel(64);
    assert_eq!(
        old.module
            .registry
            .route_subscribe_live(&old.module.key, old_sink)
            .await
            .unwrap(),
        crate::actor::SubscribeOutcome::Attached
    );
    assert_eq!(
        new.module
            .registry
            .route_subscribe_live(&new.module.key, new_sink)
            .await
            .unwrap(),
        crate::actor::SubscribeOutcome::Attached
    );
    let deadline = NOW + SECONDS * 1000;
    old.advance(deadline);
    new.advance(deadline);
    old.deleted().await;
    new.deleted().await;
    for stream in [&mut old_stream, &mut new_stream] {
        broca_test_support::wait_until("subscriber receives the expiry terminal", || {
            std::future::ready(match stream.try_recv() {
                Ok(crate::fanout::Delivery::Expired { expired_at_ms }) => {
                    assert_eq!(expired_at_ms, deadline);
                    Ok(())
                }
                other => Err(format!("{other:?}")),
            })
        })
        .await;
    }
    old.expired_read(deadline).await;
    new.expired_read(deadline).await;
    old.stop().await;
    new.stop().await;
}

async fn recreated_retained_sessions_carry_numbering_floors_alike(cipher: Cipher) {
    use broca_session_store::backend::SessionAddr;
    use broca_wal::StorageClass;
    let old = retained(false, cipher, Duration::from_millis(1), None).await;
    let new = retained(true, cipher, Duration::from_millis(1), None).await;
    old.send("r1", Some(SECONDS)).await.unwrap();
    new.send("r1", Some(SECONDS)).await.unwrap();
    old.module.closed().await;
    new.module.closed().await;
    let deadline = NOW + SECONDS * 1000;
    for rig in [&old, &new] {
        rig.advance(deadline);
        assert_eq!(rig.module.registry.sweep_once(deadline).await.unwrap(), 1);
    }
    // Simulate an older deleted session whose numbers are above the most recent one's. Its
    // content is already gone; only the durable floors may decide the next session's numbers.
    let dir = old
        .module
        .wal
        .ephemeral_wal_path(&old.module.key)
        .with_extension("expired");
    let path = std::fs::read_dir(dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let mut stone: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    stone["next_tag"] = 42.into();
    stone["next_notice_seq"] = 77.into();
    std::fs::write(path, serde_json::to_vec(&stone).unwrap()).unwrap();
    let mut marker = new
        .runtime
        .expiry_markers(&new.module.key)
        .unwrap()
        .pop()
        .unwrap();
    marker.next_tag = 42;
    marker.next_notice_seq = 77;
    broca_session_store::expired::write(
        &StorageClass::Ephemeral.store_root(&new.module.dir),
        &SessionAddr(broca_core::session_addr(&new.module.key)),
        &broca_session_store::keys::random_id(),
        marker,
        cipher,
        new.runtime.deployment(),
    )
    .unwrap();
    assert_eq!(
        new.send("r2", Some(SECONDS)).await.unwrap(),
        old.send("r2", Some(SECONDS)).await.unwrap()
    );
    old.module.closed().await;
    new.module.closed().await;
    let view = new
        .runtime
        .current_view(&new.module.key)
        .await
        .unwrap()
        .unwrap();
    let start = view
        .operations
        .run
        .as_ref()
        .unwrap()
        .start
        .as_ref()
        .unwrap();
    let config = broca_session_loop::run::run_config_of_start(
        start,
        new.runtime.parts_for(StorageClass::Ephemeral, cipher),
    )
    .unwrap();
    let old_checkpoint = old
        .module
        .wal
        .read_records(&old.module.key)
        .unwrap()
        .into_iter()
        .find_map(|frame| match frame.record {
            broca_wal::WalRecord::RunStarted { config, .. } => {
                config.computed_tags.map(|tags| tags.checkpoint)
            }
            _ => None,
        });
    assert_eq!(old_checkpoint, Some(41));
    assert_eq!(
        config.computed_tags.map(|tags| tags.checkpoint),
        old_checkpoint
    );
    assert_eq!(
        new.runtime
            .expiry_markers(&new.module.key)
            .unwrap()
            .iter()
            .map(|m| m.next_notice_seq)
            .max(),
        Some(
            old.module
                .wal
                .retired_notice_floor(&old.module.key)
                .unwrap()
        )
    );
    old.stop().await;
    new.stop().await;
}

async fn expiry_reads_before_the_key_load_never_become_absent(cipher: Cipher) {
    let rig = retained(true, cipher, Duration::from_millis(1), None).await;
    rig.send("r1", Some(SECONDS)).await.unwrap();
    rig.module.closed().await;
    let deadline = NOW + SECONDS * 1000;
    rig.advance(deadline);
    assert_eq!(rig.module.registry.sweep_once(deadline).await.unwrap(), 1);
    rig.stop().await;
    let executor: Arc<dyn RunExecutor> = Arc::new(executor(vec![], None));
    let runtime = Arc::new(
        crate::store_open::StoreRuntime::open_without_key(
            &rig.module.dir,
            1,
            cipher,
            executor.clone(),
            TTL,
        )
        .await
        .unwrap(),
    );
    let registry = LineageRegistry::new_with_reader(
        Arc::new(
            crate::opener::DefaultOpener::new(
                Arc::new(FileLeaseStore::new(rig.module.dir.join("leases"))),
                rig.module.wal.clone(),
                executor,
            )
            .with_store_runtime(runtime),
        ),
        rig.module.wal.clone(),
        None,
    );
    let error = registry
        .read(SessionReadRequest::new(rig.module.key.clone(), None, None))
        .await
        .unwrap_err();
    match cipher {
        Cipher::None => assert!(
            matches!(error, ServeError::Expired { expired_at_ms } if expired_at_ms == deadline)
        ),
        Cipher::XChaCha20Poly1305 => assert!(
            matches!(error, ServeError::Open(ref message, Some(broca_protocol::ErrorClass::Transient)) if message.starts_with("key_unavailable:"))
        ),
    }
    assert!(registry.stop_and_seal().await);
}

async fn computed_numbering_resets_after_retention_alike(cipher: Cipher) {
    use broca_wal::StorageClass;
    let old = retained(false, cipher, Duration::from_millis(1), None).await;
    let new = retained(true, cipher, Duration::from_millis(1), None).await;
    let plan = || {
        let mut manifest =
            (*super::controls::manifest(serde_json::json!({"render_tags":true}), vec![])).clone();
        manifest.system_text.clear();
        crate::actor::SendPlan {
            identity: manifest.plan_identity.clone(),
            manifest: Some(Arc::new(manifest)),
            model_check: None,
        }
    };
    assert_eq!(
        new.send_with_plan("r1", Some(SECONDS), Some(plan()))
            .await
            .unwrap(),
        old.send_with_plan("r1", Some(SECONDS), Some(plan()))
            .await
            .unwrap()
    );
    old.module.closed().await;
    new.module.closed().await;
    let deadline = NOW + SECONDS * 1000;
    for rig in [&old, &new] {
        rig.advance(deadline);
        assert_eq!(rig.module.registry.sweep_once(deadline).await.unwrap(), 1);
    }
    assert_eq!(
        old.module.wal.retired_tag_floor(&old.module.key).unwrap(),
        1
    );
    assert_eq!(
        new.runtime.expiry_markers(&new.module.key).unwrap()[0].next_tag,
        1
    );
    assert_eq!(
        new.send_with_plan("r2", Some(SECONDS), Some(plan()))
            .await
            .unwrap(),
        old.send_with_plan("r2", Some(SECONDS), Some(plan()))
            .await
            .unwrap()
    );
    old.module.closed().await;
    new.module.closed().await;
    let view = new
        .runtime
        .current_view(&new.module.key)
        .await
        .unwrap()
        .unwrap();
    let start = view
        .operations
        .run
        .as_ref()
        .unwrap()
        .start
        .as_ref()
        .unwrap();
    let config = broca_session_loop::run::run_config_of_start(
        start,
        new.runtime.parts_for(StorageClass::Ephemeral, cipher),
    )
    .unwrap();
    assert!(config.render_tags);
    assert_eq!(config.computed_tags.unwrap().checkpoint, 0);
    assert_eq!(
        read_json(&new.module, None, None, None).await.0,
        read_json(&old.module, None, None, None).await.0
    );
    assert_eq!(
        new.send("r3", None).await.unwrap(),
        old.send("r3", None).await.unwrap()
    );
    old.module.closed().await;
    new.module.closed().await;
    let old_high = old
        .module
        .wal
        .read_records(&old.module.key)
        .unwrap()
        .into_iter()
        .find_map(|frame| match frame.record {
            broca_wal::WalRecord::RunStarted { run_id, config, .. } if run_id.0 == "r3" => {
                config.computed_tags.map(|tags| tags.checkpoint)
            }
            _ => None,
        })
        .unwrap();
    assert!(old_high > 0, "the first recreated run handed out numbers");
    let view = new
        .runtime
        .current_view(&new.module.key)
        .await
        .unwrap()
        .unwrap();
    let start = view
        .operations
        .run
        .as_ref()
        .unwrap()
        .start
        .as_ref()
        .unwrap();
    let config = broca_session_loop::run::run_config_of_start(
        start,
        new.runtime.parts_for(StorageClass::Ephemeral, cipher),
    )
    .unwrap();
    assert_eq!(config.computed_tags.unwrap().checkpoint, old_high);
    old.stop().await;
    new.stop().await;
}

async fn stored_ledger_tags_carry_the_same_floor_as_today(cipher: Cipher) {
    use broca_session_store::record::Record;
    let old = retained(false, cipher, Duration::from_millis(1), None).await;
    let new = retained(true, cipher, Duration::from_millis(1), None).await;
    old.send("r1", Some(SECONDS)).await.unwrap();
    new.send("r1", Some(SECONDS)).await.unwrap();
    old.module.closed().await;
    new.module.closed().await;
    // Converted sessions retain v1's arrays. Give the same recorded messages ledger-stamped
    // numbers, instead of the computed rules that new runs always freeze.
    let mut frames = old.module.wal.read_records(&old.module.key).unwrap();
    for frame in &mut frames {
        match &mut frame.record {
            broca_wal::WalRecord::RunStarted { config, tags, .. } => {
                config.computed_tags = None;
                *tags = Some(Arc::new(vec![broca_protocol::tags::MessageTags {
                    parts: vec![41],
                }]));
            }
            broca_wal::WalRecord::ModelStepFinished { tags, .. } => {
                *tags = Some(Box::new(broca_protocol::tags::MessageTags {
                    parts: vec![42],
                }));
            }
            _ => {}
        }
    }
    let view = new
        .runtime
        .current_view(&new.module.key)
        .await
        .unwrap()
        .unwrap();
    let mut records = view.records_from(0).await.unwrap();
    for (_, record) in &mut records {
        match record {
            Record::RunStart(start) => {
                start.run_choices.as_mut().unwrap()["config"]["computed_tags"] =
                    serde_json::Value::Null;
            }
            Record::Input(input) => input.tags_v1 = Some(serde_json::json!([41])),
            Record::Assistant(assistant) => assistant.tags_v1 = Some(serde_json::json!([42])),
            _ => {}
        }
    }
    let expected = broca_core::tags::lineage_tags(frames.iter().map(|frame| &frame.record)).next();
    assert_eq!(expected, Some(43));
    assert_eq!(
        crate::retention::store::counters::next_tag(&records).unwrap(),
        expected
    );
    old.stop().await;
    new.stop().await;
}

async fn legacy_fallback_history_is_visible_but_old_runs_stay_expired(cipher: Cipher) {
    let rig = retained(true, cipher, Duration::from_millis(1), None).await;
    rig.send("r1", Some(SECONDS)).await.unwrap();
    rig.module.closed().await;
    let deadline = NOW + SECONDS * 1000;
    rig.advance(deadline);
    assert_eq!(rig.module.registry.sweep_once(deadline).await.unwrap(), 1);
    rig.stop().await;
    // Reopen without with_store_new_sessions(): an unserved creating send takes this same
    // legacy fallback. This test pins history visibility, not cross-format counter carryover.
    let executor = rig.executor.clone();
    let registry = LineageRegistry::new_with_reader(
        Arc::new(
            crate::opener::DefaultOpener::new(
                Arc::new(FileLeaseStore::new(rig.module.dir.join("leases"))),
                rig.module.wal.clone(),
                executor,
            )
            .with_idle_ttl(Duration::from_millis(1))
            .with_store_runtime(rig.runtime.clone()),
        ),
        rig.module.wal.clone(),
        None,
    );
    registry.set_retention_service(rig.service.clone());
    let fallback = Module {
        dir: rig.module.dir.clone(),
        registry,
        wal: rig.module.wal.clone(),
        key: rig.module.key.clone(),
        caller: Default::default(),
    };
    assert!(matches!(
        fallback.send("r2", "hello", None).await,
        SendOutcome::Active { .. }
    ));
    fallback.closed().await;
    assert!(!rig.runtime.holds(&fallback.key).unwrap());
    assert!(read_json(&fallback, None, None, None)
        .await
        .0
        .to_string()
        .contains("hello"));
    assert!(
        matches!(fallback.registry.check_expired(&fallback.key, Some("r1")).await,
        Err(crate::registry::OpenError::Expired { expired_at_ms }) if expired_at_ms == deadline)
    );
    assert!(fallback.registry.stop_and_seal().await);
}

async fn an_active_run_outlives_the_retention_deadline_alike(cipher: Cipher) {
    let old_gate = Arc::new(Notify::new());
    let new_gate = Arc::new(Notify::new());
    let old = retained(false, cipher, TTL, Some(old_gate.clone())).await;
    let new = retained(true, cipher, TTL, Some(new_gate.clone())).await;
    assert_eq!(
        new.send("r1", Some(SECONDS)).await.unwrap(),
        old.send("r1", Some(SECONDS)).await.unwrap()
    );
    let crossed = NOW + SECONDS * 1000;
    old.advance(crossed);
    new.advance(crossed);
    for rig in [&old, &new] {
        rig.module
            .registry
            .check_expired(&rig.module.key, None)
            .await
            .unwrap();
        assert!(rig.module.registry.slot_state(&rig.module.key).is_some());
    }
    old_gate.notify_one();
    new_gate.notify_one();
    old.idle().await;
    new.idle().await;
    for rig in [&old, &new] {
        rig.module
            .registry
            .check_expired(&rig.module.key, None)
            .await
            .unwrap();
    }
    let deadline = crossed + SECONDS * 1000;
    old.advance(deadline);
    new.advance(deadline);
    old.deleted().await;
    new.deleted().await;
    old.expired_read(deadline).await;
    new.expired_read(deadline).await;
    old.stop().await;
    new.stop().await;
}

cipher_tests!(
    retained_creating_sends_validate_alike,
    swept_retained_reads_and_fresh_sends_match,
    closed_retained_sessions_expire_before_any_sweep_alike,
    an_open_retained_session_expires_at_its_deadline,
    recreated_retained_sessions_carry_numbering_floors_alike,
    expiry_reads_before_the_key_load_never_become_absent,
    computed_numbering_resets_after_retention_alike,
    stored_ledger_tags_carry_the_same_floor_as_today,
    legacy_fallback_history_is_visible_but_old_runs_stay_expired,
    an_active_run_outlives_the_retention_deadline_alike,
);
