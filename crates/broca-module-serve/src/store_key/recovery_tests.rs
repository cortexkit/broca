use super::*;
use broca_module::executor::{RunExecutor, StoreRunEnd, StoreRunJob, StoreRunMode};
use broca_protocol::{BindIdentity, PauseReason, RunFinishReason};
use broca_session_loop::Writer;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::Cipher;
use broca_session_store::record::{Pause, Record, Resumed, RunEnd, RunStart};
use broca_session_store::session::Session;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
struct Continuing(AtomicUsize);

#[async_trait]
impl RunExecutor for Continuing {
    async fn execute_on_store(
        &self,
        mut job: StoreRunJob,
        _: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
    ) -> StoreRunEnd {
        assert_eq!(job.mode, StoreRunMode::Resume);
        self.0.fetch_add(1, Ordering::SeqCst);
        let reason = if job
            .control
            .cancel
            .as_ref()
            .is_some_and(|cancel| cancel.is_cancelled())
        {
            RunFinishReason::Cancelled
        } else {
            RunFinishReason::Completed
        };
        let run_id = job.writer.operations().run.as_ref().unwrap().run_id.clone();
        // Stand in for an admitted continuation, using the actor's real durable writer.
        job.writer
            .append(
                &[
                    Record::Resumed(Resumed {
                        run_id: run_id.clone(),
                        resumed_from_seq: 1,
                        messages: 0,
                        model_steps: 0,
                        tool_results: 0,
                        redone: vec![],
                        indeterminate: vec![],
                    }),
                    Record::RunEnd(RunEnd {
                        run_id,
                        reason,
                        usage: None,
                        ts_ms: None,
                        retries_used: 0,
                        interrupt_reason: None,
                        error: None,
                    }),
                ],
                true,
            )
            .await
            .unwrap();
        StoreRunEnd {
            parts: job.parts,
            result: Ok(broca_session_loop::run::Finished {
                sign_in: None,
                reason,
                usage: None,
                retries_used: 0,
                paused: None,
            }),
        }
    }
}

struct DelayedKey {
    requested: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[async_trait]
impl DeploymentKeySource for DelayedKey {
    async fn read(&self, _: &str) -> broca_subc::VaultGet {
        self.requested.notify_one();
        self.release.notified().await;
        broca_subc::VaultGet {
            result: Ok(broca_subc::VaultToken {
                token: "07".repeat(32),
                expires_at_ms: None,
                record_version: 1,
                project_id: None,
                account_id: None,
                resolved_credential_id: None,
                hosts: None,
                gateway: None,
            }),
            vault_replied: true,
        }
    }
}

struct Rig {
    root: std::path::PathBuf,
    registry: broca_module::LineageRegistry,
    slot: Arc<StoreSlot>,
    executor: Arc<Continuing>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

impl Rig {
    async fn new(name: &str) -> Self {
        let root = broca_test_support::scratch_root()
            .join(format!("key-recovery-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let executor = Arc::new(Continuing::default());
        let slot = Arc::new(StoreSlot::awaiting_key(&root, 1).creating(Cipher::None));
        let registry =
            broca_module::LineageRegistry::new(Arc::new(broca_module::DefaultOpener::new(
                Arc::new(broca_core::FileLeaseStore::new(root.join("leases"))),
                executor.clone(),
                slot.clone(),
            )));
        registry.enable_restart_resumes();
        let rig = Self {
            root,
            registry,
            slot,
            executor,
        };
        rig.slot
            .open_without_key(&rig.loader(None).setup)
            .await
            .unwrap();
        rig
    }

    fn loader(&self, source: Option<Arc<dyn DeploymentKeySource>>) -> KeyLoader {
        KeyLoader {
            source,
            slot: self.slot.clone(),
            setup: StoreRuntimeSetup {
                state_root: self.root.clone(),
                executor: self.executor.clone(),
                idle_ttl: Duration::from_secs(600),
                import_roots: None,
                billing: None,
                run_events: None,
                send_states: None,
            },
            backoff: Backoff::default(),
        }
    }

    async fn paused(&self, name: &str, cipher: Cipher) -> BindIdentity {
        self.paused_for(name, cipher, 0, false).await
    }

    async fn paused_for(
        &self,
        name: &str,
        cipher: Cipher,
        age_ms: u64,
        cancelled: bool,
    ) -> BindIdentity {
        let key = BindIdentity::new(self.root.join("project"), "recovery", name);
        let root = broca_records::session_storage::StorageClass::Ordinary.store_root(&self.root);
        let addr = SessionAddr(broca_core::session_addr(&key));
        broca_module::store_open::recovery::register_candidate(&root, &addr).unwrap();
        let mut session = Session::create(
            LocalBackend::new(root),
            addr,
            SessionTriple {
                project_root: key.project_root.display().to_string(),
                harness: key.harness.clone(),
                session: key.session.clone(),
            },
            &DeploymentKey::new(1, &[7; 32]).creating(cipher),
            false,
        )
        .await
        .unwrap();
        let run_id = format!("run-{name}");
        session
            .append(
                &[
                    Record::RunStart(Box::new(RunStart {
                        run_id: run_id.clone(),
                        model: broca_protocol::request::ModelHandle::new("stub", "model"),
                        setup: vec![],
                        renderer_version: 1,
                        submission_id: None,
                        send_id: None,
                        request_identity_digest: None,
                        ts_ms: None,
                        written_at_ms: None,
                        session_class: None,
                        retention_secs: None,
                        title: None,
                        context_limit: None,
                        service_tier: None,
                        route_decision_id: None,
                        compaction_plan_version: None,
                        auth_selection: None,
                        run_choices: None,
                        origin: None,
                        principal: None,
                        scope: None,
                        hooks: None,
                        keep_warm: None,
                        on_restart: Some(serde_json::json!("resume")),
                        seeded: None,
                        refusal_schedule: None,
                        imported_ids: None,
                        render_identity: None,
                        message_marks: None,
                        named_speaker: None,
                        kept_notice: None,
                    })),
                    Record::Pause(Pause {
                        run_id: run_id.clone(),
                        reason: PauseReason::Restart,
                        ts_ms: Some(broca_core::clock::epoch_millis().unwrap() - age_ms),
                        error: None,
                        account_limit: None,
                    }),
                ],
                true,
            )
            .await
            .unwrap();
        if cancelled {
            session
                .append(
                    &[Record::CancelRequest(
                        broca_session_store::record::CancelRequest {
                            run_id,
                            at: broca_session_store::record::CancelPoint::Now,
                        },
                    )],
                    true,
                )
                .await
                .unwrap();
        }
        key
    }

    async fn assert_continued_once(&self, key: &BindIdentity) {
        let view = self
            .registry
            .store_runtime()
            .unwrap()
            .current_view(key)
            .await
            .unwrap()
            .unwrap();
        let records = view.records_from(0).await.unwrap();
        let run_id = format!("run-{}", key.session);
        assert_eq!(
            records
                .iter()
                .filter(|(_, record)| matches!(record,
            Record::Resumed(resume) if resume.run_id == run_id))
                .count(),
            1
        );
    }
}

#[tokio::test]
async fn encrypted_startup_sweep_defers_without_failures() {
    let rig = Rig::new("deferred").await;
    rig.paused("untouched", Cipher::XChaCha20Poly1305).await;
    let sweep = rig.registry.recover_store_crash_orphans().await.unwrap();
    eprintln!(
        "startup restart sweep continued {} runs; {sweep:?}",
        rig.executor.0.load(Ordering::SeqCst)
    );
    assert_eq!(rig.executor.0.load(Ordering::SeqCst), 0);
    assert!(
        sweep.failed.is_empty(),
        "key readiness is not a failure: {sweep:?}"
    );
    assert_eq!(sweep.deferred, 1);
    assert!(rig.registry.stop_and_seal().await);
}

/// Boot recovery of held calls opens registered sessions before the key loads. An encrypted one
/// is refused with the error `is_key_unavailable` recognises, so the recovery defers it to the key
/// loader instead of warning that it could not be opened.
#[tokio::test]
async fn a_registered_session_opened_before_the_key_reads_as_waiting_for_it() {
    let rig = Rig::new("registered").await;
    let encrypted = rig.paused("encrypted", Cipher::XChaCha20Poly1305).await;
    let (opened, failed) = rig
        .registry
        .open_registered_sessions(vec![encrypted.clone()], 2)
        .await;
    assert_eq!(opened, 0);
    assert_eq!(failed.len(), 1);
    assert!(
        broca_module::store_open::is_key_unavailable(&failed[0].1),
        "{:?}",
        failed[0].1
    );
    assert!(!broca_module::store_open::is_key_unavailable(
        &broca_module::registry::OpenError::UnknownSession
    ));
    assert_eq!(rig.slot.take_refused(), vec![encrypted]);
    assert!(rig.registry.stop_and_seal().await);
}

#[tokio::test]
async fn delayed_key_continues_all_restart_candidates_once_and_logs_each_run() {
    let rig = Rig::new("delayed").await;
    let untouched = rig.paused("untouched", Cipher::XChaCha20Poly1305).await;
    let refused = rig.paused("refused", Cipher::XChaCha20Poly1305).await;
    assert!(rig.registry.actor_for(&refused).await.is_err());
    let logs = broca_test_support::LogCapture::observe(|event| {
        matches!(event.target, "lifecycle" | "session_storage")
    });
    let source = Arc::new(DelayedKey {
        requested: Default::default(),
        release: Default::default(),
    });
    let loader = tokio::spawn(rig.loader(Some(source.clone())).run(rig.registry.clone()));
    tokio::time::timeout(
        broca_test_support::EVENT_HANG_GUARD,
        source.requested.notified(),
    )
    .await
    .unwrap();
    assert!(!rig.slot.key_loaded());
    assert_eq!(rig.executor.0.load(Ordering::SeqCst), 0);
    // The key is read beside the first pass, not after it, so the pass may still be running
    // when the key is asked for; it defers both encrypted sessions either way.
    tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        while !logs.events().iter().any(|event| {
            event.message() == "store restart resume candidate sweep complete"
                && event.fields.get("deferred").map(String::as_str) == Some("2")
                && event.fields.get("failed").map(String::as_str) == Some("0")
        }) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the first pass deferred both encrypted sessions");
    source.release.notify_one();
    tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, loader)
        .await
        .unwrap()
        .unwrap();
    assert!(rig.slot.key_loaded());
    assert_eq!(rig.executor.0.load(Ordering::SeqCst), 2);
    // A refused-session reopen and another caller open must reuse the recovered actors.
    let (opened, failed) = rig
        .registry
        .open_registered_sessions(vec![refused.clone(), untouched.clone()], 2)
        .await;
    assert_eq!(opened, 2);
    assert!(failed.is_empty(), "{failed:?}");
    assert!(logs.events().iter().any(|event| event
        .message()
        .starts_with("opened the sessions refused")
        && event.fields.get("opened").map(String::as_str) == Some("1")));
    for key in [&untouched, &refused] {
        rig.assert_continued_once(key).await;
        let address = broca_core::session_addr(key);
        let run_id = format!("run-{}", key.session);
        assert_eq!(
            logs.events()
                .iter()
                .filter(|event| event.level == tracing::Level::INFO
                    && event.fields.get("session") == Some(&address)
                    && event.fields.get("run_id") == Some(&run_id)
                    && event.fields.get("outcome").map(String::as_str) == Some("continued"))
                .count(),
            1
        );
    }
    assert_eq!(rig.executor.0.load(Ordering::SeqCst), 2);
    assert!(!logs.events().iter().any(|event| event
        .fields
        .values()
        .any(|value| value.contains("Head(NotLoaded)"))));
    assert!(logs.events().iter().any(|event| event.message()
        == "store restart resume candidate sweep complete"
        && event.fields.get("deferred").map(String::as_str) == Some("0")
        && event.fields.get("recovered").map(String::as_str) == Some("2")));
    assert!(rig.registry.stop_and_seal().await);
}

#[tokio::test]
async fn plain_restart_candidates_continue_without_a_key_source() {
    let rig = Rig::new("plain").await;
    let key = rig.paused("plain", Cipher::None).await;
    rig.loader(None).run(rig.registry.clone()).await;
    assert!(!rig.slot.key_loaded());
    assert_eq!(rig.executor.0.load(Ordering::SeqCst), 1);
    rig.assert_continued_once(&key).await;
    assert!(rig.registry.stop_and_seal().await);
}

/// Every restart's first pass reads every head of the durable class, plain ones included, and
/// defers the encrypted ones. The pass after the key loads reads only those it deferred: the
/// plain ones are done, and reading their heads and views again finds nothing new.
#[tokio::test]
async fn the_pass_after_the_key_loads_reads_only_the_sessions_the_first_pass_left() {
    let rig = Rig::new("second-pass").await;
    let idle = BindIdentity::new(rig.root.join("project"), "recovery", "idle-durable");
    let durable = StorageClass::Durable.store_root(&rig.root);
    std::fs::create_dir_all(&durable).unwrap();
    Session::create(
        LocalBackend::new(durable),
        SessionAddr(broca_core::session_addr(&idle)),
        SessionTriple {
            project_root: idle.project_root.display().to_string(),
            harness: idle.harness.clone(),
            session: idle.session.clone(),
        },
        &DeploymentKey::new(1, &[7; 32]).creating(Cipher::None),
        false,
    )
    .await
    .unwrap();
    let locked = rig.paused("locked", Cipher::XChaCha20Poly1305).await;
    // Any process's sweep reaches this observer; the loader's own scope below sees only its
    // second pass, which runs inside the loader's task (the first runs in a task of its own).
    let observed = broca_test_support::LogCapture::observe(|event| event.target == "lifecycle");
    let scoped = broca_test_support::LogCapture::new();
    let source = Arc::new(DelayedKey {
        requested: Default::default(),
        release: Default::default(),
    });
    let loader = tokio::spawn(
        scoped.scope_future(rig.loader(Some(source.clone())).run(rig.registry.clone())),
    );
    let sweeps = |logs: &broca_test_support::LogCapture| {
        logs.events()
            .iter()
            .filter(|event| event.message() == "store restart resume candidate sweep complete")
            .map(|event| {
                (
                    event.fields.get("inspected").cloned().unwrap_or_default(),
                    event.fields.get("deferred").cloned().unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>()
    };
    let first = ("1".to_owned(), "1".to_owned());
    tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        while !sweeps(&observed).contains(&first) {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the first pass read the plain head and deferred the encrypted one");
    source.release.notify_one();
    tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, loader)
        .await
        .unwrap()
        .unwrap();
    assert!(rig.slot.key_loaded());
    rig.assert_continued_once(&locked).await;
    assert_eq!(
        sweeps(&scoped),
        [("1".to_owned(), "0".to_owned())],
        "the pass after the key loaded read a session the first pass had finished"
    );
    assert!(rig.registry.stop_and_seal().await);
}

#[tokio::test]
async fn expired_and_cancelled_restart_candidates_log_their_terminal_outcome() {
    for (name, cancelled, outcome) in [
        ("expired", false, "interrupted"),
        ("cancelled", true, "cancelled"),
    ] {
        let rig = Rig::new(name).await;
        let key = rig
            .paused_for(name, Cipher::None, 2 * 60 * 60 * 1000, cancelled)
            .await;
        let logs = broca_test_support::LogCapture::observe(|event| event.target == "lifecycle");
        rig.loader(None).run(rig.registry.clone()).await;
        let address = broca_core::session_addr(&key);
        let run_id = format!("run-{name}");
        let outcomes: Vec<_> = logs
            .events()
            .into_iter()
            .filter(|event| {
                event.fields.get("session") == Some(&address)
                    && event.fields.get("run_id") == Some(&run_id)
                    && event.fields.contains_key("outcome")
            })
            .collect();
        assert_eq!(outcomes.len(), 1, "{outcomes:?}");
        assert_eq!(outcomes[0].level, tracing::Level::INFO);
        assert_eq!(outcomes[0].fields["outcome"], outcome);
        if !cancelled {
            assert_eq!(outcomes[0].fields["code"], "restart_resume_expired");
            assert_eq!(rig.executor.0.load(Ordering::SeqCst), 0);
        }
        assert!(rig.registry.stop_and_seal().await);
    }
}

#[tokio::test]
async fn stopping_before_key_load_leaves_candidates_deferred_and_logs_once() {
    let rig = Rig::new("stopping").await;
    rig.paused("stopping", Cipher::XChaCha20Poly1305).await;
    let logs = broca_test_support::LogCapture::observe(|event| event.target == "lifecycle");
    let source = Arc::new(DelayedKey {
        requested: Default::default(),
        release: Default::default(),
    });
    let loader = tokio::spawn(rig.loader(Some(source.clone())).run(rig.registry.clone()));
    tokio::time::timeout(
        broca_test_support::EVENT_HANG_GUARD,
        source.requested.notified(),
    )
    .await
    .unwrap();
    assert!(rig.registry.stop_and_seal().await);
    tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, loader)
        .await
        .unwrap()
        .unwrap();
    assert!(!rig.slot.key_loaded());
    assert_eq!(rig.executor.0.load(Ordering::SeqCst), 0);
    assert_eq!(
        logs.events()
            .iter()
            .filter(|event| event
                .message()
                .starts_with("store recovery deferred until next startup:"))
            .count(),
        1
    );
    assert!(rig.registry.stop_and_seal().await);
}
