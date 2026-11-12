//! Background conversion uses the cutover tests' real registry and stores.

use super::*;
use broca_session_loop::writer::Writer;

fn background_probe() -> Arc<crate::session_conversion::BackgroundProbe> {
    Arc::new(crate::session_conversion::BackgroundProbe {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
        sessions: Mutex::new(Vec::new()),
    })
}

async fn wait_background_probe(probe: &crate::session_conversion::BackgroundProbe) {
    broca_test_support::wait_for(
        "background conversion fence",
        probe.entered.notified(),
        || format!("fenced sessions: {:?}", probe.sessions.lock().unwrap()),
    )
    .await;
}

fn background_request(session: &BindIdentity, run: &str) -> broca_core::RunRequest {
    broca_core::RunRequest::from_submission(
        serde_json::from_value(json!({
            "model":{"provider_module_id":"anthropic","model_id":"claude-test"},
            "input":[{"role":"user","content":[{"type":"text","text":format!("Hello from {run}.")}]}]
        })).unwrap(),
        broca_protocol::RunId(run.into()),
        session.clone(),
        broca_protocol::SubmissionId(format!("submission-{run}")),
    )
}

struct HeldBackgroundRun {
    release: tokio::sync::Notify,
}

#[async_trait]
impl RunExecutor for HeldBackgroundRun {
    async fn execute(
        &self,
        mut job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let mut started = run_started(&job.req.run_id.0, 1);
        if let WalRecord::RunStarted { session, .. } = &mut started {
            *session = job.req.session.clone();
        }
        job.writer.record(started);
        job.writer.sync().await.unwrap();
        job.on_admitted
            .take()
            .unwrap()
            .send(Ok(job.req.run_id.clone()))
            .unwrap();
        self.release.notified().await;
        job.writer.record(record(
            json!({"type":"run_finished","reason":"completed","usage":{},"retries_used":0}),
        ));
        job.writer.sync().await.unwrap();
        Ok(RunSummary {
            reason: broca_protocol::RunFinishReason::Completed,
            paused: None,
            steps: 0,
            usage: Default::default(),
            retries_used: 0,
            final_step_finish_reason: None,
        })
    }

    async fn execute_on_store(
        &self,
        job: broca_module::executor::StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> broca_module::executor::StoreRunEnd {
        use broca_module::executor::{StoreRunEnd, StoreRunError, StoreRunJob, StoreRunMode};
        use broca_protocol::{NormalizedEvent, ProviderControl};
        let StoreRunJob {
            mut req,
            control,
            mut writer,
            mut parts,
            mode,
            ..
        } = job;
        assert_eq!(mode, StoreRunMode::Fresh);
        let provider = broca_core::provider::StubProvider::new(vec![vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: broca_protocol::content::AssistantMessage {
                    message_id: broca_protocol::MessageId("background-answer".into()),
                    content: vec![broca_protocol::ContentBlock::Text {
                        text: "converted session answered".into(),
                    }],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Default::default(),
                finish_reason: broca_protocol::FinishReason::Stop,
                retries_used: 0,
            }),
        ]]);
        let tools = broca_core::stub_plane::StubToolPlane::new();
        let approval = broca_core::AllowAll;
        let now = || 10_000;
        let result = async {
            let admission = broca_session_loop::admit::admission(
                &mut req,
                &control,
                &provider,
                writer.operations(),
                Some(now()),
            )
            .map_err(StoreRunError::Admission)?;
            let seams = broca_session_loop::run::Seams {
                provider: &provider,
                tools: &tools,
                bind: req.session.clone(),
                out: Some(&out),
                cancel: control.cancel.clone().unwrap_or_default(),
                stop: control.stop_requested.clone().unwrap_or_default(),
                held: None,
                approval: &approval,
                hook_plane: None,
                now_ms: &now,
                session_address: None,
                compaction_plane: None,
                compaction_session: None,
                compaction_changes: true,
                keep_warm: None,
                account_reports: None,
                max_same_account_wait: Default::default(),
                auth_selection: None,
                step_progress: control.step_progress.clone(),
                model_call_ready: None,
                pause_for_auth_at_start: None,
            };
            broca_session_loop::run::run(&mut writer, parts.as_mut(), admission, &seams)
                .await
                .map_err(StoreRunError::Loop)
        }
        .await;
        StoreRunEnd { parts, result }
    }
}

#[tokio::test]
async fn background_live_converts_while_another_session_serves_a_run() {
    let executor = Arc::new(HeldBackgroundRun {
        release: tokio::sync::Notify::new(),
    });
    let rig = Rig::start_with_executor(
        "background-serving",
        Cipher::XChaCha20Poly1305,
        executor.clone(),
    )
    .await;
    rig.write_session();
    let other = BindIdentity::new("/project", "runner", "other-running");
    let sent = rig
        .registry
        .route_send(
            &other,
            background_request(&other, "other-run"),
            broca_module::executor::RunKind::FreshOrResume,
        )
        .await
        .unwrap();
    assert!(
        matches!(sent, broca_module::actor::SendOutcome::Active { .. }),
        "{sent:?}"
    );
    let engine = super::engine(&rig, true, false);
    engine
        .run_archives(
            &deployment(),
            Some(&rig.runtime),
            Some(&rig.registry),
            Duration::ZERO,
        )
        .await
        .unwrap();
    assert!(rig.registry.kept_in_new_storage(&key()).unwrap());
    assert_eq!(rig.registry.slot_counts().runs_in_flight, 1);
    rig.registry.route_status(&other).await.unwrap();
    assert!(rig.wal_store.wal_path(&other).exists());
    assert_eq!(engine.health_for_test().live_background_converted, 1);
    assert_eq!(engine.health_for_test().live_skipped_busy, 1);
    executor.release.notify_one();
}

#[tokio::test]
async fn background_live_send_is_refused_under_fence_and_retry_succeeds_after_publish() {
    let rig = Rig::start_with_executor(
        "background-send",
        Cipher::XChaCha20Poly1305,
        Arc::new(HeldBackgroundRun {
            release: tokio::sync::Notify::new(),
        }),
    )
    .await;
    rig.write_session();
    let before = std::fs::read(rig.wal()).unwrap();
    let probe = background_probe();
    let mut engine = super::engine(&rig, true, false);
    engine.background_fenced = Some(probe.clone());
    let engine = Arc::new(engine);
    engine.install_guard(&rig.registry);
    let task = {
        let engine = engine.clone();
        let runtime = rig.runtime.clone();
        let registry = rig.registry.clone();
        tokio::spawn(async move {
            engine
                .run_archives(
                    &deployment(),
                    Some(&runtime),
                    Some(&registry),
                    Duration::ZERO,
                )
                .await
        })
    };
    wait_background_probe(&probe).await;
    assert!(rig.registry.conversion_refusal(&key()).unwrap().0);
    // Authorization reads can start after the request's initial admission check. They must
    // report SessionConverting too, so an already-dispatched send is not refused as moving.
    assert!(matches!(
        rig.registry.check_move(&key(), false),
        Err(broca_module::registry::OpenError::SessionConverting)
    ));
    let refused = rig
        .registry
        .route_send(
            &key(),
            background_request(&key(), "r3"),
            broca_module::executor::RunKind::AppendEpisode,
        )
        .await;
    assert!(
        matches!(
            refused,
            Err(broca_module::registry::OpenError::SessionConverting)
        ),
        "{refused:?}"
    );
    assert_eq!(
        rig.registry.slot_state(&key()),
        None,
        "a send cannot open an actor after selection"
    );
    assert_eq!(std::fs::read(rig.wal()).unwrap(), before);
    probe.release.add_permits(1);
    task.await.unwrap().unwrap();
    assert_eq!(rig.registry.conversion_refusal(&key()), None);
    let retried = rig
        .registry
        .route_send(
            &key(),
            background_request(&key(), "r3"),
            broca_module::executor::RunKind::AppendEpisode,
        )
        .await
        .unwrap();
    assert!(
        matches!(retried, broca_module::actor::SendOutcome::Active { .. }),
        "{retried:?}"
    );
    broca_test_support::wait_until("retried run finishes from the converted head", || async {
        let view = rig.runtime.current_view(&key()).await.unwrap().unwrap();
        let run = view.operations.run.as_ref().unwrap();
        if run.start.as_ref().is_some_and(|start| start.run_id == "r3")
            && run.finished == Some(broca_protocol::RunFinishReason::Completed)
        {
            Ok(())
        } else {
            Err(format!(
                "run start={:?}, finished={:?}",
                run.start.as_ref().map(|start| &start.run_id),
                run.finished
            ))
        }
    })
    .await;
}

#[tokio::test]
async fn background_live_skips_open_actor_then_converts_on_later_pass() {
    let rig = Rig::start("background-busy").await;
    let before = rig.write_session();
    rig.registry.actor_for(&key()).await.unwrap();
    let engine = super::engine(&rig, true, false);
    engine
        .run_archives(
            &deployment(),
            Some(&rig.runtime),
            Some(&rig.registry),
            Duration::ZERO,
        )
        .await
        .unwrap();
    assert_eq!(std::fs::read(rig.wal()).unwrap(), before);
    let row = engine.ledger.rows().unwrap().remove(0);
    assert_eq!(row.step, "discovered");
    assert_eq!(
        row.outcome, None,
        "busy is not a permanent conversion failure"
    );
    assert_eq!(engine.health_for_test().live_skipped_busy, 1);
    assert_eq!(engine.health_for_test().live_remaining, 1);
    let closed = rig
        .registry
        .lock_move_sessions_with_mode(&[key()], false, broca_module::registry::CutLockMode::Retire)
        .await
        .unwrap();
    drop(closed);
    assert_eq!(rig.registry.slot_state(&key()), None);
    engine
        .run_archives(
            &deployment(),
            Some(&rig.runtime),
            Some(&rig.registry),
            Duration::ZERO,
        )
        .await
        .unwrap();
    assert!(rig.registry.kept_in_new_storage(&key()).unwrap());
    assert_eq!(engine.health_for_test().live_background_converted, 1);
    assert_eq!(engine.health_for_test().live_remaining, 0);
}

#[tokio::test]
async fn background_live_defers_nonterminal_durable_work_and_identityless_sources() {
    let rig = Rig::start("background-unfinished").await;
    rig.write(&[run_started("paused-or-active", 1)]);
    let before = std::fs::read(rig.wal()).unwrap();
    let engine = super::engine(&rig, true, false);
    engine
        .run_archives(
            &deployment(),
            Some(&rig.runtime),
            Some(&rig.registry),
            Duration::ZERO,
        )
        .await
        .unwrap();
    assert_eq!(std::fs::read(rig.wal()).unwrap(), before);
    assert_eq!(engine.ledger.rows().unwrap()[0].outcome, None);
    assert_eq!(engine.health_for_test().live_skipped_busy, 1);
    assert_eq!(rig.registry.conversion_refusal(&key()), None);
    std::fs::write(rig.wal(), b"undecodable header").unwrap();
    engine
        .run_archives(
            &deployment(),
            Some(&rig.runtime),
            Some(&rig.registry),
            Duration::ZERO,
        )
        .await
        .unwrap();
    assert_eq!(std::fs::read(rig.wal()).unwrap(), b"undecodable header");
    assert_eq!(engine.ledger.rows().unwrap()[0].outcome, None);
    assert_eq!(engine.health_for_test().deferred_no_identity, 1);
    assert_eq!(engine.health_for_test().live_remaining, 1);
}

#[tokio::test]
async fn background_live_cancellation_and_panic_release_admission_and_lease() {
    for panic in [false, true] {
        let rig = Rig::start(&format!("background-release-{panic}")).await;
        let before = rig.write_session();
        let probe = background_probe();
        let mut engine = super::engine(&rig, true, false);
        engine.background_fenced = Some(probe.clone());
        let task = {
            let runtime = rig.runtime.clone();
            let registry = rig.registry.clone();
            tokio::spawn(async move {
                engine
                    .run_archives(
                        &deployment(),
                        Some(&runtime),
                        Some(&registry),
                        Duration::ZERO,
                    )
                    .await
            })
        };
        wait_background_probe(&probe).await;
        assert!(rig.registry.conversion_refusal(&key()).unwrap().0);
        if panic {
            probe.release.close();
        } else {
            task.abort();
        }
        let error = task.await.unwrap_err();
        assert_eq!(error.is_panic(), panic);
        assert_eq!(rig.registry.conversion_refusal(&key()), None);
        assert_eq!(std::fs::read(rig.wal()).unwrap(), before);
        let engine = super::engine(&rig, true, false);
        engine
            .run_archives(
                &deployment(),
                Some(&rig.runtime),
                Some(&rig.registry),
                Duration::ZERO,
            )
            .await
            .unwrap();
        assert!(rig.registry.kept_in_new_storage(&key()).unwrap());
    }
}

#[tokio::test]
async fn background_live_crashes_resume_each_ledger_and_publication_boundary() {
    for boundary in [
        "recovering",
        "staging",
        "verifying",
        "verified",
        "publishing",
        "rename_logs",
        "sync_data",
        "publish_head",
        "awaiting_backup",
        "backing_up",
        "done",
    ] {
        let rig = Rig::start(&format!("background-crash-{boundary}")).await;
        let before = rig.write_session();
        let mut engine = super::engine(&rig, true, false);
        engine.crash = Some(boundary);
        let first = engine
            .run_archives(
                &deployment(),
                Some(&rig.runtime),
                Some(&rig.registry),
                Duration::ZERO,
            )
            .await;
        assert!(first.is_err(), "cut {boundary} wasn't reached");
        assert_eq!(rig.registry.conversion_refusal(&key()), None, "{boundary}");
        drop(engine);
        let engine = Engine::configured(
            &rig.dir,
            Config {
                archives: false,
                live_at_startup: true,
                backup_root: Some(rig.dir.with_extension("converted-sources")),
            },
        )
        .unwrap()
        .unwrap();
        engine
            .run_live(&deployment(), Some(&rig.runtime), Some(&rig.registry))
            .await
            .unwrap();
        let row = engine.ledger.rows().unwrap().remove(0);
        assert_eq!(row.step, "done", "{boundary}: {row:?}");
        assert_eq!(row.outcome.as_deref(), Some("converted"));
        assert_eq!(std::fs::read(&row.source.backup).unwrap(), before);
        assert!(rig.registry.kept_in_new_storage(&key()).unwrap());
        assert!(!rig.wal().exists());
    }
}

#[tokio::test]
async fn background_live_is_skipped_by_startup_and_stops_starting_when_draining() {
    let rig = Rig::start("background-startup-skip").await;
    rig.write_session();
    let engine = super::engine(&rig, true, false);
    engine
        .run_archives(
            &deployment(),
            Some(&rig.runtime),
            Some(&rig.registry),
            Duration::ZERO,
        )
        .await
        .unwrap();
    let rows = engine.ledger.rows().unwrap();
    let before = std::fs::read(&rows[0].source.backup).unwrap();
    drop(engine);
    let mut startup = Engine::configured(
        &rig.dir,
        Config {
            archives: false,
            live_at_startup: true,
            backup_root: Some(rig.dir.with_extension("converted-sources")),
        },
    )
    .unwrap()
    .unwrap();
    startup.crash = Some("recovering");
    startup
        .run_live(&deployment(), Some(&rig.runtime), Some(&rig.registry))
        .await
        .unwrap();
    assert_eq!(startup.ledger.rows().unwrap(), rows);
    assert_eq!(std::fs::read(&rows[0].source.backup).unwrap(), before);

    let draining = Rig::start("background-draining").await;
    let before = draining.write_session();
    draining.registry.begin_stop();
    let engine = super::engine(&draining, true, false);
    engine
        .run_archives(
            &deployment(),
            Some(&draining.runtime),
            Some(&draining.registry),
            Duration::ZERO,
        )
        .await
        .unwrap();
    assert_eq!(std::fs::read(draining.wal()).unwrap(), before);
    assert_eq!(engine.health_for_test().live_background_converted, 0);
    assert_eq!(engine.health_for_test().live_remaining, 1);
}

#[tokio::test]
async fn cutover_verification_failure_retains_check_and_first_difference_in_ledger() {
    let rig = Rig::start("cutover-verification-detail").await;
    let before = rig.write_session();
    let mut engine = super::engine(&rig, false, true);
    engine.verification_records = Some(|records| {
        for frame in records {
            if let WalRecord::RunStarted { input, .. } = &mut frame.record {
                *input = Arc::new(vec![serde_json::from_value(
                    json!({"role":"user","content":[{"type":"text","text":"different input"}]}),
                )
                .unwrap()]);
                break;
            }
        }
    });
    engine
        .run_live(&deployment(), Some(&rig.runtime), Some(&rig.registry))
        .await
        .unwrap();
    let row = engine.ledger.rows().unwrap().remove(0);
    assert_eq!(row.outcome.as_deref(), Some("failed"));
    assert_eq!(row.refusal.as_deref(), Some("verification_failed"));
    let detail = row
        .detail
        .as_deref()
        .expect("verification mismatch must be retained");
    assert!(detail.contains("differs at"), "{detail}");
    assert!(
        detail.contains("input") || detail.contains("content"),
        "{detail}"
    );
    assert_eq!(std::fs::read(rig.wal()).unwrap(), before);
    assert!(!Path::new(&row.source.backup).exists());
    drop(engine);
    let ledger = crate::conversion_ledger::Ledger::open(&rig.dir).unwrap();
    assert_eq!(
        ledger.row(&row.source.id).unwrap().unwrap().detail,
        row.detail
    );
}

/// A session whose recorded request digest today's offline replay can't reproduce still
/// converts, because its converted requests equal today's replay; the converted row keeps the
/// affected step, also after the ledger is reopened.
#[tokio::test]
async fn cutover_keeps_a_legacy_replay_limit_on_the_converted_row() {
    let rig = Rig::start("cutover-replay-limit").await;
    // The log records a request digest no render of its records produces.
    let mut records = session_records(&rig.wal());
    for record in &mut records {
        if let WalRecord::ModelAttemptFinished {
            request_body_sha256,
            ..
        } = record
        {
            *request_body_sha256 = "0".repeat(64);
        }
    }
    rig.write(&records);
    let engine = super::engine(&rig, false, true);
    engine
        .run_live(&deployment(), Some(&rig.runtime), Some(&rig.registry))
        .await
        .unwrap();
    let row = engine.ledger.rows().unwrap().remove(0);
    assert_eq!(row.outcome.as_deref(), Some("converted"), "{row:?}");
    let detail = row
        .detail
        .as_deref()
        .expect("the replay limit must stay on the converted row");
    assert!(
        detail.starts_with("legacy_replay_limit: 1 step(s)"),
        "{detail}"
    );
    drop(engine);
    let ledger = crate::conversion_ledger::Ledger::open(&rig.dir).unwrap();
    assert_eq!(
        ledger.row(&row.source.id).unwrap().unwrap().detail,
        row.detail
    );
}

#[tokio::test]
async fn background_live_takes_oldest_last_write_first_and_stops_next_conversion_on_drain() {
    let rig = Rig::start("background-oldest").await;
    let original = rig.write_session();
    let older = BindIdentity::new("/project", "runner", "older");
    let path = rig.wal_store.wal_path(&older);
    let mut records = session_records(&path);
    for record in &mut records {
        if let WalRecord::RunStarted { session, .. } = record {
            *session = older.clone();
        }
    }
    let mut wal = broca_wal::FileWal::open(&path).unwrap();
    for record in records {
        wal.append(0, &record, false).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_times(
            std::fs::FileTimes::new()
                .set_modified(std::time::UNIX_EPOCH + Duration::from_secs(100)),
        )
        .unwrap();
    let probe = background_probe();
    let mut engine = super::engine(&rig, true, false);
    engine.background_fenced = Some(probe.clone());
    let engine = Arc::new(engine);
    let task = {
        let runtime = rig.runtime.clone();
        let registry = rig.registry.clone();
        let engine = engine.clone();
        tokio::spawn(async move {
            engine
                .run_archives(
                    &deployment(),
                    Some(&runtime),
                    Some(&registry),
                    Duration::ZERO,
                )
                .await
        })
    };
    wait_background_probe(&probe).await;
    assert_eq!(
        *probe.sessions.lock().unwrap(),
        vec![broca_core::session_addr(&older)]
    );
    rig.registry.begin_stop();
    probe.release.add_permits(1);
    task.await.unwrap().unwrap();
    assert!(rig.registry.kept_in_new_storage(&older).unwrap());
    assert_eq!(std::fs::read(rig.wal()).unwrap(), original);
    assert_eq!(engine.health_for_test().live_background_converted, 1);
    assert_eq!(engine.health_for_test().live_remaining, 1);
}

#[tokio::test]
async fn background_live_skips_held_calls_without_recording_permanent_failure() {
    let rig = Rig::start("background-held-call").await;
    let before = rig.write_session();
    let late = Arc::new(
        broca_module::late_results_store::LateResultsStore::open(&rig.dir.join("late.db")).unwrap(),
    );
    late.register_calls(&[broca_core::tool_plane::LateResultRegistration {
        lineage: "lineage".into(),
        call_key: "held-call".into(),
        module: "provider".into(),
        bind: key(),
        scope: None,
    }])
    .unwrap();
    let engine = Engine::configured(
        &rig.dir,
        Config {
            archives: true,
            live_at_startup: false,
            backup_root: Some(rig.dir.with_extension("converted-sources")),
        },
    )
    .unwrap()
    .unwrap();
    engine.set_mover(Arc::new(
        rig.mover()
            .with_late_results(late, broca_module::held_slots::HeldSlots::new()),
    ));
    engine
        .run_archives(
            &deployment(),
            Some(&rig.runtime),
            Some(&rig.registry),
            Duration::ZERO,
        )
        .await
        .unwrap();
    assert_eq!(std::fs::read(rig.wal()).unwrap(), before);
    assert_eq!(engine.health_for_test().live_skipped_busy, 1);
    assert_eq!(engine.ledger.rows().unwrap()[0].outcome, None);
    assert_eq!(rig.registry.conversion_refusal(&key()), None);
}
