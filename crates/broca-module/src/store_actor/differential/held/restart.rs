use super::*;

#[tokio::test]
async fn run_awaits_restart_pause_waits_on_the_runner() {
    for store in [false, true] {
        let side = paused(store).await;
        crate::store_actor::awaits_tests::prove(
            &side.registry,
            &side.key,
            if store {
                "restart.after"
            } else {
                "restart.before"
            },
            store.then_some(("paused", "runner", Some("restart"))),
        )
        .await;
        assert!(side.registry.stop_and_seal().await);
    }
}

async fn submit(side: &Side, run: &str, text: &str) -> SendOutcome {
    let mut req = request(text);
    req.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    if run != "r1" {
        req.tools = None;
    }
    side.registry
        .route_submission_with_order(
            &side.key,
            RunId(run.into()),
            req,
            RunKind::FreshOrResume,
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
        .unwrap()
}

async fn paused(store: bool) -> Side {
    paused_for(store, 0).await
}

/// A session whose run paused for a restart `age_ms` ago.
async fn paused_for(store: bool, age_ms: u64) -> Side {
    let dir = scratch_dir();
    let key = BindIdentity::new(dir.join("project"), "restart-comparison", "s1");
    let side = Side::at(
        dir,
        key,
        store,
        Cipher::None,
        vec![super::super::super::tests::auth_failure()],
        Arc::default(),
        None,
    )
    .await;
    // Reuse the recorded-pause fixture so the comparison isolates recovery, not shutdown.
    // Both loops durably record the same restart pause and frozen on_restart preference.
    *side.executor.pause_as.lock().unwrap() = Some(broca_protocol::PauseReason::Restart);
    side.executor.pause_age_ms.store(age_ms, Ordering::SeqCst);
    assert!(matches!(
        submit(&side, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    side.idle().await;
    side
}

async fn seed_queued_send(side: &Side) {
    let mut req = request("queued");
    req.tools = None;
    req.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    // The restart image is captured before a queued send's coordinator can drain it.
    assert!(side.registry.slot_state(&side.key).is_none());
    if side.store {
        let runtime = side.registry.store_runtime().unwrap();
        let class = runtime.class_of(&side.key).unwrap().unwrap();
        let mut session = Session::load(
            broca_session_store::backend::LocalBackend::new(class.store_root(&side.dir)),
            broca_session_store::backend::SessionAddr(broca_core::session_addr(&side.key)),
            &crate::store_open::triple(&side.key),
            runtime.deployment(),
        )
        .await
        .unwrap();
        let mut parts = runtime.parts_for(class, side.cipher).clone();
        let queued = broca_session_loop::queue::queued_send(
            "sub-r2",
            None,
            &req,
            &broca_session_loop::queue::Receipt::default(),
            broca_session_loop::queue::Queued {
                written_at_ms: Some(1),
                ts_ms: None,
                delivery: None,
            },
            None,
            &mut parts,
        )
        .unwrap();
        Writer::append(
            &mut session,
            &[broca_session_store::record::Record::QueuedSend(queued)],
            true,
        )
        .await
        .unwrap();
        session.close().await.unwrap();
    } else {
        side.executor
            .wal
            .open(&side.key)
            .unwrap()
            .append(
                1,
                &broca_wal::WalRecord::Queued {
                    submission_id: broca_protocol::SubmissionId("sub-r2".into()),
                    request: Box::new(req),
                    written_at_ms: Some(1),
                    ts_ms: None,
                    delivery: None,
                    request_identity_digest: None,
                    principal: None,
                    sent_by: None,
                    retention: None,
                    keep_warm: None,
                    conversation_key: None,
                    send_id: None,
                    title: None,
                    hooks: None,
                    message_marks: None,
                },
                true,
            )
            .unwrap();
    }
}

async fn resume_command(side: &Side) {
    let actor = side.registry.actor_for(&side.key).await.unwrap();
    let (reply, answer) = tokio::sync::oneshot::channel();
    assert!(actor
        .send(crate::actor::LineageCmd::ResumeRestart { reply })
        .await
        .is_ok());
    answer.await.unwrap().unwrap();
}

async fn ended_without_reading(side: &Side, run: &str) {
    broca_test_support::wait_until("background restart continuation ends", || async {
        // The terminal can be durable just before the actor consumes the task's completion.
        // Observe its published busy gauge as well, without sending any session command.
        let idle = side
            .registry
            .open_handle(&side.key)
            .is_none_or(|handle| handle.run_in_flight.load(Ordering::Acquire) == 0);
        if side.store {
            let runtime = side.registry.store_runtime().unwrap();
            let view = runtime
                .current_view(&side.key)
                .await
                .map_err(|e| e.to_string())?
                .unwrap();
            if view
                .operations
                .run
                .as_ref()
                .is_some_and(|state| state.run_id == run && state.finished.is_some())
                && idle
            {
                Ok(())
            } else {
                Err(format!("{:?}", view.operations.run))
            }
        } else {
            let records = side
                .executor
                .wal
                .read_records(&side.key)
                .map_err(|e| e.to_string())?;
            let state = broca_core::reconcile_run_state(records.iter().map(|r| &r.record), run);
            if state.is_some_and(|state| state.state.is_terminal()) && idle {
                Ok(())
            } else {
                Err("legacy continuation is unfinished".into())
            }
        }
    })
    .await;
}

async fn story(side: &Side) -> Vec<String> {
    let mut result = Vec::new();
    if side.store {
        let view = side
            .registry
            .store_runtime()
            .unwrap()
            .current_view(&side.key)
            .await
            .unwrap()
            .unwrap();
        for (_, record) in view.records_from(0).await.unwrap() {
            match record {
                broca_session_store::record::Record::RunStart(start) => {
                    result.push(format!("start {}", start.run_id))
                }
                broca_session_store::record::Record::Pause(pause) => {
                    result.push(format!("pause {} {:?}", pause.run_id, pause.reason))
                }
                broca_session_store::record::Record::Resumed(resume) => result.push(format!(
                    "resume {} {} {} {}",
                    resume.run_id, resume.messages, resume.model_steps, resume.tool_results
                )),
                broca_session_store::record::Record::RunEnd(end) => result.push(format!(
                    "end {:?} {:?}",
                    end.reason,
                    end.error
                        .as_ref()
                        .and_then(|error| error.get("provider_code"))
                        .and_then(Value::as_str)
                )),
                _ => {}
            }
        }
    } else {
        for record in side.executor.wal.read_records(&side.key).unwrap() {
            match record.record {
                broca_wal::WalRecord::RunStarted { run_id, .. } => {
                    result.push(format!("start {}", run_id.0))
                }
                broca_wal::WalRecord::RunPaused { run_id, reason, .. } => {
                    result.push(format!("pause {} {:?}", run_id.0, reason))
                }
                broca_wal::WalRecord::ResumeStarted {
                    episode, replayed, ..
                } => result.push(format!(
                    "resume {} {} {} {}",
                    episode.0, replayed.messages, replayed.model_steps, replayed.tool_results
                )),
                broca_wal::WalRecord::RunFinished { reason, error, .. } => result.push(format!(
                    "end {:?} {:?}",
                    reason,
                    error
                        .as_ref()
                        .and_then(|error| error.provider_code.as_deref())
                )),
                _ => {}
            }
        }
    }
    result
}

#[tokio::test]
async fn restart_pauses_resume_after_reopen_with_the_same_records_and_replies() {
    let mut originals = Vec::new();
    let mut resumed = Vec::new();
    for store in [false, true] {
        let original = paused(store).await;
        let side = original.restarted().await;
        side.registry.handle().enable_restart_resumes_for_test();
        resume_command(&side).await;
        ended_without_reading(&side, "r1").await;
        assert_eq!(side.status().await["state"], "completed");
        assert_eq!(side.executor.provider.inner.calls().len(), 1);
        originals.push(original);
        resumed.push(side);
    }
    assert_eq!(story(&resumed[1]).await, story(&resumed[0]).await);
    assert_eq!(resumed[1].status().await, resumed[0].status().await);
    let mut replies = [resumed[0].result().await, resumed[1].result().await];
    for reply in &mut replies {
        reply["final_message"]
            .as_object_mut()
            .unwrap()
            .remove("mid");
    }
    assert_eq!(replies[1], replies[0]);
    for side in originals.iter().chain(&resumed) {
        assert!(side.registry.stop_and_seal().await);
    }
}

#[tokio::test]
async fn refused_restart_resume_is_interrupted_and_drains_the_next_send() {
    let mut originals = Vec::new();
    let mut resumed = Vec::new();
    for store in [false, true] {
        let original = paused(store).await;
        let side = original.restarted().await;
        seed_queued_send(&side).await;
        side.executor.refuse_restart.store(true, Ordering::SeqCst);
        side.registry.handle().enable_restart_resumes_for_test();
        resume_command(&side).await;
        ended_without_reading(&side, "r2").await;
        let status = side.status().await;
        assert_eq!(status["state"], "interrupted");
        assert_eq!(status["error"]["provider_code"], "restart_resume_refused");
        originals.push(original);
        resumed.push(side);
    }
    assert_eq!(story(&resumed[1]).await, story(&resumed[0]).await);
    assert_eq!(resumed[1].status().await, resumed[0].status().await);
    assert_eq!(resumed[1].result().await, resumed[0].result().await);
    for side in originals.iter().chain(&resumed) {
        assert!(side.registry.stop_and_seal().await);
    }
}

#[tokio::test]
async fn restart_command_waits_for_readiness_and_the_later_command_resumes() {
    for store in [false, true] {
        let original = paused(store).await;
        let side = original.restarted().await;
        resume_command(&side).await;
        assert_eq!(side.executor.provider.inner.calls().len(), 0);
        if store {
            let view = side
                .registry
                .store_runtime()
                .unwrap()
                .current_view(&side.key)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                view.operations.run.as_ref().unwrap().paused,
                Some(broca_protocol::PauseReason::Restart)
            );
        }
        side.registry.handle().enable_restart_resumes_for_test();
        resume_command(&side).await;
        ended_without_reading(&side, "r1").await;
        assert_eq!(side.status().await["state"], "completed");
        assert!(side.registry.stop_and_seal().await);
        assert!(original.registry.stop_and_seal().await);
    }
}

#[tokio::test]
async fn background_candidates_resume_an_untouched_restart_paused_store_session() {
    let original = paused(true).await;
    let side = original.restarted().await;
    assert!(side.registry.slot_state(&side.key).is_none());
    // No store session is supplied by the legacy index: only durable store candidates can
    // discover this continuation. The observer below reads storage, never an actor route.
    side.registry
        .resume_restart_pauses(side.executor.wal.clone(), Vec::new())
        .await;
    side.registry.recover_store_crash_orphans().await.unwrap();
    ended_without_reading(&side, "r1").await;
    assert_eq!(side.executor.provider.inner.calls().len(), 1);
    assert_eq!(side.status().await["state"], "completed");
    assert!(side.registry.stop_and_seal().await);
    assert!(original.registry.stop_and_seal().await);
}

/// A restart pause older than the continuation window is not continued on the new storage:
/// the run ends Interrupted without a model call, and the send queued behind it starts. The
/// old storage is not compared: it resumes every restart pause at its next start, so its
/// pauses never age, and it gets no such limit.
#[tokio::test]
async fn an_old_restart_pause_ends_interrupted_and_the_next_send_starts() {
    let two_hours_ms = 2 * 60 * 60 * 1000;
    let original = paused_for(true, two_hours_ms).await;
    let side = original.restarted().await;
    seed_queued_send(&side).await;
    side.registry.handle().enable_restart_resumes_for_test();
    resume_command(&side).await;
    ended_without_reading(&side, "r2").await;
    let status = side.status().await;
    assert_eq!(status["state"], "interrupted", "{status}");
    assert_eq!(
        status["error"]["provider_code"], "restart_resume_expired",
        "{status}"
    );
    // The one model call is the queued send's: the old run sent nothing.
    assert_eq!(side.executor.provider.inner.calls().len(), 1);
    let story = story(&side).await;
    assert!(
        story.contains(&"end Interrupted Some(\"restart_resume_expired\")".to_owned()),
        "{story:?}"
    );
    assert!(side.registry.stop_and_seal().await);
    assert!(original.registry.stop_and_seal().await);
}
