use super::*;
use broca_wal::{framing, WalConfig};

fn store(root: &std::path::Path) -> crate::FileWalStore {
    crate::FileWalStore::with_config(root, WalConfig::OVERHAUL)
}

fn rebind_planes(rig: &mut Rig, root: &std::path::Path) {
    let keys = rig.questions.keys.lock().unwrap().clone();
    let answer = *rig.questions.answer.lock().unwrap();
    rig.tools = tools_for_store(store(root), rig.bind.clone());
    rig.questions = Arc::new(Questions {
        store: store(root),
        memory: None,
        completed: rig.tools.completed.clone(),
        lineage: rig.writer.lineage_id().unwrap(),
        head: rig.writer.durable_head(),
        keys: Mutex::new(keys),
        attempts: AtomicUsize::new(0),
        polls: AtomicUsize::new(0),
        answer_gate: Mutex::new(None),
        settlement_pages: AtomicUsize::new(0),
        awaits: AtomicUsize::new(0),
        gets: AtomicUsize::new(0),
        outstanding: AtomicUsize::new(0),
        ack_calls: AtomicUsize::new(0),
        reports: Mutex::new(Vec::new()),
        detaches: Mutex::new(Vec::new()),
        detach_answer: Mutex::new("pending"),
        transient: false,
        cursors: Mutex::new(Vec::new()),
        answer: Mutex::new(answer),
        stop: None,
    });
    rig.control.elicitation_plane = Some(rig.questions.clone());
}

async fn replace_writer(rig: &mut Rig, root: &std::path::Path) {
    let wal_store = store(root);
    let writer = WalHandle::spawn_in_store(
        wal_store.open(&rig.bind).unwrap(),
        &wal_store,
        &rig.bind,
        1,
        None,
    )
    .unwrap();
    let old = std::mem::replace(&mut rig.writer, writer);
    old.shutdown().await.unwrap();
    rebind_planes(rig, root);
}

async fn expired_overhaul() -> (Rig, String) {
    let mut rig = Rig::file_backed("pass");
    let root = rig.root.join("overhaul/wal");
    replace_writer(&mut rig, &root).await;
    let rig = expire_rig(rig).await;
    record_late_consent(&rig).await;
    let prepared = rig
        .frames()
        .into_iter()
        .find(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
        .unwrap();
    let key = crate::context::call_key(rig.writer.lineage_id().unwrap(), prepared.seq);
    assert!(rig.frames().iter().any(|frame| matches!(
        frame.record,
        WalRecord::RunFinished {
            reason: RunFinishReason::Completed,
            ..
        }
    )));
    (rig, key)
}

async fn next_turn(rig: &Rig, id: &str) {
    let mut request = hook_tests::request();
    request.session = rig.bind.clone();
    request.run_id = RunId(id.into());
    request.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    request.scope = Some(
        serde_json::from_value(json!({
            "owner":{"kind":"reserved","module_id":"agent"},"ref":"s","scope_epoch":1
        }))
        .unwrap(),
    );
    assert!(rig.control.cancel.is_none());
    assert!(rig.control.stop_requested.is_none());
    let provider = crate::StubProvider::new(vec![vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: broca_protocol::MessageId(format!("message-{id}")),
                content: vec![ContentBlock::Text {
                    text: "done".into(),
                }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]]);
    let (out, _rx) = mpsc::channel(1000);
    let summary = tokio::time::timeout(
        HANG_GUARD,
        run_loop_on_writer(
            request,
            &rig.writer,
            StartMode::FreshOrResume,
            &provider,
            &rig.tools,
            &crate::AllowAll,
            &rig.tools.store,
            None,
            rig.control.clone(),
            &out,
        ),
    )
    .await
    .expect("next-turn run must finish")
    .unwrap();
    assert_eq!(summary.reason, RunFinishReason::Completed);
    for call in provider.calls() {
        broca_crash_harness::check_tool_pairing(&call.prompt).unwrap();
    }
    assert!(rig.frames().iter().any(|frame| matches!(&frame.record,
        WalRecord::RunStarted { run_id, .. } if run_id.0 == id)));
}

#[tokio::test]
async fn completed_origin_without_a_late_intent_still_executes_once() {
    let (mut rig, key) = expired_overhaul().await;
    let root = rig.root.join("overhaul/wal");
    rebind_planes(&mut rig, &root);
    next_turn(&rig, "late-positive-control").await;
    assert_eq!(rig.tools.sent.lock().unwrap().len(), 1);
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 1);
    assert_eq!(
        rig.frames()
            .iter()
            .filter(|frame| matches!(&frame.record,
                WalRecord::LateResult { call_key, .. } if call_key == &key))
            .count(),
        1
    );
    rig.finish().await;
}

#[tokio::test]
async fn recovery_cut_late_intent_never_resends_from_completed_origin() {
    check_cut_late_intent(false, false).await;
}

#[tokio::test]
async fn recovery_cut_late_intent_keeps_its_prepared_owner_after_a_later_episode() {
    check_cut_late_intent(true, false).await;
}

#[tokio::test]
async fn recovery_undecodable_late_intent_never_resends_from_completed_origin() {
    check_cut_late_intent(false, true).await;
}

async fn check_cut_late_intent(later_episode: bool, undecodable: bool) {
    let (mut rig, key) = expired_overhaul().await;
    let origin = rig
        .frames()
        .iter()
        .find_map(|frame| match &frame.record {
            WalRecord::RunStarted { run_id, .. } => Some(run_id.clone()),
            _ => None,
        })
        .unwrap();
    if later_episode {
        let mut start = rig
            .frames()
            .into_iter()
            .find_map(|frame| {
                matches!(frame.record, WalRecord::RunStarted { .. }).then_some(frame.record)
            })
            .unwrap();
        if let WalRecord::RunStarted {
            run_id,
            submission_id,
            send_id,
            request_identity_digest,
            ..
        } = &mut start
        {
            *run_id = RunId("later-completed-episode".into());
            *submission_id = None;
            *send_id = None;
            *request_identity_digest = None;
        }
        rig.writer.record(start).into_result().unwrap();
        rig.writer
            .record(WalRecord::RunFinished {
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                retries_used: 0,
                error: None,
                interrupt_reason: None,
                ts_ms: None,
            })
            .into_result()
            .unwrap();
        rig.writer.sync().await.unwrap();
    }
    rig.writer
        .record(WalRecord::LateDispatchIntent {
            call_key: key.clone(),
            approval_digest: "digest".into(),
            batch_id: BatchId(format!("late:{key}")),
        })
        .into_result()
        .unwrap();
    if undecodable {
        // Keep a complete later frame so damage to the authorized late intent
        // cannot be mistaken for an ordinary unsynced final append.
        rig.writer
            .record(WalRecord::StepStarted {
                step_id: broca_protocol::StepId(99),
            })
            .into_result()
            .unwrap();
    }
    rig.writer.sync().await.unwrap();
    let source = rig.tools.store.wal_path(&rig.bind);
    let mut bytes = std::fs::read(&source).unwrap();
    let mut offset = 0;
    let (cut_at, removed) = loop {
        match framing::decode_next(&bytes[offset..]) {
            framing::DecodeStep::Lineage { consumed, .. } => offset += consumed,
            framing::DecodeStep::Record { framed, consumed } => {
                if matches!(&framed.record, WalRecord::LateDispatchIntent { call_key, .. } if call_key == &key)
                {
                    if undecodable {
                        bytes[offset..offset + consumed].fill(0);
                        break (offset, bytes[offset..].to_vec());
                    }
                    let mut metadata = framing::v4::decode_header(&bytes[offset..])
                        .unwrap()
                        .unwrap()
                        .metadata;
                    metadata.seq += 1;
                    let wrong_sequence = framing::encode_v4(
                        metadata,
                        &[framing::FRAME_STAMPS_FEATURE],
                        &framed.record,
                    )
                    .unwrap();
                    assert_eq!(wrong_sequence.len(), consumed);
                    bytes[offset..offset + consumed].copy_from_slice(&wrong_sequence);
                    assert!(matches!(framing::decode_next(&bytes[offset..]),
                        framing::DecodeStep::Record { framed, .. }
                        if matches!(&framed.record, WalRecord::LateDispatchIntent { call_key, .. } if call_key == &key)));
                    break (offset, bytes[offset..].to_vec());
                }
                offset += consumed;
            }
            _ => panic!("the synced fixture must decode before its late intent"),
        }
    };
    let root = rig.root.join("repaired/wal");
    let wal_store = store(&root);
    let path = wal_store.wal_path(&rig.bind);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, &bytes).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .sync_all()
        .unwrap();
    replace_writer(&mut rig, &root).await;
    let repaired_before_drive = rig.frames();

    // The next-turn request enters the actual run loop with fresh routing counters.
    // Its only late work belongs to the already Completed origin episode.
    next_turn(&rig, "late-after-recovery").await;
    assert!(
        rig.tools.sent.lock().unwrap().is_empty(),
        "a cut late intent must never be sent again"
    );
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
    assert!(
        rig.tools.routes.lock().unwrap().is_empty(),
        "no late batch may be opened"
    );
    let frames = rig.frames();
    let cuts: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.record {
            WalRecord::RecoveryCut { recovery } => Some(recovery.as_ref()),
            _ => None,
        })
        .collect();
    assert_eq!(cuts.len(), 1);
    let cut = cuts[0];
    assert_eq!(cut.cut_offset, cut_at as u64);
    assert_eq!(
        std::fs::read(path.parent().unwrap().join(&cut.sidecar_basename)).unwrap(),
        removed
    );
    assert!(
        cut.runs
            .iter()
            .any(|run| run.run_id == origin
                && run.calls.iter().any(|call| call.tool_call_id.0 == "c"))
    );
    assert!(!frames.iter().any(|frame| matches!(&frame.record,
        WalRecord::LateDispatchIntent { call_key, .. } if call_key == &key)));
    let unknown = |frames: &[broca_wal::FramedRecord]| {
        frames.iter().filter(|frame| matches!(&frame.record,
        WalRecord::RecoveryCallClosed { run_id, call_key: Some(call_key),
            outcome: broca_wal::recovery::RecoveryOutcome::Unknown, result, .. }
        if run_id == &origin && call_key == &key && result.as_ref() == &crate::outcome_unknown_result(&broca_protocol::ToolCallId("c".into())))).count()
    };
    assert_eq!(unknown(&frames), 1);
    let split = repaired_before_drive
        .iter()
        .position(|frame| matches!(frame.record, WalRecord::RecoveryCut { .. }))
        .unwrap();
    let mut full_projection =
        crate::replay::DeferredCallProjection::new(rig.writer.lineage_id().unwrap());
    full_projection.fold_frames(&repaired_before_drive);
    let mut split_projection =
        crate::replay::DeferredCallProjection::new(rig.writer.lineage_id().unwrap());
    split_projection.fold_frames(&repaired_before_drive[..split]);
    split_projection =
        serde_json::from_value(serde_json::to_value(split_projection).unwrap()).unwrap();
    split_projection.fold_frames(&repaired_before_drive[split..]);
    assert_eq!(split_projection, full_projection);
    assert!(
        !full_projection.late_allowed(full_projection.calls.get(&key).unwrap()),
        "Completed origin must not authorize a recovery-tombstoned call"
    );
    assert!(rig
        .questions
        .reports
        .lock()
        .unwrap()
        .iter()
        .any(|(_, outcome)| matches!(outcome, ApprovalExecution::OutcomeUnknown)));
    next_turn(&rig, "late-after-recovery-again").await;
    assert_eq!(unknown(&rig.frames()), 1);
    assert_eq!(
        rig.frames()
            .iter()
            .filter(|frame| matches!(&frame.record,
                WalRecord::LateResult { call_key, .. }
                | WalRecord::RecoveryCallClosed { call_key: Some(call_key), .. }
                if call_key == &key))
            .count(),
        1
    );
    assert!(rig.tools.sent.lock().unwrap().is_empty());
    rig.finish().await;
}

#[tokio::test]
async fn approved_prepared_control_dispatches_once_without_recovery() {
    check_prepared_recovery(false).await;
}

#[tokio::test]
async fn recovered_prepared_approval_closes_unknown_without_dispatch() {
    check_prepared_recovery(true).await;
}

async fn check_prepared_recovery(cut: bool) {
    let mut rig = Rig::file_backed("hold");
    let root = rig.root.join("prepared/wal");
    replace_writer(&mut rig, &root).await;
    *rig.questions.answer.lock().unwrap() = "approve";
    let stop = CancellationToken::new();
    rig.control.stop_requested = Some(stop.clone());
    let batch = rig.batch(&crate::AllowAll, vec![call("c")], None, true);
    let interrupt = async {
        broca_test_support::wait_until("second approval validation", || async {
            let count = rig.hooks.validates.load(Ordering::SeqCst);
            if count >= 2 {
                Ok(())
            } else {
                Err(format!("validation count={count}"))
            }
        })
        .await;
        stop.cancel();
    };
    let (paused, ()) = tokio::time::timeout(HANG_GUARD, async { tokio::join!(batch, interrupt) })
        .await
        .unwrap();
    assert_eq!(paused.interruption, Some(BatchInterruption::RestartPause));
    assert!(paused.open[0].approved);
    let frames = rig.frames();
    let approval = frames
        .iter()
        .find_map(|frame| {
            matches!(
                &frame.record,
                WalRecord::ApprovalRecorded {
                    decision: broca_wal::DeferredDecision::Approved,
                    ..
                }
            )
            .then_some(Arc::new(frame.record.clone()))
        })
        .unwrap();
    let prepared = frames
        .iter()
        .find(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
        .unwrap();
    let key = crate::context::call_key(rig.writer.lineage_id().unwrap(), prepared.seq);
    let resume = ResumeCtx {
        batch_already_started: true,
        prepared: vec![crate::replay::PreparedCall {
            record_index: 0,
            prepared: paused.open[0].prepared.clone(),
            approval: Some(approval),
            elicitation_id: paused.open[0].id.clone(),
        }],
        ..Default::default()
    };
    if cut {
        let intent: WalRecord = serde_json::from_value(json!({"type":"tool_dispatch_intent",
            "batch_id":"r-b1", "tool_call_id":"c", "args":{}, "call_key":key}))
        .unwrap();
        rig.writer.record(intent).into_result().unwrap();
        rig.writer.sync().await.unwrap();
        let mut bytes = std::fs::read(rig.tools.store.wal_path(&rig.bind)).unwrap();
        let mut offset = 0;
        loop {
            match framing::decode_next(&bytes[offset..]) {
                framing::DecodeStep::Lineage { consumed, .. } => offset += consumed,
                framing::DecodeStep::Record { framed, consumed } => {
                    if matches!(framed.record, WalRecord::ToolDispatchIntent { .. }) {
                        let mut metadata = framing::v4::decode_header(&bytes[offset..])
                            .unwrap()
                            .unwrap()
                            .metadata;
                        // Keep the intent readable for attribution, but outside
                        // the contiguous prefix retained by physical recovery.
                        metadata.seq += 1;
                        let frame = framing::encode_v4(
                            metadata,
                            &[framing::FRAME_STAMPS_FEATURE],
                            &framed.record,
                        )
                        .unwrap();
                        assert_eq!(frame.len(), consumed);
                        bytes[offset..offset + consumed].copy_from_slice(&frame);
                        break;
                    }
                    offset += consumed;
                }
                _ => panic!("the prepared fixture must decode before its damaged intent"),
            }
        }
        let repaired = rig.root.join("prepared-repaired/wal");
        let path = store(&repaired).wal_path(&rig.bind);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
        replace_writer(&mut rig, &repaired).await;
        assert!(rig.writer.recovery_tombstoned_key(&key));
    }
    // Direct batch tests bypass the run loop's tag initialization after reopen.
    rig.writer.seed_lineage_tags(Some(crate::tags::lineage_tags(
        rig.frames().iter().map(|frame| &frame.record),
    )));
    rig.control.stop_requested = None;
    rig.hooks = Arc::new(Hooks {
        validates: AtomicUsize::new(0),
        second: "pass",
        expires: rig.hooks.expires,
        seen: Mutex::new(Vec::new()),
    });
    rig.control.hook_plane = Some(rig.hooks.clone());
    let result = rig
        .batch(&crate::AllowAll, vec![call("c")], Some(resume), true)
        .await;
    assert_eq!(result.results.len(), 1);
    assert_eq!(
        rig.tools.sent.lock().unwrap().len(),
        usize::from(!cut),
        "a recovered approved Prepared call must not execute"
    );
    let frames = rig.frames();
    if cut {
        assert!(frames.iter().any(|frame| matches!(&frame.record,
            WalRecord::ToolResult { outcome: DispatchOutcome::Unknown, result, .. }
            if result.tool_call_id.0 == "c")));
        assert!(!frames
            .iter()
            .any(|frame| matches!(frame.record, WalRecord::ToolDispatchIntent { .. })));
        assert_eq!(rig.hooks.validates.load(Ordering::SeqCst), 0);
    }
    rig.finish().await;
}

#[tokio::test]
async fn a_new_run_reuses_removed_sequences_without_reusing_physical_call_keys() {
    let mut rig = Rig::file_backed("pass");
    let root = rig.root.join("generation-before/wal");
    replace_writer(&mut rig, &root).await;
    rig.control.manifest = Some(Arc::new(frozen(
        vec![vec![
            sub("pre_tool", Some("approve"), "refuse"),
            sub("pre_tool", Some("validate"), "pass"),
        ]],
        None,
        false,
    )));
    next_turn(&rig, "completed-before-repair").await;
    let first_removed = rig.writer.next_record_seq();
    for index in 0..80 {
        let intent: WalRecord = serde_json::from_value(json!({"type":"tool_dispatch_intent",
            "batch_id":"old-batch", "tool_call_id":format!("old-{index}"), "args":{}}))
        .unwrap();
        rig.writer.record(intent).into_result().unwrap();
    }
    rig.writer.sync().await.unwrap();
    let mut bytes = std::fs::read(rig.tools.store.wal_path(&rig.bind)).unwrap();
    let mut offset = 0;
    loop {
        match framing::decode_next(&bytes[offset..]) {
            framing::DecodeStep::Lineage { consumed, .. } => offset += consumed,
            framing::DecodeStep::Record { framed, consumed } => {
                if framed.seq == first_removed {
                    let mut metadata = framing::v4::decode_header(&bytes[offset..])
                        .unwrap()
                        .unwrap()
                        .metadata;
                    metadata.seq += 1;
                    let changed = framing::encode_v4(
                        metadata,
                        &[framing::FRAME_STAMPS_FEATURE],
                        &framed.record,
                    )
                    .unwrap();
                    assert_eq!(changed.len(), consumed);
                    bytes[offset..offset + consumed].copy_from_slice(&changed);
                    break;
                }
                offset += consumed;
            }
            _ => panic!("the source must be intact before its removed intent interval"),
        }
    }
    let repaired = rig.root.join("generation-after/wal");
    let path = store(&repaired).wal_path(&rig.bind);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
    replace_writer(&mut rig, &repaired).await;
    let cuts = broca_wal::recovery::manifests(&path, 1_000_000).unwrap();
    let old_keys: std::collections::BTreeSet<_> = cuts
        .iter()
        .flat_map(|cut| {
            cut.recovery
                .runs
                .iter()
                .flat_map(|run| run.calls.iter())
                .flat_map(|call| call.call_keys.iter().cloned())
                .chain(
                    cut.recovery
                        .unowned_calls
                        .iter()
                        .flat_map(|call| call.call_keys.iter().cloned()),
                )
        })
        .collect();
    assert!(old_keys.len() >= 79);
    *rig.questions.answer.lock().unwrap() = "approve";
    rig.control.manifest = Some(Arc::new(frozen(
        vec![vec![
            sub("pre_tool", Some("approve"), "refuse"),
            sub("pre_tool", Some("validate"), "pass"),
        ]],
        None,
        false,
    )));
    let event = |index, content, reason| {
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: broca_protocol::MessageId(format!("generation-{index}")),
                    content,
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: reason,
                retries_used: 0,
            }),
        ]
    };
    let provider = crate::StubProvider::new(vec![
        event(
            1,
            vec![ContentBlock::ToolCall(call("new-call"))],
            FinishReason::ToolCalls,
        ),
        event(
            2,
            vec![ContentBlock::Text {
                text: "done".into(),
            }],
            FinishReason::Stop,
        ),
    ]);
    let mut request = hook_tests::request();
    request.session = rig.bind.clone();
    request.run_id = RunId("new-after-repair".into());
    request.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    request.scope = Some(
        serde_json::from_value(
            json!({"owner":{"kind":"reserved","module_id":"agent"},"ref":"s","scope_epoch":1}),
        )
        .unwrap(),
    );
    let (out, _rx) = mpsc::channel(1000);
    let summary = tokio::time::timeout(
        HANG_GUARD,
        run_loop_on_writer(
            request,
            &rig.writer,
            StartMode::FreshOrResume,
            &provider,
            &rig.tools,
            &crate::AllowAll,
            &rig.tools.store,
            None,
            rig.control.clone(),
            &out,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        rig.tools.sent.lock().unwrap().len(),
        1,
        "new work in the removed sequence interval must execute"
    );
    assert_eq!(summary.reason, RunFinishReason::Completed);
    let prepared = rig
        .frames()
        .into_iter()
        .find(|frame| {
            matches!(&frame.record,
        WalRecord::ToolCallDeferred { call_id, .. } if call_id.0 == "new-call")
        })
        .unwrap();
    assert!(prepared.seq > first_removed && prepared.seq < first_removed + 80);
    let old_spelling = format!("{}:{}", rig.writer.lineage_id().unwrap(), prepared.seq);
    assert!(
        old_keys.contains(&old_spelling),
        "the fixture must reach a genuinely tombstoned physical sequence"
    );
    let keys = rig.questions.keys.lock().unwrap().clone();
    assert_eq!(keys.len(), 1);
    assert!(
        !old_keys.contains(&keys[0]),
        "the post-repair physical key must never alias a removed key"
    );
    let mut projection =
        crate::replay::DeferredCallProjection::new(rig.writer.lineage_id().unwrap());
    projection.fold_frames(&rig.frames());
    assert!(
        projection.calls.contains_key(&keys[0]),
        "replay must reconstruct the same generated key that was filed"
    );
    assert!(!projection.recovery_tombstoned(&projection.calls[&keys[0]]));
    let restored: crate::replay::DeferredCallProjection =
        serde_json::from_str(&serde_json::to_string(&projection).unwrap()).unwrap();
    assert_eq!(restored, projection);

    let ordinary: WalRecord = serde_json::from_value(json!({"type":"tool_dispatch_intent",
        "batch_id":"next-ordinary", "tool_call_id":"new-ordinary", "args":{}}))
    .unwrap();
    let ordinary_seq = rig.writer.record(ordinary).into_result().unwrap();
    rig.writer.sync().await.unwrap();
    assert!(ordinary_seq > first_removed && ordinary_seq < first_removed + 80);
    let plain = format!("{}:{ordinary_seq}", rig.writer.lineage_id().unwrap());
    assert!(old_keys.contains(&plain));
    let frames = rig.frames();
    let WalRecord::ToolDispatchIntent {
        call_key: Some(ordinary_key),
        ..
    } = &frames
        .iter()
        .find(|frame| frame.seq == ordinary_seq)
        .unwrap()
        .record
    else {
        panic!("post-repair ordinary intents must persist their generated key");
    };
    assert!(!old_keys.contains(ordinary_key));
    assert!(ordinary_key.is_ascii() && ordinary_key.len() < 256);
    rig.finish().await;
}

#[tokio::test]
async fn a_readable_cut_model_step_interrupts_without_requesting_the_model_again() {
    let mut rig = Rig::file_backed("pass");
    let source = rig.root.join("readable-step/wal");
    replace_writer(&mut rig, &source).await;
    next_turn(&rig, "readable-step-run").await;
    let mut bytes = std::fs::read(rig.tools.store.wal_path(&rig.bind)).unwrap();
    let mut offset = 0;
    loop {
        match framing::decode_next(&bytes[offset..]) {
            framing::DecodeStep::Lineage { consumed, .. } => offset += consumed,
            framing::DecodeStep::Record { framed, consumed } => {
                if matches!(framed.record, WalRecord::ModelStepFinished { .. }) {
                    let header = framing::v4::decode_header(&bytes[offset..])
                        .unwrap()
                        .unwrap();
                    let mut metadata = header.metadata;
                    metadata.seq += 1;
                    let requires = header
                        .requires
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>();
                    let changed = framing::encode_v4(metadata, &requires, &framed.record).unwrap();
                    assert_eq!(changed.len(), consumed);
                    bytes[offset..offset + consumed].copy_from_slice(&changed);
                    break;
                }
                offset += consumed;
            }
            _ => panic!("source must decode before the model step"),
        }
    }
    let repaired = rig.root.join("readable-step-repaired/wal");
    let path = store(&repaired).wal_path(&rig.bind);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, bytes).unwrap();
    replace_writer(&mut rig, &repaired).await;
    let cuts = broca_wal::recovery::manifests(&path, 1_000_000).unwrap();
    assert_eq!(cuts.len(), 1);
    assert!(!cuts[0].recovery.unattributed_tool_risk);
    assert!(
        cuts[0].recovery.runs.is_empty(),
        "the regression must not rely on a tool-call tombstone"
    );
    let mut request = hook_tests::request();
    request.session = rig.bind.clone();
    request.run_id = RunId("readable-step-run".into());
    request.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    let provider = crate::StubProvider::new(vec![]);
    let (out, _rx) = mpsc::channel(1000);
    let summary = tokio::time::timeout(
        HANG_GUARD,
        run_loop_on_writer(
            request,
            &rig.writer,
            StartMode::FreshOrResume,
            &provider,
            &rig.tools,
            &crate::AllowAll,
            &rig.tools.store,
            None,
            rig.control.clone(),
            &out,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(summary.reason, RunFinishReason::Interrupted);
    assert!(
        provider.calls().is_empty(),
        "a cut answer must not be silently requested again"
    );
    assert!(rig.tools.sent.lock().unwrap().is_empty());
    rig.finish().await;
}
