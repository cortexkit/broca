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
    check_cut_late_intent(false).await;
}

#[tokio::test]
async fn recovery_cut_late_intent_keeps_its_prepared_owner_after_a_later_episode() {
    check_cut_late_intent(true).await;
}

async fn check_cut_late_intent(later_episode: bool) {
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
        WalRecord::LateResult { call_key, outcome: broca_wal::LateOutcome::Unknown, result }
        if call_key == &key && result.as_ref() == &crate::outcome_unknown_result(&broca_protocol::ToolCallId("c".into())))).count()
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
        WalRecord::LateResult { call_key, .. } if call_key == &key))
            .count(),
        1
    );
    assert!(rig.tools.sent.lock().unwrap().is_empty());
    rig.finish().await;
}
