use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use broca_core::{
    read_session_page, replay, resume_state_from_store, run_import, FileWalStore, ImportError,
    ImportOrigin, ImportRequest, ReplayResolution, StorePageSnapshot, StorePrefix, WalHandle,
    WalStore,
};
use broca_protocol::content::{ContentBlock, Message, Role, ToolCall, ToolOutput};
use broca_protocol::{BindIdentity, RunFinishReason, RunId, ToolCallId, Usage};
use broca_wal::{RunOrigin, WalRecord};

static NEXT_DIR: AtomicU64 = AtomicU64::new(1);

fn temp_dir(label: &str) -> PathBuf {
    let path = broca_test_support::scratch_root().join(format!(
        "broca-import-{label}-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).expect("create temp dir");
    path
}

fn text(role: Role, value: &str) -> Message {
    Message::new(
        role,
        vec![ContentBlock::Text {
            text: value.to_string(),
        }],
    )
}

fn request(session: &BindIdentity, run_id: &str, messages: Vec<Message>) -> ImportRequest {
    ImportRequest {
        project_id: None,
        principal: None,
        run_id: RunId(run_id.into()),
        session: session.clone(),
        messages,
        origin: ImportOrigin {
            source_ref: "cc:source-1".into(),
        },
    }
}

#[tokio::test]
async fn import_replays_idle_and_is_idempotent() {
    let root = temp_dir("idempotent");
    let store = FileWalStore::new(root.join("wal"));
    let session = BindIdentity::new("/project", "runner", "import-idempotent");
    let writer =
        WalHandle::spawn(store.open(&session).expect("open WAL"), 1).expect("spawn writer");
    let messages = vec![text(Role::User, "hello"), text(Role::Assistant, "world")];

    let first = run_import(
        request(&session, "run-import-1", messages.clone()),
        &writer,
        &store,
    )
    .await
    .expect("import");
    assert!(!first.already_imported);
    let retry = run_import(
        request(&session, "run-import-1", messages.clone()),
        &writer,
        &store,
    )
    .await
    .expect("idempotent retry");
    assert!(retry.already_imported);
    assert_eq!(
        run_import(
            request(&session, "run-import-1", vec![text(Role::User, "changed")]),
            &writer,
            &store,
        )
        .await,
        Err(ImportError::ImportIdReuse)
    );

    let records: Vec<_> = store
        .read_records(&session)
        .expect("read WAL")
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    assert_eq!(records.len(), 2, "retry must not append records");
    let state = replay(&records).expect("replay import");
    let mut expected = messages;
    broca_protocol::content::stamp_ordinals(&mut expected);
    assert_eq!(state.prompt, expected);
    assert_eq!(
        state.resolution,
        ReplayResolution::LineageIdle {
            last_reason: RunFinishReason::Completed
        }
    );
    assert_eq!(state.completed_steps, 0);
    let page = read_session_page(
        &StorePageSnapshot::empty(),
        &store.read_records(&session).unwrap(),
        Some(0),
        Some(500),
    )
    .expect("read imported transcript");
    assert_eq!(
        page.messages
            .into_iter()
            .map(|entry| entry.message)
            .collect::<Vec<_>>(),
        state.prompt
    );

    let _ = writer.shutdown().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn dangling_import_is_interrupted_then_repaired_and_tool_pairs_are_normalized() {
    let source_root = temp_dir("source");
    let source_store = FileWalStore::new(source_root.join("wal"));
    let source_session = BindIdentity::new("/project", "runner", "import-repair");
    let source_writer = WalHandle::spawn(source_store.open(&source_session).expect("open WAL"), 1)
        .expect("spawn writer");
    run_import(
        request(
            &source_session,
            "run-source",
            vec![text(Role::User, "source")],
        ),
        &source_writer,
        &source_store,
    )
    .await
    .expect("source import");
    let _ = source_writer.shutdown().await;
    let started = source_store
        .read_records(&source_session)
        .expect("read source")
        .remove(0)
        .record;

    let root = temp_dir("repair");
    let store = FileWalStore::new(root.join("wal"));
    let session = BindIdentity::new("/project", "runner", "import-repair");
    let mut raw_wal = store.open(&session).expect("open raw WAL");
    raw_wal.append(1, &started, true).expect("write torn start");
    drop(raw_wal);

    let torn_records: Vec<_> = store
        .read_records(&session)
        .expect("read torn WAL")
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    assert!(matches!(
        replay(&torn_records)
            .expect("replay torn import")
            .resolution,
        ReplayResolution::Interrupted { .. }
    ));

    let call_id = ToolCallId("call-1".into());
    let dangling = Message::new(
        Role::Assistant,
        vec![ContentBlock::ToolCall(ToolCall {
            tool_call_id: call_id.clone(),
            tool_name: "read".into(),
            input: serde_json::json!({"path": "x"}),
            thought_signature: None,
        })],
    );
    let orphan = Message::new(
        Role::Tool,
        vec![ContentBlock::ToolResult(broca_protocol::ToolResult {
            tool_call_id: ToolCallId("orphan".into()),
            output: ToolOutput::Text { text: "no".into() },
            is_error: false,
        })],
    );
    let writer =
        WalHandle::spawn(store.open(&session).expect("reopen WAL"), 1).expect("spawn writer");
    let outcome = run_import(
        request(&session, "run-repair", vec![dangling, orphan]),
        &writer,
        &store,
    )
    .await
    .expect("repair import");
    assert_eq!(outcome.synthesized_tool_results, 1);
    assert_eq!(outcome.dropped_orphan_tool_results, 1);

    let records: Vec<_> = store
        .read_records(&session)
        .expect("read repaired WAL")
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    assert_eq!(records.len(), 4);
    assert!(matches!(
        records[1],
        WalRecord::RunFinished {
            reason: RunFinishReason::Interrupted,
            ..
        }
    ));
    assert!(matches!(
        records[2],
        WalRecord::RunStarted {
            origin: RunOrigin::SeededFrom { .. },
            ..
        }
    ));
    let state = replay(&records).expect("replay repaired import");
    assert!(matches!(
        state.resolution,
        ReplayResolution::LineageIdle { .. }
    ));
    assert_eq!(state.prompt.len(), 2);
    let ContentBlock::ToolResult(result) = &state.prompt[1].content[0] else {
        panic!("expected synthesized tool result");
    };
    assert_eq!(result.tool_call_id, call_id);
    assert!(result.is_error);

    let _ = writer.shutdown().await;
    let _ = std::fs::remove_dir_all(root);
    let _ = std::fs::remove_dir_all(source_root);
}

#[tokio::test]
async fn materialized_import_prefix_merges_with_a_following_episode_tail() {
    let root = temp_dir("store-merge");
    let store = FileWalStore::new(root.join("wal"));
    let session = BindIdentity::new("/project", "runner", "import-store-merge");
    let writer =
        WalHandle::spawn(store.open(&session).expect("open WAL"), 1).expect("spawn writer");
    let imported = vec![
        text(Role::User, "old user"),
        text(Role::Assistant, "old answer"),
    ];
    run_import(
        request(&session, "run-import", imported.clone()),
        &writer,
        &store,
    )
    .await
    .expect("import");

    let import_frames = store.read_records(&session).expect("read imported frames");
    assert_eq!(
        broca_core::latest_projection_boundary(&import_frames, 0),
        Some(1)
    );
    let WalRecord::RunStarted { config, .. } = &import_frames[0].record else {
        panic!("expected import start");
    };
    let next_config = (**config).clone();
    writer.record(WalRecord::RunStarted {
        session_class: None,
        retention: None,
        hooks: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        marker_order_version: None,
        manifest: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        ts_ms: None,
        run_id: RunId("run-next".into()),
        session: session.clone(),
        config: Box::new(next_config.clone()),
        input: vec![text(Role::User, "next")].into(),
        origin: RunOrigin::Fresh,
        submission_id: None,
    });
    writer.record(WalRecord::RunFinished {
        ts_ms: None,
        reason: RunFinishReason::Completed,
        usage: Usage::default(),
        error: None,
        retries_used: 0,
    });
    writer.sync().await.expect("sync following episode");

    let frames = store.read_records(&session).expect("read full lineage");
    let records: Vec<_> = frames.iter().map(|frame| frame.record.clone()).collect();
    let full = replay(&records).expect("full replay");
    let mut prefix_messages = imported;
    broca_protocol::content::stamp_ordinals(&mut prefix_messages);
    let prefix = StorePrefix {
        prefix: prefix_messages,
        completed_steps: 0,
        usage: Usage::default(),
        retries_used: 0,
        last_step_usage: None,
        last_step_finish_reason: None,
        watermark_seq: 1,
        context_epoch: 0,
    };
    let tail: Vec<_> = frames
        .iter()
        .filter(|frame| frame.seq > prefix.watermark_seq)
        .map(|frame| frame.record.clone())
        .collect();
    let merged = resume_state_from_store(
        &prefix,
        &tail,
        Some(next_config),
        Some(RunId("run-next".into())),
        None,
    )
    .expect("store merge");
    assert_eq!(merged.prompt, full.prompt);
    assert_eq!(merged.resolution, full.resolution);

    let _ = writer.shutdown().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn repair_retry_survives_a_crash_after_every_repair_record() {
    let source_root = temp_dir("repair-crash-source");
    let source_store = FileWalStore::new(source_root.join("wal"));
    let source_session = BindIdentity::new("/project", "runner", "repair-crash-source");
    let source_writer =
        WalHandle::spawn(source_store.open(&source_session).expect("open source"), 1)
            .expect("spawn writer");
    let imported = vec![text(Role::User, "replacement")];
    run_import(
        request(&source_session, "replacement-import", imported.clone()),
        &source_writer,
        &source_store,
    )
    .await
    .expect("create complete import records");
    let _ = source_writer.shutdown().await;
    let complete_records: Vec<_> = source_store
        .read_records(&source_session)
        .expect("read source")
        .into_iter()
        .map(|frame| frame.record)
        .collect();

    let torn_start = complete_records[0].clone();
    let repair_records = [
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Interrupted,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
        complete_records[0].clone(),
        complete_records[1].clone(),
    ];

    for cut in 0..=repair_records.len() {
        let root = temp_dir(&format!("repair-crash-cut-{cut}"));
        let store = FileWalStore::new(root.join("wal"));
        let session = BindIdentity::new("/project", "runner", format!("repair-crash-cut-{cut}"));
        let mut raw = store.open(&session).expect("open cut WAL");
        let mut cut_start = torn_start.clone();
        if let WalRecord::RunStarted {
            session: durable_session,
            ..
        } = &mut cut_start
        {
            durable_session.clone_from(&session);
        }
        raw.append(1, &cut_start, true).expect("append torn start");
        for (index, record) in repair_records[..cut].iter().enumerate() {
            let mut record = record.clone();
            if let WalRecord::RunStarted {
                session: durable_session,
                ..
            } = &mut record
            {
                durable_session.clone_from(&session);
            }
            raw.append(index as u64 + 2, &record, true)
                .expect("append repair prefix");
        }
        drop(raw);

        let writer = WalHandle::spawn(store.open(&session).expect("reopen cut WAL"), 1)
            .expect("spawn writer");
        let outcome = run_import(
            request(&session, "replacement-import", imported.clone()),
            &writer,
            &store,
        )
        .await
        .unwrap_or_else(|error| panic!("repair cut {cut} must be retryable: {error}"));
        if cut == repair_records.len() {
            assert!(outcome.already_imported);
        }
        let records: Vec<_> = store
            .read_records(&session)
            .expect("read repaired WAL")
            .into_iter()
            .map(|frame| frame.record)
            .collect();
        let state = replay(&records).expect("repaired lineage replays");
        let mut expected = imported.clone();
        broca_protocol::content::stamp_ordinals(&mut expected);
        assert_eq!(state.prompt, expected, "repair cut {cut}");
        let _ = writer.shutdown().await;
        let _ = std::fs::remove_dir_all(root);
    }
    let _ = std::fs::remove_dir_all(source_root);
}

#[test]
fn interrupted_import_merge_matches_full_replay_at_every_watermark() {
    let session = BindIdentity::new("/project", "runner", "import-merge-cuts");
    let config = match complete_import_records_for_test(&session, "old", "old prompt")[0].clone() {
        WalRecord::RunStarted { config, .. } => config,
        _ => unreachable!(),
    };
    let records = vec![
        WalRecord::RunStarted {
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("old".into()),
            session: session.clone(),
            ts_ms: None,
            config: config.clone(),
            input: vec![text(Role::User, "ghost prompt")].into(),
            origin: RunOrigin::SeededFrom {
                origin_kind: broca_wal::SeedOriginKind::Import,
                source_ref: "cc:old".into(),
                origin_device_pseudonym: None,
                base_seq: None,
            },
            submission_id: None,
        },
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Interrupted,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
        WalRecord::RunStarted {
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("new".into()),
            session,
            ts_ms: None,
            config: config.clone(),
            input: vec![text(Role::User, "replacement prompt")].into(),
            origin: RunOrigin::SeededFrom {
                origin_kind: broca_wal::SeedOriginKind::Import,
                source_ref: "cc:new".into(),
                origin_device_pseudonym: None,
                base_seq: None,
            },
            submission_id: None,
        },
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ];
    let full = replay(&records).expect("full replay");

    for watermark in 0..=records.len() {
        let projected = replay(&records[..watermark]).expect("prefix replay");
        let mut stored = projected.prompt;
        for message in &mut stored {
            message.ordinal = None;
        }
        let prefix = StorePrefix {
            prefix: stored,
            completed_steps: projected.completed_steps,
            usage: projected.usage,
            retries_used: projected.retries_used,
            last_step_usage: projected.last_step_usage,
            last_step_finish_reason: projected.last_step_finish_reason,
            watermark_seq: watermark as u64,
            context_epoch: 0,
        };
        let merged = resume_state_from_store(
            &prefix,
            &records[watermark..],
            Some((*config).clone()),
            Some(RunId("new".into())),
            None,
        )
        .unwrap_or_else(|error| panic!("watermark {watermark} merges: {error}"));
        assert_eq!(merged.prompt, full.prompt, "watermark {watermark}");
    }
}

fn complete_import_records_for_test(
    session: &BindIdentity,
    run_id: &str,
    prompt: &str,
) -> Vec<WalRecord> {
    let config = broca_wal::RunConfig {
        model: broca_protocol::request::ModelHandle::new("import", "import"),
        tools: Vec::new().into(),
        tool_choice: Default::default(),
        generation: Default::default(),
        response_format: Default::default(),
        provider_options: serde_json::json!({"import_request_digest": "test"}),
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: 1,
        frozen_render_config: serde_json::Value::Null,
        auth_selection: None,
        tools_were_omitted: false,
        budget: None,
        context_limit: None,
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: Vec::new(),
        context_management: None,
    };
    vec![
        WalRecord::RunStarted {
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId(run_id.into()),
            session: session.clone(),
            ts_ms: None,
            config: Box::new(config),
            input: vec![text(Role::User, prompt)].into(),
            origin: RunOrigin::SeededFrom {
                origin_kind: broca_wal::SeedOriginKind::Import,
                source_ref: format!("cc:{run_id}"),
                origin_device_pseudonym: None,
                base_seq: None,
            },
            submission_id: None,
        },
        WalRecord::RunFinished {
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ]
}

#[tokio::test]
async fn import_rejects_an_empty_canonical_message_with_its_ordinal() {
    let root = temp_dir("empty-message");
    let store = FileWalStore::new(root.join("wal"));
    let session = BindIdentity::new("/project", "runner", "empty-message");
    let writer =
        WalHandle::spawn(store.open(&session).expect("open WAL"), 1).expect("spawn writer");

    assert_eq!(
        run_import(
            request(
                &session,
                "empty-import",
                vec![
                    text(Role::User, "kept"),
                    Message::new(Role::Assistant, vec![])
                ],
            ),
            &writer,
            &store,
        )
        .await,
        Err(ImportError::EmptyMessage { ordinal: 1 })
    );
    assert!(store.read_records(&session).expect("read WAL").is_empty());

    let _ = writer.shutdown().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn duplicate_tool_call_ids_are_tracked_as_a_multiset() {
    let root = temp_dir("duplicate-tool-ids");
    let store = FileWalStore::new(root.join("wal"));
    let session = BindIdentity::new("/project", "runner", "import-duplicate-tool-ids");
    let writer =
        WalHandle::spawn(store.open(&session).expect("open WAL"), 1).expect("spawn writer");
    let duplicate_id = ToolCallId("duplicate".into());
    let calls = Message::new(
        Role::Assistant,
        ["first", "second"]
            .into_iter()
            .map(|name| {
                ContentBlock::ToolCall(ToolCall {
                    tool_call_id: duplicate_id.clone(),
                    tool_name: name.into(),
                    input: serde_json::Value::Null,
                    thought_signature: None,
                })
            })
            .collect(),
    );
    let one_result = Message::new(
        Role::Tool,
        vec![ContentBlock::ToolResult(broca_protocol::ToolResult {
            tool_call_id: duplicate_id.clone(),
            output: ToolOutput::Text {
                text: "done".into(),
            },
            is_error: false,
        })],
    );

    let outcome = run_import(
        request(&session, "run-duplicate-tool-ids", vec![calls, one_result]),
        &writer,
        &store,
    )
    .await
    .expect("import duplicate ids");
    assert_eq!(outcome.synthesized_tool_results, 1);
    assert_eq!(outcome.dropped_orphan_tool_results, 0);

    let records = store.read_records(&session).expect("read import");
    let WalRecord::RunStarted { input, .. } = &records[0].record else {
        panic!("expected import start");
    };
    let result_count = input
        .iter()
        .flat_map(|message| &message.content)
        .filter(|block| matches!(block, ContentBlock::ToolResult(result) if result.tool_call_id == duplicate_id))
        .count();
    assert_eq!(result_count, 2);

    let _ = writer.shutdown().await;
    let _ = std::fs::remove_dir_all(root);
}

/// An import with nothing to import must leave the lineage untouched and importable.
///
/// The failure this pins is not the refusal itself but what accepting one costs: an empty
/// episode is durable, so it consumes the lineage and every later attempt fails --
/// `LineageNotEmpty` for a new import id, `ImportIdReuse` for the same one carrying real
/// content. The session becomes permanently unfillable, and the request that did it returned
/// success. So both arms assert the refusal AND that a real import still lands afterwards;
/// checking only the error would pass even if the WAL had been written.
#[tokio::test]
async fn an_import_with_nothing_to_import_leaves_the_lineage_importable() {
    for (label, messages) in [
        ("empty", Vec::new()),
        // Non-empty on the wire, emptied by normalization: system messages belong to the
        // continuing session rather than the transcript, so a system-only export arrives with
        // content and has none left by the time the episode would be written.
        ("system-only", vec![text(Role::System, "you are a bot")]),
    ] {
        let root = temp_dir(&format!("no-content-{label}"));
        let store = FileWalStore::new(root.join("wal"));
        let session = BindIdentity::new("/project", "runner", "import-no-content");
        let writer =
            WalHandle::spawn(store.open(&session).expect("open WAL"), 1).expect("spawn writer");

        assert_eq!(
            run_import(
                request(&session, "run-import-empty", messages),
                &writer,
                &store
            )
            .await,
            Err(ImportError::NoImportableContent),
            "{label}: an import with no importable content must be refused"
        );
        assert!(
            store.read_records(&session).expect("read WAL").is_empty(),
            "{label}: the refusal must not write to the WAL"
        );

        let real = run_import(
            request(
                &session,
                "run-import-real",
                vec![text(Role::User, "hello"), text(Role::Assistant, "world")],
            ),
            &writer,
            &store,
        )
        .await
        .expect("a real import still lands after a refused one");
        assert!(!real.already_imported, "{label}: the lineage stayed empty");
    }
}
