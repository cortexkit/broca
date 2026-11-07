use super::*;

#[tokio::test]
async fn budget_finalize_answer_has_its_model_step_completion_time() {
    let rig = Rig::new("budget-finalize-completion", Arc::new(NoopExec));
    let key = rig.key("finalize");
    let mut records = dispatch_module_episode(&key, "budget-run", "call-1");
    // Keep the completed tool turn, replacing the ordinary final answer with a budget finalize.
    records.truncate(records.len() - 2);
    let WalRecord::RunStarted { config, .. } = &mut records[0] else {
        unreachable!()
    };
    config.budget = Some(BudgetConfig {
        max_input_tokens: None,
        nudges: vec![],
        finalize_at_tool_calls: 1,
        finalize_text: "Finish now.".into(),
    });
    let WalRecord::ModelStepFinished { written_at_ms, .. } = &mut records[1] else {
        unreachable!()
    };
    *written_at_ms = Some(110);
    records.extend([
        WalRecord::BudgetFired {
            tags: None,
            run_id: RunId("budget-run".into()),
            kind: BudgetKind::Finalize,
            threshold: 1,
            ordinal: 3,
            caller_text: Some("Finish now.".into()),
        },
        WalRecord::StepStarted { step_id: StepId(1) },
        serde_json::from_value(serde_json::json!({
            "type": "model_attempt_finished", "step_id": 1, "attempt": 0,
            "outcome": "complete", "ts_ms": 210,
            "request_body_len": 0, "request_body_sha256": "a",
            "response_body_len": 0, "response_body_sha256": "b"
        }))
        .unwrap(),
        serde_json::from_value(serde_json::json!({
            "type": "model_step_finished", "step_id": 1, "written_at_ms": 220,
            "assistant_message": {
                "message_id": "finalize-answer",
                "content": [
                    {"type": "reasoning", "text": "The budget requires a final answer."},
                    {"type": "text", "text": "Final answer."}
                ]
            },
            "usage": {}, "retries_used": 0, "finish_reason": "stop"
        }))
        .unwrap(),
        WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: Some(230),
            reason: RunFinishReason::Completed,
            usage: usage(),
            error: None,
            retries_used: 0,
        },
    ]);
    append_seed(&rig.wal_store, &key, &records);

    let mut request = read_request(&key, Some(0), Some(32));
    request.include_display_facts = true;
    let assert_completions = |page: &broca_module::serve::SessionReadResponse| {
        let ordinary = &page.messages[1];
        assert_eq!(ordinary.message.role, Role::Assistant);
        assert_eq!(ordinary.completed_at, Some(110));
        let finalize = page.messages.last().unwrap();
        assert_eq!(finalize.message.role, Role::Assistant);
        assert!(matches!(
            finalize.message.content.last(),
            Some(ContentBlock::Text { text }) if text == "Final answer."
        ));
        assert_eq!(finalize.written_at, Some(220));
        assert_eq!(finalize.completed_at, Some(220));
    };
    let tail = rig.registry.read(request.clone()).await.unwrap();
    assert_completions(&tail);
    materialize_all(&rig, &key);
    let cached = rig.registry.read(request).await.unwrap();
    assert_completions(&cached);
    assert_eq!(tail, cached);
}

#[tokio::test]
async fn old_display_seed_rebuilds_only_on_its_sessions_opt_in_read() {
    use broca_core::display_facts::{
        DisplayFactVersion, DisplayFacts, DisplayFactsSeed, DISPLAY_FACTS_VERSION,
    };
    let (rig, zero_ranges) = Rig::with_counting_zero_ranges("old-display-fact-seed");
    let key = rig.key("old-seed");
    let other = rig.key("untouched");
    append_seed(
        &rig.wal_store,
        &key,
        &[
            started(&key, "run", "question"),
            assistant_text(0, "answer"),
            finished_ok(),
        ],
    );
    materialize_all(&rig, &key);
    let plain = rig
        .registry
        .read(read_request(&key, None, Some(32)))
        .await
        .unwrap();
    let lineage = plain.lineage_id.as_ref().unwrap();
    let mut old = DisplayFactsSeed::default();
    old.version -= 1;
    old.through_seq = u64::MAX;
    rig.store
        .publish_display_facts(
            &key,
            lineage,
            None,
            None,
            &old,
            &[DisplayFactVersion {
                ordinal: 0,
                seq: 1,
                facts: DisplayFacts {
                    client_message_id: Some("stale".into()),
                    ..Default::default()
                },
            }],
        )
        .unwrap();
    zero_ranges.store(0, Ordering::SeqCst);
    let default = rig
        .registry
        .read(read_request(&key, None, Some(32)))
        .await
        .unwrap();
    assert_eq!(default, plain);
    assert_eq!(
        zero_ranges.load(Ordering::SeqCst),
        0,
        "default reads do not rebuild display facts"
    );
    let mut request = read_request(&key, None, Some(32));
    request.include_display_facts = true;
    let rebuilt = rig.registry.read(request.clone()).await.unwrap();
    assert_eq!(
        zero_ranges.swap(0, Ordering::SeqCst),
        1,
        "the old seed requires one cold rebuild"
    );
    assert!(rebuilt
        .messages
        .iter()
        .all(|message| message.client_message_id.as_deref() != Some("stale")));
    let cache = rig
        .store
        .read_display_facts_cache(&key, lineage, u64::MAX, &[])
        .unwrap()
        .unwrap();
    assert_eq!(cache.seed.unwrap().version, DISPLAY_FACTS_VERSION);
    assert_eq!(rig.registry.read(request).await.unwrap(), rebuilt);
    assert_eq!(
        zero_ranges.load(Ordering::SeqCst),
        0,
        "the rebuilt seed is reused"
    );
    assert!(rig
        .store
        .read_display_facts_cache(&other, lineage, u64::MAX, &[])
        .unwrap()
        .unwrap()
        .seed
        .is_none());
}

#[tokio::test]
async fn display_fact_backfill_is_lazy_persistent_and_reads_are_bounded_at_1k_and_150k() {
    let mut warm_io = None;
    let mut continuation_io = None;
    let signature = |operation: &broca_wal::access_counter::OperationAccess| {
        (
            operation
                .reads
                .iter()
                .map(|read| (read.length, read.class))
                .collect::<Vec<_>>(),
            operation.frames_decoded,
        )
    };
    for size in [1_000, 10_000, 150_000] {
        let counter = broca_wal::access_counter::WalAccessCounter::default();
        let fixture_started = std::time::Instant::now();
        let (rig, zero_ranges) = Rig::with_counting_zero_ranges("display-fact-cost");
        let key = rig.key("facts");
        let mut records = vec![started(&key, "prefix", "start")];
        records.extend((0..size - 5).map(|step| assistant_text(step, "history")));
        records.push(finished_ok());
        let mut tool = dispatch_module_episode(&key, "tools", "fact-call");
        for record in &mut tool {
            match record {
                WalRecord::ToolDispatchIntent {
                    dispatched_at_ms, ..
                } => *dispatched_at_ms = Some(100),
                WalRecord::ToolResult { finished_at_ms, .. } => *finished_at_ms = Some(150),
                _ => {}
            }
        }
        records.extend(tool);
        let records_ms = fixture_started.elapsed().as_secs_f64() * 1000.0;
        let append_started = std::time::Instant::now();
        // This setup does not test sync behavior. Batch syncs so fixture writes do
        // not dominate the page-read measurements on Windows filesystems.
        {
            let mut wal = FileWal::open(rig.wal_store.wal_path(&key))
                .expect("open display-facts fixture WAL")
                .with_feature_floor(rig.wal_store.feature_floor_path());
            for batch in records.chunks(1_000) {
                for (index, record) in batch.iter().enumerate() {
                    wal.append(1, record, index + 1 == batch.len())
                        .expect("append fixture record");
                }
            }
        }
        let append_ms = append_started.elapsed().as_secs_f64() * 1000.0;
        eprintln!("display-facts fixture messages={size} records_ms={records_ms:.3} append_ms={append_ms:.3}");
        let materialize_started = std::time::Instant::now();
        materialize_all(&rig, &key);
        let materialize_ms = materialize_started.elapsed().as_secs_f64() * 1000.0;
        eprintln!("display-facts fixture messages={size} materialize_ms={materialize_ms:.3}");
        zero_ranges.store(0, Ordering::SeqCst);
        let without_tools = rig
            .registry
            .read(read_request(&key, None, Some(1)))
            .await
            .unwrap();
        assert!(without_tools
            .messages
            .iter()
            .all(|entry| entry.message.role != broca_protocol::Role::Tool));
        assert!(
            rig.store
                .read_display_facts_cache(
                    &key,
                    without_tools.lineage_id.as_ref().unwrap(),
                    u64::MAX,
                    &[]
                )
                .unwrap()
                .unwrap()
                .seed
                .is_none(),
            "a default page without tools still avoids backfill"
        );
        zero_ranges.store(0, Ordering::SeqCst);
        let now = std::time::Instant::now();
        let plain = counter
            .operation("cold native-output sidecar")
            .run_future(rig.registry.read(read_request(&key, None, Some(32))))
            .await
            .unwrap();
        let default_ms = now.elapsed().as_secs_f64() * 1000.0;
        let default_full = zero_ranges.swap(0, Ordering::SeqCst);
        assert_eq!(plain.messages.last().unwrap().ordinal, size);
        let lineage = plain.lineage_id.as_ref().unwrap();
        // The first default read builds the display-facts cache: each tool call's native
        // output, kept beside the transcript so later reads skip the session log. That one full
        // decode of the log also records which run dispatched each call, so no second pass over
        // the history is needed. Later (warm) reads decode neither.
        assert_eq!(default_full, 1, "one shared output and dispatch backfill");
        let cold = &counter.snapshot()[0];
        println!(
            "native-output cold messages={size} bytes={} decoded={} elapsed_ms={default_ms:.3}",
            cold.reads.iter().map(|read| read.length).sum::<u64>(),
            cold.frames_decoded
        );
        assert!(
            rig.store
                .read_display_facts_cache(&key, lineage, u64::MAX, &[])
                .unwrap()
                .unwrap()
                .seed
                .is_some(),
            "tool output separation needs the sidecar even without opt-in display facts"
        );
        let mut request = read_request(&key, None, Some(32));
        request.include_display_facts = true;
        let now = std::time::Instant::now();
        let first = rig.registry.read(request.clone()).await.unwrap();
        let backfill_ms = now.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(
            zero_ranges.swap(0, Ordering::SeqCst),
            0,
            "the first opt-in read reuses the tool read's completed backfill"
        );
        let now = std::time::Instant::now();
        let warm = counter
            .operation("warm opted-in page")
            .run_future(rig.registry.read(request))
            .await
            .unwrap();
        let warm_ms = now.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(
            zero_ranges.swap(0, Ordering::SeqCst),
            0,
            "warm opt-in reads no whole WAL"
        );
        assert_eq!(first, warm);
        let times = warm
            .messages
            .iter()
            .find(|entry| !entry.tool_times.is_empty())
            .unwrap();
        assert_eq!(
            serde_json::to_value(&times.tool_times).unwrap(),
            serde_json::json!([{"tool_call_id":"fact-call","started_at":100,"finished_at":150,"call_key":times.tool_calls[0].call_key}])
        );
        let reopened = broca_store::SqliteStore::open(rig.dir.join("store.db")).unwrap();
        assert!(
            reopened
                .read_display_facts_cache(&key, lineage, u64::MAX, &[])
                .unwrap()
                .unwrap()
                .seed
                .is_some(),
            "completion survives another store connection"
        );
        let now = std::time::Instant::now();
        let after = counter
            .operation("warm default page")
            .run_future(rig.registry.read(read_request(&key, None, Some(32))))
            .await
            .unwrap();
        let default_warm_ms = now.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(
            zero_ranges.swap(0, Ordering::SeqCst),
            0,
            "warm default tool reads no whole WAL"
        );
        assert_eq!(plain, after);
        let operations = counter.snapshot();
        let observed = (signature(&operations[1]), signature(&operations[2]));
        assert_eq!(
            &observed,
            warm_io.get_or_insert_with(|| observed.clone()),
            "warm WAL bytes and decoded frames cannot grow with history"
        );
        println!("display-facts messages={size} default_cold_ms={default_ms:.3} default_cold_full_reads={default_full} opt_in_backfill_ms={backfill_ms:.3} opt_in_warm_ms={warm_ms:.3} default_warm_ms={default_warm_ms:.3}");
        let mut next = started(&key, "next", "next prompt");
        if let WalRecord::RunStarted {
            request_identity_digest,
            ..
        } = &mut next
        {
            *request_identity_digest = Some(serde_json::json!({"version":1,"model":"","input":"","tools":"","policy_allowed":"","tool_choice":"","generation":"","cache":"","budget":"","stop_when":"","client_message_id":"phone_next"}).to_string());
        }
        append_seed(
            &rig.wal_store,
            &key,
            &[next, assistant_text(0, "next answer"), finished_ok()],
        );
        zero_ranges.store(0, Ordering::SeqCst);
        let mut request = read_request(&key, None, Some(2));
        request.include_display_facts = true;
        let extended = counter
            .operation("continued sidecar fold")
            .run_future(rig.registry.read(request))
            .await
            .unwrap();
        assert_eq!(
            extended.messages[0].client_message_id.as_deref(),
            Some("phone_next")
        );
        assert_eq!(
            zero_ranges.load(Ordering::SeqCst),
            0,
            "a completed backfill continues from its byte anchor"
        );
        let operations = counter.snapshot();
        let observed = signature(&operations[3]);
        assert_eq!(
            &observed,
            continuation_io.get_or_insert_with(|| observed.clone()),
            "continuation WAL bytes and decoded frames cannot grow with the old prefix"
        );
        println!(
            "native-output bounded messages={size} warm={:?} continuation={observed:?}",
            warm_io.as_ref().unwrap()
        );
    }
}
