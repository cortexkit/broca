use super::*;

#[path = "turn_heavy/parity.rs"]
mod parity;
#[cfg(target_os = "linux")]
#[path = "turn_heavy/publisher.rs"]
mod publisher;

fn report(
    media: bool,
    steps: usize,
    class: &str,
    elapsed: std::time::Duration,
    operations: &[OperationAccess],
) {
    let reads: Vec<_> = operations
        .iter()
        .flat_map(|operation| &operation.reads)
        .filter(|read| read.class != ReadClass::WriterOpen)
        .collect();
    let origins = reads
        .iter()
        .filter(|read| read.class == ReadClass::Origin)
        .count();
    let positioned = reads.len() - origins;
    let bytes: u64 = reads.iter().map(|read| read.length).sum();
    let writer_decodes: u64 = operations
        .iter()
        .flat_map(|operation| &operation.writer_opens)
        .map(|open| open.frames_decoded)
        .sum();
    let decodes: u64 = operations
        .iter()
        .map(|operation| operation.frames_decoded)
        .sum::<u64>()
        .saturating_sub(writer_decodes);
    let mut reasons = BTreeMap::new();
    for reason in operations
        .iter()
        .flat_map(|operation| &operation.seed_fallbacks)
    {
        *reasons.entry(*reason).or_insert(0usize) += 1;
    }
    let writer_bytes: u64 = operations
        .iter()
        .flat_map(|operation| &operation.writer_opens)
        .map(|open| open.bytes)
        .sum();
    println!("TURN_HEAVY media={media} steps={steps} class={class} wall_ms={:.3} origin_reads={origins} positioned_reads={positioned} bytes_read={bytes} frames_decoded={decodes} writer_open_bytes={writer_bytes} fallbacks={reasons:?}", elapsed.as_secs_f64() * 1_000.0);
    if operations.len() > 1 {
        for operation in operations {
            let origins = operation
                .reads
                .iter()
                .filter(|read| read.class == ReadClass::Origin)
                .count();
            let positioned = operation
                .reads
                .iter()
                .filter(|read| !matches!(read.class, ReadClass::Origin | ReadClass::WriterOpen))
                .count();
            let bytes: u64 = operation
                .reads
                .iter()
                .filter(|read| read.class != ReadClass::WriterOpen)
                .map(|read| read.length)
                .sum();
            println!("TURN_HEAVY_COMPONENT media={media} steps={steps} class={class} name={:?} origin_reads={origins} positioned_reads={positioned} bytes_read={bytes}", operation.name);
        }
    }
}

fn setup(to: u64, media: bool, label: &str) -> WalRecord {
    let mut content = vec![ContentBlock::Text {
        text: "bounded summary".into(),
    }];
    if media {
        content.push(ContentBlock::Media(broca_protocol::MediaRef {
            kind: broca_protocol::MediaKind::Image,
            mime: "image/png".into(),
            byte_size: 69,
            sha256: "a".repeat(64),
            width: Some(1),
            height: Some(1),
        }));
    }
    let replacement = std::sync::Arc::new(vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        content,
    )]);
    WalRecord::CompactionMessage {
        provider: "compactor".into(),
        kind: broca_wal::CompactionMessageKind::Setup,
        compaction_id: label.into(),
        version: 1,
        range: broca_wal::CompactionRange { from: 1, to },
        sha256: broca_wal::compaction_replacement_sha256(&replacement).unwrap(),
        replacement,
        request_id: None,
        cursor: None,
        call_when: None,
        stability: None,
        ceiling: None,
        tag_checkpoint: None,
        ride_wait_ms: None,
        view_features: None,
        view_due: None,
    }
}

fn retarget(value: &mut serde_json::Value, step: usize, calls: &BTreeMap<String, String>) {
    match value {
        serde_json::Value::Object(object) => {
            for (key, value) in object {
                match key.as_str() {
                    "step_id" => *value = json!(step + 1),
                    "batch_id" => *value = json!(format!("batch-{step}")),
                    "message_id" => *value = json!(format!("message-{step}")),
                    _ => retarget(value, step, calls),
                }
            }
        }
        serde_json::Value::Array(array) => {
            for value in array {
                retarget(value, step, calls);
            }
        }
        serde_json::Value::String(value) => {
            if let Some(new) = calls.get(value) {
                *value = new.clone();
            }
        }
        _ => {}
    }
}

fn records(template: &[WalRecord], steps: usize, media: bool) -> Vec<WalRecord> {
    let mut start = template[0].clone();
    if let WalRecord::RunStarted {
        input,
        manifest,
        config,
        ..
    } = &mut start
    {
        *input = vec![
            broca_protocol::Message::new(
                broca_protocol::Role::User,
                vec![ContentBlock::Text {
                    text: "fixed input".into()
                }]
            );
            8
        ]
        .into();
        *manifest = Some(compaction_plan());
        config.stop_when = vec![
            broca_protocol::request::StopCondition::StepCount {
                n: steps as u64 + 2,
            },
            broca_protocol::request::StopCondition::LoopFinished,
        ];
    }
    let first = template
        .iter()
        .position(|record| matches!(record, WalRecord::StepStarted { .. }))
        .unwrap();
    let turn = &template[first..];
    let calls: Vec<String> = turn
        .iter()
        .flat_map(|record| match record {
            WalRecord::ModelStepFinished {
                assistant_message, ..
            } => assistant_message
                .tool_calls()
                .map(|call| call.tool_call_id.0.clone())
                .collect(),
            _ => Vec::new(),
        })
        .collect();
    let turn_len = 1 + calls.len();
    let mut result = vec![start, setup(7, false, "initial")];
    for n in 0..steps {
        if n + 1 == steps {
            // Keep the last complete tool turn and the next turn outside the summary.
            result.push(setup((8 + turn_len * (n - 1)) as u64, media, "latest"));
        }
        let replacements: BTreeMap<_, _> = calls
            .iter()
            .map(|id| (id.clone(), format!("{id}-{n}")))
            .collect();
        let time = 10_000 + n as u64;
        for record in turn {
            if matches!(record, WalRecord::ModelStepFinished { .. }) {
                result.push(
                    serde_json::from_value(json!({
                        "type":"model_attempt_finished", "step_id":n + 1, "attempt":0,
                        "request_body_len":128, "request_body_sha256":"a".repeat(64),
                        "response_body_len":64, "response_body_sha256":"b".repeat(64),
                        "outcome":"success", "ts_ms":time
                    }))
                    .unwrap(),
                );
            }
            let mut value = serde_json::to_value(record).unwrap();
            retarget(&mut value, n, &replacements);
            value.as_object_mut().unwrap().remove("tags");
            if matches!(record, WalRecord::ModelStepFinished { .. }) {
                value["written_at_ms"] = json!(time);
            }
            result.push(serde_json::from_value(value).unwrap());
        }
    }
    assert_eq!(
        result
            .iter()
            .filter(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
            .count(),
        steps
    );
    result
}

fn publish_fixture(
    daemon: &OverhaulDaemon,
    bind: &BindIdentity,
    mut records: Vec<WalRecord>,
    media: bool,
    bulk: bool,
) -> broca_wal::file_wal::DurablePublication {
    use broca_wal::Wal;
    let path = daemon.wal.wal_path(bind);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let root = path.parent().unwrap().parent().unwrap();
    let mut wal = broca_wal::FileWal::open_with_recovery(
        &path,
        broca_wal::WalConfig::OVERHAUL,
        root.join("wal-feature-floor"),
    )
    .unwrap()
    .with_feature_floor(root.join("wal-feature-floor"));
    if let WalRecord::RunStarted {
        session,
        run_id,
        submission_id,
        input,
        ..
    } = &mut records[0]
    {
        *session = bind.clone();
        *run_id = RunId("turn-run".into());
        *submission_id = Some(broca_protocol::SubmissionId("sub-turn-run".into()));
        if media {
            std::sync::Arc::make_mut(input)
                .last_mut()
                .unwrap()
                .content
                .push(ContentBlock::Media(broca_protocol::MediaRef {
                    kind: broca_protocol::MediaKind::Image,
                    mime: "image/png".into(),
                    byte_size: 69,
                    sha256: "a".repeat(64),
                    width: Some(1),
                    height: Some(1),
                }));
        }
    }
    for record in records {
        wal.append(1, &record, false).unwrap();
    }
    wal.sync().unwrap();
    let publication = wal.durable_publication().unwrap();
    let db = &daemon.stores.overhaul.as_ref().unwrap().db;
    if bulk {
        db.publish_read_fixture(
            bind,
            &broca_core::agent_session_ref(bind),
            broca_store::db::LogLocation::Live,
            &publication,
        )
        .unwrap();
    } else {
        db.publish(
            bind,
            &broca_core::agent_session_ref(bind),
            broca_store::db::LogLocation::Live,
            &publication,
        )
        .unwrap();
    }
    let observer = daemon
        .stores
        .overhaul
        .as_ref()
        .unwrap()
        .publications
        .observer();
    observer(bind, &path, publication.lineage())(publication.borrowed()).unwrap();
    publication
}

async fn measure<T>(
    access: &WalAccessCounter,
    media: bool,
    steps: usize,
    class: &str,
    future: impl std::future::Future<Output = T>,
) -> T {
    let before = access.snapshot();
    let started = std::time::Instant::now();
    let answer = access
        .operation(format!("turn-heavy {class}"))
        .run_future(future)
        .await;
    let elapsed = started.elapsed();
    let mut delta = Vec::new();
    for (index, mut operation) in access.snapshot().into_iter().enumerate() {
        if let Some(prior) = before.get(index) {
            assert_eq!(operation.name, prior.name, "operation ledger order changed");
            operation.reads = operation.reads.split_off(prior.reads.len());
            operation.seed_fallbacks = operation
                .seed_fallbacks
                .split_off(prior.seed_fallbacks.len());
            operation.writer_opens = operation.writer_opens.split_off(prior.writer_opens.len());
            operation.frames_decoded -= prior.frames_decoded;
        }
        if !operation.reads.is_empty()
            || !operation.seed_fallbacks.is_empty()
            || !operation.writer_opens.is_empty()
            || operation.frames_decoded != 0
        {
            delta.push(operation);
        }
    }
    report(media, steps, class, elapsed, &delta);
    answer
}

fn source_lists(media: bool, steps: usize, seed: &str, path: &str, value: &serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            for (key, value) in object {
                let path = format!("{path}/{key}");
                if matches!(key.as_str(), "sources" | "render_sources" | "episodes") {
                    if let Some(array) = value.as_array() {
                        println!("TURN_HEAVY_SOURCE_LIST media={media} steps={steps} seed={seed} path={path} entries={}", array.len());
                    }
                }
                source_lists(media, steps, seed, &path, value);
            }
        }
        serde_json::Value::Array(array) => {
            for (index, value) in array.iter().enumerate() {
                source_lists(media, steps, seed, &format!("{path}/{index}"), value);
            }
        }
        _ => {}
    }
}

async fn run_case(steps: usize, media: bool) {
    let template = partial_turn().await;
    let root = root(&format!("turn-heavy-{media}-{steps}"));
    let bind = BindIdentity::new(
        "/installed",
        "runner",
        format!("turn-heavy-{media}-{steps}"),
    );
    let access = WalAccessCounter::default();
    let daemon = OverhaulDaemon::boot(&root, access.clone()).await;
    let records = records(&template, steps, media);
    let publication = access
        .operation("turn-heavy fixture publication")
        .run(|| publish_fixture(&daemon, &bind, records, media, true));
    let seq = publication.sequence();
    daemon.stores.overhaul.as_ref().unwrap().db.read_snapshot(|tx| {
                let (locator, admission, tail): (String, String, String) = tx.query_row(
                    "SELECT locator_seed,admission_seed,tail_seed FROM session", [],
                    |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
                )?;
                let auth: u64 = tx.query_row("SELECT count(*) FROM source_auth", [], |r| r.get(0))?;
                broca_core::admission::AdmissionContinuation::decode_for_execution(&admission)
                    .expect("the turn-heavy fixture must publish a usable admission continuation");
                broca_core::projection_plan::ProjectionContinuation::decode_for_execution(&tail)
                    .expect("the turn-heavy fixture must publish a usable projection continuation");
                println!("TURN_HEAVY_SEED media={media} steps={steps} locator_bytes={} admission_bytes={} tail_bytes={} source_auth={auth}", locator.len(), admission.len(), tail.len());
                for (name, encoded) in [("admission", admission), ("tail", tail)] {
                    source_lists(media, steps, name, "", &serde_json::from_str(&encoded).unwrap());
                }
                Ok(())
            }).unwrap();
    let mut raw = SessionReadRequest::new(bind.clone(), None, Some(2));
    raw.media = cortexkit_role_llm_runner::read::MediaMode::Descriptors;
    let page = measure(&access, media, steps, "raw_page", daemon.registry.read(raw))
        .await
        .unwrap();
    assert_eq!(page.messages.len(), 2);
    measure(
        &access,
        media,
        steps,
        "cold_head",
        daemon.registry.head(&bind),
    )
    .await
    .unwrap();
    measure(
        &access,
        media,
        steps,
        "model_page",
        daemon
            .registry
            .read_model(SessionReadRequest::new(bind.clone(), None, Some(2))),
    )
    .await
    .unwrap();
    let admission = daemon.stores.overhaul.as_ref().unwrap().admission_reader();
    measure(&access, media, steps, "admission", async {
        admission.prior_snapshot(&bind, seq)
    })
    .await
    .unwrap();
    measure(
        &access,
        media,
        steps,
        "resume_end_to_end",
        resume(&daemon, &bind, "turn-run"),
    )
    .await;
    {
        let prompts = daemon.resumed_prompts.lock().unwrap();
        assert_eq!(prompts.len(), 1);
        assert!(
            prompts[0].len() < 12,
            "the compacted model window grew to {} messages",
            prompts[0].len()
        );
    }
    measure(
        &access,
        media,
        steps,
        "warm_head",
        daemon.registry.head(&bind),
    )
    .await
    .unwrap();
    measure(&access, media, steps, "status", async {
        daemon
            .registry
            .run_status(&bind, RunId("turn-run".into()))
            .await
            .unwrap();
        daemon.registry.status(&bind).await.unwrap();
    })
    .await;
    measure(
        &access,
        media,
        steps,
        "result",
        daemon.registry.run_result(&bind, RunId("turn-run".into())),
    )
    .await
    .unwrap();
    measure(&access, media, steps, "live_attach", async {
        let (tx, mut rx) = tokio::sync::mpsc::channel(256);
        let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
        let result = daemon
            .registry
            .subscribe(&bind, SubscribeFrom::Live, tx)
            .await;
        drain.abort();
        let _ = drain.await;
        result.unwrap();
    })
    .await;
    no_legacy_index(&root);
    daemon.shutdown().await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_turn_heavy_compaction_read_measurements_media_free_200() {
    run_case(200, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_turn_heavy_compaction_read_measurements_media_free_5000() {
    run_case(5_000, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_turn_heavy_compaction_read_measurements_media_200() {
    run_case(200, true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn installed_turn_heavy_compaction_read_measurements_media_5000() {
    run_case(5_000, true).await;
}
