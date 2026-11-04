use super::*;
use serde_json::{json, Value};

#[derive(Clone, Copy)]
enum Measurement {
    Legacy,
    Complete,
    MissingLast,
}

fn append_run(
    wal: &mut broca_wal::FileWal,
    session: &BindIdentity,
    id: &str,
    measurements: &[Measurement],
) {
    let start = serde_json::from_value(json!({
        "type":"run_started", "run_id":id, "session":session,
        "config":min_config(), "input":[{"role":"user","content":[{"type":"text","text":"go"}]}],
        "origin":{"kind":"fresh"}
    }))
    .unwrap();
    wal.append(1, &start, false).unwrap();
    for (index, measurement) in measurements.iter().enumerate() {
        let step_id = index as u64 + 1;
        wal.append(
            1,
            &WalRecord::StepStarted {
                step_id: broca_protocol::StepId(step_id),
            },
            false,
        )
        .unwrap();
        if !matches!(measurement, Measurement::Legacy) {
            let last = if matches!(measurement, Measurement::Complete) {
                Some(1100)
            } else {
                None
            };
            let attempt = serde_json::from_value(json!({
                "type":"model_attempt_finished", "step_id":step_id, "attempt":0,
                "request_body_len":0, "request_body_sha256":"00", "response_body_len":0,
                "response_body_sha256":"00", "outcome":"complete", "ts_ms":1200,
                "sent_at_ms":1000, "first_content_at_ms":1040, "last_content_at_ms":last
            }))
            .unwrap();
            wal.append(1, &attempt, false).unwrap();
        }
        let step = serde_json::from_value(json!({
        "type":"model_step_finished", "step_id":step_id,
        "assistant_message":{"message_id":format!("{id}-message-{step_id}"),"content":[{"type":"text","text":"answer"}]},
        "usage":{"output_tokens":30,"reasoning_tokens":10}, "retries_used":0,
        "finish_reason":"stop", "service_tier_used":"standard", "speed_used":"fast"
    })).unwrap();
        wal.append(1, &step, false).unwrap();
    }
    let calls = measurements.len() as u64;
    wal.append(1, &serde_json::from_value(json!({
        "type":"run_finished", "reason":"completed", "usage":{"output_tokens":30*calls,"reasoning_tokens":10*calls},"retries_used":0
    })).unwrap(), false).unwrap();
}

async fn prepared(messages: usize, measurements: &[Measurement]) -> PreparedWalRoute {
    PreparedWalRoute::start_prepared(
        "generation",
        |path, session| {
            let mut wal = broca_wal::FileWal::open(path).unwrap();
            for episode in 0..messages / 2 - 1 {
                append_run(
                    &mut wal,
                    session,
                    &format!("history-{episode}"),
                    &[Measurement::Legacy],
                );
            }
            append_run(&mut wal, session, "sample", measurements);
            wal.sync().unwrap();
            drop(wal);
            let dir = session.project_root.parent().unwrap();
            let records = FileWalStore::new(dir.join("wal"))
                .read_records(session)
                .unwrap();
            let store = broca_store::SqliteStore::open(dir.join("store.db")).unwrap();
            store.project(session, &records).unwrap();
        },
        None,
        None,
        None,
    )
    .await
}

async fn reply(route: &mut PreparedWalRoute, corr: u64, method: &str) -> Vec<u8> {
    write_req(
        &mut route.writer,
        7,
        corr,
        json!({"method":method,"params":{"run_id":"sample"}}),
    )
    .await;
    let frame = tokio::time::timeout(
        broca_test_support::EVENT_HANG_GUARD,
        read_one(&mut route.reader),
    )
    .await
    .expect("the run reply must arrive within the shared event hang guard");
    assert_eq!(
        frame.header.ty,
        FrameType::Response,
        "{}",
        String::from_utf8_lossy(&frame.body)
    );
    frame.body
}

#[tokio::test]
async fn unmeasured_replies_match_pre_generation_bytes() {
    let mut route = prepared(2, &[Measurement::Legacy]).await;
    for (corr, method) in [(2, "run.result"), (3, "run.status")] {
        let bytes = reply(&mut route, corr, method).await;
        let name = method.replace('.', "_");
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(format!("tests/golden/{name}.no_generation.json"));
        assert_eq!(bytes, std::fs::read(path).unwrap());
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(value["result"]["usage"].get("generation").is_none());
    }
    route.finish().await;
}

fn capture(name: &str, bytes: &[u8]) {
    if let Some(directory) = std::env::var_os("BROCA_GENERATION_SAMPLE_DIR") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(name), bytes).unwrap();
    }
}

#[tokio::test]
async fn generation_samples_from_real_serve_exclude_missing_last_content() {
    let mut route = prepared(2, &[Measurement::Complete]).await;
    for (corr, method, name) in [
        (2, "run.result", "run-result.json"),
        (3, "run.status", "run-status.json"),
    ] {
        let bytes = reply(&mut route, corr, method).await;
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            value["result"]["usage"]["generation"],
            json!([{
                "model":"fake", "service_tier":"standard", "speed":"fast", "model_calls":1,
                "output_tokens":30, "reasoning_tokens":10,
                "sent_to_last_content_ms":100, "first_to_last_content_ms":60
            }])
        );
        capture(name, &bytes);
    }
    route.finish().await;

    let mut route = prepared(2, &[Measurement::Complete, Measurement::MissingLast]).await;
    let bytes = reply(&mut route, 2, "run.result").await;
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(value["result"]["usage"]["output_tokens"], 60);
    assert_eq!(value["result"]["usage"]["generation"][0]["model_calls"], 1);
    assert_eq!(
        value["result"]["usage"]["generation"][0]["output_tokens"],
        30
    );
    assert_eq!(
        value["result"]["usage"]["generation"][0]["sent_to_last_content_ms"],
        100
    );
    capture("missing-last-content.json", &bytes);
    route.finish().await;

    let mut route = prepared(2, &[Measurement::MissingLast]).await;
    let bytes = reply(&mut route, 2, "run.result").await;
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(value["result"]["usage"].get("generation").is_none());
    capture("no-generation.json", &bytes);
    route.finish().await;
}

#[tokio::test]
async fn store_rebuild_reproduces_generation_for_both_run_replies() {
    let mut route = prepared(2, &[Measurement::Complete, Measurement::MissingLast]).await;
    let before: Value = serde_json::from_slice(&reply(&mut route, 2, "run.result").await).unwrap();
    let wal = Arc::new(FileWalStore::new(route.dir.join("wal")));
    let session = BindIdentity::new(route.dir.join("proj"), "runner", "s1");
    let records = wal.read_records(&session).unwrap();
    let rebuilt = Arc::new(broca_store::SqliteStore::open(route.dir.join("rebuilt.db")).unwrap());
    let seed = broca_core::Persister::projection_seed(rebuilt.as_ref(), &session)
        .unwrap()
        .unwrap();
    assert_eq!(seed.watermark_seq, 0);
    assert_eq!(seed.prefix_len, 0);
    rebuilt.project(&session, &records).unwrap();
    let opener = DefaultOpener::new(
        Arc::new(FileLeaseStore::new(route.dir.join("rebuilt-leases"))),
        wal.clone(),
        Arc::new(FakeExec {
            wal_store: wal.clone(),
            started: Arc::new(AtomicUsize::new(0)),
            hold: None,
        }),
    )
    .with_run_index(rebuilt.clone())
    .with_materialized_store(rebuilt.clone());
    broca_store::session_refs::migrate(
        &route.dir.join("rebuilt.db"),
        &broca_core::ReadOnlyWalReader::new(&route.dir),
    )
    .unwrap();
    let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal, Some(rebuilt.clone()));
    registry.set_session_address_inventory(rebuilt);
    let result = registry
        .run_result(&session, RunId("sample".into()))
        .await
        .unwrap();
    let status = registry
        .run_status(&session, RunId("sample".into()))
        .await
        .unwrap();
    assert_eq!(
        serde_json::to_value(result.usage).unwrap(),
        before["result"]["usage"]
    );
    assert_eq!(
        serde_json::to_value(status).unwrap()["usage"],
        before["result"]["usage"]
    );
    assert!(registry.stop_and_seal().await);
    route.finish().await;
}

#[tokio::test]
#[ignore = "explicit before/after 1k/150k run reply latency measurement"]
async fn measure_generation_reply_cost() {
    for messages in [1_000, 150_000] {
        let mut route = prepared(messages, &[Measurement::Complete]).await;
        for (index, method) in ["run.result", "run.status"].into_iter().enumerate() {
            for sample in 0..3 {
                let start = std::time::Instant::now();
                let bytes = reply(&mut route, (index * 3 + sample + 2) as u64, method).await;
                let elapsed_us = start.elapsed().as_micros();
                let value: Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(value["result"]["state"], "completed");
                println!("GENERATION_REPLY_COST messages={messages} method={method} sample={sample} elapsed_us={elapsed_us}");
            }
        }
        route.finish().await;
    }
}
