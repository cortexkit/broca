use super::*;
use serde_json::{json, Value};

const MESSAGE: &str = "messages.89.content.10: `thinking` or `redacted_thinking` blocks in the latest assistant message cannot be modified. These blocks must remain as they were in the original response.";

fn refusal() -> broca_protocol::ProviderError {
    let body = json!({"type":"error", "error":{"type":"invalid_request_error", "message":MESSAGE}});
    broca_provider::transport::classify_http_error(400, &body.to_string(), None, None)
}

fn capture(name: &str, bytes: &[u8]) {
    if let Some(directory) = std::env::var_os("BROCA_SIGNED_REASONING_SAMPLE_DIR") {
        let directory = std::path::PathBuf::from(directory);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(name), bytes).unwrap();
    }
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
    .unwrap();
    assert_eq!(frame.header.ty, FrameType::Response);
    frame.body
}

#[tokio::test]
async fn signed_reasoning_cause_survives_wal_projection_and_real_run_surfaces() {
    for (version, cause) in [
        ("before", None),
        ("after", Some("signed_reasoning_rejected")),
    ] {
        let mut error = refusal();
        assert_eq!(error.cause.as_deref(), Some("signed_reasoning_rejected"));
        error.cause = cause.map(str::to_owned);
        let expected = serde_json::to_value(&error).unwrap();
        let terminal = WalRecord::RunFinished {
            interrupt_reason: None,
            reason: broca_protocol::caller::RunFinishReason::Error,
            ts_ms: Some(1_790_000_000_000),
            usage: Default::default(),
            error: Some(error),
            retries_used: 0,
        };
        capture(
            &format!("{version}.wal-run-finished.json"),
            &serde_json::to_vec(&terminal).unwrap(),
        );
        let mut route = PreparedWalRoute::start_prepared(
            "signed-reasoning",
            |path, session| {
                let mut wal = broca_wal::FileWal::open(path).unwrap();
                let start = serde_json::from_value(json!({
                    "type":"run_started", "run_id":"sample", "session":session,
                    "config":min_config(), "input":[{"role":"user","content":[{"type":"text","text":"synthetic signed reasoning refusal"}]}],
                    "origin":{"kind":"fresh"}
                })).unwrap();
                wal.append(1, &start, false).unwrap();
                wal.append(1, &terminal, false).unwrap();
                wal.sync().unwrap();
                drop(wal);
                let dir = session.project_root.parent().unwrap();
                let records = FileWalStore::new(dir.join("wal")).read_records(session).unwrap();
                assert_eq!(records.last().unwrap().record, terminal);
                let store = broca_store::SqliteStore::open(dir.join("store.db")).unwrap();
                store.project(session, &records).unwrap();
            },
            None, None, None,
        ).await;
        for (corr, method) in [(2, "run.status"), (3, "run.result")] {
            let bytes = reply(&mut route, corr, method).await;
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(value["result"]["error"], expected);
            capture(&format!("{version}.{method}.json"), &bytes);
        }
        write_req(
            &mut route.writer,
            7,
            4,
            json!({"method":"session.subscribe","params":{"from":"start"}}),
        )
        .await;
        let bytes = tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
            loop {
                let frame = read_one(&mut route.reader).await;
                assert_ne!(
                    frame.header.ty,
                    FrameType::Error,
                    "{}",
                    String::from_utf8_lossy(&frame.body)
                );
                let value: Value = serde_json::from_slice(&frame.body).unwrap();
                if value["unit"]["type"] == "error" {
                    assert_eq!(value["unit"]["error"], expected);
                    break frame.body;
                }
            }
        })
        .await
        .expect("the replay must include the terminal error");
        capture(&format!("{version}.subscribe-error.json"), &bytes);
        route.finish().await;
    }
}

#[test]
fn signed_reasoning_cause_survives_new_storage_and_legacy_rebuild() {
    use broca_session_store::record::{Record, RunEnd};

    let error = refusal();
    let expected = serde_json::to_value(&error).unwrap();
    let record = Record::RunEnd(RunEnd {
        run_id: "sample".into(),
        reason: broca_protocol::caller::RunFinishReason::Error,
        usage: Some(Default::default()),
        ts_ms: Some(1_790_000_000_000),
        retries_used: 0,
        interrupt_reason: None,
        error: Some(expected.clone()),
    });
    let bytes = record.encode_body();
    capture("after.store-run-end.bin", &bytes);
    let decoded = Record::decode(record.kind(), &bytes).unwrap();
    assert_eq!(decoded, record);
    let Record::RunEnd(end) = decoded else {
        unreachable!()
    };
    let typed: broca_protocol::ProviderError =
        serde_json::from_value(end.error.clone().unwrap()).unwrap();
    assert_eq!(typed, error);
    let rebuilt: WalRecord =
        serde_json::from_value(broca_session_loop::today::run_finished(&end)).unwrap();
    let WalRecord::RunFinished {
        error: Some(rebuilt),
        ..
    } = rebuilt
    else {
        unreachable!()
    };
    assert_eq!(rebuilt, error);
    assert_eq!(
        serde_json::to_value(broca_protocol::caller::SharedProviderError::from(rebuilt)).unwrap(),
        expected
    );
}
