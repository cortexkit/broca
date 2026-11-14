use super::*;

struct Served {
    store: bool,
    status: Value,
    head: Value,
    result: Value,
    frames: Vec<(&'static str, Value)>,
    stored_error: Value,
    ended_at_ms: Option<u64>,
}

fn sample(name: &str, bytes: &[u8]) {
    if let Some(directory) = std::env::var_os("BROCA_PROVIDER_CODE_SAMPLE_DIR") {
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(std::path::PathBuf::from(directory).join(name), bytes).unwrap();
    }
}

async fn stored_error(side: &Side) -> (Value, Option<u64>) {
    if side.store {
        let view = side
            .registry
            .store_runtime()
            .unwrap()
            .current_view(&side.key)
            .await
            .unwrap()
            .unwrap();
        view.records_from(1)
            .await
            .unwrap()
            .into_iter()
            .find_map(|(_, record)| match record {
                broca_session_store::record::Record::RunEnd(end) => {
                    end.error.map(|error| (error.parse(), end.ts_ms))
                }
                broca_session_store::record::Record::Pause(pause) => {
                    pause.error.map(|error| (error.parse(), None))
                }
                _ => None,
            })
            .expect("the store recorded the provider error")
    } else {
        side.executor
            .wal
            .read_records(&side.key)
            .unwrap()
            .into_iter()
            .find_map(|frame| match frame.record {
                broca_wal::WalRecord::RunFinished {
                    error: Some(error), ..
                }
                | broca_wal::WalRecord::RunPaused {
                    error: Some(error), ..
                } => Some((serde_json::to_value(error).unwrap(), None)),
                _ => None,
            })
            .expect("the WAL recorded the provider error")
    }
}

async fn capture(side: &Side, name: &str) -> Served {
    let storage = if side.store { "store" } else { "legacy" };
    let phase =
        std::env::var("BROCA_PROVIDER_CODE_SAMPLE_PHASE").unwrap_or_else(|_| "after".into());
    let prefix = format!("{name}.{storage}.{phase}");
    let status = side
        .registry
        .run_status(&side.key, RunId("r1".into()))
        .await
        .unwrap();
    let result = side
        .registry
        .run_result(&side.key, RunId("r1".into()))
        .await
        .unwrap();
    let head = side
        .registry
        .head_for(&side.key, None, &Default::default())
        .await
        .unwrap();
    sample(
        &format!("{prefix}.run.status.json"),
        &serde_json::to_vec(&status).unwrap(),
    );
    sample(
        &format!("{prefix}.run.result.json"),
        &serde_json::to_vec(&result).unwrap(),
    );
    sample(
        &format!("{prefix}.session.head.json"),
        &serde_json::to_vec(&head).unwrap(),
    );
    let status = serde_json::to_value(status).unwrap();
    let result = serde_json::to_value(result).unwrap();
    let head = serde_json::to_value(head).unwrap();
    let (sink, mut events) = mpsc::channel(4096);
    crate::serve::ServeHandler::subscribe(
        &side.registry,
        &side.key,
        crate::serve::SubscribeFrom::Start,
        sink,
    )
    .await
    .unwrap();
    let frames = broca_test_support::wait_for(
        "the provider error's subscribe frames",
        async {
            let mut frames = Vec::new();
            let mut seq = 0;
            while let Some(delivery) = events.recv().await {
                if let crate::fanout::Delivery::Control(event) = delivery {
                    seq += 1;
                    let kind = match &event.1 {
                        broca_protocol::ControlUnit::Error { .. } => "error",
                        broca_protocol::ControlUnit::RunFinished { .. } => "run_finished",
                        broca_protocol::ControlUnit::Paused { .. } => "paused",
                        _ => continue,
                    };
                    let bytes = broca_wire::encode_live_subscribe_control_with_cursor(
                        event.0.into(),
                        &event.1,
                        seq,
                        &event.2,
                    )
                    .unwrap();
                    assert!(broca_wire::decode_subscribe_event(&bytes).is_ok());
                    sample(&format!("{prefix}.subscribe.{kind}.json"), &bytes);
                    frames.push((kind, serde_json::from_slice(&bytes).unwrap()));
                    if kind != "error" {
                        return frames;
                    }
                }
            }
            panic!("the subscription ended without its error or pause");
        },
        || prefix.clone(),
    )
    .await;
    let (stored_error, ended_at_ms) = stored_error(side).await;
    Served {
        store: side.store,
        status,
        head,
        result,
        frames,
        stored_error,
        ended_at_ms,
    }
}

async fn scenario(class: &str, status: u16, name: &str) -> Vec<Served> {
    let mut served = Vec::new();
    for store in [false, true] {
        let error = serde_json::from_value(json!({
            "class":class, "message":"synthetic codeless provider refusal", "status":status,
            "provider_subcode":"upstream_detail", "provider_details":{}
        }))
        .unwrap();
        let side = Side::new(
            store,
            Cipher::None,
            vec![vec![NormalizedEvent::Control(
                broca_protocol::provider::ProviderControl::Error {
                    error,
                    retries_used: 0,
                },
            )]],
        )
        .await;
        side.send().await;
        side.idle().await;
        served.push(capture(&side, name).await);
        assert!(side.registry.stop_and_seal().await);
    }
    served
}

fn assert_error(error: &Value, code: &str) {
    assert_eq!(
        error["provider_code"], code,
        "a served error needs its class-derived code: {error}"
    );
    assert_eq!(error["provider_subcode"], "upstream_detail");
    assert_eq!(error["provider_details"], json!({}));
    let _: cortexkit_role_llm_runner::run::RunError =
        serde_json::from_value(error.clone()).unwrap();
}

fn assert_terminal(served: Served, code: &str, party: &str) {
    assert_eq!(served.status["state"], "error");
    assert_error(&served.status["error"], code);
    assert_error(&served.result["error"], code);
    assert_eq!(served.head["last_run_error"]["provider_code"], code);
    let _: cortexkit_role_llm_runner::run::RunStatus =
        serde_json::from_value(served.status.clone()).unwrap();
    assert!(
        served.stored_error.get("provider_code").is_none(),
        "serving must not rewrite the stored error"
    );
    assert_eq!(served.stored_error["provider_subcode"], "upstream_detail");
    let wait = &served.status["awaits"];
    if served.store {
        assert_eq!(wait["party"], party);
        assert_eq!(wait["since_ms"].as_u64(), served.ended_at_ms);
        assert_eq!(served.result["awaits"], *wait);
        assert_eq!(served.head["last_run_state"]["awaits"], *wait);
    } else {
        assert!(wait.is_null(), "legacy storage still omits awaits");
    }
    assert_eq!(
        served
            .frames
            .iter()
            .map(|(kind, _)| *kind)
            .collect::<Vec<_>>(),
        ["error", "run_finished"]
    );
    for (kind, frame) in served.frames {
        if kind == "error" {
            assert_error(&frame["unit"]["error"], code);
        }
        assert_eq!(frame["unit"]["awaits"], *wait);
        if kind == "run_finished" {
            assert!(
                frame["unit"].get("error").is_none(),
                "the ending frame keeps its existing shape"
            );
        }
    }
}

#[tokio::test]
async fn run_provider_code_codeless_permanent_is_derived_on_every_surface() {
    for served in scenario("permanent", 400, "terminal-unknown-code").await {
        assert_terminal(served, "provider_error", "nobody");
    }
}

#[tokio::test]
async fn run_provider_code_codeless_transient_is_derived_on_every_surface() {
    for served in scenario("transient", 503, "transient-codeless").await {
        assert_terminal(served, "transient", "caller");
    }
}

#[tokio::test]
async fn run_provider_code_codeless_auth_pause_is_derived_where_errors_are_served() {
    for served in scenario("auth", 401, "auth-required-codeless").await {
        assert_eq!(served.status["state"], "paused");
        assert_error(&served.status["error"], "auth_required");
        assert_error(&served.result["error"], "auth_required");
        assert!(
            served.head.get("last_run_error").is_none(),
            "a pause is not a terminal error"
        );
        assert!(served.stored_error.get("provider_code").is_none());
        assert_eq!(served.frames.len(), 1);
        assert_eq!(served.frames[0].0, "paused");
        assert_error(&served.frames[0].1["unit"]["error"], "auth_required");
        if served.store {
            assert_eq!(served.status["awaits"]["party"], "person");
            assert_eq!(
                served.head["last_run_state"]["awaits"],
                served.status["awaits"]
            );
            assert_eq!(
                served.frames[0].1["unit"]["awaits"],
                served.status["awaits"]
            );
        } else {
            assert!(served.status.get("awaits").is_none());
        }
    }
}
