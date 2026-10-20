use super::*;
use serde_json::{json, Value};

fn measured_step(index: usize, usage: Usage) -> Vec<NormalizedEvent> {
    let mut events = if index < 2 {
        tool_step()
    } else {
        text_step("answer")
    };
    for event in &mut events {
        match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                for block in &mut message.content {
                    if let ContentBlock::ToolCall(call) = block {
                        call.tool_call_id = ToolCallId(format!("call-{index}"));
                    }
                }
            }
            NormalizedEvent::Control(ProviderControl::StepFinished { usage: value, .. })
            | NormalizedEvent::Control(ProviderControl::Finished { usage: value, .. }) => {
                *value = usage
            }
            _ => {}
        }
    }
    events
}

fn reported() -> Usage {
    Usage {
        input_tokens: Some(400),
        cached_input_tokens: Some(16000),
        cache_write_tokens: Some(1000),
        output_tokens: Some(220),
        reasoning_tokens: None,
    }
}

async fn replies(rig: &Rig, key: &BindIdentity) -> (String, Value) {
    let status = rig
        .registry
        .run_status(key, RunId("usage-run".into()))
        .await
        .unwrap();
    let answer = rig
        .registry
        .run_result(key, RunId("usage-run".into()))
        .await
        .unwrap();
    let status = serde_json::to_string(&status).unwrap();
    let answer = serde_json::to_value(answer).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&status).unwrap()["usage"],
        answer["usage"]
    );
    (status, answer)
}

async fn vector(file: &str, measurements: [Usage; 3], switch: bool) -> Value {
    let scripts: Vec<_> = measurements
        .into_iter()
        .enumerate()
        .map(|(index, usage)| measured_step(index, usage))
        .collect();
    let rig = Rig::with_exec_factory(file, |wal_store, store| {
        Arc::new(ScriptedCoreExec {
            wal_store,
            store,
            scripts: Mutex::new(VecDeque::from([scripts])),
        })
    });
    let key = BindIdentity::new(rig.dir.join("proj"), "runner", "usage");
    send(&rig, &key, "usage-run", "hello").await;
    wait_status(&rig, &key, "usage-run", "completed").await;
    let live = replies(&rig, &key).await;
    let records: Vec<_> = rig
        .wal_store
        .read_records(&key)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::ModelStepFinished { .. }))
            .count(),
        3
    );

    // Replay the loop's records without an actor or index so both serving paths must agree.
    let cold = Rig::new(&format!("cold-{file}"), Arc::new(NoopExec));
    let cold_key = BindIdentity::new(cold.dir.join("proj"), "runner", "usage");
    let mut seed = Vec::new();
    let mut completed = 0;
    let mut episode = None;
    for mut record in records {
        if let WalRecord::RunStarted { session, .. } = &mut record {
            *session = cold_key.clone();
            episode = Some(record.clone());
        }
        if matches!(record, WalRecord::StepStarted { .. }) && completed == 1 && switch {
            // A seeded continuation episode represents a runner that changes its frozen
            // selection. Ordinary broca authentication resume keeps the original selection.
            let mut resumed = episode.take().unwrap();
            if let WalRecord::RunStarted { config, .. } = &mut resumed {
                config.model = ModelHandle::new("stub", "other-model");
                config.variant = Some("high".into());
            }
            seed.push(resumed);
        }
        if matches!(record, WalRecord::ModelStepFinished { .. }) {
            completed += 1;
        }
        seed.push(record);
    }
    append_seed(&cold.wal_store, &cold_key, &seed);
    let (status, answer) = replies(&cold, &cold_key).await;
    if !switch {
        assert_eq!((status.clone(), answer.clone()), live);
    }
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden")
        .join(file);
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, format!("{status}\n")).unwrap();
        println!("{file}: {status}");
    }
    assert_eq!(status, std::fs::read_to_string(path).unwrap().trim_end());
    let usage = answer["usage"].clone();
    assert_eq!(usage["steps"], 3);
    let entries = usage["by_model"].as_array().unwrap();
    for field in [
        "steps",
        "unreported_steps",
        "input_tokens",
        "cached_input_tokens",
        "cache_write_tokens",
        "output_tokens",
        "reasoning_tokens",
    ] {
        let values: Vec<_> = entries
            .iter()
            .filter_map(|entry| entry[field].as_u64())
            .collect();
        if values.is_empty() {
            assert!(usage.get(field).is_none());
        } else {
            assert_eq!(usage[field], values.into_iter().sum::<u64>(), "{field}");
        }
    }
    usage
}

#[tokio::test]
async fn usage_plain_golden() {
    let usage = vector("run_status.usage_plain.json", [reported(); 3], false).await;
    assert_eq!(usage["input_tokens"], 1200);
    assert_eq!(usage["unreported_steps"], 0);
    assert!(usage.get("reasoning_tokens").is_none());
    assert!(usage["by_model"][0].get("variant").is_none());
}

#[tokio::test]
async fn usage_unreported_only_when_all_fields_absent_golden() {
    let usage = vector(
        "run_status.usage_unreported_step.json",
        [
            reported(),
            Usage::default(),
            Usage {
                output_tokens: Some(0),
                ..Usage::default()
            },
        ],
        false,
    )
    .await;
    assert_eq!(usage["unreported_steps"], 1);
    assert_eq!(usage["input_tokens"], 400);
    assert_eq!(usage["output_tokens"], 220);
}

#[tokio::test]
async fn usage_model_switch_golden() {
    let usage = vector("run_status.usage_model_switch.json", [reported(); 3], true).await;
    assert_eq!(usage["by_model"].as_array().unwrap().len(), 2);
    assert_eq!(usage["by_model"][0]["steps"], 1);
    assert_eq!(usage["by_model"][1]["steps"], 2);
    assert_eq!(usage["by_model"][1]["variant"], "high");
}

#[tokio::test]
async fn usage_reasoning_is_reported_lower_bound_golden() {
    let mut first = reported();
    first.reasoning_tokens = Some(80);
    let mut last = reported();
    last.reasoning_tokens = Some(120);
    let usage = vector(
        "run_status.usage_reasoning.json",
        [first, reported(), last],
        false,
    )
    .await;
    assert_eq!(usage["reasoning_tokens"], 200);
    assert_eq!(usage["output_tokens"], 660);
    assert_eq!(usage["unreported_steps"], 0);
}

#[tokio::test]
async fn usage_zero_steps_omits_all_token_totals() {
    let rig = Rig::new("usage-no-steps", Arc::new(NoopExec));
    let key = BindIdentity::new(rig.dir.join("proj"), "runner", "usage");
    append_seed(
        &rig.wal_store,
        &key,
        &[
            started(&key, "usage-run", "hello"),
            WalRecord::RunPaused {
                run_id: RunId("usage-run".into()),
                reason: PauseReason::AuthRequired,
                error: None,
                ts_ms: None,
            },
        ],
    );
    let (status, _) = replies(&rig, &key).await;
    assert_eq!(
        serde_json::from_str::<Value>(&status).unwrap()["usage"],
        json!({"steps":0,"unreported_steps":0,"by_model":[]})
    );
}
