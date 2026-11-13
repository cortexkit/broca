use super::*;
use broca_protocol::usage::UsageIteration;
use cortexkit_role_llm_runner::usage::{RunUsage, SessionUsage};

fn iteration(model: &str, total: Option<u64>, split: Option<(u64, u64)>) -> UsageIteration {
    let mut value = json!({
        "type": "message", "model": model, "input_tokens": 10, "output_tokens": 5,
    });
    if let Some(total) = total {
        value["cache_creation_input_tokens"] = json!(total);
    }
    if let Some((five, hour)) = split {
        value["cache_creation"] = json!({
            "ephemeral_5m_input_tokens": five, "ephemeral_1h_input_tokens": hour,
        });
    }
    serde_json::from_value(value).unwrap()
}

fn script(iterations: &[UsageIteration]) -> Vec<Vec<NormalizedEvent>> {
    let mut events = text_step("cache-answer", "synthetic answer");
    events.insert(
        1,
        NormalizedEvent::Control(
            serde_json::from_value(json!({
                "type": "step_finished", "step_id": 1, "finish_reason": "stop",
                "usage": {"input_tokens": 10, "output_tokens": 5},
                "usage_iterations": iterations,
            }))
            .unwrap(),
        ),
    );
    vec![events]
}

fn assert_pair(value: &Value, expected: Option<(u64, u64)>) {
    match expected {
        Some((five, hour)) => {
            assert_eq!(value["cache_write_5m_tokens"], json!(five), "{value}");
            assert_eq!(value["cache_write_1h_tokens"], json!(hour), "{value}");
            assert_eq!(value["cache_write_tokens"], json!(five + hour), "{value}");
        }
        None => {
            assert!(value.get("cache_write_5m_tokens").is_none(), "{value}");
            assert!(value.get("cache_write_1h_tokens").is_none(), "{value}");
        }
    }
}

fn capture(case: &str, storage: &str, method: &str, bytes: &[u8]) {
    if !matches!(case, "complete" | "incomplete") {
        return;
    }
    if let Some(directory) = std::env::var_os("BROCA_CACHE_WRITE_SAMPLE_DIR") {
        let directory = std::path::PathBuf::from(directory).join(storage).join(case);
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(directory.join(format!("{method}.json")), bytes).unwrap();
    }
}

async fn served(
    case: &str,
    iterations: Vec<UsageIteration>,
    expected_run: Option<(u64, u64)>,
    expected_models: &[(&str, Option<(u64, u64)>)],
) {
    let [(today, _), (store, _)] = pair(|| script(&iterations), false).await;
    for (storage, side) in [("legacy", &today), ("store", &store)] {
        assert!(matches!(
            side.send("cache-run", "synthetic cache usage").await,
            SendOutcome::Active { .. }
        ));
        side.closed().await;
        let status = side
            .registry
            .run_status_reply(&side.key, RunId("cache-run".into()))
            .await
            .unwrap();
        let result = side
            .registry
            .run_result(&side.key, RunId("cache-run".into()))
            .await
            .unwrap();
        let head = side.registry.head(&side.key).await.unwrap();
        for (method, bytes) in [
            ("run.status", serde_json::to_vec(&status).unwrap()),
            ("run.result", serde_json::to_vec(&result).unwrap()),
        ] {
            let reply: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(reply["state"], "completed", "{storage}: {reply}");
            let value = &reply["usage"];
            let usage: RunUsage = serde_json::from_value(value.clone()).unwrap();
            assert!(usage.consistent(), "{storage} {method}: {value}");
            assert_eq!(usage.steps, iterations.len() as u64);
            assert_eq!(usage.by_model.len(), expected_models.len());
            assert_pair(value, expected_run);
            for (entry, (model, expected)) in value["by_model"]
                .as_array()
                .unwrap()
                .iter()
                .zip(expected_models)
            {
                assert_eq!(entry["model"], *model);
                assert_pair(entry, *expected);
            }
            capture(case, storage, method, &bytes);
        }
        let bytes = serde_json::to_vec(&head).unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        if case == "overflow" {
            // Session usage rejects totals outside JSON's exact integer range independently
            // of whether run usage has a lifetime breakdown.
            assert!(value.get("usage").is_none());
            continue;
        }
        let usage: SessionUsage = serde_json::from_value(value["usage"].clone()).unwrap();
        assert!(usage.consistent(), "{storage} session.head: {value}");
        assert_eq!(usage.totals.steps, iterations.len() as u64);
        assert_pair(&value["usage"], None);
        for model in value["usage"]["by_model"].as_array().unwrap() {
            assert_pair(model, None);
        }
        capture(case, storage, "session.head", &bytes);
    }
    assert!(store.in_store() && !today.in_store());
}

#[tokio::test]
async fn complete_lifetimes_fill_both_run_replies_on_both_storages() {
    served(
        "complete",
        vec![
            iteration("claude-sonnet", Some(30), Some((10, 20))),
            iteration("claude-sonnet", Some(70), Some((20, 50))),
        ],
        Some((30, 70)),
        &[("claude-sonnet", Some((30, 70)))],
    )
    .await;
}

#[tokio::test]
async fn missing_lifetimes_omit_both_members_on_both_storages() {
    served(
        "incomplete",
        vec![
            iteration("claude-sonnet", Some(30), Some((10, 20))),
            iteration("claude-sonnet", Some(70), None),
            iteration("claude-sonnet", Some(30), Some((10, 20))),
        ],
        None,
        &[("claude-sonnet", None)],
    )
    .await;
}

#[tokio::test]
async fn mixed_models_keep_only_the_complete_models_split() {
    served(
        "mixed",
        vec![
            iteration("claude-sonnet", Some(30), Some((10, 20))),
            iteration("claude-haiku", Some(40), None),
            iteration("claude-sonnet", Some(70), Some((20, 50))),
            iteration("claude-haiku", Some(30), Some((10, 20))),
        ],
        None,
        &[("claude-sonnet", Some((30, 70))), ("claude-haiku", None)],
    )
    .await;
}

#[tokio::test]
async fn absent_cache_write_total_is_not_a_reported_zero() {
    served(
        "absent-total",
        vec![
            iteration("claude-sonnet", Some(30), Some((10, 20))),
            iteration("claude-sonnet", None, Some((0, 0))),
        ],
        None,
        &[("claude-sonnet", None)],
    )
    .await;
    served(
        "reported-zero",
        vec![
            iteration("claude-sonnet", Some(0), Some((0, 0))),
            iteration("claude-sonnet", Some(30), Some((10, 20))),
        ],
        Some((10, 20)),
        &[("claude-sonnet", Some((10, 20)))],
    )
    .await;
}

#[tokio::test]
async fn malformed_step_lifetimes_never_become_a_run_breakdown() {
    for split in [Some((1, 2)), Some((u64::MAX, 1)), None] {
        let mut broken = iteration("claude-sonnet", Some(30), split);
        if split.is_none() {
            broken.other.insert(
                "cache_creation".into(),
                json!({"ephemeral_5m_input_tokens": 30}),
            );
        }
        served(
            "malformed",
            vec![broken, iteration("claude-sonnet", Some(30), Some((10, 20)))],
            None,
            &[("claude-sonnet", None)],
        )
        .await;
    }
}

#[tokio::test]
async fn overflowing_aggregate_lifetimes_are_omitted() {
    served(
        "overflow",
        vec![
            iteration("claude-sonnet", Some(u64::MAX), Some((u64::MAX, 0))),
            iteration("claude-sonnet", Some(1), Some((0, 1))),
        ],
        None,
        &[("claude-sonnet", None)],
    )
    .await;
}
