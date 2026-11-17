//! A session's prompt-cache policy (llm-runner `cache_policy`). A planned session's runs take
//! `plan.cache`, whoever sends their message: an app's message or a module's notice carries no
//! cache, and its run still keeps the owner's lifetime. A planless session's creating send sets
//! the policy for the session's life, and a later send carrying `cache` is refused.

use super::*;
use broca_protocol::request::KeepWarm;
use cortexkit_role_llm_runner::cache::Cache;

/// With `BROCA_CAPTURE_SAMPLES` set, prints `frame`'s body under `name` for readers' samples.
fn sample(name: &str, frame: &subc_protocol::Frame) {
    if std::env::var_os("BROCA_CAPTURE_SAMPLES").is_some() {
        eprintln!(
            "BROCA_SAMPLE {name} {}",
            String::from_utf8_lossy(&frame.body)
        );
    }
}

fn policy(value: Value) -> Cache {
    serde_json::from_value(value).unwrap()
}

/// What each `RunStart` froze as its warming.
async fn frozen_warming(rig: &Rig) -> Vec<Option<Value>> {
    rig.records()
        .await
        .into_iter()
        .filter_map(|record| match record {
            Record::RunStart(start) => Some(start.keep_warm),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn every_run_of_a_planned_session_takes_the_plans_cache() {
    let (mut rig, _admitter) = plan_rig("plan-cache").await;
    let cache = json!({"ttl": "long", "keep_warm": "auto"});
    let mut plan = vector("plans/broca-head-no-compaction.json");
    plan["cache"] = cache.clone();
    rig.script
        .push(&[Step::Text("first"), Step::Text("second")]);
    result(&send_plan(&mut rig, 7, "turn-1", "hi", Some(&plan), json!({})).await);
    rig.wait_for_finished(1).await;
    result(&send_plan(&mut rig, 7, "turn-2", "next", None, json!({})).await);
    rig.wait_for_finished(2).await;
    let expected = RunCache {
        cache: Some(policy(cache)),
        keep_warm: Some(KeepWarm::Auto),
        inherit: false,
    };
    assert_eq!(
        *rig.script.run_caches.lock().unwrap(),
        [expected.clone(), expected],
        "the later run takes the plan's cache, not its send's"
    );
    assert_eq!(
        frozen_warming(&rig).await,
        [Some(json!("auto")), Some(json!("auto"))]
    );
    rig.finish().await;
}

#[tokio::test]
async fn a_planless_sessions_creating_send_sets_its_cache_for_life() {
    let mut rig = Rig::start("planless-cache").await;
    rig.script
        .push(&[Step::Text("first"), Step::Text("second")]);
    let cache = json!({
        "ttl": "long",
        "keep_warm": "hours",
        "hours": {"from": "09:00", "to": "18:00", "tz": "Europe/Istanbul"},
    });
    result(
        &rig.send_with("turn-1", "hi", None, json!({"cache": cache.clone()}))
            .await,
    );
    rig.wait_for_finished(1).await;
    result(&rig.send("turn-2", "next", None).await);
    rig.wait_for_finished(2).await;
    let warming = KeepWarm::of(&policy(cache.clone()));
    assert!(matches!(warming, Some(KeepWarm::Hours(_))));
    assert_eq!(
        *rig.script.run_caches.lock().unwrap(),
        [
            RunCache {
                cache: Some(policy(cache)),
                keep_warm: warming.clone(),
                inherit: false,
            },
            RunCache {
                cache: None,
                keep_warm: warming,
                inherit: true,
            },
        ],
        "the second run inherits the first run's policy and warming"
    );

    // A later send can't change it, even to the same policy.
    let before = rig.records().await;
    let refused = rig
        .send_with("turn-3", "again", None, json!({"cache": {"ttl": "long"}}))
        .await;
    sample("cache-on-a-later-send", &refused);
    let error = error_body(&refused);
    assert_eq!(error["code"], "invalid_params", "{error}");
    assert_eq!(error["detail"]["field"], "cache", "{error}");
    assert_eq!(rig.records().await, before, "a refused send writes nothing");
    rig.finish().await;
}

#[tokio::test]
async fn a_send_cache_is_refused_beside_a_plan_and_by_its_own_field() {
    let (mut rig, admitter) = plan_rig("send-cache-refusals").await;
    let plan = vector("plans/broca-head-no-compaction.json");
    for (case, plan, cache, field) in [
        ("beside-plan", Some(&plan), json!({"ttl": "long"}), "cache"),
        ("bad-key", None, json!({"key": "has space"}), "cache.key"),
        (
            "warm-without-cache",
            None,
            json!({"ttl": "none", "keep_warm": "auto"}),
            "cache.keep_warm",
        ),
        (
            "unknown-zone",
            None,
            json!({
                "keep_warm": "hours",
                "hours": {"from": "09:00", "to": "18:00", "tz": "Mars/Olympus"},
            }),
            "cache.hours.tz",
        ),
    ] {
        let refused = send_plan(&mut rig, 7, case, "hi", plan, json!({"cache": cache})).await;
        sample(case, &refused);
        let error = error_body(&refused);
        assert_eq!(error["code"], "invalid_params", "{case}: {error}");
        assert_eq!(error["detail"]["field"], field, "{case}: {error}");
    }
    assert!(rig.records().await.is_empty(), "nothing was written");
    assert!(admitter.calls().is_empty(), "no plan was fetched");
    rig.finish().await;
}
