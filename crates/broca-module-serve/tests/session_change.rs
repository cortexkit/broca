//! Session-change operations through the real serve loop, WAL writer and episode executor.
mod delivery_rig;

use delivery_rig::*;
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use subc_protocol::{Frame, FrameType, Principal};

#[derive(Default)]
struct Admitter(AtomicUsize);

#[async_trait::async_trait]
impl broca_fetch_plan::PlanAdmitter for Admitter {
    async fn admit(
        &self,
        plan: &broca_fetch_plan::Plan,
        _: &broca_protocol::BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
    ) -> Result<broca_fetch_plan::Admitted, broca_fetch_plan::Refusal> {
        self.0.fetch_add(1, Ordering::SeqCst);
        let raw: Value = serde_json::from_str(&plan.text).unwrap();
        let tools = raw["composition"]["providers"].as_array().unwrap().iter().flat_map(|provider| provider["tools"].as_array().unwrap()).map(|tool| {
            serde_json::from_value::<broca_protocol::Tool>(json!({"name":tool["name"],"description":"fixture","input_schema":{"type":"object"},"module":"fixture"})).unwrap()
        }).collect();
        let text = raw["system_text_items"][0]["params"]["text"]
            .as_str()
            .unwrap_or("");
        let mut composition = raw["composition"].clone();
        composition["providers"]
            .as_array_mut()
            .unwrap()
            .retain(|provider| {
                !raw["tool_items"].as_array().unwrap().iter().any(|item| {
                    item["provider"] == provider["provider"] && item["optional"] == true
                })
            });
        Ok(broca_fetch_plan::Admitted {
            manifest: broca_wal::PlanManifest {
                rich_ui: None,
                plan: broca_wal::OpaqueJson::from_json_text(plan.text.clone()).unwrap(),
                plan_identity: plan.identity.clone(),
                composition_digest: cortexkit_role_tool_provider::catalog::composition_digest(
                    &composition,
                )
                .unwrap(),
                fetches: raw["tool_items"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|item| item["optional"] == true)
                    .map(|item| broca_wal::ProviderFetch {
                        provider: item["provider"].as_str().unwrap().into(),
                        outcome: broca_wal::FetchOutcome::Absent {
                            reason: "timeout".into(),
                        },
                    })
                    .collect(),
                tools: Arc::new(tools),
                system_text: text.into(),
                system_digest: text.into(),
                join_version: 1,
                role_versions: Default::default(),
                declarations: vec![],
            },
        })
    }
}

fn plan(names: &[&str], text: &str) -> Value {
    let providers = vec![
        json!({"provider":"fixture","tools":names.iter().map(|name| json!({"name":name,"capabilities":[]})).collect::<Vec<_>>()}),
    ];
    let composition = json!({"providers":providers,"step_transform_items":[]});
    json!({"tool_items":if names.is_empty() {vec![]} else {vec![json!({"provider":"fixture","params":{}})]},
        "system_text_items":[{"provider":"fixture","params":{"text":text}}], "step_transform_items":[],
        "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),"composition":composition})
}

fn plan_with_absent_optional(text: &str) -> Value {
    let mut next = plan(&[], text);
    next["tool_items"] = json!([{"provider":"optional","params":{},"optional":true}]);
    next["composition"]["providers"]
        .as_array_mut()
        .unwrap()
        .push(json!({"provider":"optional","tools":[]}));
    next["composition_digest"] =
        cortexkit_role_tool_provider::catalog::composition_digest(&next["composition"])
            .unwrap()
            .into();
    next
}

fn assert_effective_manifest(rig: &Rig, baseline: &Value) {
    let manifest = rig
        .records()
        .into_iter()
        .rev()
        .find_map(|record| {
            if let broca_wal::WalRecord::RunStarted { manifest, .. } = record {
                manifest
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(
        baseline["effective"]["composition_digest"],
        manifest.composition_digest
    );
    assert_eq!(
        baseline["effective"]["absent_items"],
        json!([
            {"provider":"optional","kind":"tool","reason":"timeout"}
        ])
    );
    assert_eq!(
        baseline["effective"]["absent_items"],
        serde_json::to_value(broca_fetch_plan::baseline_facts(&manifest).absent_items).unwrap()
    );
}

async fn call(rig: &mut Rig, method: &str, params: Value) -> Frame {
    rig.request(7, json!({"method":method,"params":params}))
        .await
}

async fn send(rig: &mut Rig, id: &str, supplied: Option<Value>, model: &str) -> Frame {
    let mut params =
        json!({"send_id":id,"prompt":"hello","model":{"provider":"fake","model":model}});
    if let Some(plan) = supplied {
        params["plan"] = plan;
    }
    call(rig, "session.send", params).await
}

async fn ready(tag: &str) -> (Rig, Arc<Admitter>, Value) {
    let mut rig = Rig::start(tag).await;
    rig.bind_as(
        7,
        Some(Principal::Reserved {
            module_id: "owner".into(),
        }),
    )
    .await;
    let admitter = Arc::new(Admitter::default());
    rig.registry().set_plan_admitter(admitter.clone());
    let initial = plan(&["work"], "original system");
    rig.script.push(&[Step::Text("first")]);
    let sent = send(&mut rig, "first", Some(initial.clone()), "fake").await;
    assert_eq!(sent.header.ty, FrameType::Response, "{sent:?}");
    assert!(result(&sent)["baseline"]["rungs"]["tools"]
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r == "on_prefix_rebuild"));
    rig.wait_idle().await;
    (rig, admitter, initial)
}

async fn refresh(rig: &mut Rig, plan: &Value, policy: &str, generation: u64) -> Frame {
    call(
        rig,
        "session.refresh",
        json!({"plan":plan,"policy":policy,"generation":generation}),
    )
    .await
}

async fn baseline(rig: &mut Rig) -> Value {
    result(&call(rig, "session.baseline", json!({})).await)["baseline"].clone()
}

fn capture(name: &str, frame: &Frame) {
    if std::env::var_os("BROCA_CAPTURE_SAMPLES").is_some() {
        println!(
            "BROCA_SAMPLE {name} {}",
            std::str::from_utf8(&frame.body).unwrap()
        );
    }
}

#[tokio::test]
async fn refresh_applies_once_and_keeps_later_episodes_on_the_effective_manifest() {
    let (mut rig, admitter, _) = ready("change-next-run").await;
    let describe = call(&mut rig, "role.describe", json!({})).await;
    capture("role.describe.session_change.json", &describe);
    let initial = call(&mut rig, "session.baseline", json!({})).await;
    capture("session.baseline.initial.json", &initial);
    assert!(result(&initial)["baseline"].get("effective").is_none());
    let next = plan_with_absent_optional("refreshed system");
    let accepted = refresh(&mut rig, &next, "immediately", 1).await;
    assert_eq!(
        result(&accepted),
        json!({"accepted_generation":1,"pending":{"generation":1,"policy":"immediately"},"run_in_progress":false,"applies_from_next_run":true})
    );
    capture("session.refresh.json", &accepted);
    let pending = call(&mut rig, "session.baseline", json!({})).await;
    capture("session.baseline.pending.json", &pending);
    assert_eq!(result(&pending)["baseline"]["applied_generation"], 0);
    for id in ["second", "third"] {
        rig.script.push(&[Step::Text("next")]);
        let sent = send(&mut rig, id, Some(next.clone()), "fake").await;
        assert_eq!(sent.header.ty, FrameType::Response, "{sent:?}");
        rig.wait_idle().await;
    }
    assert_eq!(
        *rig.script.offered_tools.lock().unwrap(),
        vec![vec!["work".to_string()], vec![], vec![]]
    );
    for prompt in rig.script.prompts.lock().unwrap().iter().skip(1) {
        let text = serde_json::to_string(prompt).unwrap();
        assert!(text.contains("refreshed system"));
        assert!(!text.contains("original system"));
    }
    let applied = call(&mut rig, "session.baseline", json!({})).await;
    capture("session.baseline.applied.json", &applied);
    assert_eq!(result(&applied)["baseline"]["applied_generation"], 1);
    assert!(result(&applied)["baseline"].get("pending").is_none());
    assert_eq!(result(&applied)["baseline"]["tool_names"], json!(["work"]));
    let applied_baseline = &result(&applied)["baseline"];
    assert_eq!(applied_baseline["effective"]["applied_by"], "append");
    assert_effective_manifest(&rig, applied_baseline);
    assert_eq!(
        applied_baseline["composition_digest"],
        result(&initial)["baseline"]["composition_digest"]
    );
    assert_ne!(
        applied_baseline["composition_digest"],
        applied_baseline["effective"]["composition_digest"]
    );
    assert!(applied_baseline.get("absent_items").is_none());
    let before = rig.wal_bytes();
    assert_eq!(
        refresh(&mut rig, &next, "immediately", 1).await.body,
        accepted.body
    );
    assert_eq!(rig.wal_bytes(), before);
    assert_eq!(admitter.0.load(Ordering::SeqCst), 2);
    rig.finish().await;
}

#[tokio::test]
async fn refresh_waits_for_real_rebuild_and_flush_survives_restart() {
    let (mut rig, _, initial) = ready("change-flush-recovery").await;
    let next = plan_with_absent_optional("after flush");
    let accepted = refresh(&mut rig, &next, "on_prefix_rebuild", 3).await;
    assert_eq!(result(&accepted)["applies_from_next_run"], false);
    rig.script.push(&[Step::Text("still old")]);
    assert_eq!(
        send(&mut rig, "before-flush", Some(initial), "fake")
            .await
            .header
            .ty,
        FrameType::Response
    );
    rig.wait_idle().await;
    assert_eq!(rig.script.offered_tools.lock().unwrap()[1], vec!["work"]);
    assert_eq!(baseline(&mut rig).await["pending"]["generation"], 3);
    rig.wait_closed().await;
    rig = rig.restart().await;
    rig.bind_as(
        7,
        Some(Principal::Reserved {
            module_id: "owner".into(),
        }),
    )
    .await;
    let flush = call(&mut rig, "session.flush_prefix", json!({"send_id":"flush"})).await;
    assert_eq!(result(&flush), json!({"pending_generation":3}));
    capture("session.flush_prefix.json", &flush);
    rig.wait_closed().await;
    rig = rig.restart().await;
    rig.bind_as(
        7,
        Some(Principal::Reserved {
            module_id: "owner".into(),
        }),
    )
    .await;
    let before = rig.wal_bytes();
    assert_eq!(
        call(&mut rig, "session.flush_prefix", json!({"send_id":"flush"}))
            .await
            .body,
        flush.body
    );
    assert_eq!(rig.wal_bytes(), before);
    rig.script.push(&[Step::Text("new")]);
    assert_eq!(
        send(&mut rig, "after-flush", Some(next), "fake")
            .await
            .header
            .ty,
        FrameType::Response
    );
    rig.wait_idle().await;
    assert!(rig.script.offered_tools.lock().unwrap()[0].is_empty());
    let applied = baseline(&mut rig).await;
    assert_eq!(applied["effective"]["generation"], 3);
    assert_eq!(applied["effective"]["applied_by"], "fold");
    assert_effective_manifest(&rig, &applied);
    assert_eq!(
        applied["composition_digest"],
        applied["effective"]["composition_digest"]
    );
    assert_eq!(
        applied["absent_items"],
        applied["effective"]["absent_items"]
    );
    assert_eq!(applied["tool_names"], json!([]));
    rig.finish().await;
}

#[tokio::test]
async fn flush_waiting_before_refresh_and_model_switch_are_rebuild_points() {
    for flush_first in [true, false] {
        let (mut rig, _, _) = ready(if flush_first {
            "change-flush-first"
        } else {
            "change-model-switch"
        })
        .await;
        if flush_first {
            let flush = call(
                &mut rig,
                "session.flush_prefix",
                json!({"send_id":"empty-flush"}),
            )
            .await;
            assert_eq!(result(&flush), json!({}));
        }
        let next = plan(&[], "new");
        let accepted = refresh(&mut rig, &next, "on_prefix_rebuild", 1).await;
        assert_eq!(result(&accepted)["applies_from_next_run"], flush_first);
        rig.script.push(&[Step::Text("new")]);
        let sent = send(
            &mut rig,
            "new-run",
            None,
            if flush_first { "fake" } else { "other" },
        )
        .await;
        assert_eq!(sent.header.ty, FrameType::Response, "{sent:?}");
        rig.wait_idle().await;
        assert_eq!(baseline(&mut rig).await["applied_generation"], 1);
        assert!(rig.script.offered_tools.lock().unwrap()[1].is_empty());
        rig.finish().await;
    }
}

#[tokio::test]
async fn refresh_never_changes_a_running_episode_and_repeats_keep_the_original_reply() {
    let (mut rig, _, _) = ready("change-active").await;
    rig.script.hold_tool(true);
    rig.script
        .push(&[Step::Call("held"), Step::Text("old run ends")]);
    let started = send(&mut rig, "held-run", None, "fake").await;
    assert_eq!(started.header.ty, FrameType::Response);
    rig.wait_entered().await;
    let next = plan(&[], "new");
    let accepted = refresh(&mut rig, &next, "immediately", 1).await;
    assert_eq!(result(&accepted)["run_in_progress"], true);
    rig.script.hold_tool(false);
    rig.wait_idle().await;
    let tools = rig.script.offered_tools.lock().unwrap().clone();
    assert_eq!(tools, vec![vec!["work"], vec!["work"], vec!["work"]]);
    let before = rig.wal_bytes();
    assert_eq!(
        refresh(&mut rig, &next, "immediately", 1).await.body,
        accepted.body
    );
    assert_eq!(rig.wal_bytes(), before);
    rig.wait_closed().await;
    rig = rig.restart().await;
    rig.bind_as(
        7,
        Some(Principal::Reserved {
            module_id: "owner".into(),
        }),
    )
    .await;
    let before = rig.wal_bytes();
    assert_eq!(
        refresh(&mut rig, &next, "immediately", 1).await.body,
        accepted.body
    );
    assert_eq!(rig.wal_bytes(), before);
    rig.script.push(&[Step::Text("new run")]);
    assert_eq!(
        send(&mut rig, "new-run", Some(next), "fake")
            .await
            .header
            .ty,
        FrameType::Response
    );
    rig.wait_idle().await;
    assert!(rig.script.offered_tools.lock().unwrap()[0].is_empty());
    rig.finish().await;
}

#[tokio::test]
async fn episode_started_before_refresh_resumes_with_its_original_manifest() {
    for refreshed_episode in [false, true] {
        let (mut rig, _, _) = ready(if refreshed_episode {
            "change-resume-refreshed"
        } else {
            "change-resume-old"
        })
        .await;
        if refreshed_episode {
            let accepted = refresh(
                &mut rig,
                &plan(&["work"], "episode system"),
                "immediately",
                1,
            )
            .await;
            assert_eq!(accepted.header.ty, FrameType::Response);
        }
        rig.script.hold_tool(true);
        rig.script.push(&[Step::Call("held"), Step::Text("resume")]);
        let started = call(&mut rig, "session.send", json!({"send_id":"old-episode","prompt":"old","model":{"provider":"fake","model":"fake"},"on_restart":"resume"})).await;
        let old_run = result(&started)["run_id"].as_str().unwrap().to_string();
        rig.wait_entered().await;
        let before = rig.records().into_iter().find(|r| matches!(r, broca_wal::WalRecord::RunStarted {run_id, ..} if run_id.0 == old_run)).unwrap();
        let generation = if refreshed_episode { 2 } else { 1 };
        let accepted = refresh(
            &mut rig,
            &plan(&[], "pending system"),
            "immediately",
            generation,
        )
        .await;
        assert_eq!(accepted.header.ty, FrameType::Response);
        rig = rig.restart_at_tool_boundary().await;
        rig.bind_as(
            7,
            Some(Principal::Reserved {
                module_id: "owner".into(),
            }),
        )
        .await;
        rig.script.push(&[Step::Text("resumed")]);
        assert_eq!(
            rig.registry()
                .resume_restart_pauses(
                    Arc::new(broca_core::FileWalStore::new(rig.dir.join("wal"))),
                    vec![(old_run.clone(), rig.session.clone())]
                )
                .await,
            (1, 0)
        );
        rig.wait_idle().await;
        let after = rig.records().into_iter().find(|r| matches!(r, broca_wal::WalRecord::RunStarted {run_id, ..} if run_id.0 == old_run)).unwrap();
        assert_eq!(
            serde_json::to_vec(&after).unwrap(),
            serde_json::to_vec(&before).unwrap()
        );
        assert_eq!(rig.script.offered_tools.lock().unwrap()[0], vec!["work"]);
        let rendered = serde_json::to_string(&rig.script.prompts.lock().unwrap()[0]).unwrap();
        assert!(rendered.contains(if refreshed_episode {
            "episode system"
        } else {
            "original system"
        }));
        assert!(!rendered.contains("pending system"));
        assert_eq!(
            baseline(&mut rig).await["applied_generation"],
            u64::from(refreshed_episode)
        );
        rig.finish().await;
    }
}

fn error(frame: &Frame) -> Value {
    assert_eq!(frame.header.ty, FrameType::Error, "{frame:?}");
    serde_json::from_slice(&frame.body).unwrap()
}

fn vector(code: &str) -> Value {
    let vectors: Value = serde_json::from_str(
        cortexkit_role_llm_runner::vectors::file("session-change.json").unwrap(),
    )
    .unwrap();
    vectors["refusals"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["refusal"]["code"] == code)
        .unwrap()["refusal"]
        .clone()
}

#[tokio::test]
async fn refresh_and_policy_refusals_match_the_pinned_vectors_and_write_nothing() {
    let (mut rig, _, _) = ready("change-vector-refusals").await;
    let next = plan(&[], "new");
    let before = rig.wal_bytes();
    let unsupported = error(&refresh(&mut rig, &next, "on_history_rebuild", 3).await);
    assert_eq!(unsupported, vector("rung_unsupported"));
    assert_eq!(rig.wal_bytes(), before);
    assert_eq!(
        refresh(&mut rig, &next, "on_prefix_rebuild", 3)
            .await
            .header
            .ty,
        FrameType::Response
    );
    assert_eq!(
        refresh(
            &mut rig,
            &plan(&["different"], "latest"),
            "on_prefix_rebuild",
            4
        )
        .await
        .header
        .ty,
        FrameType::Response
    );
    let before = rig.wal_bytes();
    let superseded = error(
        &call(
            &mut rig,
            "session.refresh_policy",
            json!({"generation":3,"policy":"immediately","send_id":"superseded"}),
        )
        .await,
    );
    assert_eq!(superseded, vector("pending_superseded"));
    let stale = error(&refresh(&mut rig, &next, "immediately", 4).await);
    assert_eq!(stale["code"], "generation_stale");
    assert_eq!(stale["detail"], vector("generation_stale")["detail"]);
    assert_eq!(rig.wal_bytes(), before);
    // Apply generation 3 in a separate session so the vector's exact generation and cause match.
    rig.finish().await;
    let (mut rig, _, _) = ready("change-already-applied").await;
    refresh(&mut rig, &next, "on_prefix_rebuild", 3).await;
    call(&mut rig, "session.flush_prefix", json!({"send_id":"apply"})).await;
    rig.script.push(&[Step::Text("applied")]);
    assert_eq!(
        send(&mut rig, "apply-run", None, "fake").await.header.ty,
        FrameType::Response
    );
    rig.wait_idle().await;
    let before = rig.wal_bytes();
    let applied = error(
        &call(
            &mut rig,
            "session.refresh_policy",
            json!({"generation":3,"policy":"immediately","send_id":"late"}),
        )
        .await,
    );
    assert_eq!(applied["code"], vector("already_applied")["code"]);
    assert_eq!(applied["detail"], vector("already_applied")["detail"]);
    assert_eq!(rig.wal_bytes(), before);
    rig.finish().await;
}

#[tokio::test]
async fn policy_repeat_remains_identical_after_application_and_restart() {
    let (mut rig, _, _) = ready("change-policy-repeat").await;
    let next = plan(&[], "new");
    let original = refresh(&mut rig, &next, "on_prefix_rebuild", 1).await;
    let params = json!({"generation":1,"policy":"immediately","send_id":"policy-1"});
    let changed = call(&mut rig, "session.refresh_policy", params.clone()).await;
    assert_eq!(
        result(&changed),
        json!({"pending":{"generation":1,"policy":"immediately"}})
    );
    capture("session.refresh_policy.json", &changed);
    assert_eq!(
        refresh(&mut rig, &next, "on_prefix_rebuild", 1).await.body,
        original.body
    );
    rig.script.push(&[Step::Text("applied")]);
    assert_eq!(
        send(&mut rig, "apply", None, "fake").await.header.ty,
        FrameType::Response
    );
    rig.wait_idle().await;
    rig.wait_closed().await;
    rig = rig.restart().await;
    rig.bind_as(
        7,
        Some(Principal::Reserved {
            module_id: "owner".into(),
        }),
    )
    .await;
    let before = rig.wal_bytes();
    assert_eq!(
        call(&mut rig, "session.refresh_policy", params).await.body,
        changed.body
    );
    let reuse = error(
        &call(
            &mut rig,
            "session.refresh_policy",
            json!({"generation":1,"policy":"on_prefix_rebuild","send_id":"policy-1"}),
        )
        .await,
    );
    assert_eq!(reuse, vector("send_id_reuse"));
    assert_eq!(
        refresh(&mut rig, &next, "on_prefix_rebuild", 1).await.body,
        original.body
    );
    assert_eq!(rig.wal_bytes(), before);
    rig.finish().await;
}

#[tokio::test]
async fn all_three_ops_are_owner_only_and_requests_are_strict() {
    let (mut rig, _, _) = ready("change-owner").await;
    rig.bind_as(
        9,
        Some(Principal::Reserved {
            module_id: "other".into(),
        }),
    )
    .await;
    let before = rig.wal_bytes();
    for (method, params) in [
        (
            "session.refresh",
            json!({"plan":plan(&[], "new"),"policy":"immediately","generation":1}),
        ),
        (
            "session.refresh_policy",
            json!({"generation":1,"policy":"immediately","send_id":"p"}),
        ),
        ("session.flush_prefix", json!({"send_id":"f"})),
    ] {
        let frame = rig
            .request(9, json!({"method":method,"params":params}))
            .await;
        assert_eq!(error(&frame)["code"], "scope_owner_mismatch");
    }
    assert_eq!(rig.wal_bytes(), before);
    for (method, params) in [
        (
            "session.refresh",
            json!({"plan":plan(&[], "new"),"policy":"unknown","generation":1}),
        ),
        (
            "session.refresh",
            json!({"plan":plan(&[], "new"),"policy":"immediately","generation":1,"extra":true}),
        ),
        (
            "session.refresh_policy",
            json!({"generation":1,"policy":"immediately","send_id":"p","extra":true}),
        ),
        ("session.flush_prefix", json!({"send_id":"f","extra":true})),
    ] {
        assert_eq!(
            error(&call(&mut rig, method, params).await)["code"],
            "invalid_params"
        );
    }
    assert_eq!(rig.wal_bytes(), before);
    rig.finish().await;
}

#[tokio::test]
async fn concurrent_identical_refreshes_fetch_and_write_once() {
    use broca_module::serve::ServeHandler;
    let (rig, admitter, _) = ready("change-concurrent").await;
    let next = plan(&[], "new").as_object().unwrap().clone();
    let owner = broca_protocol::BindPrincipal::Reserved {
        module_id: "owner".into(),
    };
    let scope = broca_protocol::SendScope::DaemonWithoutScopes;
    let registry = rig.registry().clone();
    let (one, two) = tokio::join!(
        registry.session_refresh(
            &rig.session,
            Some(&owner),
            &scope,
            next.clone(),
            "immediately".into(),
            1
        ),
        registry.session_refresh(
            &rig.session,
            Some(&owner),
            &scope,
            next,
            "immediately".into(),
            1
        ),
    );
    assert_eq!(one, two);
    assert!(one.is_ok());
    assert_eq!(admitter.0.load(Ordering::SeqCst), 2);
    assert_eq!(
        rig.records()
            .iter()
            .filter(|r| matches!(r, broca_wal::WalRecord::SessionRefreshAccepted { .. }))
            .count(),
        1
    );
    rig.finish().await;
}

#[tokio::test]
async fn pending_model_mismatch_writes_nothing_keeps_pending_and_allows_both_recoveries() {
    for supersede in [false, true] {
        let (mut rig, _, initial) = ready(if supersede {
            "change-model-supersede"
        } else {
            "change-model-recovery"
        })
        .await;
        let mut next = plan(&[], "model-specific system");
        next["system_text_items"][0]["params"]["model"] = json!({"provider":"fake","id":"other"});
        assert_eq!(
            refresh(&mut rig, &next, "immediately", 1).await.header.ty,
            FrameType::Response
        );
        let before = rig.wal_bytes();
        for supplied in [None, Some(initial)] {
            let frame = send(&mut rig, "mismatched", supplied, "fake").await;
            capture("session.refresh.plan_model_mismatch.json", &frame);
            let refused = error(&frame);
            assert_eq!(refused["code"], "plan_model_mismatch");
            assert_eq!(refused["detail"]["generation"], 1);
            assert_eq!(
                refused["detail"]["send_model"],
                json!({"provider":"fake","id":"fake"})
            );
            assert_eq!(refused["detail"]["class"], "permanent");
            assert_eq!(rig.wal_bytes(), before);
        }
        let inherited = error(
            &call(
                &mut rig,
                "session.send",
                json!({"send_id":"inherited","prompt":"hello","delivery":["steer","idle_start"]}),
            )
            .await,
        );
        assert_eq!(inherited["code"], "plan_model_mismatch", "{inherited}");
        assert_eq!(
            inherited["detail"]["send_model"],
            json!({"provider":"fake","id":"fake"})
        );
        assert_eq!(rig.wal_bytes(), before);
        assert_eq!(baseline(&mut rig).await["pending"]["generation"], 1);
        rig.script.push(&[Step::Text("recovered")]);
        if supersede {
            let safe = plan(&[], "superseding system");
            assert_eq!(
                refresh(&mut rig, &safe, "immediately", 2).await.header.ty,
                FrameType::Response
            );
            assert_eq!(
                send(&mut rig, "mismatched", None, "fake").await.header.ty,
                FrameType::Response
            );
        } else {
            assert_eq!(
                send(&mut rig, "mismatched", Some(next), "other")
                    .await
                    .header
                    .ty,
                FrameType::Response
            );
        }
        rig.wait_idle().await;
        assert_eq!(
            baseline(&mut rig).await["applied_generation"],
            if supersede { 2 } else { 1 }
        );
        rig.finish().await;
    }
}
