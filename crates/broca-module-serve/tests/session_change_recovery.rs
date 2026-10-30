//! Cross-release rollback: export with the writer, then rebuild and replay with the reader only.
mod delivery_rig;
use delivery_rig::*;
use serde_json::{json, Value};
use std::{path::PathBuf, sync::Arc};
use subc_protocol::{Frame, FrameType, Principal};

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/scratch/session-change-rollback")
}

struct Admitter;
#[async_trait::async_trait]
impl broca_fetch_plan::PlanAdmitter for Admitter {
    async fn admit(
        &self,
        plan: &broca_fetch_plan::Plan,
        _: &broca_protocol::BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
    ) -> Result<broca_fetch_plan::Admitted, broca_fetch_plan::Refusal> {
        let raw: Value = serde_json::from_str(&plan.text).unwrap();
        let tools = raw["composition"]["providers"][0]["tools"].as_array().unwrap().iter().map(|tool| serde_json::from_value(json!({"name":tool["name"],"description":"fixture","input_schema":{"type":"object"}})).unwrap()).collect();
        let text = raw["system_text_items"][0]["params"]["text"]
            .as_str()
            .unwrap();
        Ok(broca_fetch_plan::Admitted {
            manifest: broca_wal::PlanManifest {
                rich_ui: None,
                plan: broca_wal::OpaqueJson::from_json_text(plan.text.clone()).unwrap(),
                plan_identity: plan.identity.clone(),
                composition_digest: plan.composition_digest.clone(),
                fetches: vec![],
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
    let composition = json!({"providers":[{"provider":"fixture","tools":names.iter().map(|name| json!({"name":name,"capabilities":[]})).collect::<Vec<_>>()}],"step_transform_items":[]});
    json!({"tool_items":if names.is_empty() {vec![]} else {vec![json!({"provider":"fixture","params":{}})]},
        "system_text_items":[{"provider":"fixture","params":{"text":text}}],"step_transform_items":[],
        "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),"composition":composition})
}

async fn call(rig: &mut Rig, method: &str, params: Value) -> Frame {
    rig.request(7, json!({"method":method,"params":params}))
        .await
}

async fn owner(rig: &mut Rig) {
    rig.bind_as(
        7,
        Some(Principal::Reserved {
            module_id: "owner".into(),
        }),
    )
    .await;
}

async fn send(rig: &mut Rig, key: &str, model: &str, plan: Option<Value>) {
    rig.script.push(&[Step::Text("answer")]);
    let mut params = json!({"send_id":key,"prompt":key,"model":{"provider":"fake","model":model}});
    if let Some(plan) = plan {
        params["plan"] = plan;
    }
    let reply = call(rig, "session.send", params).await;
    assert_eq!(reply.header.ty, FrameType::Response, "{reply:?}");
    rig.wait_idle().await;
}

fn manifests(rig: &Rig, skip: usize) -> Vec<Value> {
    rig.records()
        .into_iter()
        .filter_map(|record| match record {
            broca_wal::WalRecord::RunStarted { manifest, .. } => {
                Some(serde_json::to_value(manifest).unwrap())
            }
            _ => None,
        })
        .skip(skip)
        .collect()
}

async fn future_runs(rig: &mut Rig, flushed: bool) {
    for i in 0..3 {
        send(
            rig,
            &format!("reader-{i}"),
            if !flushed && i > 0 { "other" } else { "fake" },
            None,
        )
        .await;
        let baseline = result(&call(rig, "session.baseline", json!({})).await);
        assert_eq!(
            baseline["baseline"]["applied_generation"],
            if !flushed && i == 0 { 1 } else { 2 }
        );
    }
}

fn assert_surfaces(rig: &Rig, flushed: bool, skip: usize) {
    let expected: Vec<Vec<String>> = if flushed {
        vec![vec![], vec![], vec![]]
    } else {
        vec![vec!["next".into()], vec![], vec![]]
    };
    assert_eq!(rig.script.offered_tools.lock().unwrap()[skip..], expected);
    for (i, prompt) in rig
        .script
        .prompts
        .lock()
        .unwrap()
        .iter()
        .skip(skip)
        .enumerate()
    {
        let text = serde_json::to_string(prompt).unwrap();
        let wanted = if !flushed && i == 0 {
            "prefix one"
        } else {
            "prefix two"
        };
        assert!(text.contains(wanted), "{text}");
        assert!(!text.contains("original prefix"));
    }
}

#[tokio::test]
#[ignore = "must run on the writer build before the reader-only replay"]
async fn export_writer_rollback_fixture() {
    std::fs::create_dir_all(root()).unwrap();
    for flushed in [false, true] {
        let case = if flushed { "flushed" } else { "waiting" };
        let dir = root().join(format!("{case}-rig"));
        let mut rig = Rig::open_fixture(dir).await;
        owner(&mut rig).await;
        rig.registry().set_plan_admitter(Arc::new(Admitter));
        let describe = result(&call(&mut rig, "role.describe", json!({})).await);
        assert!(describe["capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|cap| cap == "session_change"));
        send(
            &mut rig,
            "initial",
            "fake",
            Some(plan(&["work"], "original prefix")),
        )
        .await;
        let first = call(
            &mut rig,
            "session.refresh",
            json!({"plan":plan(&["next"], "prefix one"),"generation":1,"policy":"immediately"}),
        )
        .await;
        assert_eq!(first.header.ty, FrameType::Response);
        assert_eq!(
            result(&call(&mut rig, "session.baseline", json!({})).await)["baseline"]["pending"]
                ["generation"],
            1
        );
        send(&mut rig, "apply-first", "fake", None).await;
        let second = call(
            &mut rig,
            "session.refresh",
            json!({"plan":plan(&[], "prefix two"),"generation":2,"policy":"on_prefix_rebuild"}),
        )
        .await;
        assert_eq!(second.header.ty, FrameType::Response);
        if flushed {
            assert_eq!(
                call(
                    &mut rig,
                    "session.flush_prefix",
                    json!({"send_id":"flush-before-rollback"})
                )
                .await
                .header
                .ty,
                FrameType::Response
            );
        }
        rig.wait_closed().await;
        let wal = rig.wal_bytes();
        std::fs::write(root().join(format!("{case}.wal")), &wal).unwrap();
        println!(
            "WRITER_FIXTURE {case}: accepted=2 applied=1 pending=2 wal_bytes={}",
            wal.len()
        );
        future_runs(&mut rig, flushed).await;
        assert_surfaces(&rig, flushed, 2);
        let expected = json!({"requests":rig.script.rendered.lock().unwrap()[2..],"manifests":manifests(&rig, 2)});
        std::fs::write(
            root().join(format!("{case}.json")),
            serde_json::to_vec(&expected).unwrap(),
        )
        .unwrap();
        rig.finish().await;
    }
}

#[tokio::test]
#[ignore = "requires WAL and request-byte fixtures exported by the writer build"]
async fn reader_only_replays_writer_refreshes_at_their_recorded_boundaries() {
    for flushed in [false, true] {
        let case = if flushed { "flushed" } else { "waiting" };
        let expected: Value = serde_json::from_slice(
            &std::fs::read(root().join(format!("{case}.json")))
                .expect("writer-generated request fixture"),
        )
        .unwrap();
        let mut rig = Rig::open_fixture(root().join(format!("{case}-rig"))).await;
        rig.seed_fixture_wal(
            &std::fs::read(root().join(format!("{case}.wal"))).expect("writer-generated WAL"),
        );
        owner(&mut rig).await;
        let describe = result(&call(&mut rig, "role.describe", json!({})).await);
        assert!(
            !describe["capabilities"]
                .as_array()
                .unwrap()
                .iter()
                .any(|cap| cap == "session_change"),
            "reader-only binary required"
        );
        let before = result(&call(&mut rig, "session.baseline", json!({})).await);
        assert_eq!(before["baseline"]["accepted_generation"], 2);
        assert_eq!(before["baseline"]["applied_generation"], 1);
        assert_eq!(before["baseline"]["pending"]["generation"], 2);
        future_runs(&mut rig, flushed).await;
        assert_surfaces(&rig, flushed, 0);
        assert_eq!(
            serde_json::to_value(&*rig.script.rendered.lock().unwrap()).unwrap(),
            expected["requests"],
            "exact provider request bytes on {case} rollback"
        );
        assert_eq!(
            serde_json::to_value(manifests(&rig, 2)).unwrap(),
            expected["manifests"],
            "each episode freezes the writer's exact manifest"
        );
        assert_eq!(
            rig.records()
                .iter()
                .filter(|record| matches!(
                    record,
                    broca_wal::WalRecord::SessionRefreshAccepted { .. }
                ))
                .count(),
            2
        );
        println!("READER_ONLY_REPLAY {case}: three request bodies and three frozen manifests match the writer byte-for-byte; applied=2 pending=none");
        rig.finish().await;
    }
}
