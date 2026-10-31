use super::*;
use broca_core::compaction_provider::{
    lineage::MessageRef,
    notice::ViewReady,
    setup::{SetupAnswer, SetupRequest},
    view::{CompactionView, ViewGetAnswer, ViewGetRequest, ViewRange},
};
use std::sync::Mutex;

#[derive(Default)]
struct Plane(Mutex<Option<CompactionView>>);

#[async_trait::async_trait]
impl broca_core::CompactionPlane for Plane {
    async fn setup(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
        request: SetupRequest,
    ) -> Result<SetupAnswer, broca_core::CompactionUnavailable> {
        Ok(serde_json::from_value(json!({
            "answer":"ready", "request_id":request.request_id,
            "initial":{"compaction_id":"head","version":1,"range":{"from":0,"to":0},"replacement":[]}
        })).unwrap())
    }

    async fn view_get(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
        _: ViewGetRequest,
    ) -> Result<ViewGetAnswer, broca_core::CompactionUnavailable> {
        Ok(ViewGetAnswer::view(self.0.lock().unwrap().clone().unwrap()))
    }
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
        Ok(broca_fetch_plan::Admitted {
            manifest: broca_wal::PlanManifest {
                rich_ui: None,
                plan: broca_wal::OpaqueJson::from_json_text(plan.text.clone()).unwrap(),
                plan_identity: plan.identity.clone(),
                composition_digest: raw["composition_digest"].as_str().unwrap().into(),
                tools: Arc::new(vec![serde_json::from_value(json!({
                    "name":"work","description":"does work","input_schema":{"type":"object"}
                }))
                .unwrap()]),
                system_text: String::new(),
                system_digest: String::new(),
                join_version: 1,
                role_versions: Default::default(),
                fetches: vec![],
                declarations: vec![],
            },
        })
    }
}

#[tokio::test]
async fn send_between_idle_view_application_and_queue_wake_uses_the_applied_view() {
    let mut rig = Rig::start("idle-view-before-wake").await;
    broca_core::media_store::create_media_directories(&rig.dir).unwrap();
    *rig.script.media_frozen.lock().unwrap() = Some(frozen(WireFamilyId::AnthropicMessages));
    let plane = Arc::new(Plane::default());
    rig.set_compaction_plane(plane.clone());
    rig.registry().set_plan_admitter(Arc::new(Admitter));
    let composition = json!({"providers":[{"provider":"fixture","tools":[{"name":"work","capabilities":[]}]}],"compaction":{"provider":"fixture"},"step_transform_items":[]});
    let plan = json!({
        "tool_items":[{"provider":"fixture","params":{}}],"system_text_items":[],"step_transform_items":[],
        "compaction_item":{"provider":"fixture","preset":"head","params":{}},
        "composition_digest":cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
        "composition":composition
    });
    rig.script.push(&[Step::Text("ready")]);
    result(&rig.request(7, json!({"method":"session.send","params":{
        "send_id":"first","prompt":"original question","model":{"provider":"fake","model":"fake"},"plan":plan
    }})).await);
    rig.wait_idle().await;
    let head = result(&call(&mut rig, "session.head", json!({})).await);
    let lineage = head["lineage_id"].as_str().unwrap();
    let view = CompactionView::new(
        broca_core::agent_session_ref(&rig.session),
        2,
        lineage,
        MessageRef {
            ordinal: 0,
            mid: "m0".into(),
        },
        vec![ViewRange::replace(
            0,
            1,
            json!([{"role":"user","content":[{"type":"text","text":"applied summary"}]}])
                .to_string(),
        )],
    );
    let ready = ViewReady::for_view(&view);
    *plane.0.lock().unwrap() = Some(view);
    assert!(rig
        .registry()
        .route_compaction_ready(&rig.session, ready)
        .await
        .unwrap());
    // Send after ApplyCompaction and before CompactionChanged, without relying on timing.
    rig.script.push(&[Step::Text("next answer")]);
    result(
        &rig.request(
            7,
            json!({"method":"session.send","params":{
                "send_id":"between-commands","prompt":"next question","model":{"provider":"fake","model":"fake"}
            }}),
        )
        .await,
    );
    rig.wait_idle().await;
    let request = rig.script.rendered.lock().unwrap().last().cloned().unwrap();
    let request = String::from_utf8(request).unwrap();
    assert!(request.contains("applied summary"), "{request}");
    assert!(!request.contains("original question"), "{request}");
    rig.registry()
        .route_compaction_changed(&rig.session)
        .await
        .unwrap();
    rig.finish().await;
}
