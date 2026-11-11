use super::*;
use broca_core::ProviderCapabilities;
use serde_json::json;

/// The rig's transport remembers a successful list but still attempts every catalog read.
struct CatalogPlane {
    catalog: Mutex<Result<Vec<String>, ()>>,
    kept: Mutex<Option<Vec<String>>>,
    reads: AtomicUsize,
    setups: AtomicUsize,
}

#[async_trait::async_trait]
impl broca_core::CompactionPlane for CatalogPlane {
    async fn provider_capabilities(&self, _: &str) -> ProviderCapabilities {
        self.reads.fetch_add(1, Ordering::SeqCst);
        if let Ok(provides) = &*self.catalog.lock().unwrap() {
            *self.kept.lock().unwrap() = Some(provides.clone());
        }
        self.kept.lock().unwrap().clone().map_or(
            ProviderCapabilities::Unknown,
            ProviderCapabilities::Declared,
        )
    }

    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::CompactionUnavailable,
    > {
        panic!("v2 uses call_v2")
    }

    async fn call_v2(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        method: &str,
        _: Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        let answer = match method {
            "compaction.setup" => {
                self.setups.fetch_add(1, Ordering::SeqCst);
                "ready"
            }
            "compaction.notice" => "received",
            "compaction.ride" => "nothing",
            other => panic!("unexpected compaction method {other}"),
        };
        Ok(serde_json::value::to_raw_value(&json!({"answer": answer})).unwrap())
    }
}

fn provides() -> Vec<String> {
    vec![
        "compaction-provider/v2".into(),
        "compaction-entry-speaks-for/v1".into(),
    ]
}

async fn marked_session(catalog: Result<Vec<String>, ()>) -> (Module, Arc<CatalogPlane>) {
    let plane = Arc::new(CatalogPlane {
        catalog: Mutex::new(catalog),
        kept: Mutex::new(None),
        reads: AtomicUsize::new(0),
        setups: AtomicUsize::new(0),
    });
    let m = module_with(|gate| {
        let mut e = executor(vec![text_step("answer", "done")], Some(gate));
        e.planes.compaction = Some(plane.clone());
        Arc::new(e)
    })
    .await;
    let manifest = broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(
            json!({"compaction_item":{"provider":"magic","preset":"default"}}).to_string(),
        )
        .unwrap(),
        plan_identity: "plan".into(),
        composition_digest: "composition".into(),
        tools: Arc::new(vec![]),
        system_text: "instructions".into(),
        system_digest: "system".into(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    };
    let mut req = request("agent words");
    req.tools = None;
    req.policy_allowed = Default::default();
    req.context_limit = Some(200_000);
    let owner = super::super::differential::core();
    req.principal = Some(owner.clone());
    req.scope = super::super::differential::head_scope(owner);
    req.message_marks = json!({"speaks_for":{"agent_id":"agent-a","name":"Ada"}})
        .as_object()
        .unwrap()
        .clone();
    active(
        m.registry
            .route_submission_with_order(
                &m.key,
                RunId("r1".into()),
                m.caller.shape("r1", &SendKey::Minted, req),
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                Some(crate::actor::SendPlan {
                    identity: "plan".into(),
                    manifest: Some(Arc::new(manifest)),
                    model_check: None,
                }),
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .unwrap(),
    );
    m.gate.notify_one();
    m.ended("r1", "completed").await;
    assert!(
        plane.setups.load(Ordering::SeqCst) > 0,
        "the session has sent Setup"
    );
    (m, plane)
}

async fn entries(m: &Module) -> Result<Value, Value> {
    m.registry
        .route_compaction_v2(
            &m.key,
            Some("magic".into()),
            "session.entries".into(),
            serde_json::value::to_raw_value(&json!({"select":{"by":"after"},"detail":"full"}))
                .unwrap(),
        )
        .await
        .map(|answer| serde_json::from_str(answer.get()).unwrap())
        .map_err(|refusal| serde_json::to_value(refusal).unwrap())
}

#[tokio::test]
async fn unknown_setup_refuses_entries_retryably_until_a_catalog_read_succeeds() {
    let (m, plane) = marked_session(Err(())).await;
    assert!(
        m.registry.open_handle(&m.key).is_some(),
        "exercise the actor's Setup knowledge"
    );
    for _ in 0..2 {
        let before = plane.reads.load(Ordering::SeqCst);
        let refusal = entries(&m).await.unwrap_err();
        assert_eq!(refusal["code"], "projection_not_ready", "{refusal}");
        assert_eq!(refusal["detail"]["class"], "transient", "{refusal}");
        assert!(refusal.get("entries").is_none(), "{refusal}");
        assert_eq!(
            plane.reads.load(Ordering::SeqCst),
            before + 1,
            "unknown is not learned as false"
        );
    }
    *plane.catalog.lock().unwrap() = Ok(provides());
    let page = entries(&m).await.unwrap();
    assert_eq!(
        page["entries"][0]["speaks_for"],
        json!({"agent_id":"agent-a","name":"Ada"})
    );
    let before = plane.reads.load(Ordering::SeqCst);
    assert_eq!(entries(&m).await.unwrap(), page);
    assert_eq!(
        plane.reads.load(Ordering::SeqCst),
        before,
        "a definite answer is learned"
    );
    m.close().await;
}

#[tokio::test]
async fn entries_keep_speakers_during_a_failed_catalog_read_after_the_actor_closes() {
    let (m, plane) = marked_session(Ok(provides())).await;
    let page = entries(&m).await.unwrap();
    assert_eq!(
        page["entries"][0]["speaks_for"],
        json!({"agent_id":"agent-a","name":"Ada"})
    );
    m.close().await;
    *plane.catalog.lock().unwrap() = Err(());
    let before = plane.reads.load(Ordering::SeqCst);
    let kept = entries(&m).await.unwrap();
    assert_eq!(kept["entries"], page["entries"]);
    assert_eq!(
        plane.reads.load(Ordering::SeqCst),
        before + 1,
        "the actorless read tried the catalog"
    );
    assert!(m.registry.open_handle(&m.key).is_none());
}
