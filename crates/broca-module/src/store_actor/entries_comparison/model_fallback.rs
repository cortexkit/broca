//! Reply facts exercised through a real store actor, not inferred from reply text or usage.

use super::*;
use crate::serve::{ServeHandler, SessionReadRequest};
use broca_protocol::request::ModelHandle;

async fn session(steps: Vec<Vec<NormalizedEvent>>) -> Side {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "model-fallback-{}-{}",
        std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let provider = broca_core::provider::StubProvider::new(steps).with_refusal_fallback(
        ModelHandle {
            provider_module_id: "stub".into(),
            model_id: "claude-opus-4-8".into(),
        },
        None,
    );
    let executor: Arc<dyn RunExecutor> = Arc::new(Loops {
        today: Gated {
            inner: provider.clone(),
            gate: None,
            render_config: None,
            notes_sends: false,
            hold: None,
        },
        wal: wal.clone(),
        store: provider.clone(),
        compaction: None,
        hooks: None,
    });
    let idle = Duration::from_secs(600);
    let runtime = crate::store_open::StoreRuntime::open(
        &dir,
        broca_session_store::keys::DeploymentKey::new(1, &[93; 32]),
        executor.clone(),
        idle,
    )
    .await
    .unwrap();
    let opener = crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(dir.join("leases"))),
        wal.clone(),
        executor,
    )
    .with_idle_ttl(idle)
    .with_store_runtime(Arc::new(runtime));
    Side {
        registry: LineageRegistry::new_with_reader(Arc::new(opener), wal, None)
            .with_store_new_sessions(),
        key: BindIdentity::new(dir.join("project"), "model-fallback", "s1"),
        dir,
        gate: None,
        provider,
        caller: Default::default(),
    }
}

fn refused() -> Vec<NormalizedEvent> {
    let mut step = text_step("refused", "I cannot answer.");
    if let NormalizedEvent::Control(ProviderControl::Finished { finish_reason, .. }) =
        step.last_mut().unwrap()
    {
        *finish_reason = FinishReason::ContentFilter;
    }
    step
}

async fn send(store: &Side, run: &str) {
    assert!(matches!(
        store.send_request(run, untooled("answer"), None).await,
        SendOutcome::Active { .. } | SendOutcome::Pending { .. }
    ));
    store.settled(run).await;
    store.assert_completed(run).await;
}

fn sample(name: &str, value: &impl serde::Serialize) {
    if let Ok(directory) = std::env::var("BROCA_MODEL_FALLBACK_SAMPLES") {
        let directory = std::path::Path::new(&directory);
        std::fs::create_dir_all(directory).unwrap();
        std::fs::write(
            directory.join(format!("{name}.json")),
            serde_json::to_vec_pretty(value).unwrap(),
        )
        .unwrap();
    }
}

#[tokio::test]
async fn refused_reply_exposes_recorded_model_fallback_on_read_and_history() {
    let store = session(vec![
        text_step("ordinary", "claude-opus-4-8 is only text here."),
        refused(),
        text_step("fallback", "The fallback answer."),
    ])
    .await;
    send(&store, "ordinary").await;
    send(&store, "fallback").await;
    let mut reads = Vec::new();
    for include_display_facts in [false, true] {
        let mut request = SessionReadRequest::new(store.key.clone(), Some(1), None);
        request.include_display_facts = include_display_facts;
        let response = store.registry.read(request).await.unwrap();
        sample(
            if include_display_facts {
                "session-read-with-display-facts"
            } else {
                "session-read"
            },
            &response,
        );
        reads.push(serde_json::to_value(response).unwrap());
    }
    let history = store
        .history(json!({"form": "display", "from_ordinal": 1}))
        .await
        .unwrap();
    sample("session-history", &history);
    let replies = |page: &Value| {
        page["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| entry["message"]["role"] == "assistant")
            .cloned()
            .collect::<Vec<_>>()
    };
    let plain = replies(&reads[0]);
    let facts = replies(&reads[1]);
    let rows: Vec<_> = history["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|entry| entry["row"]["role"] == "assistant")
        .collect();
    assert_eq!(plain.len(), 2, "the refused response is discarded");
    assert_eq!(facts.len(), 2);
    assert_eq!(rows.len(), 2);
    assert!(plain
        .iter()
        .all(|entry| entry.get("model_fallback").is_none()));
    assert!(facts[0].get("model_fallback").is_none());
    assert!(rows[0]["row"].get("model_fallback").is_none());
    let expected = json!({"requested": "stub/m", "answered": "stub/claude-opus-4-8"});
    assert_eq!(facts[1]["model_fallback"], expected);
    assert_eq!(rows[1]["row"]["model_fallback"], expected);
    assert_eq!(rows[1]["row"]["model"], "stub/claude-opus-4-8");
    let view = store
        .registry
        .store_runtime()
        .unwrap()
        .current_view(&store.key)
        .await
        .unwrap()
        .unwrap();
    let mid = facts[1]["mid"].as_str().unwrap();
    let folded = view
        .reply_model_fallback(mid)
        .expect("the open log rebuilds the reply fact");
    assert_eq!(folded.requested, "stub/m");
    assert_eq!(folded.answered, "stub/claude-opus-4-8");
    for (plain, facts) in plain.iter().zip(&facts) {
        assert_eq!(
            plain["message"], facts["message"],
            "facts never alter the body"
        );
        let checked: cortexkit_role_llm_runner::read::ReadMessage =
            serde_json::from_value(facts.clone()).unwrap();
        checked.check_model_fallback("assistant").unwrap();
    }
}

#[tokio::test]
async fn requested_model_probe_reply_has_no_fallback_fact() {
    let mut steps = vec![refused()];
    steps.extend((0..10).map(|_| text_step("fallback", "fallback answer")));
    steps.push(text_step("probe", "requested model answer"));
    let store = session(steps).await;
    for run in 0..11 {
        send(&store, &format!("r{run}")).await;
    }
    let mut request = SessionReadRequest::new(store.key.clone(), None, Some(1));
    request.include_display_facts = true;
    let response = serde_json::to_value(store.registry.read(request).await.unwrap()).unwrap();
    let reply = &response["messages"][0];
    assert_eq!(reply["message"]["origin"]["model_id"], "m");
    assert!(reply.get("model_fallback").is_none());
    let history = store
        .history(json!({"form": "display", "limit": 1}))
        .await
        .unwrap();
    assert_eq!(history["messages"][0]["row"]["model"], "stub/m");
    assert!(history["messages"][0]["row"]
        .get("model_fallback")
        .is_none());
}
