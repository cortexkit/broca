use super::*;
use crate::system_one::{
    BatchCatalog, ClassifierProvider, Config, ModelPin, ProviderReply, SystemOne,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, Semaphore};

struct Catalog;

impl BatchCatalog for Catalog {
    fn row(&self, _: &str) -> Option<broca_catalog::ModelBatchRow> {
        Some(broca_catalog::ModelBatchRow {
            kind: Some("decision".into()),
            context_tokens: None,
            attachment: false,
            input_rate_nanos: Some(42_000_000),
            output_rate_nanos: Some(0),
            model_name: None,
            provider_name: None,
        })
    }
}

struct Provider {
    calls: AtomicUsize,
    entered: mpsc::UnboundedSender<()>,
    release: Semaphore,
}

#[async_trait::async_trait]
impl ClassifierProvider for Provider {
    fn egress_host(&self) -> String {
        "localhost".into()
    }

    fn credential_id(&self) -> Option<String> {
        Some("apikey:typesafe".into())
    }

    async fn call(&self, _: String) -> ProviderReply {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.send(()).unwrap();
        let _permit = self.release.acquire().await.unwrap();
        ProviderReply::Answered(br#"{"model":"jev-1.13.0","answers":{"q":{"type":"noul","noul":0.5}},"usage":{"input_tokens":100}}"#.to_vec())
    }
}

struct Rig {
    tools: Arc<LlmTools>,
    provider: Arc<Provider>,
    store: Arc<broca_store::SystemOneStore>,
    export: Arc<broca_store::SqliteStore>,
    provider_entered: mpsc::UnboundedReceiver<()>,
    publish_entered: mpsc::UnboundedReceiver<()>,
    publish_release: std::sync::mpsc::Sender<()>,
}

impl Rig {
    fn new() -> Self {
        let (entered, provider_entered) = mpsc::unbounded_channel();
        let provider = Arc::new(Provider {
            calls: AtomicUsize::new(0),
            entered,
            release: Semaphore::new(0),
        });
        let store = Arc::new(broca_store::SystemOneStore::open_in_memory().unwrap());
        let export = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
        let system = SystemOne::new(
            store.clone(),
            "spend-ordering",
            Arc::new(Catalog),
            vec![ModelPin::jev(provider.clone())],
            Config::default(),
            Arc::default(),
        )
        .with_store_role(crate::system_one::LLM_CLASSIFY_STORE_ROLE)
        .with_export(
            export.clone(),
            Arc::new(crate::ChargeBasisTable::shipped_defaults()),
        );
        let (entered, publish_entered) = mpsc::unbounded_channel();
        let (publish_release, released) = std::sync::mpsc::channel();
        let released = Mutex::new(released);
        *system.fact_publish_hook.lock().unwrap() = Some(Arc::new(move || {
            entered.send(()).unwrap();
            // Dropping publish_release lets recv return if the test fails before opening the gate.
            let _ = released.lock().unwrap().recv();
        }));
        let tools = Arc::new(LlmTools::new(
            Arc::new(system),
            crate::config::LlmToolsConfig {
                advertise_tags: true,
                private_content_providers: vec!["typesafe".into()],
            },
            Arc::new(NoPrivateContent),
        ));
        Self {
            tools,
            provider,
            store,
            export,
            provider_entered,
            publish_entered,
            publish_release,
        }
    }

    fn invoke(&self) -> tokio::task::JoinHandle<(Answer, usize)> {
        let tools = self.tools.clone();
        let export = self.export.clone();
        tokio::spawn(async move {
            let answer = tools
                .answer(
                    &serde_json::to_vec(&json!({
                        "name": CLASSIFY_TOOL_NAME,
                        "call_key": "single-flight",
                        "arguments": {
                            "model": "typesafe/jev-1.13.0",
                            "item": {"state": "traffic"},
                            "questions": {"q": {"type": "noul", "instructions": "Is traffic urgent?"}},
                            "max_cost_usd": 1.0
                        }
                    }))
                    .unwrap(),
                    Some(&BindPrincipal::Reserved { module_id: "owner".into() }),
                    &SendScope::Stamped(broca_protocol::BindScope {
                        scope: broca_protocol::SessionScope {
                            owner: BindPrincipal::Reserved { module_id: "owner".into() },
                            scope_ref: "run".into(),
                            scope_epoch: 1,
                            flow_id: None,
                            sender: None,
                            agent_id: Some("agent".into()),
                            run_id: Some("run".into()),
                        },
                        owner_authorized: true,
                        kind: broca_protocol::ScopeKind::Head,
                        parent: None,
                        parent_state: None,
                    }),
                )
                .await;
            let facts_at_answer = export.read_export_page(None, 100).unwrap().facts.len();
            (answer, facts_at_answer)
        })
    }

    fn flight(&self) -> (String, watch::Receiver<Option<Answer>>) {
        let flights = self.tools.flights.lock().unwrap();
        let (batch, flight) = flights.iter().next().unwrap();
        (batch.clone(), flight.result.clone())
    }

    fn facts(&self) -> usize {
        self.export.read_export_page(None, 100).unwrap().facts.len()
    }
}

#[tokio::test]
async fn stored_replay_waits_for_spend_publication_after_leader_disconnects() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut rig = Rig::new();
        let leader = rig.invoke();
        rig.provider_entered.recv().await.unwrap();
        let (batch, flight) = rig.flight();
        rig.provider.release.add_permits(1);
        rig.publish_entered.recv().await.unwrap();
        leader.abort();
        assert!(leader.await.unwrap_err().is_cancelled());

        let stored = rig.store.lookup(crate::system_one::LLM_CLASSIFY_STORE_ROLE, &batch).unwrap().unwrap();
        assert_eq!(stored.outcomes.len(), 1);
        assert_eq!(rig.store.pending_facts().unwrap().len(), 1);
        assert_eq!(rig.facts(), 0);

        // The follower sees the stored outcome while fact_publish_hook holds its export pending.
        let mut follower = rig.invoke();
        let premature = tokio::select! {
            answer = &mut follower => Some(answer.unwrap()),
            entered = rig.publish_entered.recv() => { entered.unwrap(); None },
        };
        if premature.is_some() {
            assert_eq!(rig.facts(), 0);
        }
        rig.publish_release.send(()).unwrap();
        rig.publish_release.send(()).unwrap();
        let shared = await_flight(flight).await.unwrap();
        let (answer, facts_at_answer) = match premature {
            Some(answer) => answer,
            None => follower.await.unwrap(),
        };
        let answer = answer.unwrap();
        assert_eq!(shared.get(), answer.get());
        assert_eq!(rig.facts(), 1);
        assert_eq!(rig.provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(facts_at_answer, 1, "stored replay released an answer before its spend fact; the detached publisher committed the fact only after its gate opened");
    })
    .await
    .expect("spend publication must not strand a replay waiter");
}

#[tokio::test]
async fn shared_execution_publishes_spend_before_waking_waiters() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut rig = Rig::new();
        let leader = rig.invoke();
        rig.provider_entered.recv().await.unwrap();
        let (_, flight) = rig.flight();
        leader.abort();
        assert!(leader.await.unwrap_err().is_cancelled());
        assert!(rig.store.pending_facts().unwrap().is_empty());
        rig.provider.release.add_permits(1);

        // Join the actual flight before a stored replay can bypass its result channel.
        let follower = await_flight(flight);
        tokio::pin!(follower);
        let answer = tokio::select! {
            answer = &mut follower => answer.unwrap(),
            entered = rig.publish_entered.recv() => {
                entered.unwrap();
                assert_eq!(rig.facts(), 0);
                rig.publish_release.send(()).unwrap();
                follower.await.unwrap()
            },
        };
        assert_eq!(
            rig.facts(),
            1,
            "shared answer must have a published spend fact"
        );
        let (replay, facts_at_answer) = rig.invoke().await.unwrap();
        assert_eq!(replay.unwrap().get(), answer.get());
        assert_eq!(facts_at_answer, 1);
        assert_eq!(rig.facts(), 1);
        assert_eq!(rig.provider.calls.load(Ordering::SeqCst), 1);
    })
    .await
    .expect("leader cancellation must not strand the shared execution");
}
