use super::*;

use std::sync::atomic::{AtomicUsize, Ordering};

/// Answers every catalog request with `text` and records each request it was sent, and how
/// often admission asked whether it reads system-text facts.
struct FactsProvider {
    reads_facts: bool,
    asked: AtomicUsize,
    requests: std::sync::Mutex<Vec<Value>>,
    text: &'static str,
}

impl FactsProvider {
    fn new(reads_facts: bool) -> Self {
        Self {
            reads_facts,
            asked: AtomicUsize::new(0),
            requests: Default::default(),
            text: "text rendered from the plan's facts",
        }
    }
}

#[async_trait]
impl Fetcher for FactsProvider {
    async fn system_text_facts_supported(&self, _: &str) -> Result<bool, FetchError> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        Ok(self.reads_facts)
    }
    async fn catalog(&self, _: &str, request: &CatalogRequest) -> Result<Value, FetchError> {
        self.requests
            .lock()
            .unwrap()
            .push(serde_json::to_value(request).unwrap());
        let digest = request
            .composition
            .as_ref()
            .map(|composition| {
                cortexkit_role_tool_provider::catalog::composition_digest(&Value::Object(
                    composition.clone(),
                ))
                .unwrap()
            })
            .unwrap();
        Ok(json!({
            "generation": "1", "catalog_digest": "catalog", "composition_digest": digest,
            "tools": [],
            "system_text": {
                "text": self.text,
                "item_digest": cortexkit_role_tool_provider::catalog::system_text_digest(self.text),
                "preflight_digest": "preflight", "composition_digest": digest, "tool_names": []
            }
        }))
    }
    async fn declare(&self, _: &str, _: &DeclareRequest) -> Result<Value, FetchError> {
        unreachable!()
    }
    async fn describe_compaction(&self, _: &str) -> Result<DescribeAnswer, FetchError> {
        unreachable!()
    }
}

/// A composer's canonical facts: members and lists in UTF-8 byte order.
fn facts() -> Value {
    json!({
        "harness": {"facts": ["files", "shell"], "id": "host"},
        "idle_start": ["sender.a", "sender.b"],
        "model": {"id": "model", "provider": "vendor"},
        "tools": ["read", "write", "é"]
    })
}

/// A plan whose one system-text item is `persona-text`'s, carrying `facts` when given.
fn plan_with(facts: Option<Value>) -> Value {
    let composition =
        json!({"providers":[{"provider":"persona-text","tools":[]}],"step_transform_items":[]});
    let mut item = json!({"provider": "persona-text", "preset": "default", "params": {}});
    if let Some(facts) = facts {
        item["facts"] = facts;
    }
    json!({"tool_items": [], "system_text_items": [item], "step_transform_items": [],
        "composition_digest": cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
        "composition": composition})
}

fn session() -> broca_protocol::BindIdentity {
    broca_protocol::BindIdentity::new("/synthetic/plan-facts", "runner", "plan-facts")
}

#[tokio::test]
async fn facts_reach_a_provider_that_reads_them_byte_for_byte_and_no_other() {
    let plan = crate::parse_plan(
        &plan_with(Some(facts())),
        crate::CompactionSupport::Unsupported,
    )
    .unwrap();
    for reads_facts in [true, false] {
        let provider = FactsProvider::new(reads_facts);
        let admitted = admit(&plan, &provider, &session(), Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(admitted.manifest.system_text, provider.text);
        assert_eq!(provider.asked.load(Ordering::SeqCst), 1);
        let requests = provider.requests.lock().unwrap();
        assert!(!requests.is_empty());
        for request in requests.iter() {
            match request.get("facts") {
                Some(sent) if reads_facts => assert_eq!(
                    serde_json::to_vec(sent).unwrap(),
                    serde_json::to_vec(&facts()).unwrap()
                ),
                None if !reads_facts => {}
                other => panic!("reads facts: {reads_facts}; request facts: {other:?}"),
            }
            // The facts go beside the system-text item, never inside it or its params.
            assert!(request["system_text"].get("facts").is_none());
            assert!(request["params"].get("facts").is_none());
        }
    }
}

#[tokio::test]
async fn a_plan_without_facts_never_asks_whether_its_provider_reads_them() {
    let plan = crate::parse_plan(&plan_with(None), crate::CompactionSupport::Unsupported).unwrap();
    let provider = FactsProvider::new(true);
    admit(&plan, &provider, &session(), Duration::from_secs(1))
        .await
        .unwrap();
    assert_eq!(provider.asked.load(Ordering::SeqCst), 0);
    assert!(provider
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|request| request.get("facts").is_none()));
}

#[test]
fn malformed_facts_are_refused_by_their_field_before_any_fetch() {
    // Admission parses the plan before it fetches anything, so a refusal here means nothing
    // was fetched or written.
    let mut unsorted = facts();
    unsorted["tools"] = json!(["z", "a"]);
    let mut unknown = facts();
    unknown["effort"] = json!("high");
    for bad in [unsorted, unknown, json!("facts")] {
        let refusal =
            crate::parse_plan(&plan_with(Some(bad)), crate::CompactionSupport::Unsupported)
                .unwrap_err();
        assert_eq!(
            refusal.code(),
            cortexkit_role_llm_runner::errors::INVALID_PARAMS
        );
        assert_eq!(
            refusal.detail(),
            json!({"field": "plan.system_text_items[0].facts"})
        );
    }
}
