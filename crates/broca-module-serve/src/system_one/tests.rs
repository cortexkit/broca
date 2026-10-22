//! System One's `classify.run` engine against an in-process stand-in provider. The provider
//! side is gated by held futures (a semaphore the test releases), never by sleeps.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_catalog::ModelBatchRow;
use broca_store::SystemOneStore;
use serde_json::{json, Value};
use tokio::sync::{Notify, Semaphore};

use super::*;

const JEV: &str = JEV_1_13_0;

/// What the stand-in answers for one item, by its state.
#[derive(Clone)]
enum Script {
    Answer(String),
    Status(u16, Option<u64>),
    /// Wait for a permit on the semaphore, then answer.
    Held(Arc<Semaphore>, String),
    Panic,
}

#[derive(Default)]
struct StandIn {
    scripts: Mutex<HashMap<String, Vec<Script>>>,
    calls: Mutex<HashMap<String, usize>>,
    bodies: Mutex<Vec<String>>,
    held: AtomicUsize,
    entered: Notify,
}

fn key(state: &Value) -> String {
    String::from_utf8(serde_jcs::to_vec(state).unwrap()).unwrap()
}

impl StandIn {
    /// Script the answers for `state`, one per call; the last repeats.
    fn script(&self, state: &str, scripts: Vec<Script>) {
        self.scripts
            .lock()
            .unwrap()
            .insert(key(&json!(state)), scripts);
    }

    fn calls(&self, state: &str) -> usize {
        self.calls
            .lock()
            .unwrap()
            .get(&key(&json!(state)))
            .copied()
            .unwrap_or(0)
    }
}

#[async_trait]
impl ClassifierProvider for StandIn {
    fn egress_host(&self) -> String {
        "stand-in.invalid".into()
    }

    fn credential_id(&self) -> Option<String> {
        Some("apikey:typesafe".into())
    }

    async fn call(&self, body: String) -> ProviderReply {
        self.bodies.lock().unwrap().push(body.clone());
        let request: Value = serde_json::from_str(&body).unwrap();
        let key = key(&request["state"]);
        let n = {
            let mut calls = self.calls.lock().unwrap();
            let n = calls.entry(key.clone()).or_default();
            *n += 1;
            *n
        };
        let script = {
            let scripts = self.scripts.lock().unwrap();
            scripts
                .get(&key)
                .map(|list| list[(n - 1).min(list.len() - 1)].clone())
        };
        match script {
            None => ProviderReply::Status {
                status: 500,
                retry_after_ms: None,
            },
            Some(Script::Answer(body)) => ProviderReply::Answered(body.into_bytes()),
            Some(Script::Status(status, retry_after_ms)) => ProviderReply::Status {
                status,
                retry_after_ms,
            },
            Some(Script::Held(gate, body)) => {
                self.held.fetch_add(1, Ordering::SeqCst);
                self.entered.notify_waiters();
                let _permit = gate.acquire().await.unwrap();
                self.held.fetch_sub(1, Ordering::SeqCst);
                ProviderReply::Answered(body.into_bytes())
            }
            Some(Script::Panic) => panic!("the stand-in provider panicked on purpose"),
        }
    }
}

struct Rows(HashMap<String, ModelBatchRow>);

impl BatchCatalog for Rows {
    fn row(&self, catalog_id: &str) -> Option<ModelBatchRow> {
        self.0.get(catalog_id).cloned()
    }
}

fn row(kind: Option<&str>) -> ModelBatchRow {
    ModelBatchRow {
        kind: kind.map(str::to_owned),
        context_tokens: None,
        attachment: false,
        input_rate_nanos: Some(42_000_000),
        output_rate_nanos: Some(0),
    }
}

fn catalog() -> Arc<Rows> {
    Arc::new(Rows(HashMap::from([
        (JEV.to_owned(), row(Some("decision"))),
        ("openai/gpt-chat".to_owned(), row(Some("chat"))),
        ("openai/kindless".to_owned(), row(None)),
        (
            "cloudflare-workers-ai/@cf/cloudflare/clef".to_owned(),
            row(Some("decision")),
        ),
    ])))
}

struct Rig {
    service: Arc<SystemOne>,
    stand_in: Arc<StandIn>,
    gauge: Arc<AtomicUsize>,
    store: Arc<SystemOneStore>,
}

fn rig_with(store: Arc<SystemOneStore>, config: Config) -> Rig {
    rig_owned(store, config, "this-process")
}

/// A rig whose service holds batches as `owner`, so two rigs on one store play two processes.
fn rig_owned(store: Arc<SystemOneStore>, config: Config, owner: &str) -> Rig {
    let stand_in = Arc::new(StandIn::default());
    let gauge = Arc::new(AtomicUsize::new(0));
    let service = Arc::new(SystemOne::new(
        Arc::clone(&store),
        owner,
        catalog(),
        vec![ModelPin::jev(stand_in.clone())],
        config,
        Arc::clone(&gauge),
    ));
    Rig {
        service,
        stand_in,
        gauge,
        store,
    }
}

fn rig() -> Rig {
    rig_with(
        Arc::new(SystemOneStore::open_in_memory().unwrap()),
        Config::default(),
    )
}

fn noul(value: &str, usage: Option<&str>) -> String {
    let usage = usage.map_or(String::new(), |usage| format!(",\"usage\":{usage}"));
    format!(r#"{{"model":"jev-1.13.0","answers":{{"q":{{"type":"noul","noul":{value}}}}}{usage}}}"#)
}

fn params(batch: &str, model: &str, states: &[&str], max_cost_usd: Option<f64>) -> String {
    let mut params = json!({
        "batch_id": batch,
        "model": model,
        "questions": {"q": {"type": "noul", "instructions": "Is it flagged?"}},
        "items": states.iter().map(|state| json!({"state": state})).collect::<Vec<_>>(),
    });
    if let Some(max) = max_cost_usd {
        params["max_cost_usd"] = json!(max);
    }
    params.to_string()
}

/// The reply text of an admitted call.
async fn admitted(rig: &Rig, params: String) -> String {
    match Arc::clone(&rig.service).spawn_call(params).await {
        Ok(reply) => reply.get().to_owned(),
        Err(body) => panic!("refused: {}", String::from_utf8_lossy(&body)),
    }
}

async fn refused(rig: &Rig, params: String) -> Value {
    match Arc::clone(&rig.service).spawn_call(params).await {
        Ok(reply) => panic!("admitted: {}", reply.get()),
        Err(body) => serde_json::from_slice(&body).unwrap(),
    }
}

fn items(reply: &str) -> Vec<Value> {
    serde_json::from_str::<Value>(reply).unwrap()["items"]
        .as_array()
        .unwrap()
        .clone()
}

/// The exact text of item `i`'s answers in a reply.
fn answers_text(reply: &str, i: usize) -> String {
    #[derive(serde::Deserialize)]
    struct Raw<'a> {
        #[serde(borrow)]
        items: Vec<RawItem<'a>>,
    }
    #[derive(serde::Deserialize)]
    struct RawItem<'a> {
        #[serde(borrow, default)]
        answers: Option<&'a RawValue>,
    }
    let raw: Raw<'_> = serde_json::from_str(reply).unwrap();
    raw.items[i].answers.expect("answered").get().to_owned()
}

#[tokio::test]
async fn answers_keep_the_providers_number_spelling_and_request_order() {
    let rig = rig();
    let spelled = r#"{"q":{"type":"noul","noul":0.1234567890123456789},"x":{"type":"score","score":1.50,"probabilities":{"0":5E-1}}}"#;
    rig.stand_in.script(
        "a",
        vec![Script::Answer(format!(
            r#"{{"model":"jev-1.13.0","answers":{spelled}}}"#
        ))],
    );
    rig.stand_in
        .script("b", vec![Script::Answer(noul("0.20", None))]);
    let reply = admitted(&rig, params("order", JEV, &["a", "b"], None)).await;
    assert_eq!(answers_text(&reply, 0), spelled);
    assert_eq!(
        answers_text(&reply, 1),
        r#"{"q":{"type":"noul","noul":0.20}}"#
    );
    let items = items(&reply);
    assert_eq!(
        (items[0]["index"].clone(), items[1]["index"].clone()),
        (json!(0), json!(1))
    );
    let replay = admitted(&rig, params("order", JEV, &["a", "b"], None)).await;
    assert_eq!(replay, reply, "a replay is the same reply byte for byte");
    assert_eq!(rig.stand_in.calls("a"), 1);
    let sent: Value = serde_json::from_str(&rig.stand_in.bodies.lock().unwrap()[0]).unwrap();
    assert_eq!(
        sent["model"], "jev-1.13.0",
        "the pinned row sends TypeSafe's versioned id, never the moving jev-latest alias"
    );
}

/// A reply naming another model is still answered and stored; only the difference is
/// reported, so the row's pin, not the provider's alias resolution, decides what is billed.
#[tokio::test]
async fn a_reply_from_another_model_is_answered_and_reported() {
    use super::classify::answered_by_another_model;
    assert_eq!(
        answered_by_another_model("jev-1.13.0", Some("jev-1.14.0")),
        Some("jev-1.14.0")
    );
    assert_eq!(
        answered_by_another_model("jev-1.13.0", Some("jev-1.13.0")),
        None
    );
    assert_eq!(answered_by_another_model("jev-1.13.0", None), None);
    let rig = rig();
    rig.stand_in.script(
        "a",
        vec![Script::Answer(
            r#"{"model":"jev-1.14.0","answers":{"q":{"type":"noul","noul":0.3}}}"#.to_owned(),
        )],
    );
    let reply = admitted(&rig, params("other-model", JEV, &["a"], None)).await;
    assert_eq!(
        answers_text(&reply, 0),
        r#"{"q":{"type":"noul","noul":0.3}}"#
    );
}

/// The crash test: a call is cut off after one outcome is durable (its task is dropped while
/// the provider holds the second item). The re-send asks the provider only for the items
/// with no stored outcome, and serves the stored one byte for byte.
#[tokio::test]
async fn a_resend_after_an_interrupted_call_asks_only_unanswered_items() {
    let rig = rig();
    let gate = Arc::new(Semaphore::new(0));
    rig.stand_in.script(
        "a",
        vec![Script::Answer(noul(
            "0.1000",
            Some(r#"{"input_tokens":5}"#),
        ))],
    );
    rig.stand_in.script(
        "b",
        vec![
            Script::Held(Arc::clone(&gate), noul("0.2", None)),
            Script::Answer(noul("0.2", None)),
        ],
    );
    rig.stand_in
        .script("c", vec![Script::Answer(noul("0.3", None))]);
    let body = params("crash", JEV, &["a", "b", "c"], None);
    let service = Arc::clone(&rig.service);
    let call = {
        let body = body.clone();
        tokio::spawn(async move { classify::run(&service, &body).await })
    };
    while rig.stand_in.held.load(Ordering::SeqCst) == 0 {
        rig.stand_in.entered.notified().await;
    }
    let stored = rig.store.outcomes(STORE_ROLE, "crash").unwrap();
    assert_eq!(stored.len(), 1, "item 0 is durable before item 1 is sent");
    call.abort();
    let _ = call.await;
    // The dropped call's hold is released on its own; wait for the blocking release.
    for _ in 0..1000 {
        if !rig.service.claims.lock().unwrap().contains("crash") {
            break;
        }
        tokio::task::yield_now().await;
    }
    let mut reply = None;
    for _ in 0..1000 {
        match Arc::clone(&rig.service).spawn_call(body.clone()).await {
            Ok(text) => {
                reply = Some(text.get().to_owned());
                break;
            }
            Err(_) => tokio::task::yield_now().await,
        }
    }
    let reply = reply.expect("the batch was released");
    assert_eq!(
        answers_text(&reply, 0),
        r#"{"q":{"type":"noul","noul":0.1000}}"#
    );
    assert_eq!(
        rig.stand_in.calls("a"),
        1,
        "the stored answer is never re-asked"
    );
    assert_eq!(
        rig.stand_in.calls("b"),
        2,
        "the interrupted item is asked again"
    );
    assert_eq!(
        rig.stand_in.calls("c"),
        1,
        "the never-sent item is asked once"
    );
}

/// The process-kill form of the same: a process died holding the batch (its row is still
/// `running`). Startup releases it, and the re-send asks only the unanswered item.
#[tokio::test]
async fn a_batch_a_dead_process_held_is_released_at_startup_and_resumed() {
    let path = broca_test_support::scratch_root()
        .join(format!("system-one-dead-holder-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let first = rig_with(
        Arc::new(SystemOneStore::open(&path).unwrap()),
        Config::default(),
    );
    first
        .stand_in
        .script("a", vec![Script::Answer(noul("0.5", None))]);
    let gate = Arc::new(Semaphore::new(0));
    first
        .stand_in
        .script("b", vec![Script::Held(gate, noul("0.6", None))]);
    let service = Arc::clone(&first.service);
    let body = params("dead", JEV, &["a", "b"], None);
    let call = {
        let body = body.clone();
        tokio::spawn(async move { classify::run(&service, &body).await })
    };
    while first.stand_in.held.load(Ordering::SeqCst) == 0 {
        first.stand_in.entered.notified().await;
    }
    // A killed process runs no drop: forget the call's future so its hold is never released.
    std::mem::forget(call);

    let second = rig_owned(
        Arc::new(SystemOneStore::open(&path).unwrap()),
        Config::default(),
        "next-process",
    );
    second
        .stand_in
        .script("b", vec![Script::Answer(noul("0.6", None))]);
    let blocked = refused(&second, body.clone()).await;
    assert_eq!(blocked["code"], "batch_in_progress");
    assert_eq!(second.service.release_abandoned().unwrap(), 1);
    let reply = admitted(&second, body).await;
    assert_eq!(
        answers_text(&reply, 0),
        r#"{"q":{"type":"noul","noul":0.5}}"#
    );
    assert_eq!(
        second.stand_in.calls("a"),
        0,
        "the stored answer is served from the store"
    );
    assert_eq!(second.stand_in.calls("b"), 1);
}

#[tokio::test]
async fn a_concurrent_call_for_a_held_batch_is_refused_at_once() {
    let rig = rig();
    let gate = Arc::new(Semaphore::new(0));
    rig.stand_in.script(
        "a",
        vec![Script::Held(Arc::clone(&gate), noul("0.4", None))],
    );
    let body = params("flight", JEV, &["a"], None);
    let first = tokio::spawn(Arc::clone(&rig.service).spawn_call(body.clone()));
    while rig.stand_in.held.load(Ordering::SeqCst) == 0 {
        rig.stand_in.entered.notified().await;
    }
    assert_eq!(
        rig.gauge.load(Ordering::SeqCst),
        1,
        "the running call is counted"
    );
    let second = refused(&rig, body.clone()).await;
    assert_eq!(second["code"], "batch_in_progress");
    assert_eq!(second["detail"]["class"], "transient");
    assert!(second["detail"]["retry_after_ms"].is_u64());
    gate.add_permits(1);
    first.await.unwrap().unwrap();
    assert_eq!(rig.gauge.load(Ordering::SeqCst), 0);
    admitted(&rig, body).await;
    assert_eq!(rig.stand_in.calls("a"), 1);
}

#[tokio::test]
async fn a_different_body_under_a_recorded_id_is_refused_naming_the_field() {
    let rig = rig();
    for state in ["a", "b", "c"] {
        rig.stand_in
            .script(state, vec![Script::Answer(noul("0.1", None))]);
    }
    admitted(&rig, params("reuse", JEV, &["a", "b"], None)).await;
    let changed = refused(&rig, params("reuse", JEV, &["a", "c"], None)).await;
    assert_eq!(changed["code"], "batch_id_reuse");
    assert_eq!(changed["detail"]["field"], "items[1]");
    let added = refused(&rig, params("reuse", JEV, &["a", "b", "c"], None)).await;
    assert_eq!(added["detail"]["field"], "items");
    assert_eq!(rig.stand_in.calls("c"), 0);
    // A different ceiling is not a different body.
    admitted(&rig, params("reuse", JEV, &["a", "b"], Some(5.0))).await;
}

/// Usage that costs `usd` at Jev's 0.042 USD per million input tokens.
fn costing(usd: f64) -> String {
    format!(
        r#"{{"input_tokens":{}}}"#,
        (usd * 1e6 / 0.042).ceil() as u64 + 1
    )
}

#[tokio::test]
async fn a_looser_resend_cannot_raise_the_ceiling() {
    let rig = rig();
    rig.stand_in.script(
        "t",
        vec![Script::Status(503, None), Script::Answer(noul("0.7", None))],
    );
    rig.stand_in
        .script("s", vec![Script::Answer(noul("0.7", Some(&costing(2.0))))]);
    let first = admitted(&rig, params("loose", JEV, &["t", "s"], Some(1.0))).await;
    assert_eq!(items(&first)[0]["error"]["code"], "provider_error");
    let resend = admitted(&rig, params("loose", JEV, &["t", "s"], Some(1000.0))).await;
    assert_eq!(items(&resend)[0]["error"]["code"], "cost_exceeded");
    assert_eq!(
        rig.stand_in.calls("t"),
        1,
        "an item over the ceiling is not sent"
    );
}

#[tokio::test]
async fn a_tighter_resend_is_recorded_before_anything_is_sent() {
    let rig = rig();
    rig.stand_in.script(
        "a",
        vec![
            Script::Status(503, None),
            Script::Status(503, None),
            Script::Answer(noul("0.7", Some(&costing(2.0)))),
        ],
    );
    rig.stand_in.script(
        "x",
        vec![
            Script::Status(503, None),
            Script::Status(503, None),
            Script::Answer(noul("0.7", None)),
        ],
    );
    admitted(&rig, params("tight", JEV, &["a", "x"], Some(1000.0))).await;
    admitted(&rig, params("tight", JEV, &["a", "x"], Some(1.0))).await;
    assert_eq!(
        rig.store.ledger(STORE_ROLE, "tight").unwrap().unwrap().0,
        Some(1_000_000_000)
    );
    let third = admitted(&rig, params("tight", JEV, &["a", "x"], Some(1000.0))).await;
    let third = items(&third);
    assert!(third[0]["answers"].is_object());
    assert_eq!(third[1]["error"]["code"], "cost_exceeded");
    assert_eq!(rig.stand_in.calls("x"), 2);
}

#[tokio::test]
async fn stored_permanent_errors_are_served_and_transient_ones_retried() {
    let rig = rig();
    rig.stand_in.script(
        "p",
        vec![Script::Status(422, None), Script::Answer(noul("0.1", None))],
    );
    rig.stand_in.script(
        "t",
        vec![Script::Status(503, None), Script::Answer(noul("0.2", None))],
    );
    let first = admitted(&rig, params("stored", JEV, &["p", "t"], None)).await;
    let first = items(&first);
    assert_eq!(first[0]["error"]["code"], "invalid_item");
    assert_eq!(first[0]["error"]["class"], "permanent");
    assert_eq!(first[1]["error"]["code"], "provider_error");
    let again = admitted(&rig, params("stored", JEV, &["p", "t"], None)).await;
    let again = items(&again);
    assert_eq!(again[0]["error"], first[0]["error"]);
    assert!(again[1]["answers"].is_object());
    assert_eq!((rig.stand_in.calls("p"), rig.stand_in.calls("t")), (1, 2));
}

#[tokio::test]
async fn an_auth_failure_stops_the_call_and_is_never_stored() {
    let rig = rig();
    rig.stand_in
        .script("a", vec![Script::Answer(noul("0.1", None))]);
    rig.stand_in.script(
        "b",
        vec![Script::Status(401, None), Script::Answer(noul("0.2", None))],
    );
    rig.stand_in
        .script("c", vec![Script::Answer(noul("0.3", None))]);
    let first = admitted(&rig, params("auth", JEV, &["a", "b", "c"], None)).await;
    let first = items(&first);
    assert!(first[0]["answers"].is_object());
    assert_eq!(first[1]["error"]["code"], "auth_failed");
    assert_eq!(first[2]["error"]["code"], "auth_failed");
    assert_eq!(rig.stand_in.calls("c"), 0, "nothing is sent after the stop");
    let again = admitted(&rig, params("auth", JEV, &["a", "b", "c"], None)).await;
    assert!(items(&again).iter().all(|item| item["answers"].is_object()));
    assert_eq!(
        (
            rig.stand_in.calls("a"),
            rig.stand_in.calls("b"),
            rig.stand_in.calls("c")
        ),
        (1, 2, 1)
    );
}

#[tokio::test]
async fn an_auth_failure_on_the_first_provider_call_is_a_refusal_that_writes_nothing() {
    let rig = rig();
    rig.stand_in.script(
        "a",
        vec![Script::Status(401, None), Script::Answer(noul("0.1", None))],
    );
    rig.stand_in
        .script("b", vec![Script::Answer(noul("0.2", None))]);
    let refusal = refused(&rig, params("first", JEV, &["a", "b"], None)).await;
    assert_eq!(refusal["code"], "auth_failed");
    assert_eq!(refusal["detail"]["credential"], "apikey:typesafe");
    assert_eq!(rig.store.ledger(STORE_ROLE, "first").unwrap(), None);
    assert_eq!(rig.stand_in.calls("b"), 0);
    // Nothing was recorded, so any body may use the id now.
    rig.stand_in
        .script("z", vec![Script::Answer(noul("0.3", None))]);
    admitted(&rig, params("first", JEV, &["z"], None)).await;
}

#[tokio::test]
async fn a_rate_limit_is_stored_for_its_item_and_stops_the_rest_unstored() {
    let rig = rig();
    rig.stand_in
        .script("a", vec![Script::Answer(noul("0.1", None))]);
    rig.stand_in.script(
        "b",
        vec![
            Script::Status(429, Some(60_000)),
            Script::Answer(noul("0.2", None)),
        ],
    );
    rig.stand_in
        .script("c", vec![Script::Answer(noul("0.3", None))]);
    let first = admitted(&rig, params("limit", JEV, &["a", "b", "c"], None)).await;
    let first = items(&first);
    assert_eq!(first[1]["error"]["code"], "rate_limited");
    assert_eq!(first[1]["error"]["retry_after_ms"], 60_000);
    assert_eq!(first[2]["error"]["code"], "rate_limited");
    assert_eq!(
        rig.stand_in.calls("b"),
        1,
        "a wait longer than the bound is not sat out"
    );
    assert_eq!(rig.store.outcomes(STORE_ROLE, "limit").unwrap().len(), 2);
    admitted(&rig, params("limit", JEV, &["a", "b", "c"], None)).await;
    assert_eq!((rig.stand_in.calls("b"), rig.stand_in.calls("c")), (2, 1));
}

#[tokio::test]
async fn only_a_refusal_the_provider_marks_retryable_is_retried_in_the_call() {
    let config = Config {
        max_retry_wait: std::time::Duration::from_millis(5),
        ..Config::default()
    };
    let rig = rig_with(Arc::new(SystemOneStore::open_in_memory().unwrap()), config);
    rig.stand_in.script(
        "marked",
        vec![
            Script::Status(529, Some(1)),
            Script::Answer(noul("0.1", None)),
        ],
    );
    rig.stand_in.script(
        "unmarked",
        vec![Script::Status(429, None), Script::Answer(noul("0.1", None))],
    );
    let reply = admitted(&rig, params("retry-a", JEV, &["marked"], None)).await;
    assert!(items(&reply)[0]["answers"].is_object());
    assert_eq!(rig.stand_in.calls("marked"), 2);
    let reply = admitted(&rig, params("retry-b", JEV, &["unmarked"], None)).await;
    assert_eq!(items(&reply)[0]["error"]["code"], "rate_limited");
    assert_eq!(rig.stand_in.calls("unmarked"), 1);
}

#[tokio::test]
async fn the_catalog_row_decides_the_kind_refusals() {
    let rig = rig();
    let chat = refused(&rig, params("k1", "openai/gpt-chat", &["a"], None)).await;
    assert_eq!(chat["code"], "model_not_classifier");
    assert_eq!(chat["detail"]["kind"], "chat");
    assert_eq!(chat["detail"]["model"], "openai/gpt-chat");
    let kindless = refused(&rig, params("k2", "openai/kindless", &["a"], None)).await;
    assert_eq!(kindless["code"], "model_not_classifier");
    assert!(kindless["detail"].get("kind").is_none(), "{kindless}");
    let unknown = refused(&rig, params("k3", "nobody/nothing", &["a"], None)).await;
    assert_eq!(unknown["code"], "model_unknown");
    let unrouted = refused(
        &rig,
        params(
            "k4",
            "cloudflare-workers-ai/@cf/cloudflare/clef",
            &["a"],
            None,
        ),
    )
    .await;
    assert_eq!(unrouted["code"], "model_unavailable");
    assert_eq!(rig.stand_in.calls("a"), 0);
    assert_eq!(rig.store.ledger(STORE_ROLE, "k1").unwrap(), None);
}

#[tokio::test]
async fn validation_refusals_name_the_field_and_write_nothing() {
    let rig = rig();
    let null_state = json!({
        "batch_id": "v", "model": JEV,
        "questions": {"q": {"type": "noul", "instructions": "?"}},
        "items": [{"state": "a"}, {"state": null}],
    });
    let refusal = refused(&rig, null_state.to_string()).await;
    assert_eq!(
        (refusal["code"].clone(), refusal["detail"]["field"].clone()),
        (json!("invalid_params"), json!("items[1].state"))
    );
    let big = params("v", JEV, &[&"x".repeat(1024 * 1024)], None);
    let refusal = refused(&rig, big).await;
    assert_eq!(refusal["detail"]["field"], "params");
    assert_eq!(rig.store.ledger(STORE_ROLE, "v").unwrap(), None);
}

#[tokio::test]
async fn a_panic_answers_its_call_and_releases_the_batch() {
    let rig = rig();
    rig.stand_in
        .script("a", vec![Script::Panic, Script::Answer(noul("0.9", None))]);
    let refusal = refused(&rig, params("panic", JEV, &["a"], None)).await;
    assert_eq!(refusal["code"], INTERNAL_ERROR);
    assert_eq!(refusal["detail"]["class"], "transient");
    assert_eq!(rig.gauge.load(Ordering::SeqCst), 0);
    let mut reply = None;
    for _ in 0..1000 {
        if let Ok(text) = Arc::clone(&rig.service)
            .spawn_call(params("panic", JEV, &["a"], None))
            .await
        {
            reply = Some(text);
            break;
        }
        tokio::task::yield_now().await;
    }
    assert!(reply.is_some(), "the panicked call's hold was released");
}

#[tokio::test]
async fn usage_and_cost_stay_absent_where_unreported() {
    let rig = rig();
    rig.stand_in
        .script("a", vec![Script::Answer(noul("0.1", None))]);
    rig.stand_in.script(
        "b",
        vec![Script::Answer(noul("0.2", Some(r#"{"input_tokens":7}"#)))],
    );
    let reply: Value =
        serde_json::from_str(&admitted(&rig, params("usage", JEV, &["a", "b"], None)).await)
            .unwrap();
    assert!(reply["items"][0].get("usage").is_none());
    assert_eq!(reply["items"][1]["usage"], json!({"input_tokens": 7}));
    assert_eq!(reply["usage"], json!({"input_tokens": 7}));
    assert!(reply.get("cost_usd").is_none(), "item 0 reported no tokens");

    rig.stand_in.script(
        "c",
        vec![Script::Answer(noul(
            "0.3",
            Some(r#"{"input_tokens":1000000}"#),
        ))],
    );
    let reply: Value =
        serde_json::from_str(&admitted(&rig, params("cost", JEV, &["c"], None)).await).unwrap();
    assert_eq!(reply["cost_usd"], json!(0.042));
}

#[tokio::test]
async fn describe_lists_exactly_the_routed_classifier_rows() {
    let rig = rig();
    let describe = serde_json::to_value(rig.service.describe()).unwrap();
    assert_eq!(describe["role"], "classifier/v1");
    let models = describe["models"].as_array().unwrap();
    assert_eq!(models.len(), 1, "{describe}");
    assert_eq!(models[0]["model"], JEV);
    assert_eq!(models[0]["images"], Value::Null);
    assert!(models[0].get("context_tokens").is_none());
    assert_eq!(models[0]["egress_host"], "stand-in.invalid");
    assert_eq!(models[0]["price"]["input_per_mtok_usd"], json!(0.042));
    cortexkit_role_classifier::describe::check_describe(&rig.service.describe()).unwrap();

    let empty = SystemOne::new(
        Arc::new(SystemOneStore::open_in_memory().unwrap()),
        "p",
        Arc::new(Rows(HashMap::new())),
        vec![ModelPin::jev(rig.stand_in.clone())],
        Config::default(),
        Arc::new(AtomicUsize::new(0)),
    );
    assert!(
        empty.describe().models.is_empty(),
        "a model the catalog drops is not listed"
    );
}

#[tokio::test]
async fn retention_deletes_a_batch_seven_days_after_its_first_admission() {
    let now = Arc::new(std::sync::atomic::AtomicI64::new(1_000_000));
    let clock = Arc::clone(&now);
    let store = Arc::new(SystemOneStore::open_in_memory().unwrap());
    let stand_in = Arc::new(StandIn::default());
    let service = Arc::new(
        SystemOne::new(
            Arc::clone(&store),
            "p",
            catalog(),
            vec![ModelPin::jev(stand_in.clone())],
            Config::default(),
            Arc::new(AtomicUsize::new(0)),
        )
        .with_clock(Arc::new(move || clock.load(Ordering::SeqCst))),
    );
    stand_in.script("a", vec![Script::Answer(noul("0.1", None))]);
    Arc::clone(&service)
        .spawn_call(params("old", JEV, &["a"], None))
        .await
        .unwrap();
    now.fetch_add(broca_store::system_one::RETENTION_MS - 1, Ordering::SeqCst);
    assert_eq!(service.sweep().await.unwrap(), 0);
    now.fetch_add(1, Ordering::SeqCst);
    assert_eq!(service.sweep().await.unwrap(), 1);
    Arc::clone(&service)
        .spawn_call(params("old", JEV, &["a"], None))
        .await
        .unwrap();
    assert_eq!(
        stand_in.calls("a"),
        2,
        "after retention a re-send is a new batch"
    );
}

/// The batch store is its own file: serving batches writes nothing beside it in broca's state
/// root, and leaves the run index and the session WAL tree byte for byte as they were.
#[tokio::test]
async fn the_store_never_touches_the_run_index_or_any_wal() {
    let root = broca_test_support::scratch_root()
        .join(format!("system-one-state-root-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(root.join("wal/ab")).unwrap();
    std::fs::write(root.join("run-index.db"), b"run index bytes").unwrap();
    std::fs::write(root.join("wal/ab/session.wal"), b"wal bytes").unwrap();
    let snapshot = |root: &std::path::Path| {
        let mut files = Vec::new();
        let mut stack = vec![root.to_path_buf()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    stack.push(path);
                } else {
                    let meta = std::fs::metadata(&path).unwrap();
                    files.push((
                        path.clone(),
                        std::fs::read(&path).unwrap(),
                        meta.modified().unwrap(),
                    ));
                }
            }
        }
        files.sort();
        files
    };
    let before = snapshot(&root);
    let rig = rig_with(
        Arc::new(SystemOneStore::open(root.join("system-one.db")).unwrap()),
        Config::default(),
    );
    rig.stand_in
        .script("a", vec![Script::Answer(noul("0.1", None))]);
    rig.stand_in.script("b", vec![Script::Status(503, None)]);
    admitted(&rig, params("files", JEV, &["a", "b"], None)).await;
    admitted(&rig, params("files", JEV, &["a", "b"], None)).await;
    rig.service.sweep().await.unwrap();
    let after: Vec<_> = snapshot(&root)
        .into_iter()
        .filter(|(path, _, _)| path != &root.join("system-one.db"))
        .collect();
    assert_eq!(
        after, before,
        "only system-one.db is new; nothing else changed"
    );
}
