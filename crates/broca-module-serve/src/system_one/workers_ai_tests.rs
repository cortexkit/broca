//! The Workers AI client (Clef, Clef-flash) through the `classify.run` engine, against a
//! loopback stand-in for Cloudflare's API that answers in Cloudflare's documented shapes: the
//! `{result, success, errors, messages}` envelope, and errors as an HTTP status with
//! `errors[].code`. The real client sends real HTTP to it, so the URL, the headers and the body
//! it checks are the bytes Cloudflare would receive.

use std::collections::HashMap;
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_catalog::ModelBatchRow;
use broca_store::SystemOneStore;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::credential::{CredentialSource, ProviderSecret};
use super::workers_ai::{WorkersAi, CLEF_FLASH_MODEL_PATH, CLEF_MODEL_PATH, CREDENTIAL_ID};
use super::*;

/// A Cloudflare account id shaped like a real one (32 hex digits).
const ACCOUNT: &str = "0123456789abcdef0123456789abcdef";
const TOKEN: &str = "cf-test-token-not-a-real-secret";

/// What the stand-in answers for one item, by the JCS of its `state`.
#[derive(Clone)]
enum Answer {
    /// HTTP 200 with the success envelope around this exact `result` text.
    Result(String),
    /// This status with Cloudflare's error envelope naming `code`, and a `Retry-After` in
    /// seconds when given.
    Error {
        status: u16,
        code: Option<u64>,
        retry_after_s: Option<u64>,
    },
    /// This status with exactly this body.
    Raw(u16, String),
}

/// One request the stand-in received.
#[derive(Clone, Debug)]
struct Received {
    path: String,
    authorization: Option<String>,
    body: String,
}

#[derive(Default)]
struct Cloudflare {
    scripts: Mutex<HashMap<String, Vec<Answer>>>,
    calls: Mutex<HashMap<String, usize>>,
    received: Mutex<Vec<Received>>,
}

fn key(state: &Value) -> String {
    String::from_utf8(serde_jcs::to_vec(state).unwrap()).unwrap()
}

impl Cloudflare {
    /// Script the answers for a string `state`, one per call; the last repeats.
    fn script(&self, state: &str, answers: Vec<Answer>) {
        self.scripts
            .lock()
            .unwrap()
            .insert(key(&json!(state)), answers);
    }

    fn calls(&self, state: &str) -> usize {
        self.calls
            .lock()
            .unwrap()
            .get(&key(&json!(state)))
            .copied()
            .unwrap_or(0)
    }

    fn received(&self) -> Vec<Received> {
        self.received.lock().unwrap().clone()
    }

    fn answer_for(&self, request: &Received) -> Answer {
        let state: Value = serde_json::from_str::<Value>(&request.body)
            .map(|body| body["state"].clone())
            .unwrap_or(Value::Null);
        let key = key(&state);
        let n = {
            let mut calls = self.calls.lock().unwrap();
            let n = calls.entry(key.clone()).or_default();
            *n += 1;
            *n
        };
        self.scripts
            .lock()
            .unwrap()
            .get(&key)
            .map(|list| list[(n - 1).min(list.len() - 1)].clone())
            .unwrap_or(Answer::Error {
                status: 500,
                code: None,
                retry_after_s: None,
            })
    }
}

/// Start the stand-in; returns the base URL a client takes in place of
/// `https://api.cloudflare.com/client/v4`.
async fn start(cloudflare: Arc<Cloudflare>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let cloudflare = Arc::clone(&cloudflare);
            tokio::spawn(async move {
                let _ = serve_one(stream, cloudflare).await;
            });
        }
    });
    format!("http://{address}/client/v4")
}

async fn serve_one(
    mut stream: tokio::net::TcpStream,
    cloudflare: Arc<Cloudflare>,
) -> std::io::Result<()> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 16384];
    let body_start = loop {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&chunk[..n]);
        if let Some(at) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
            break at + 4;
        }
    };
    let head = String::from_utf8_lossy(&buffer[..body_start]).into_owned();
    let mut lines = head.lines();
    let path = lines
        .next()
        .and_then(|line| line.split(' ').nth(1))
        .unwrap_or_default()
        .to_owned();
    let mut length = 0usize;
    let mut authorization = None;
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            match name.trim().to_ascii_lowercase().as_str() {
                "content-length" => length = value.trim().parse().unwrap_or(0),
                "authorization" => authorization = Some(value.trim().to_owned()),
                _ => {}
            }
        }
    }
    while buffer.len() < body_start + length {
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..n]);
    }
    let received = Received {
        path,
        authorization,
        body: String::from_utf8_lossy(&buffer[body_start..]).into_owned(),
    };
    cloudflare.received.lock().unwrap().push(received.clone());
    let (status, body, retry) = match cloudflare.answer_for(&received) {
        Answer::Result(result) => (
            200,
            format!(r#"{{"result":{result},"success":true,"errors":[],"messages":[]}}"#),
            None,
        ),
        Answer::Error {
            status,
            code,
            retry_after_s,
        } => {
            let errors = code.map_or(String::new(), |code| {
                format!(r#"{{"code":{code},"message":"scripted Workers AI error"}}"#)
            });
            (
                status,
                format!(r#"{{"result":null,"success":false,"errors":[{errors}],"messages":[]}}"#),
                retry_after_s,
            )
        }
        Answer::Raw(status, body) => (status, body, None),
    };
    let retry = retry.map_or(String::new(), |s| format!("retry-after: {s}\r\n"));
    let response = format!(
        "HTTP/1.1 {status} Scripted\r\ncontent-type: application/json\r\ncontent-length: {}\r\n{retry}connection: close\r\n\r\n{body}",
        body.len()
    );
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await
}

/// A vault credential as the vault resolves `apikey:cloudflare-workers-ai`: a token and the
/// account id beside it.
struct Vaulted {
    account_id: Option<String>,
}

#[async_trait]
impl CredentialSource for Vaulted {
    fn credential_id(&self) -> Option<String> {
        Some(CREDENTIAL_ID.to_owned())
    }

    async fn bearer(&self) -> Result<Option<String>, String> {
        Ok(Some(TOKEN.to_owned()))
    }

    async fn secret(&self) -> Result<Option<ProviderSecret>, String> {
        Ok(Some(ProviderSecret {
            token: TOKEN.to_owned(),
            account_id: self.account_id.clone(),
            auth_selection: Some(format!("{CREDENTIAL_ID}:resolved")),
        }))
    }
}

struct Rows(HashMap<String, ModelBatchRow>);

impl BatchCatalog for Rows {
    fn row(&self, catalog_id: &str) -> Option<ModelBatchRow> {
        self.0.get(catalog_id).cloned()
    }
}

/// The catalog's Clef rows: decision models that take attachments, priced on input only
/// (0.24 and 0.038 USD per million tokens), output stated free, contexts 65,536 and 24,576.
fn catalog() -> Arc<Rows> {
    let row = |context, input| ModelBatchRow {
        model_name: None,
        provider_name: None,
        kind: Some("decision".into()),
        context_tokens: Some(context),
        attachment: true,
        input_rate_nanos: Some(input),
        output_rate_nanos: Some(0),
    };
    Arc::new(Rows(HashMap::from([
        (CLEF.to_owned(), row(65_536, 240_000_000)),
        (CLEF_FLASH.to_owned(), row(24_576, 38_000_000)),
    ])))
}

struct Rig {
    service: Arc<SystemOne>,
    cloudflare: Arc<Cloudflare>,
    store: Arc<SystemOneStore>,
    export: Arc<broca_store::SqliteStore>,
}

async fn rig_with(account_id: Option<&str>, config: Config) -> Rig {
    let cloudflare = Arc::new(Cloudflare::default());
    let base = start(Arc::clone(&cloudflare)).await;
    let credential: Arc<dyn CredentialSource> = Arc::new(Vaulted {
        account_id: account_id.map(str::to_owned),
    });
    let client = |path| {
        Arc::new(
            WorkersAi::new(
                Some(&base),
                path,
                Arc::clone(&credential),
                Duration::from_secs(10),
            )
            .unwrap(),
        )
    };
    let store = Arc::new(SystemOneStore::open_in_memory().unwrap());
    let export = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
    let service = Arc::new(
        SystemOne::new(
            Arc::clone(&store),
            "this-process",
            catalog(),
            vec![
                ModelPin::clef(client(CLEF_MODEL_PATH)),
                ModelPin::clef_flash(client(CLEF_FLASH_MODEL_PATH)),
            ],
            config,
            Arc::new(AtomicUsize::new(0)),
        )
        .with_export(
            export.clone(),
            Arc::new(crate::billing_routing::ChargeBasisTable::shipped_defaults()),
        ),
    );
    Rig {
        service,
        cloudflare,
        store,
        export,
    }
}

async fn rig() -> Rig {
    rig_with(Some(ACCOUNT), Config::default()).await
}

fn result(model: &str, noul: &str, usage: Option<&str>) -> String {
    let usage = usage.map_or(String::new(), |usage| format!(",\"usage\":{usage}"));
    format!(r#"{{"model":"{model}","answers":{{"q":{{"type":"noul","noul":{noul}}}}}{usage}}}"#)
}

fn params(batch: &str, model: &str, states: &[&str]) -> String {
    json!({
        "batch_id": batch,
        "model": model,
        "questions": {"q": {"type": "noul", "instructions": "Is it flagged?"}},
        "items": states.iter().map(|state| json!({"state": state})).collect::<Vec<_>>(),
    })
    .to_string()
}

async fn admitted(rig: &Rig, params: String) -> String {
    match Arc::clone(&rig.service)
        .spawn_call(params, test_scope())
        .await
    {
        Ok(reply) => reply.get().to_owned(),
        Err(body) => panic!("refused: {}", String::from_utf8_lossy(&body)),
    }
}

async fn refused(rig: &Rig, params: String) -> Value {
    match Arc::clone(&rig.service)
        .spawn_call(params, test_scope())
        .await
    {
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

/// The exact text of item `i`'s answers.
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

fn stored_indexes(rig: &Rig, batch: &str) -> Vec<u64> {
    let mut indexes: Vec<u64> = rig
        .store
        .outcomes(STORE_ROLE, batch)
        .unwrap()
        .iter()
        .map(|outcome| outcome.index)
        .collect();
    indexes.sort_unstable();
    indexes
}

/// Cloudflare's documented clef request (the role's `provider-examples.json`, verbatim from the
/// model page) is exactly what the client sends for the same questions and state, to the
/// documented URL with the bearer token; the documented `result` comes back as the item's
/// answers byte for byte, with its usage, and the usage at the frozen catalog rate is the cost.
#[tokio::test]
async fn clef_requests_and_replies_take_cloudflares_documented_shapes() {
    let rig = rig().await;
    let examples: Value =
        serde_json::from_str(cortexkit_role_classifier::vectors::PROVIDER_EXAMPLES_JSON).unwrap();
    let documented = &examples["clef_request"];
    let state = documented["state"].as_str().unwrap();
    // The answers spell numbers the way a provider may, so a respelling would show.
    let answers = r#"{"urgent":{"type":"noul","noul":0.980},"team":{"type":"choice","choice":"technical","probabilities":{"billing":1E-1,"technical":0.9,"sales":0.0},"confidence":0.90},"severity":{"type":"score","score":1.8,"legend":{"0":"No impact","1":"Minor","2":"Major","3":"Critical"},"probabilities":{"0":0.0,"1":0.1,"2":0.2,"3":0.7},"confidence":0.8}}"#;
    rig.cloudflare.script(
        state,
        vec![Answer::Result(format!(
            r#"{{"model":"clef","answers":{answers},"usage":{{"input_tokens":1000000,"output_tokens":4}}}}"#
        ))],
    );
    let request = json!({
        "batch_id": "documented",
        "model": CLEF,
        "questions": documented["questions"],
        "items": [{"state": state}],
    });
    let reply = admitted(&rig, request.to_string()).await;

    let received = rig.cloudflare.received();
    assert_eq!(received.len(), 1);
    assert_eq!(
        received[0].path,
        format!("/client/v4/accounts/{ACCOUNT}/ai/run/@cf/cloudflare/clef")
    );
    assert_eq!(
        received[0].authorization.as_deref(),
        Some(format!("Bearer {TOKEN}").as_str())
    );
    let sent: Value = serde_json::from_str(&received[0].body).unwrap();
    assert_eq!(
        &sent, documented,
        "the body is Cloudflare's documented request"
    );

    assert_eq!(answers_text(&reply, 0), answers);
    let reply: Value = serde_json::from_str(&reply).unwrap();
    assert_eq!(
        reply["items"][0]["usage"],
        json!({"input_tokens": 1000000, "output_tokens": 4})
    );
    // Input at 0.24 USD per million; output is stated free, so its tokens cost nothing.
    assert_eq!(reply["cost_usd"], json!(0.24));
    assert_eq!(
        rig.store
            .ledger(STORE_ROLE, "documented")
            .unwrap()
            .unwrap()
            .1,
        240_000_000
    );
}

/// Clef-flash is its own endpoint and its own model value; images go to Clef as the caller
/// wrote them; and an answer without usage leaves the usage and the cost absent.
#[tokio::test]
async fn clef_flash_takes_its_own_endpoint_and_images_are_forwarded_as_written() {
    let rig = rig().await;
    rig.cloudflare.script(
        "flash",
        vec![Answer::Result(result("clef-flash", "0.5", None))],
    );
    let reply = admitted(&rig, params("flash", CLEF_FLASH, &["flash"])).await;
    let received = rig.cloudflare.received();
    assert_eq!(
        received[0].path,
        format!("/client/v4/accounts/{ACCOUNT}/ai/run/@cf/cloudflare/clef-flash")
    );
    assert_eq!(
        serde_json::from_str::<Value>(&received[0].body).unwrap()["model"],
        "clef-flash"
    );
    let reply: Value = serde_json::from_str(&reply).unwrap();
    assert!(reply["items"][0].get("usage").is_none(), "{reply}");
    assert!(
        reply.get("cost_usd").is_none(),
        "unreported usage has no cost: {reply}"
    );

    const PIXEL: &str = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
    rig.cloudflare.script(
        "pictured",
        vec![Answer::Result(result("clef", "0.5", None))],
    );
    let images = format!(r#"["{PIXEL}"]"#);
    let request = format!(
        r#"{{"batch_id":"pictured","model":"{CLEF}","questions":{{"q":{{"type":"noul","instructions":"?"}}}},"items":[{{"state":"pictured","images":{images}}}]}}"#
    );
    admitted(&rig, request).await;
    let body = &rig.cloudflare.received()[1].body;
    assert!(
        body.ends_with(&format!(r#","images":{images}}}"#)),
        "the images reach Clef as written: {body}"
    );
}

/// Every Workers AI row of the role's code table, sent by the stand-in as Cloudflare sends it,
/// gives the item the table's code and class, is stored or not as the table says, and stops the
/// call or not as it says.
#[tokio::test]
async fn workers_ai_errors_map_as_the_roles_code_table_states() {
    let table: Value =
        serde_json::from_str(cortexkit_role_classifier::vectors::WORKERS_AI_CODES_JSON).unwrap();
    for (n, row) in table["clef"].as_array().unwrap().iter().enumerate() {
        let rig = rig().await;
        let status = row["status"].as_u64().unwrap() as u16;
        let code = row["code"].as_u64();
        let batch = format!("row-{n}");
        rig.cloudflare
            .script("first", vec![Answer::Result(result("clef", "0.1", None))]);
        rig.cloudflare.script(
            "failing",
            vec![Answer::Error {
                status,
                code,
                retry_after_s: None,
            }],
        );
        rig.cloudflare
            .script("after", vec![Answer::Result(result("clef", "0.3", None))]);
        let reply = admitted(&rig, params(&batch, CLEF, &["first", "failing", "after"])).await;
        let failed = &items(&reply)[1]["error"];
        let what = format!("HTTP {status} code {code:?}");
        assert_eq!(failed["code"], row["item_code"], "{what}");
        assert_eq!(failed["class"], row["class"], "{what}");
        if let Some(code) = code {
            assert!(
                failed["message"]
                    .as_str()
                    .unwrap()
                    .contains(&code.to_string()),
                "{what}: the message names the code: {failed}"
            );
        }
        let stored = stored_indexes(&rig, &batch).contains(&1);
        assert_eq!(stored, row["stored"].as_bool().unwrap(), "{what}: stored");
        let stops = rig.cloudflare.calls("after") == 0;
        assert_eq!(
            stops,
            row["stops_the_call"].as_bool().unwrap(),
            "{what}: stops"
        );
    }
}

/// A daily allocation used up (429, code 3036) belongs to the account, not to the item that met
/// it: nothing more is sent, the item and every one after it carry the code, none of them is
/// recorded, and a short `Retry-After` does not make it retried. A later re-send asks exactly
/// those items again and serves the answered one byte for byte.
#[tokio::test]
async fn an_account_level_error_ends_the_whole_batch_and_records_no_unsent_item() {
    let config = Config {
        max_retry_wait: Duration::from_secs(2),
        ..Config::default()
    };
    let rig = rig_with(Some(ACCOUNT), config).await;
    rig.cloudflare.script(
        "a",
        vec![Answer::Result(result(
            "clef",
            "0.10",
            Some(r#"{"input_tokens":5,"output_tokens":1}"#),
        ))],
    );
    rig.cloudflare.script(
        "b",
        vec![
            Answer::Error {
                status: 429,
                code: Some(3036),
                retry_after_s: Some(1),
            },
            Answer::Result(result("clef", "0.20", None)),
        ],
    );
    for state in ["c", "d"] {
        rig.cloudflare
            .script(state, vec![Answer::Result(result("clef", "0.30", None))]);
    }
    let first = admitted(&rig, params("daily", CLEF, &["a", "b", "c", "d"])).await;
    let listed = items(&first);
    assert!(listed[0]["answers"].is_object());
    for item in &listed[1..] {
        assert_eq!(item["error"]["code"], "account_limit_exhausted", "{item}");
        assert_eq!(item["error"]["class"], "transient", "{item}");
    }
    assert_eq!(
        (
            rig.cloudflare.calls("b"),
            rig.cloudflare.calls("c"),
            rig.cloudflare.calls("d")
        ),
        (1, 0, 0),
        "one call met the code, it was not retried, and nothing was sent after it"
    );
    assert_eq!(
        stored_indexes(&rig, "daily"),
        [0],
        "only the answered item is recorded"
    );

    let again = admitted(&rig, params("daily", CLEF, &["a", "b", "c", "d"])).await;
    assert!(items(&again).iter().all(|item| item["answers"].is_object()));
    assert_eq!(answers_text(&again, 0), answers_text(&first, 0));
    assert_eq!(
        answers_text(&again, 0),
        r#"{"q":{"type":"noul","noul":0.10}}"#
    );
    assert_eq!(
        (
            rig.cloudflare.calls("a"),
            rig.cloudflare.calls("b"),
            rig.cloudflare.calls("c"),
            rig.cloudflare.calls("d")
        ),
        (1, 2, 1, 1)
    );
}

/// On the call's first provider call, an account-level code refuses the whole call with the
/// role's refusal (its documented body exactly), and writes nothing.
#[tokio::test]
async fn an_account_level_error_on_the_first_call_is_a_refusal_that_writes_nothing() {
    let table: Value =
        serde_json::from_str(cortexkit_role_classifier::vectors::WORKERS_AI_CODES_JSON).unwrap();
    let refusals = table["first_call_refusals"].as_array().unwrap();
    for (batch, status, code, expected) in [
        ("exhausted", 429, 3036, &refusals[0]),
        ("unentitled", 403, 5035, &refusals[1]),
    ] {
        let rig = rig().await;
        rig.cloudflare.script(
            "a",
            vec![Answer::Error {
                status,
                code: Some(code),
                retry_after_s: None,
            }],
        );
        rig.cloudflare
            .script("b", vec![Answer::Result(result("clef", "0.2", None))]);
        let refusal = refused(&rig, params(batch, CLEF, &["a", "b"])).await;
        assert_eq!(&refusal, expected, "{batch}");
        assert_eq!(rig.cloudflare.calls("b"), 0);
        assert_eq!(rig.store.ledger(STORE_ROLE, batch).unwrap(), None);
    }
}

/// Out of capacity (429, code 3040) with a short `Retry-After` is sat out inside the call, as a
/// rate limit is; a 200 whose envelope reports failure is a provider error naming its code.
#[tokio::test]
async fn capacity_is_retried_and_an_unsuccessful_envelope_is_a_provider_error() {
    let config = Config {
        max_retry_wait: Duration::from_secs(2),
        ..Config::default()
    };
    let rig = rig_with(Some(ACCOUNT), config).await;
    rig.cloudflare.script(
        "busy",
        vec![
            Answer::Error {
                status: 429,
                code: Some(3040),
                retry_after_s: Some(1),
            },
            Answer::Result(result("clef", "0.4", None)),
        ],
    );
    rig.cloudflare.script(
        "odd",
        vec![Answer::Raw(
            200,
            r#"{"result":null,"success":false,"errors":[{"code":7000,"message":"x"}],"messages":[]}"#
                .into(),
        )],
    );
    let reply = admitted(&rig, params("capacity", CLEF, &["busy", "odd"])).await;
    let listed = items(&reply);
    assert!(listed[0]["answers"].is_object(), "{reply}");
    assert_eq!(rig.cloudflare.calls("busy"), 2);
    assert_eq!(listed[1]["error"]["code"], "provider_error");
    assert!(listed[1]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("7000"));
}

/// A credential without an account id cannot address Cloudflare: the call is refused
/// `auth_failed` naming the credential (never a secret), and nothing is sent. An account id
/// that could change the URL's path is refused the same way.
#[tokio::test]
async fn a_credential_without_a_usable_account_id_sends_nothing() {
    for (batch, account) in [("none", None), ("pathy", Some("abc/../../x"))] {
        let rig = rig_with(account, Config::default()).await;
        let refusal = refused(&rig, params(batch, CLEF, &["a"])).await;
        assert_eq!(refusal["code"], "auth_failed", "{refusal}");
        assert_eq!(refusal["detail"]["credential"], CREDENTIAL_ID);
        let text = refusal.to_string();
        assert!(!text.contains(TOKEN) && !text.contains("abc/"), "{text}");
        assert!(rig.cloudflare.received().is_empty());
    }
    let secret = ProviderSecret {
        token: TOKEN.into(),
        account_id: Some(ACCOUNT.into()),
        auth_selection: Some(CREDENTIAL_ID.into()),
    };
    let shown = format!("{secret:?}");
    assert!(
        !shown.contains(TOKEN) && !shown.contains(ACCOUNT),
        "{shown}"
    );
}

/// `role.describe` lists Clef and Clef-flash from their catalog rows: contexts, input prices,
/// no output price beyond the stated zero, four images each, and Cloudflare's API host.
#[tokio::test]
async fn describe_lists_clef_and_clef_flash_from_their_rows() {
    let credential: Arc<dyn CredentialSource> = Arc::new(Vaulted {
        account_id: Some(ACCOUNT.into()),
    });
    let client = |path| {
        Arc::new(
            WorkersAi::new(None, path, Arc::clone(&credential), Duration::from_secs(1)).unwrap(),
        )
    };
    let service = SystemOne::new(
        Arc::new(SystemOneStore::open_in_memory().unwrap()),
        "p",
        catalog(),
        vec![
            ModelPin::clef(client(CLEF_MODEL_PATH)),
            ModelPin::clef_flash(client(CLEF_FLASH_MODEL_PATH)),
        ],
        Config::default(),
        Arc::new(AtomicUsize::new(0)),
    );
    let describe = serde_json::to_value(service.describe()).unwrap();
    cortexkit_role_classifier::describe::check_describe(&service.describe()).unwrap();
    let models = describe["models"].as_array().unwrap();
    let summary: Vec<_> = models
        .iter()
        .map(|m| {
            (
                m["model"].clone(),
                m["context_tokens"].clone(),
                m["images"].clone(),
                m["price"].clone(),
                m["egress_host"].clone(),
                m["max_questions"].clone(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            (
                json!(CLEF),
                json!(65536),
                json!({"max": 4}),
                json!({"input_per_mtok_usd": 0.24, "output_per_mtok_usd": 0}),
                json!("api.cloudflare.com"),
                json!(64)
            ),
            (
                json!(CLEF_FLASH),
                json!(24576),
                json!({"max": 4}),
                json!({"input_per_mtok_usd": 0.038, "output_per_mtok_usd": 0}),
                json!("api.cloudflare.com"),
                json!(64)
            ),
        ]
    );
}

fn test_scope() -> broca_store::system_one::BatchScope {
    broca_store::system_one::BatchScope::Run {
        agent_id: "agent".into(),
        run_id: "run".into(),
    }
}

#[tokio::test]
async fn workers_ai_metering_uses_the_attempts_resolved_credential_and_only_measured_usage() {
    let rig = rig().await;
    rig.cloudflare.script(
        "measured",
        vec![Answer::Result(result(
            "clef",
            "0.5",
            Some(r#"{"input_tokens":7,"output_tokens":0}"#),
        ))],
    );
    rig.cloudflare.script(
        "unreported",
        vec![Answer::Result(result("clef", "0.5", None))],
    );
    admitted(&rig, params("metered", CLEF, &["measured", "unreported"])).await;
    let page = rig.export.read_export_page(None, 100).unwrap();
    assert_eq!(page.facts.len(), 2);
    let first = serde_json::to_value(&page.facts[0].segment).unwrap();
    assert_eq!(first["auth_selection"], format!("{CREDENTIAL_ID}:resolved"));
    assert_eq!(first["account_ref"], ACCOUNT);
    assert_eq!(
        first["usage"],
        json!({"input_tokens": 7, "output_tokens": 0})
    );
    let second = serde_json::to_value(&page.facts[1].segment).unwrap();
    assert_eq!(second["usage"], json!({}));
    assert!(second.get("cost_nanos").is_none());
    admitted(&rig, params("metered", CLEF, &["measured", "unreported"])).await;
    assert_eq!(
        rig.export.read_export_page(None, 100).unwrap().facts.len(),
        2
    );
}
