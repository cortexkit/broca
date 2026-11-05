//! Loopback-only coverage of the Decisions translation, error boundary, and catalog gate.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use broca_catalog::ModelBatchRow;
use broca_store::SystemOneStore;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::credential::CredentialSource;
use super::openai_decisions::{OpenAiDecisions, CREDENTIAL_ID, MODEL};
use super::*;

const PIXEL: &str = "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==";
const PREDICATE: &str =
    r#"{"model":"gpt-6-luna","answers":[{"name":"q","type":"predicate","probability":0.10}]}"#;
const ALL_ANSWERS: &str = r#"{"model":"gpt-6-luna","answers":[{"name":"a_predicate","type":"predicate","probability":0.10},{"name":"b_choice","type":"choice","choice":"b","confidence":8.00e-1,"probabilities":[{"value":"a","probability":1e-3},{"value":"b","probability":0.999}]},{"name":"c_score","type":"score","score":1.00,"confidence":0.90,"probabilities":[{"value":0,"label":"0","probability":0.10},{"value":1,"label":"1","probability":9e-1}]}],"usage":{"input_tokens":1000000,"output_tokens":12,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":0}}}"#;

#[derive(Default)]
struct Stub {
    responses: Mutex<VecDeque<(u16, String, Option<String>)>>,
    received: Mutex<Vec<(String, String)>>,
}

struct Token;
#[async_trait]
impl CredentialSource for Token {
    fn credential_id(&self) -> Option<String> {
        Some(CREDENTIAL_ID.into())
    }
    async fn bearer(&self) -> Result<Option<String>, String> {
        Ok(Some("test-token-not-a-secret".into()))
    }
}

async fn start(responses: Vec<(u16, String, Option<String>)>) -> (Arc<Stub>, Arc<OpenAiDecisions>) {
    let stub = Arc::new(Stub {
        responses: Mutex::new(responses.into()),
        ..Stub::default()
    });
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    let server = stub.clone();
    tokio::spawn(async move {
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let server = server.clone();
            tokio::spawn(async move {
                let mut buffer = Vec::new();
                let mut chunk = [0u8; 16384];
                let body_start = loop {
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(n, 0);
                    buffer.extend_from_slice(&chunk[..n]);
                    if let Some(at) = buffer.windows(4).position(|w| w == b"\r\n\r\n") {
                        break at + 4;
                    }
                };
                let head = String::from_utf8(buffer[..body_start].to_vec()).unwrap();
                let length: usize = head
                    .lines()
                    .find_map(|line| {
                        let (key, value) = line.split_once(':')?;
                        key.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse().unwrap())
                    })
                    .unwrap();
                while buffer.len() < body_start + length {
                    let n = stream.read(&mut chunk).await.unwrap();
                    assert_ne!(n, 0);
                    buffer.extend_from_slice(&chunk[..n]);
                }
                server.received.lock().unwrap().push((
                    head,
                    String::from_utf8(buffer[body_start..body_start + length].to_vec()).unwrap(),
                ));
                let (status, body, retry) = server
                    .responses
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("scripted request");
                let retry =
                    retry.map_or(String::new(), |retry| format!("retry-after: {retry}\r\n"));
                let response = format!("HTTP/1.1 {status} Scripted\r\ncontent-type: application/json\r\ncontent-length: {}\r\n{retry}connection: close\r\n\r\n{body}", body.len());
                stream.write_all(response.as_bytes()).await.unwrap();
                stream.shutdown().await.unwrap();
            });
        }
    });
    let client = Arc::new(
        OpenAiDecisions::new(Some(&base), Arc::new(Token), Duration::from_secs(10)).unwrap(),
    );
    (stub, client)
}

struct Rows(HashMap<String, ModelBatchRow>);
impl BatchCatalog for Rows {
    fn row(&self, id: &str) -> Option<ModelBatchRow> {
        self.0.get(id).cloned()
    }
}

fn service(client: Arc<OpenAiDecisions>, kind: Option<&str>) -> Arc<SystemOne> {
    let rows = kind
        .map(|kind| {
            HashMap::from([(
                OPENAI_DECISIONS.into(),
                ModelBatchRow {
                    model_name: None,
                    provider_name: None,
                    kind: Some(kind.into()),
                    context_tokens: None,
                    attachment: true,
                    input_rate_nanos: Some(100_000_000),
                    output_rate_nanos: Some(0),
                },
            )])
        })
        .unwrap_or_default();
    Arc::new(SystemOne::new(
        Arc::new(SystemOneStore::open_in_memory().unwrap()),
        "openai-test",
        Arc::new(Rows(rows)),
        vec![ModelPin::openai_decisions(client)],
        Config {
            max_attempts: 1,
            ..Config::default()
        },
        Arc::new(AtomicUsize::new(0)),
    ))
}

fn scope() -> broca_store::system_one::BatchScope {
    broca_store::system_one::BatchScope::Run {
        agent_id: "agent-openai-test".into(),
        run_id: "run-openai-test".into(),
    }
}

fn simple() -> Value {
    json!({"model": MODEL, "state":"evidence", "questions":{"q":{"type":"noul","instructions":"Is it true?"}}})
}

fn params() -> String {
    json!({"batch_id":"openai-batch", "model":OPENAI_DECISIONS, "questions":simple()["questions"], "items":[{"state":"evidence"}]}).to_string()
}

async fn admitted(service: &Arc<SystemOne>, params: String) -> String {
    service
        .clone()
        .spawn_call(params, scope())
        .await
        .unwrap_or_else(|error| panic!("{}", String::from_utf8_lossy(&error)))
        .get()
        .to_owned()
}

fn answered(reply: ProviderReply) -> String {
    match reply {
        ProviderReply::AttributedAnswer { body, .. } => String::from_utf8(body).unwrap(),
        other => panic!("not answered: {other:?}"),
    }
}

fn failed(reply: ProviderReply) -> cortexkit_role_classifier::errors::ItemError {
    match reply {
        ProviderReply::ClassifiedError { error, .. } => error,
        other => panic!("not a classified error: {other:?}"),
    }
}

#[tokio::test]
async fn golden_request_and_answers_keep_all_three_kinds_and_numeric_text() {
    let (stub, client) = start(vec![(200, ALL_ANSWERS.into(), None)]).await;
    let service = service(client, Some("decision"));
    let reply = admitted(&service, json!({
        "batch_id":"golden", "model":OPENAI_DECISIONS,
        "questions": {
            "a_predicate":{"type":"noul","instructions":{"ask":"urgent"},"criteria":{"true":"It is urgent.","false":"It can wait."}},
            "b_choice":{"type":"choice","instructions":"Pick a team", "criteria":{"a":null,"b":{"team":"engineering"}}},
            "c_score":{"type":"score","instructions":["Rate severity"], "criteria":["Low", "High"]}
        },
        "items":[{"state":{"ticket":"broken"},"images":[PIXEL]}]
    }).to_string()).await;
    let received = stub.received.lock().unwrap();
    assert_eq!(received.len(), 1);
    assert!(received[0].0.starts_with("POST /v1/decisions HTTP/1.1\r\n"));
    assert!(received[0]
        .0
        .to_ascii_lowercase()
        .contains("authorization: bearer test-token-not-a-secret\r\n"));
    assert_eq!(
        serde_json::from_str::<Value>(&received[0].1).unwrap(),
        json!({
            "model":"gpt-6-luna",
            "prompt_cache_options":{"mode":"explicit"},
            "input":[{"role":"user","content":[{"type":"input_text","text":"{\"ticket\":\"broken\"}"},{"type":"input_image","image_url":PIXEL}]}],
            "questions":[
                {"name":"a_predicate","type":"predicate","instructions":[{"type":"input_text","text":"{\"ask\":\"urgent\"}"},{"type":"input_text","text":"Answer true when: It is urgent.\nAnswer false when: It can wait."}]},
                {"name":"b_choice","type":"choice","instructions":"Pick a team", "choices":[{"value":"a"},{"value":"b","description":"{\"team\":\"engineering\"}"}]},
                {"name":"c_score","type":"score","instructions":"[\"Rate severity\"]", "levels":[{"label":"0","description":"Low"},{"label":"1","description":"High"}]}
            ]
        })
    );
    #[derive(serde::Deserialize)]
    struct Reply {
        items: Vec<Item>,
    }
    #[derive(serde::Deserialize)]
    struct Item {
        answers: Box<RawValue>,
    }
    let parsed: Reply = serde_json::from_str(&reply).unwrap();
    assert_eq!(
        parsed.items[0].answers.get(),
        r#"{"a_predicate":{"noul":0.10,"type":"noul"},"b_choice":{"choice":"b","confidence":8.00e-1,"probabilities":{"a":1e-3,"b":0.999},"type":"choice"},"c_score":{"confidence":0.90,"legend":{"0":"Low","1":"High"},"probabilities":{"0":0.10,"1":9e-1},"score":1.00,"type":"score"}}"#
    );
    let reply: Value = serde_json::from_str(&reply).unwrap();
    assert_eq!(
        reply["items"][0]["usage"],
        json!({"input_tokens":1000000,"output_tokens":12})
    );
    assert_eq!(
        reply["usage"],
        json!({"input_tokens":1000000,"output_tokens":12})
    );
    assert_eq!(reply["cost_usd"], json!(0.10));
}

#[tokio::test]
async fn unknown_answer_name_is_rejected_without_failing_other_items() {
    let unknown = r#"{"answers":[{"name":"not_asked","type":"predicate","probability":0.7}]}"#;
    let (stub, client) = start(vec![
        (200, unknown.into(), None),
        (200, PREDICATE.into(), None),
    ])
    .await;
    let service = service(client, Some("decision"));
    let mut request: Value = serde_json::from_str(&params()).unwrap();
    request["items"] = json!([{"state":"one"},{"state":"two"}]);
    let reply: Value =
        serde_json::from_str(&admitted(&service, request.to_string()).await).unwrap();
    assert_eq!(
        reply["items"][0]["error"]["code"],
        "openai_decisions_unmappable_answer"
    );
    assert_eq!(reply["items"][0]["error"]["class"], "permanent");
    assert!(reply["items"][0]["error"]["message"]
        .as_str()
        .unwrap()
        .contains("unknown answer name not_asked"));
    assert!(reply["items"][0].get("answers").is_none());
    assert_eq!(reply["items"][1]["answers"]["q"]["noul"], json!(0.1));
    assert_eq!(stub.received.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn missing_fields_refusals_and_incomplete_distributions_are_named_errors() {
    let cases = [
        (
            r#"{"name":"q","type":"predicate"}"#,
            json!({"type":"noul","instructions":"?"}),
            "probability",
        ),
        (
            r#"{"name":"q","type":"refusal"}"#,
            json!({"type":"noul","instructions":"?"}),
            "type",
        ),
        (
            r#"{"name":"q","type":"choice","choice":"a","probabilities":[]}"#,
            json!({"type":"choice","instructions":"?","criteria":{"a":"A","b":"B"}}),
            "confidence",
        ),
        (
            r#"{"name":"q","type":"choice","choice":"a","confidence":0.9,"probabilities":[{"value":"a","probability":1}]}"#,
            json!({"type":"choice","instructions":"?","criteria":{"a":"A","b":"B"}}),
            "omit",
        ),
        (
            r#"{"name":"q","type":"score","confidence":0.9,"score":0.8}"#,
            json!({"type":"score","instructions":"?","criteria":["A","B"]}),
            "probabilities",
        ),
        (
            r#"{"name":"q","type":"score","confidence":0.9,"score":0.8,"probabilities":[{"value":0,"probability":1},{"value":1,"label":"1","probability":0}]}"#,
            json!({"type":"score","instructions":"?","criteria":["A","B"]}),
            "label",
        ),
    ];
    for (answer, question, missing) in cases {
        let (_, client) = start(vec![(200, format!(r#"{{"answers":[{answer}]}}"#), None)]).await;
        let mut request = simple();
        request["questions"]["q"] = question;
        let error = failed(client.call(request.to_string()).await);
        assert_eq!(error.code, "openai_decisions_unmappable_answer");
        assert!(error.message.contains(missing), "{missing}: {error:?}");
    }
    for body in [r#"{"answers":[]}"#.to_owned(), r#"{"answers":[{"name":"q","type":"predicate","probability":0.5},{"name":"q","type":"predicate","probability":0.5}]}"#.to_owned()] {
        let (_, client) = start(vec![(200, body, None)]).await;
        assert_eq!(failed(client.call(simple().to_string()).await).code, "openai_decisions_unmappable_answer");
    }
}

/// An account out of credit (OpenAI's 429 `credit_balance_exhausted`, as a live call returned
/// it) or over its billing limit (`insufficient_quota`) refuses the call as the permanent
/// `account_not_entitled`, so nothing retries an account that can't pay.
#[tokio::test]
async fn out_of_credit_is_the_permanent_account_refusal() {
    for openai_code in ["credit_balance_exhausted", "insufficient_quota"] {
        let body = format!(r#"{{"error":{{"code":"{openai_code}","type":"insufficient_quota"}}}}"#);
        let (_, client) = start(vec![(429, body.clone(), None)]).await;
        let reply = client.call(simple().to_string()).await;
        let ProviderReply::ClassifiedError {
            error,
            provider_code,
            may_have_spent,
        } = reply
        else {
            panic!("not a classified error: {reply:?}");
        };
        assert_eq!(provider_code.as_deref(), Some(openai_code));
        assert!(!may_have_spent);
        let error = serde_json::to_value(error).unwrap();
        assert_eq!(error["code"], "account_not_entitled", "{openai_code}");
        assert_eq!(error["class"], "permanent", "{openai_code}");

        // Met on the call's first provider call, it refuses the whole call at admission.
        let (_, client) = start(vec![(429, body, None)]).await;
        let service = service(client, Some("decision"));
        let refusal = service.spawn_call(params(), scope()).await.unwrap_err();
        let refusal: Value = serde_json::from_slice(&refusal).unwrap();
        assert_eq!(refusal["code"], "account_not_entitled", "{openai_code}");
    }
}

#[tokio::test]
async fn http_errors_are_classified_at_source_and_keep_codes_and_retry_after() {
    for (status, class, code) in [
        (429, "transient", "rate_limited"),
        (500, "transient", "provider_error"),
        (503, "transient", "provider_error"),
        (401, "auth_required", "auth_failed"),
        (403, "permanent", "invalid_item"),
        (400, "permanent", "invalid_item"),
    ] {
        let (stub, client) = start(vec![(
            status,
            r#"{"error":{"code":"scripted_openai_code","message":"not used for classification"}}"#
                .into(),
            Some("7".into()),
        )])
        .await;
        let reply = client.call(simple().to_string()).await;
        let ProviderReply::ClassifiedError {
            error,
            provider_code,
            ..
        } = reply
        else {
            panic!("not a classified error: {reply:?}");
        };
        assert_eq!(provider_code.as_deref(), Some("scripted_openai_code"));
        let error = serde_json::to_value(error).unwrap();
        assert_eq!(error["class"], class, "HTTP {status}");
        assert_eq!(error["code"], code, "HTTP {status}");
        assert_eq!(error["retry_after_ms"], 7000);
        assert!(error["message"]
            .as_str()
            .unwrap()
            .contains("scripted_openai_code"));
        assert_eq!(stub.received.lock().unwrap().len(), 1);
    }
    let (_, client) = start(vec![(
        401,
        r#"{"error":{"code":"invalid_api_key"}}"#.into(),
        None,
    )])
    .await;
    let service = service(client, Some("decision"));
    let refusal = service.spawn_call(params(), scope()).await.unwrap_err();
    let refusal: Value = serde_json::from_slice(&refusal).unwrap();
    assert_eq!(refusal["code"], "auth_failed");
    assert_eq!(refusal["detail"]["class"], "auth_required");
    assert!(refusal["message"]
        .as_str()
        .unwrap()
        .contains("invalid_api_key"));
}

#[tokio::test]
async fn unreported_usage_directions_stay_absent() {
    for (suffix, expected) in [
        ("", json!({})),
        (
            r#", "usage":{"output_tokens":3}"#,
            json!({"output_tokens":3}),
        ),
        (r#", "usage":{"input_tokens":9}"#, json!({"input_tokens":9})),
    ] {
        let body = format!(
            r#"{{"answers":[{{"name":"q","type":"predicate","probability":0.10}}]{suffix}}}"#
        );
        let (_, client) = start(vec![(200, body, None)]).await;
        let service = service(client, Some("decision"));
        let reply: Value = serde_json::from_str(&admitted(&service, params()).await).unwrap();
        assert_eq!(reply["usage"], expected);
        if expected == json!({}) {
            assert!(reply["items"][0].get("usage").is_none());
        } else {
            assert_eq!(reply["items"][0]["usage"], expected);
        }
    }
}

#[tokio::test]
async fn missing_catalog_row_and_nondecision_row_refuse_without_http() {
    for (kind, code) in [
        (None, "model_unknown"),
        (Some("chat"), "model_not_classifier"),
    ] {
        let (stub, client) = start(vec![]).await;
        let service = service(client, kind);
        let describe = serde_json::to_value(service.describe()).unwrap();
        assert!(describe["models"].as_array().unwrap().is_empty());
        let error = service.spawn_call(params(), scope()).await.unwrap_err();
        assert_eq!(
            serde_json::from_slice::<Value>(&error).unwrap()["code"],
            code
        );
        assert!(stub.received.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn predicate_criteria_single_keys_and_no_criteria_have_exact_instruction_bytes() {
    for (criteria, second) in [
        (json!({"true":"yes"}), Some("Answer true when: yes")),
        (json!({"false":"no"}), Some("Answer false when: no")),
        (Value::Null, None),
    ] {
        let (stub, client) = start(vec![(200, PREDICATE.into(), None)]).await;
        let mut request = simple();
        if !criteria.is_null() {
            request["questions"]["q"]["criteria"] = criteria;
        }
        answered(client.call(request.to_string()).await);
        let received = stub.received.lock().unwrap();
        let body: Value = serde_json::from_str(&received[0].1).unwrap();
        assert_eq!(body["input"][0]["content"][0]["text"], "evidence");
        let instructions = &body["questions"][0]["instructions"];
        match second {
            Some(second) => assert_eq!(
                instructions,
                &json!([{"type":"input_text","text":"Is it true?"},{"type":"input_text","text":second}])
            ),
            None => assert_eq!(instructions, "Is it true?"),
        }
    }
}

#[tokio::test]
async fn unexpected_cache_usage_warns_once_and_keeps_answers() {
    let unexpected = r#"{"answers":[{"name":"q","type":"predicate","probability":0.10}],"usage":{"input_tokens":100,"input_tokens_details":{"cached_tokens":3,"cache_write_tokens":5}}}"#;
    let zero = r#"{"answers":[{"name":"q","type":"predicate","probability":0.10}],"usage":{"input_tokens":100,"input_tokens_details":{"cached_tokens":0,"cache_write_tokens":0}}}"#;
    let (_, client) = start(vec![
        (200, unexpected.into(), None),
        (200, zero.into(), None),
        (200, PREDICATE.into(), None),
    ])
    .await;
    let (_, events) = broca_test_support::capture_logs(async {
        for expected in [
            json!({"input_tokens":100}),
            json!({"input_tokens":100}),
            Value::Null,
        ] {
            let reply: Value =
                serde_json::from_str(&answered(client.call(simple().to_string()).await)).unwrap();
            assert_eq!(reply["usage"], expected);
            assert_eq!(reply["answers"]["q"]["noul"], json!(0.10));
        }
    })
    .await;
    let warnings: Vec<_> = events
        .iter()
        .filter(|event| event.level == tracing::Level::WARN && event.target == "system_one")
        .collect();
    assert_eq!(warnings.len(), 1, "{warnings:?}");
    assert_eq!(warnings[0].fields["model"], "gpt-6-luna");
    assert_eq!(warnings[0].fields["cached_tokens"], "Some(3)");
    assert_eq!(warnings[0].fields["cache_write_tokens"], "Some(5)");
    assert!(warnings[0].message().contains("disabled prompt caching"));
}

#[tokio::test]
async fn question_bound_is_400_and_invalid_images_keep_module_validation() {
    let (stub, client) = start(vec![]).await;
    let service = service(client, Some("decision"));
    let describe = service.describe();
    assert_eq!(describe.models.len(), 1);
    assert_eq!(describe.models[0].max_questions, 400);
    assert_eq!(describe.models[0].context_tokens, None);
    let mut request: Value = serde_json::from_str(&params()).unwrap();
    request["questions"] = Value::Object(
        (0..401)
            .map(|i| (format!("q{i}"), json!({"type":"noul","instructions":"?"})))
            .collect(),
    );
    let error = service
        .clone()
        .spawn_call(request.to_string(), scope())
        .await
        .unwrap_err();
    let error: Value = serde_json::from_slice(&error).unwrap();
    assert_eq!(error["code"], "invalid_params");
    assert_eq!(error["detail"]["field"], "questions");
    let mut request: Value = serde_json::from_str(&params()).unwrap();
    request["items"][0]["images"] = json!(["data:image/png;base64,not-base64"]);
    let error = service
        .spawn_call(request.to_string(), scope())
        .await
        .unwrap_err();
    let error: Value = serde_json::from_slice(&error).unwrap();
    assert_eq!(error["code"], "invalid_params");
    assert_eq!(error["detail"]["field"], "items[0].images[0]");
    assert!(stub.received.lock().unwrap().is_empty());
}

#[test]
fn retry_after_dates_and_seconds_are_preserved_without_guessing() {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::RETRY_AFTER,
        "Wed, 21 Oct 2015 07:28:00 GMT".parse().unwrap(),
    );
    assert_eq!(
        super::openai_decisions::retry_after_ms(&headers, 1_445_412_450_000),
        Some(30_000)
    );
    assert_eq!(
        super::openai_decisions::retry_after_ms(&headers, 1_445_412_500_000),
        Some(0)
    );
    headers.insert(reqwest::header::RETRY_AFTER, "12".parse().unwrap());
    assert_eq!(
        super::openai_decisions::retry_after_ms(&headers, 0),
        Some(12_000)
    );
    for bad in ["soon", "-1", "18446744073709551615"] {
        headers.insert(reqwest::header::RETRY_AFTER, bad.parse().unwrap());
        assert_eq!(super::openai_decisions::retry_after_ms(&headers, 0), None);
    }
}

#[test]
fn openai_uses_existing_boot_url_and_vault_id_configuration() {
    let overrides = crate::provider_overrides::ProviderOverrides::parse(
        r#"{"providers":{"openai":{"base_url":"http://proxy.example/v1"}}}"#,
    )
    .unwrap();
    let client = OpenAiDecisions::new(
        overrides.base_url("openai"),
        Arc::new(Token),
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(client.egress_host(), "proxy.example");
    let default = OpenAiDecisions::new(None, Arc::new(Token), Duration::from_secs(1)).unwrap();
    assert_eq!(default.egress_host(), "api.openai.com");
    let aliases = crate::VaultCredentialIds::parse(
        r#"{"vault":{"credential_ids":{"apikey:openai":"apikey:openai:decisions"}}}"#,
    )
    .unwrap();
    let addressing = crate::VaultAddressing::new(aliases);
    let key = crate::CredentialId::parse(CREDENTIAL_ID).unwrap();
    assert_eq!(addressing.vault_id_for(&key), "apikey:openai:decisions");
}
