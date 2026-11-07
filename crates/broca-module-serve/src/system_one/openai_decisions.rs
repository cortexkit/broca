//! OpenAI Decisions, translated at the provider boundary so the classifier role keeps its
//! own question and answer shapes. Numeric answer values remain raw JSON throughout.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use cortexkit_role_classifier::errors::{self, ErrorClass, ItemError};
use serde::Deserialize;
use serde_json::{json, value::RawValue, Value};

use super::credential::CredentialSource;
use super::{ClassifierProvider, ProviderReply};

pub const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
pub const PROVIDER_ID: &str = "openai";
pub const CREDENTIAL_ID: &str = "apikey:openai";
pub const MODEL: &str = "gpt-6-luna";
pub const MAX_QUESTIONS: u64 = 400;
// A module safety bound, not an assertion about OpenAI's image capacity. The catalog may
// advertise a tighter bound; the shared classifier image validation still applies.
pub const MAX_IMAGES: u64 = 4;

pub struct OpenAiDecisions {
    http: reqwest::Client,
    endpoint: url::Url,
    credential: Arc<dyn CredentialSource>,
    attempt_timeout: Duration,
}

impl OpenAiDecisions {
    pub fn new(
        base_url: Option<&str>,
        credential: Arc<dyn CredentialSource>,
        attempt_timeout: Duration,
    ) -> Result<Self, String> {
        let base = base_url.unwrap_or(DEFAULT_BASE_URL).trim_end_matches('/');
        let endpoint = url::Url::parse(&format!("{base}/decisions"))
            .map_err(|e| format!("OpenAI Decisions base URL {base:?}: {e}"))?;
        if !matches!(endpoint.scheme(), "http" | "https") || endpoint.host_str().is_none() {
            return Err(format!(
                "OpenAI Decisions base URL {base:?} must name an HTTP(S) host"
            ));
        }
        Ok(Self {
            http: broca_provider::transport::build_provider_http_client()
                .without_response_decoding()
                .clone(),
            endpoint,
            credential,
            attempt_timeout,
        })
    }
}

type Object = BTreeMap<String, Box<RawValue>>;

#[derive(Deserialize)]
struct Request {
    model: String,
    state: Box<RawValue>,
    questions: BTreeMap<String, Question>,
    #[serde(default)]
    images: Vec<Value>,
}

#[derive(Deserialize)]
struct Question {
    #[serde(rename = "type")]
    kind: String,
    instructions: Value,
    criteria: Option<Box<RawValue>>,
}

fn text(value: &Value) -> String {
    value
        .as_str()
        .map(str::to_owned)
        .unwrap_or_else(|| value.to_string())
}

fn request_body(request: &Request) -> Result<Value, String> {
    if !(1..=MAX_QUESTIONS as usize).contains(&request.questions.len()) {
        return Err("OpenAI Decisions requires 1 to 400 questions".into());
    }
    let mut questions = Vec::new();
    for (name, q) in &request.questions {
        let mut question = json!({"name": name, "instructions": text(&q.instructions)});
        match q.kind.as_str() {
            "noul" => {
                question["type"] = json!("predicate");
                if let Some(criteria) = &q.criteria {
                    let criteria: BTreeMap<String, String> =
                        serde_json::from_str(criteria.get())
                            .map_err(|e| format!("question {name} predicate criteria: {e}"))?;
                    let mut lines = Vec::new();
                    for (key, label) in [("true", "true"), ("false", "false")] {
                        if let Some(description) = criteria.get(key) {
                            lines.push(format!("Answer {label} when: {description}"));
                        }
                    }
                    if !lines.is_empty() {
                        question["instructions"] = json!([
                            {"type":"input_text", "text":text(&q.instructions)},
                            {"type":"input_text", "text":lines.join("\n")}
                        ]);
                    }
                }
            }
            "choice" => {
                question["type"] = json!("choice");
                let criteria: BTreeMap<String, Value> = serde_json::from_str(required_criteria(q)?)
                    .map_err(|e| format!("question {name} choice criteria: {e}"))?;
                // A null description means the option has none, so it's left out rather than
                // sent as the text "null".
                question["choices"] = criteria
                    .into_iter()
                    .map(|(value, description)| {
                        if description.is_null() {
                            json!({"value":value})
                        } else {
                            json!({"value":value, "description":text(&description)})
                        }
                    })
                    .collect();
            }
            "score" => {
                question["type"] = json!("score");
                let criteria: Vec<Value> = serde_json::from_str(required_criteria(q)?)
                    .map_err(|e| format!("question {name} score criteria: {e}"))?;
                question["levels"] = criteria.iter().enumerate().map(|(index, description)| {
                    json!({"label":index.to_string(), "description":text(description)})
                }).collect();
            }
            kind => return Err(format!("question {name} has unsupported type {kind}")),
        }
        questions.push(question);
    }
    // Structured states are evidence, not chat messages or tool calls. Keeping their JSON
    // text in a text part avoids interpreting caller-owned keys as OpenAI protocol fields.
    let state: Value = serde_json::from_str(request.state.get()).map_err(|e| e.to_string())?;
    let mut content = vec![json!({"type":"input_text", "text": if state.is_string() {
        text(&state)
    } else {
        request.state.get().to_owned()
    }})];
    for image in &request.images {
        let url = image
            .as_str()
            .ok_or("OpenAI Decisions expected a resolved image URL")?;
        content.push(json!({"type":"input_image", "image_url":url}));
    }
    // Inputs differ per item; disable caching so all input uses the frozen input rate,
    // rather than silently paying cache-write rates that batch billing does not model.
    Ok(
        json!({"model":request.model, "input":[{"role":"user", "content":content}], "questions":questions,
        "prompt_cache_options":{"mode":"explicit"}}),
    )
}

fn required_criteria(q: &Question) -> Result<&str, String> {
    q.criteria
        .as_ref()
        .map(|value| value.get())
        .ok_or_else(|| "question has no criteria".into())
}

fn raw<T: serde::Serialize>(value: &T) -> Box<RawValue> {
    serde_json::value::to_raw_value(value).expect("JSON values serialize")
}

fn field<'a>(object: &'a Object, key: &str) -> Result<&'a RawValue, String> {
    object
        .get(key)
        .map(Box::as_ref)
        .ok_or_else(|| format!("missing required field {key}"))
}

fn string(object: &Object, key: &str) -> Result<String, String> {
    serde_json::from_str(field(object, key)?.get()).map_err(|_| format!("{key} must be a string"))
}

fn number(object: &Object, key: &str) -> Result<Box<RawValue>, String> {
    let value = field(object, key)?;
    serde_json::from_str::<f64>(value.get()).map_err(|_| format!("{key} must be a number"))?;
    Ok(value.to_owned())
}

fn map_answer(q: &Question, answer: &Object) -> Result<Object, String> {
    let expected = if q.kind == "noul" {
        "predicate"
    } else {
        &q.kind
    };
    if string(answer, "type")? != expected {
        return Err(format!("answer type does not match {expected}"));
    }
    let mut mapped = Object::from([("type".into(), raw(&q.kind))]);
    if q.kind == "noul" {
        mapped.insert("noul".into(), number(answer, "probability")?);
        return Ok(mapped);
    }
    mapped.insert("confidence".into(), number(answer, "confidence")?);
    let keys: Vec<String> = if q.kind == "choice" {
        let criteria: Object =
            serde_json::from_str(required_criteria(q)?).map_err(|e| e.to_string())?;
        let choice = string(answer, "choice")?;
        if !criteria.contains_key(&choice) {
            return Err("choice was not in the requested criteria".into());
        }
        mapped.insert("choice".into(), field(answer, "choice")?.to_owned());
        criteria.into_keys().collect()
    } else {
        let levels: Vec<Box<RawValue>> =
            serde_json::from_str(required_criteria(q)?).map_err(|e| e.to_string())?;
        let legend: Object = levels
            .into_iter()
            .enumerate()
            .map(|(i, value)| (i.to_string(), value))
            .collect();
        mapped.insert("score".into(), number(answer, "score")?);
        mapped.insert("legend".into(), raw(&legend));
        legend.into_keys().collect()
    };
    let entries: Vec<Object> = serde_json::from_str(field(answer, "probabilities")?.get())
        .map_err(|_| "probabilities must be an array".to_owned())?;
    let mut probabilities = Object::new();
    for entry in entries {
        let key = if q.kind == "choice" {
            string(&entry, "value")?
        } else {
            let index: u64 = serde_json::from_str(field(&entry, "value")?.get())
                .map_err(|_| "score probability value must be an integer".to_owned())?;
            let key = index.to_string();
            if string(&entry, "label")? != key {
                return Err("score probability label does not match its level".into());
            }
            key
        };
        if !keys.contains(&key)
            || probabilities
                .insert(key, number(&entry, "probability")?)
                .is_some()
        {
            return Err("probabilities contain an unknown or duplicate option".into());
        }
    }
    if probabilities.len() != keys.len() {
        return Err("probabilities omit a requested option".into());
    }
    mapped.insert("probabilities".into(), raw(&probabilities));
    Ok(mapped)
}

fn map_response(request: &Request, body: &[u8]) -> Result<Vec<u8>, String> {
    let mut response: Object = serde_json::from_slice(body).map_err(|e| e.to_string())?;
    let answers: Vec<Object> = serde_json::from_str(field(&response, "answers")?.get())
        .map_err(|_| "answers must be an array".to_owned())?;
    let mut mapped = Object::new();
    for answer in answers {
        let name = string(&answer, "name")?;
        let question = request
            .questions
            .get(&name)
            .ok_or_else(|| format!("unknown answer name {name}"))?;
        if mapped.contains_key(&name) {
            return Err(format!("duplicate answer name {name}"));
        }
        let value = map_answer(question, &answer).map_err(|e| format!("answer {name}: {e}"))?;
        mapped.insert(name, raw(&value));
    }
    if mapped.len() != request.questions.len() {
        return Err("answers omit a requested question".into());
    }
    response.insert("answers".into(), raw(&mapped));
    if let Some(usage) = response.get("usage") {
        let usage: Value = serde_json::from_str(usage.get()).map_err(|e| e.to_string())?;
        let cached_tokens = usage["input_tokens_details"]["cached_tokens"].as_u64();
        let cache_write_tokens = usage["input_tokens_details"]["cache_write_tokens"].as_u64();
        if cached_tokens.is_some_and(|count| count > 0)
            || cache_write_tokens.is_some_and(|count| count > 0)
        {
            tracing::warn!(target: "system_one", model = %request.model, ?cached_tokens, ?cache_write_tokens,
                "OpenAI Decisions reported cache tokens despite disabled prompt caching; input-only billing may differ");
        }
        // Only the role's existing token directions cross the reply boundary.
        let mut reported = Object::new();
        let original: Object =
            serde_json::from_str(field(&response, "usage")?.get()).map_err(|e| e.to_string())?;
        for key in ["input_tokens", "output_tokens"] {
            if let Some(value) = original.get(key) {
                reported.insert(key.into(), value.clone());
            }
        }
        response.insert("usage".into(), raw(&reported));
    }
    serde_json::to_vec(&response).map_err(|e| e.to_string())
}

fn mapping_error(message: String) -> ProviderReply {
    let mut error = ItemError::for_code(
        errors::PROVIDER_REFUSED,
        format!("OpenAI Decisions unmappable answer: {message}"),
    );
    error.code = "openai_decisions_unmappable_answer".into();
    ProviderReply::ClassifiedError {
        error,
        provider_code: None,
        may_have_spent: true,
    }
}

/// The error for a non-2xx answer. Its code is the shared OpenAI mapping (`cortexkit-model-batch`
/// `RULES.md` §15): an account out of credit or over its billing limit is the permanent
/// `account_not_entitled`, since it ends only when someone pays, and every other status maps as
/// for any provider. Its class follows the code, except that a sign-in failure asks for
/// authentication.
fn http_error(status: u16, retry_after_ms: Option<u64>, body: &[u8]) -> ProviderReply {
    let value: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let provider_code = value["error"]["code"].as_str().map(str::to_owned);
    let code = cortexkit_model_batch::status::openai_item_code(status, provider_code.as_deref())
        .unwrap_or(errors::PROVIDER_ERROR);
    let mut error = ItemError::for_code(
        code,
        format!(
            "OpenAI Decisions answered HTTP {status}; error code {}",
            provider_code.as_deref().unwrap_or("unreported")
        ),
    );
    if code == errors::AUTH_FAILED {
        error.class = ErrorClass::Other("auth_required".into());
    }
    error.retry_after_ms = retry_after_ms;
    ProviderReply::ClassifiedError {
        error,
        provider_code,
        // Only a provider error (a timeout or a server failure) may have reached a billed
        // model, so only then may the call have cost money; every other refusal came before any
        // model ran.
        may_have_spent: code == errors::PROVIDER_ERROR,
    }
}

/// OpenAI's standard Retry-After header may name delay-seconds or an HTTP date.
pub(super) fn retry_after_ms(headers: &reqwest::header::HeaderMap, now_ms: i64) -> Option<u64> {
    let value = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim();
    if let Ok(seconds) = value.parse::<u64>() {
        return seconds.checked_mul(1000);
    }
    let date = chrono::DateTime::parse_from_rfc2822(value).ok()?;
    Some(date.timestamp_millis().saturating_sub(now_ms).max(0) as u64)
}

#[async_trait]
impl ClassifierProvider for OpenAiDecisions {
    fn with_credential(
        &self,
        credential: Arc<dyn CredentialSource>,
    ) -> Option<Arc<dyn ClassifierProvider>> {
        Some(Arc::new(Self {
            http: self.http.clone(),
            endpoint: self.endpoint.clone(),
            credential,
            attempt_timeout: self.attempt_timeout,
        }))
    }
    fn egress_host(&self) -> String {
        self.endpoint.host_str().unwrap_or_default().to_owned()
    }
    fn credential_id(&self) -> Option<String> {
        self.credential.credential_id()
    }

    async fn call(&self, body: String) -> ProviderReply {
        let request: Request = match serde_json::from_str(&body) {
            Ok(request) => request,
            Err(error) => return mapping_error(format!("request did not decode: {error}")),
        };
        let body = match request_body(&request) {
            Ok(body) => body,
            Err(error) => return mapping_error(error),
        };
        let secret = match self.credential.secret().await {
            Ok(secret) => secret,
            Err(message) => return ProviderReply::Credential(message),
        };
        let mut call = self
            .http
            .post(self.endpoint.clone())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(body.to_string());
        if let Some(secret) = &secret {
            call = call.bearer_auth(&secret.token);
        }
        let attempt = async {
            let response = call.send().await?;
            let status = response.status().as_u16();
            let retry_after = retry_after_ms(response.headers(), super::now_ms());
            let body = response.bytes().await?;
            Ok::<_, reqwest::Error>((status, retry_after, body))
        };
        match tokio::time::timeout(self.attempt_timeout, attempt).await {
            Err(_) => ProviderReply::Transport(format!(
                "no reply within {} ms",
                self.attempt_timeout.as_millis()
            )),
            Ok(Err(error)) if error.is_connect() && !error.is_timeout() => {
                ProviderReply::Unconnected("the connection failed".into())
            }
            Ok(Err(_)) => ProviderReply::Transport("the request failed in transport".into()),
            Ok(Ok((200, _, body))) => match map_response(&request, &body) {
                Ok(body) => ProviderReply::AttributedAnswer {
                    body,
                    auth_selection: secret.as_ref().and_then(|s| s.auth_selection.clone()),
                    account_ref: secret.and_then(|s| s.account_id),
                },
                Err(message) => mapping_error(message),
            },
            Ok(Ok((status, retry_after_ms, body))) => http_error(status, retry_after_ms, &body),
        }
    }
}
