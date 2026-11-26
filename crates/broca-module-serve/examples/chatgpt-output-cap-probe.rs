//! Compare a capped and an uncapped ChatGPT Responses WebSocket request.
//! Dry-run is the default and never reads credentials or contacts the provider.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use broca_module_serve::{vault_config::load_devtools_handle, CredentialId};
use broca_protocol::BindIdentity;
use broca_provider::credential_hosts::CredentialHosts;
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{
        client::IntoClientRequest,
        extensions::{compression::deflate::DeflateConfig, ExtensionsConfig},
        protocol::WebSocketConfig,
        Error, Message,
    },
};

const ENDPOINT: &str = "wss://chatgpt.com/backend-api/codex/responses";
const PROMPT: &str = "Write a numbered list of 400 distinct one-sentence facts about the ocean.";
const DEFAULT_MODEL: &str = "gpt-6-luna";

fn bodies(model: &str) -> [Value; 2] {
    let control = json!({
        "type": "response.create", "model": model,
        "instructions": "You are a helpful assistant.",
        "input": [{"role":"user", "content":[{"type":"input_text", "text":PROMPT}]}],
        "stream": true, "store": false,
    });
    let mut capped = control.clone();
    capped["max_output_tokens"] = json!(2000);
    [capped, control]
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[derive(Default)]
struct Observation {
    status: Option<u16>,
    error: Option<Value>,
    finish_reason: Option<String>,
    incomplete_details: Option<Value>,
    usage: Option<Value>,
    text: String,
}

impl Observation {
    fn event(&mut self, event: &Value) -> bool {
        let kind = event["type"].as_str().unwrap_or_default();
        if kind == "response.output_text.delta" {
            if let Some(delta) = event["delta"].as_str() {
                self.text.push_str(delta);
            }
        }
        if kind == "error" {
            self.status = event["status"]
                .as_u64()
                .and_then(|n| u16::try_from(n).ok())
                .or(self.status);
            self.error = Some(event.clone());
            return true;
        }
        if matches!(
            kind,
            "response.completed" | "response.incomplete" | "response.failed"
        ) {
            let response = &event["response"];
            self.finish_reason = response["status"]
                .as_str()
                .map(str::to_owned)
                .or_else(|| Some(kind.to_owned()));
            self.incomplete_details = response
                .get("incomplete_details")
                .filter(|v| !v.is_null())
                .cloned();
            self.usage = response.get("usage").filter(|v| !v.is_null()).cloned();
            self.error = response.get("error").filter(|v| !v.is_null()).cloned();
            return true;
        }
        false
    }

    fn report(&self, label: &str, request_sha256: &str) -> Value {
        json!({
            "request": label, "status": self.status, "error": self.error,
            "finish_reason": self.finish_reason, "incomplete_details": self.incomplete_details,
            "usage": {"input_tokens": self.usage.as_ref().and_then(|v| v.get("input_tokens")),
                      "output_tokens": self.usage.as_ref().and_then(|v| v.get("output_tokens")),
                      "reasoning_tokens": self.usage.as_ref().and_then(|v| v.pointer("/output_tokens_details/reasoning_tokens"))},
            "output_characters": self.text.chars().count(),
            "output_prefix": self.text.chars().take(200).collect::<String>(),
            "request_sha256": request_sha256,
        })
    }
}

fn account_id(account: Option<&str>, token: &str) -> Option<String> {
    if let Some(account) = account.filter(|s| !s.is_empty()) {
        return Some(account.to_owned());
    }
    let payload = token.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    let value: Value = serde_json::from_slice(&decoded).ok()?;
    value
        .pointer("/https:~1~1api.openai.com~1auth/chatgpt_account_id")
        .or_else(|| value.get("chatgpt_account_id"))
        .or_else(|| value.pointer("/organizations/0/id"))
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
}

async fn attempt(body: &str, token: &str, account: &str, session: &str) -> Observation {
    let mut result = Observation::default();
    let run = async {
        let mut request = ENDPOINT
            .into_client_request()
            .map_err(|_| "invalid_endpoint")?;
        for (name, value) in [
            ("authorization", format!("Bearer {token}")),
            ("chatgpt-account-id", account.to_owned()),
            ("originator", "codex_cli_rs".to_owned()),
            (
                "user-agent",
                format!(
                    "codex_cli_rs/0.163.0 ({}; {})",
                    std::env::consts::OS,
                    std::env::consts::ARCH
                ),
            ),
            ("session-id", session.to_owned()),
            ("thread-id", session.to_owned()),
            ("x-client-request-id", session.to_owned()),
            ("openai-beta", "responses_websockets=2026-02-06".to_owned()),
        ] {
            let mut header = value
                .parse::<tokio_tungstenite::tungstenite::http::HeaderValue>()
                .map_err(|_| "invalid_header")?;
            header.set_sensitive(true);
            request.headers_mut().insert(name, header);
        }
        let mut extensions = ExtensionsConfig::default();
        extensions.permessage_deflate = Some(DeflateConfig::default());
        let mut config = WebSocketConfig::default();
        config.extensions = extensions;
        let (mut socket, response) =
            match connect_async_with_config(request, Some(config), false).await {
                Ok(pair) => pair,
                Err(Error::Http(response)) => {
                    result.status = Some(response.status().as_u16());
                    if let Some(bytes) = response.body() {
                        result.error =
                            Some(serde_json::from_slice(bytes).unwrap_or_else(
                                |_| json!({"message":String::from_utf8_lossy(bytes)}),
                            ));
                    }
                    return Ok(());
                }
                Err(_) => return Err("websocket_connect_failed"),
            };
        result.status = Some(response.status().as_u16());
        socket
            .send(Message::Text(body.to_owned().into()))
            .await
            .map_err(|_| "websocket_send_failed")?;
        while let Some(message) = socket.next().await {
            match message.map_err(|_| "websocket_read_failed")? {
                Message::Text(text) => {
                    let event: Value =
                        serde_json::from_str(&text).map_err(|_| "invalid_websocket_json")?;
                    if result.event(&event) {
                        return Ok(());
                    }
                }
                Message::Ping(payload) => socket
                    .send(Message::Pong(payload))
                    .await
                    .map_err(|_| "websocket_pong_failed")?,
                Message::Close(_) => return Err("websocket_closed_before_completion"),
                _ => {}
            }
        }
        Err("websocket_closed_before_completion")
    };
    match tokio::time::timeout(Duration::from_secs(900), run).await {
        Ok(Ok(())) => {}
        Ok(Err(message)) => result.error = Some(json!({"message": message})),
        Err(_) => result.error = Some(json!({"message":"probe_deadline_reached"})),
    }
    result
}

async fn live(requests: [Value; 2]) -> Result<(), &'static str> {
    let id = CredentialId::parse("chatgpt:openai").ok_or("invalid_credential_id")?;
    let handle =
        load_devtools_handle(&id).map_err(|_| "standing_devtools_credential_unavailable")?;
    let connection_file = std::env::var_os("BROCA_LIVE_SUBC")
        .or_else(|| std::env::var_os("SUBC_SOCKET"))
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                .join(".local/share/cortexkit/run/subc-connection.json")
        });
    let session = format!("chatgpt-output-cap-{}", std::process::id());
    let bind = BindIdentity::new(
        std::env::current_dir().map_err(|_| "project_directory_unavailable")?,
        "chatgpt-output-cap-probe",
        session.clone(),
    );
    let connection = broca_subc::SubcConnection::connect(&connection_file)
        .await
        .map_err(|_| "subc_connection_unavailable")?;
    let vault = broca_subc::VaultClient::new(
        Arc::new(connection),
        "claustrum",
        &bind,
        Duration::from_secs(15),
    );
    let secret = vault
        .get(&handle, Some(35 * 60 * 1000), false)
        .await
        .result
        .map_err(|_| "vault_credential_unavailable")?;
    let hosts = secret
        .hosts
        .unwrap_or_else(|| vec!["chatgpt.com".to_owned()]);
    CredentialHosts::from_record(Some(&hosts))
        .check_url(ENDPOINT)
        .map_err(|_| "credential_host_not_allowed")?;
    let account = account_id(secret.account_id.as_deref(), &secret.token)
        .ok_or("chatgpt_account_id_unavailable")?;
    for (label, body) in ["capped", "control"].into_iter().zip(requests) {
        let body = serde_json::to_string(&body).map_err(|_| "request_serialization_failed")?;
        let hash = sha256(body.as_bytes());
        let result = attempt(&body, &secret.token, &account, &session).await;
        println!("{}", result.report(label, &hash));
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    let mut model = DEFAULT_MODEL.to_owned();
    let mut mode = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--model" => match args
                .next()
                .filter(|s| !s.is_empty() && !s.starts_with("--"))
            {
                Some(value) => model = value.strip_prefix("openai/").unwrap_or(&value).to_owned(),
                None => {
                    eprintln!("probe_error=missing_model_argument");
                    std::process::exit(2);
                }
            },
            "--dry-run" | "--live" => {
                let live = arg == "--live";
                if mode.is_some_and(|previous| previous != live) {
                    eprintln!("probe_error=conflicting_execution_modes");
                    std::process::exit(2);
                }
                mode = Some(live);
            }
            "--help" | "-h" => {
                println!("chatgpt-output-cap-probe [--dry-run|--live] [--model MODEL]");
                return;
            }
            _ => {
                eprintln!("probe_error=unknown_argument");
                std::process::exit(2);
            }
        }
    }
    let requests = bodies(&model);
    if mode.unwrap_or(false) {
        if let Err(error) = live(requests).await {
            eprintln!("probe_error={error}");
            std::process::exit(1);
        }
    } else {
        for (label, body) in ["capped", "control"].into_iter().zip(requests) {
            println!("{}", json!({"request":label, "body":body}));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn requests_differ_only_by_the_output_cap() {
        let [mut capped, control] = bodies("test-model");
        assert_eq!(
            capped.as_object_mut().unwrap().remove("max_output_tokens"),
            Some(json!(2000))
        );
        assert_eq!(capped, control);
        assert_eq!(control["input"][0]["content"][0]["text"], PROMPT);
    }

    #[test]
    fn incomplete_response_keeps_usage_and_only_a_short_unicode_prefix() {
        let mut observation = Observation::default();
        observation.event(&json!({"type":"response.output_text.delta","delta":"🌊".repeat(201)}));
        assert!(observation.event(&json!({"type":"response.incomplete", "response":{
            "status":"incomplete", "incomplete_details":{"reason":"max_output_tokens"},
            "usage":{"input_tokens":7,"output_tokens":2000,"output_tokens_details":{"reasoning_tokens":42}}
        }})));
        let report = observation.report("capped", "hash");
        assert_eq!(report["output_characters"], 201);
        assert_eq!(
            report["output_prefix"].as_str().unwrap().chars().count(),
            200
        );
        assert_eq!(report["incomplete_details"]["reason"], "max_output_tokens");
        assert_eq!(report["usage"]["reasoning_tokens"], 42);
    }

    #[test]
    fn websocket_error_preserves_message_and_status() {
        let mut observation = Observation::default();
        assert!(observation.event(
            &json!({"type":"error", "status":400, "error":{"message":"unknown parameter"}})
        ));
        let report = observation.report("capped", "hash");
        assert_eq!(report["status"], 400);
        assert_eq!(report["error"]["error"]["message"], "unknown parameter");
    }
}
