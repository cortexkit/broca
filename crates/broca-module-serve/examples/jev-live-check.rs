//! Live check of System One's TypeSafe client against Jev 1.13.0, at most five requests.
//!
//! WHAT IT SENDS. Up to five `POST https://api.typesafe.ai/v1/systemone` requests, one item
//! each, through the same client `ck-broca` uses for `classify.run` (`system_one::typesafe`):
//! a yes/no question, a choice, a score, all three together on structured state, and one with a
//! long state. `--count` sends fewer. Nothing is retried; a refused request is reported with
//! the classifier code broca would give it.
//!
//! RUN (on a machine with a vault and the dev-tools handle file):
//!   cargo run -p broca-module-serve --example jev-live-check -- --dry-run
//!   cargo run -p broca-module-serve --example jev-live-check -- --live
//! `--dry-run` (the default) checks that the handle file names `apikey:typesafe`, prints every
//! request body, and sends nothing; it never contacts the vault.
//!
//! OUTPUT. Per request: the HTTP outcome, the item code broca would record for it, the answer
//! text exactly as received, and the billed usage (`input_tokens`, `output_tokens`, each
//! "absent" when TypeSafe did not report it) with its charge at the catalog price of 0.042 USD
//! per million input tokens (output tokens are free). Then the totals.
//!
//! CREDENTIAL. The TypeSafe key comes from the credentials module over subc, by the handle
//! stored for `apikey:typesafe` in broca's `dev-tools-handles.json` under the config home
//! (override: `BROCA_DEVTOOLS_HANDLES`). It travels only in the `authorization` header and is
//! never printed: every printed line is checked for it first.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_module_serve::system_one::typesafe::{CredentialSource, TypeSafe, CREDENTIAL_ID};
use broca_module_serve::system_one::{ClassifierProvider, ProviderReply};
use broca_module_serve::vault_config::load_devtools_handle;
use broca_module_serve::CredentialId;
use clap::Parser;
use serde_json::json;

/// The most requests one run may send.
const MAX_REQUESTS: usize = 5;
/// Jev 1.13.0's catalog input price, nanodollars per million tokens.
const INPUT_NANOS_PER_MTOK: u64 = 42_000_000;

#[derive(Parser)]
#[command(about = "Send at most five Jev requests through broca's TypeSafe client")]
struct Args {
    /// Print the request bodies; send nothing (the default).
    #[arg(long, conflicts_with = "live")]
    dry_run: bool,
    /// Fetch the credential and send.
    #[arg(long)]
    live: bool,
    /// How many of the five requests to send.
    #[arg(long, default_value_t = MAX_REQUESTS)]
    count: usize,
    /// The subc daemon connection file.
    #[arg(long)]
    subc: Option<PathBuf>,
    /// Credentials module id on subc.
    #[arg(long, default_value = "claustrum")]
    credentials_module: String,
    /// Per-request timeout, in seconds.
    #[arg(long, default_value_t = 60)]
    request_timeout_secs: u64,
    /// The `model` value to send. broca sends the versioned id; pass `jev-latest` to see what
    /// the alias resolves to.
    #[arg(long, default_value = "jev-1.13.0")]
    model: String,
}

fn plan(model: &str) -> Vec<(&'static str, String)> {
    let noul =
        json!({"flagged": {"type": "noul", "instructions": "Does the message ask for a refund?"}});
    let choice = json!({"topic": {"type": "choice", "instructions": "What is the message about?",
        "criteria": {"billing": "Payments or invoices", "bug": "Something is broken", "other": null}}});
    let score = json!({"urgency": {"type": "score", "instructions": "How urgent is the message?",
        "criteria": ["not urgent", "soon", "today", "right now"]}});
    let mut all = noul.as_object().unwrap().clone();
    all.extend(choice.as_object().unwrap().clone());
    all.extend(score.as_object().unwrap().clone());
    let body = |state: serde_json::Value, questions: &serde_json::Value| {
        json!({"model": model, "state": state, "questions": questions}).to_string()
    };
    vec![
        (
            "noul",
            body(
                json!("I was charged twice this month, please give me my money back."),
                &noul,
            ),
        ),
        (
            "choice",
            body(
                json!("The export button does nothing when I click it."),
                &choice,
            ),
        ),
        (
            "score",
            body(json!("Production is down for every customer."), &score),
        ),
        (
            "all-structured",
            body(
                json!({"from": "customer", "subject": "Invoice 42", "body": "The invoice total is wrong and I need it fixed before Friday."}),
                &serde_json::Value::Object(all),
            ),
        ),
        (
            "long-state",
            body(
                json!("Please review my account history. ".repeat(200)),
                &noul,
            ),
        ),
    ]
}

/// The token, held for the client and checked against every printed line.
struct Token(String);

#[async_trait]
impl CredentialSource for Token {
    fn credential_id(&self) -> Option<String> {
        Some(CREDENTIAL_ID.to_owned())
    }
    async fn bearer(&self) -> Result<Option<String>, String> {
        Ok(Some(self.0.clone()))
    }
}

fn default_subc_path() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    home.join(".local/share/cortexkit/run/subc-connection.json")
}

async fn fetch_token(args: &Args, handle: &str) -> Result<String, String> {
    let subc = args.subc.clone().unwrap_or_else(default_subc_path);
    let conn = broca_subc::SubcConnection::connect(&subc)
        .await
        .map_err(|e| format!("connect subc ({}): {e}", subc.display()))?;
    // The vault authorizes by the credential handle; subc only needs an existing project root.
    let cwd = std::env::current_dir().map_err(|e| format!("current dir: {e}"))?;
    let identity = broca_protocol::BindIdentity::new(cwd, "jev-live-check", "jev-live-check");
    let client = broca_subc::VaultClient::new(
        Arc::new(conn),
        &args.credentials_module,
        &identity,
        Duration::from_secs(10),
    );
    client
        .get(handle, None, false)
        .await
        .result
        .map(|token| token.token)
        .map_err(|e| format!("vault credential.get: {e}"))
}

/// Print a line, with the secret replaced should it ever appear.
fn say(secret: Option<&str>, line: String) {
    match secret.filter(|secret| !secret.is_empty()) {
        Some(secret) if line.contains(secret) => println!("{}", line.replace(secret, "[REDACTED]")),
        _ => println!("{line}"),
    }
}

#[derive(serde::Deserialize, Default)]
struct UsageView {
    #[serde(default)]
    input_tokens: Option<u64>,
    #[serde(default)]
    output_tokens: Option<u64>,
}

#[derive(serde::Deserialize)]
struct AnswerView<'a> {
    #[serde(borrow)]
    answers: &'a serde_json::value::RawValue,
    #[serde(default)]
    usage: Option<UsageView>,
    #[serde(default)]
    model: Option<String>,
}

fn show(count: Option<u64>) -> String {
    count.map_or("absent".to_owned(), |n| n.to_string())
}

#[tokio::main]
async fn main() -> Result<(), String> {
    let args = Args::parse();
    if args.count == 0 || args.count > MAX_REQUESTS {
        return Err(format!("--count is 1 to {MAX_REQUESTS}"));
    }
    let plan: Vec<_> = plan(&args.model).into_iter().take(args.count).collect();
    let credential =
        CredentialId::parse(CREDENTIAL_ID).ok_or("the fixed credential id does not parse")?;
    // Both modes look the handle up first, so a dry run also proves a live run will find it.
    // The handle is itself a bearer secret and is never printed.
    let handle = load_devtools_handle(&credential)?;
    println!("credential {CREDENTIAL_ID}: handle present in the dev-tools handle file");
    if !args.live {
        drop(handle);
        for (name, body) in &plan {
            println!("request {name} ({} bytes): {body}", body.len());
        }
        println!("dry run: 0 requests sent, vault not contacted");
        return Ok(());
    }
    let token = fetch_token(&args, &handle).await?;
    drop(handle);
    let secret = Some(token.clone());
    let client = TypeSafe::new(
        None,
        Arc::new(Token(token)),
        Duration::from_secs(args.request_timeout_secs),
    )?;
    say(
        secret.as_deref(),
        format!("endpoint: {}", client.endpoint()),
    );
    let (mut input_total, mut output_total, mut unreported) = (0u64, 0u64, 0usize);
    for (sent, (name, body)) in plan.into_iter().enumerate() {
        assert!(sent < MAX_REQUESTS, "the request cap is enforced");
        let started = std::time::Instant::now();
        let reply = client.call(body).await;
        let elapsed = started.elapsed().as_millis();
        let line = match reply {
            ProviderReply::Answered(bytes) | ProviderReply::AttributedAnswer { body: bytes, .. } => match serde_json::from_slice::<AnswerView<'_>>(&bytes)
            {
                Ok(answer) => {
                    let usage = answer.usage.unwrap_or_default();
                    match usage.input_tokens {
                        Some(tokens) => input_total += tokens,
                        None => unreported += 1,
                    }
                    output_total += usage.output_tokens.unwrap_or(0);
                    let charge = usage.input_tokens.map(|tokens| {
                        cortexkit_model_batch::estimate::nanos_to_usd_text(
                            (u128::from(tokens) * u128::from(INPUT_NANOS_PER_MTOK))
                                .div_ceil(1_000_000) as u64,
                        )
                    });
                    format!(
                        "{name}: HTTP 200 in {elapsed} ms; answered by {} (sent {}); input_tokens {}, output_tokens {}, charge {} USD; answers {}",
                        answer.model.as_deref().unwrap_or("(no model named)"),
                        args.model,
                        show(usage.input_tokens),
                        show(usage.output_tokens),
                        charge.unwrap_or_else(|| "absent".into()),
                        answer.answers.get()
                    )
                }
                Err(error) => format!(
                    "{name}: HTTP 200 in {elapsed} ms but the reply did not decode ({error}), so broca would record provider_error; body {}",
                    String::from_utf8_lossy(&bytes)
                ),
            },
            ProviderReply::Status {
                status,
                retry_after_ms,
            } => format!(
                "{name}: HTTP {status} in {elapsed} ms (retry-after {}), so broca would record {}",
                retry_after_ms.map_or("none".to_owned(), |ms| format!("{ms} ms")),
                cortexkit_role_classifier::provider::item_code_for_status(status)
                    .unwrap_or("provider_error")
            ),
            ProviderReply::Transport(message) | ProviderReply::Unconnected(message) => format!(
                "{name}: no reply in {elapsed} ms ({message}), so broca would record provider_error"
            ),
            // Only the Workers AI client answers this; the TypeSafe client never does.
            ProviderReply::WorkersAiError { status, .. } => format!(
                "{name}: HTTP {status} in {elapsed} ms read as a Workers AI error, which this client never returns"
            ),
            ProviderReply::Credential(message) => {
                format!("{name}: credential unavailable ({message}); nothing sent")
            }
        };
        say(secret.as_deref(), line);
    }
    say(
        secret.as_deref(),
        format!(
            "totals: input_tokens {input_total}, output_tokens {output_total}, {unreported} answered request(s) reported no input count"
        ),
    );
    Ok(())
}
