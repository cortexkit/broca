//! Live check of System One's Workers AI client against Clef and Clef-flash, at most five
//! requests in all.
//!
//! WHAT IT SENDS. Up to five `POST
//! https://api.cloudflare.com/client/v4/accounts/{account_id}/ai/run/@cf/cloudflare/{model}`
//! requests, one item each, through the same client `ck-broca` uses for `classify.run`
//! (`system_one::workers_ai`): on Clef a yes/no question, a choice, and all three question types
//! on structured state; on Clef-flash a yes/no question and a score. `--count` sends fewer.
//! Nothing is retried; a refused request is reported with the classifier code broca would give
//! it.
//!
//! RUN (on a machine with a vault and the dev-tools handle file):
//!   cargo run -p broca-module-serve --example clef-live-check -- --dry-run
//!   cargo run -p broca-module-serve --example clef-live-check -- --live
//! `--dry-run` (the default) checks that the handle file names `apikey:cloudflare-workers-ai`,
//! prints every request body, and sends nothing; it never contacts the vault.
//!
//! OUTPUT. Per request: the model, the HTTP outcome, the item code broca would record for it,
//! the answer text exactly as received (the `result` member of Cloudflare's envelope), and the
//! billed usage (`input_tokens`, `output_tokens`, each "absent" when Cloudflare did not report
//! it) with its charge at the catalog price (Clef 0.24, Clef-flash 0.038 USD per million input
//! tokens; output tokens are free). Then the totals.
//!
//! CREDENTIAL. The token and the Cloudflare account id come from the credentials module over
//! subc, by the handle stored for `apikey:cloudflare-workers-ai` in broca's
//! `dev-tools-handles.json` under the config home (override: `BROCA_DEVTOOLS_HANDLES`). The
//! token travels only in the `authorization` header and the account id only in the URL; the
//! URL is never printed, and every printed line is checked for both values first.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_module_serve::system_one::credential::{CredentialSource, ProviderSecret};
use broca_module_serve::system_one::workers_ai::{
    WorkersAi, CLEF_FLASH_MODEL_PATH, CLEF_MODEL_PATH, CREDENTIAL_ID,
};
use broca_module_serve::system_one::{ClassifierProvider, ProviderReply};
use broca_module_serve::vault_config::load_devtools_handle;
use broca_module_serve::CredentialId;
use clap::Parser;
use serde_json::json;

/// The most requests one run may send, across both models.
const MAX_REQUESTS: usize = 5;
/// The catalog input prices, nanodollars per million tokens.
const CLEF_INPUT_NANOS_PER_MTOK: u64 = 240_000_000;
const CLEF_FLASH_INPUT_NANOS_PER_MTOK: u64 = 38_000_000;

#[derive(Parser)]
#[command(
    about = "Send at most five Clef and Clef-flash requests through broca's Workers AI client"
)]
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
}

#[derive(Clone, Copy, PartialEq)]
enum Model {
    Clef,
    ClefFlash,
}

impl Model {
    fn path(self) -> &'static str {
        match self {
            Model::Clef => CLEF_MODEL_PATH,
            Model::ClefFlash => CLEF_FLASH_MODEL_PATH,
        }
    }

    fn value(self) -> &'static str {
        match self {
            Model::Clef => "clef",
            Model::ClefFlash => "clef-flash",
        }
    }

    fn rate(self) -> u64 {
        match self {
            Model::Clef => CLEF_INPUT_NANOS_PER_MTOK,
            Model::ClefFlash => CLEF_FLASH_INPUT_NANOS_PER_MTOK,
        }
    }
}

fn plan() -> Vec<(&'static str, Model, String)> {
    let noul =
        json!({"flagged": {"type": "noul", "instructions": "Does the message ask for a refund?"}});
    let choice = json!({"topic": {"type": "choice", "instructions": "What is the message about?",
        "criteria": {"billing": "Payments or invoices", "bug": "Something is broken", "other": null}}});
    let score = json!({"urgency": {"type": "score", "instructions": "How urgent is the message?",
        "criteria": ["not urgent", "soon", "today", "right now"]}});
    let mut all = noul.as_object().unwrap().clone();
    all.extend(choice.as_object().unwrap().clone());
    all.extend(score.as_object().unwrap().clone());
    let all = serde_json::Value::Object(all);
    let body = |model: Model, state: serde_json::Value, questions: &serde_json::Value| {
        json!({"model": model.value(), "state": state, "questions": questions}).to_string()
    };
    let refund = json!("I was charged twice this month, please give me my money back.");
    vec![
        (
            "clef-noul",
            Model::Clef,
            body(Model::Clef, refund.clone(), &noul),
        ),
        (
            "clef-choice",
            Model::Clef,
            body(
                Model::Clef,
                json!("The export button does nothing when I click it."),
                &choice,
            ),
        ),
        (
            "clef-all-structured",
            Model::Clef,
            body(
                Model::Clef,
                json!({"from": "customer", "subject": "Invoice 42", "body": "The invoice total is wrong and I need it fixed before Friday."}),
                &all,
            ),
        ),
        (
            "clef-flash-noul",
            Model::ClefFlash,
            body(Model::ClefFlash, refund, &noul),
        ),
        (
            "clef-flash-score",
            Model::ClefFlash,
            body(
                Model::ClefFlash,
                json!("Production is down for every customer."),
                &score,
            ),
        ),
    ]
}

/// The credential, held for the clients and checked against every printed line.
struct Held(ProviderSecret);

#[async_trait]
impl CredentialSource for Held {
    fn credential_id(&self) -> Option<String> {
        Some(CREDENTIAL_ID.to_owned())
    }
    async fn bearer(&self) -> Result<Option<String>, String> {
        Ok(Some(self.0.token.clone()))
    }
    async fn secret(&self) -> Result<Option<ProviderSecret>, String> {
        Ok(Some(self.0.clone()))
    }
}

fn default_subc_path() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    home.join(".local/share/cortexkit/run/subc-connection.json")
}

async fn fetch_secret(args: &Args, handle: &str) -> Result<ProviderSecret, String> {
    let subc = args.subc.clone().unwrap_or_else(default_subc_path);
    let conn = broca_subc::SubcConnection::connect(&subc)
        .await
        .map_err(|e| format!("connect subc ({}): {e}", subc.display()))?;
    // The vault authorizes by the credential handle; subc only needs an existing project root.
    let cwd = std::env::current_dir().map_err(|e| format!("current dir: {e}"))?;
    let identity = broca_protocol::BindIdentity::new(cwd, "clef-live-check", "clef-live-check");
    let client = broca_subc::VaultClient::new(
        Arc::new(conn),
        &args.credentials_module,
        &identity,
        Duration::from_secs(10),
    );
    let token = client
        .get(handle, None, false)
        .await
        .result
        .map_err(|e| format!("vault credential.get: {e}"))?;
    if token.account_id.is_none() {
        return Err(format!(
            "the vault returned {CREDENTIAL_ID} without a Cloudflare account id"
        ));
    }
    Ok(ProviderSecret {
        token: token.token,
        account_id: token.account_id,
    })
}

/// Print a line, with the token and the account id replaced should either ever appear.
fn say(secrets: &[String], line: String) {
    let mut line = line;
    for secret in secrets.iter().filter(|secret| !secret.is_empty()) {
        if line.contains(secret.as_str()) {
            line = line.replace(secret.as_str(), "[REDACTED]");
        }
    }
    println!("{line}");
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
    let plan: Vec<_> = plan().into_iter().take(args.count).collect();
    let credential =
        CredentialId::parse(CREDENTIAL_ID).ok_or("the fixed credential id does not parse")?;
    // Both modes look the handle up first, so a dry run also proves a live run will find it.
    // The handle is itself a bearer secret and is never printed.
    let handle = load_devtools_handle(&credential)?;
    println!("credential {CREDENTIAL_ID}: handle present in the dev-tools handle file");
    if !args.live {
        drop(handle);
        for (name, model, body) in &plan {
            println!(
                "request {name} to {} ({} bytes): {body}",
                model.path(),
                body.len()
            );
        }
        println!("dry run: 0 requests sent, vault not contacted");
        return Ok(());
    }
    let secret = fetch_secret(&args, &handle).await?;
    drop(handle);
    let secrets: Vec<String> = std::iter::once(secret.token.clone())
        .chain(secret.account_id.clone())
        .collect();
    let source: Arc<dyn CredentialSource> = Arc::new(Held(secret));
    let timeout = Duration::from_secs(args.request_timeout_secs);
    let clef = WorkersAi::new(None, CLEF_MODEL_PATH, Arc::clone(&source), timeout)?;
    let clef_flash = WorkersAi::new(None, CLEF_FLASH_MODEL_PATH, Arc::clone(&source), timeout)?;
    say(
        &secrets,
        "endpoint: https://api.cloudflare.com/client/v4/accounts/<account>/ai/run/<model>"
            .to_owned(),
    );
    let (mut input_total, mut output_total, mut unreported) = (0u64, 0u64, 0usize);
    let mut charge_total = 0u64;
    for (sent, (name, model, body)) in plan.into_iter().enumerate() {
        assert!(sent < MAX_REQUESTS, "the request cap is enforced");
        let client = match model {
            Model::Clef => &clef,
            Model::ClefFlash => &clef_flash,
        };
        let started = std::time::Instant::now();
        let reply = client.call(body).await;
        let elapsed = started.elapsed().as_millis();
        let at = model.path();
        let line = match reply {
            ProviderReply::Answered(bytes) => match serde_json::from_slice::<AnswerView<'_>>(&bytes)
            {
                Ok(answer) => {
                    let usage = answer.usage.unwrap_or_default();
                    match usage.input_tokens {
                        Some(tokens) => input_total += tokens,
                        None => unreported += 1,
                    }
                    output_total += usage.output_tokens.unwrap_or(0);
                    let charge = usage.input_tokens.map(|tokens| {
                        (u128::from(tokens) * u128::from(model.rate())).div_ceil(1_000_000) as u64
                    });
                    charge_total += charge.unwrap_or(0);
                    format!(
                        "{name} ({at}): HTTP 200 in {elapsed} ms; answered; input_tokens {}, output_tokens {}, charge {} USD; answers {}",
                        show(usage.input_tokens),
                        show(usage.output_tokens),
                        charge.map_or("absent".into(), cortexkit_model_batch::estimate::nanos_to_usd_text),
                        answer.answers.get()
                    )
                }
                Err(error) => format!(
                    "{name} ({at}): HTTP 200 in {elapsed} ms but the result did not decode ({error}), so broca would record provider_error; result {}",
                    String::from_utf8_lossy(&bytes)
                ),
            },
            ProviderReply::WorkersAiError {
                status,
                code,
                retry_after_ms,
            } => format!(
                "{name} ({at}): HTTP {status} in {elapsed} ms, error code {}, retry-after {}, so broca would record {}",
                code.map_or("none".to_owned(), |code| code.to_string()),
                retry_after_ms.map_or("none".to_owned(), |ms| format!("{ms} ms")),
                cortexkit_role_classifier::provider::workers_ai_item_code(status, code)
                    .unwrap_or("provider_error")
            ),
            ProviderReply::Status { status, .. } => format!(
                "{name} ({at}): HTTP {status} in {elapsed} ms as a plain status, which this client never returns"
            ),
            ProviderReply::Transport(message) => format!(
                "{name} ({at}): no reply in {elapsed} ms ({message}), so broca would record provider_error"
            ),
            ProviderReply::Credential(message) => {
                format!("{name} ({at}): credential unusable ({message}); nothing sent")
            }
        };
        say(&secrets, line);
    }
    say(
        &secrets,
        format!(
            "totals: input_tokens {input_total}, output_tokens {output_total}, charge {} USD, {unreported} answered request(s) reported no input count",
            cortexkit_model_batch::estimate::nanos_to_usd_text(charge_total)
        ),
    );
    Ok(())
}
