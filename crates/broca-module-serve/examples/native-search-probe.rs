//! Send one provider request with a provider-run (server) tool added, to measure whether a
//! subscription login accepts it and what comes back.
//!
//! WHAT IT PROVES. broca has no way to request a server-run tool such as Anthropic's
//! `web_search_20250305` or the Responses API's `{"type":"web_search"}`. This tool takes the
//! frozen render config and credential id of a real session (the last `run_started` record in a
//! WAL), renders a fresh one-turn request through that session's own wire family (so the system
//! prompt identity, cache markers and body shape are exactly what broca sends), appends the
//! server tool JSON to the body's `tools` array, and sends it with the same headers the family's
//! endpoint builds. The raw response is saved, and it is also fed through the family's real
//! stream normalizer so the probe records what broca itself would make of it.
//!
//! RUN. Render only (no network, no credential):
//!   cargo run -p broca-module-serve --example native-search-probe -- \
//!       --wal ~/.local/share/cortexkit/broca/wal/<id>.wal --out /tmp/probe/a \
//!       --inject-tool '{"type":"web_search_20250305","name":"web_search","max_uses":3}'
//! Add `--send` to fetch the credential and send. `--body-file` sends a hand-built body
//! verbatim instead of rendering (used to replay a response as history), `--merge` merges a
//! JSON object into the rendered body's top level, `--extra-beta` appends a value to the
//! Anthropic `anthropic-beta` header, `--query` replaces the default user question,
//! `--wire-model` sends to another model of the same family and `--credential-id` uses another
//! login than the source session's.
//! Output in `--out`: `request-body.json`, `response.raw` (the bytes as received),
//! `normalized.jsonl` (one line per normalizer event, or its error) and `summary.json`
//! (status, response header names and values minus cookies, normalizer outcome).
//!
//! CREDENTIAL. The secret comes from the credentials module over subc by the handle in broca's
//! `dev-tools-handles.json` under the config home (override: `BROCA_DEVTOOLS_HANDLES`).
//! The daemon's `vault-handles.json` is not used. Mint with `ck auth mint-handle --id <credential>`
//! (operator only). The WAL's credential id or `--credential-id` selects the handle.
//! It is attached as a request header
//! only; request headers are never written, and every file this tool writes is checked for the
//! secret first (a hit is replaced and reported).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use broca_module_serve::vault_config::load_devtools_handle;
use broca_module_serve::CredentialId;
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::{CallOptions, StepContext, StepId};
use broca_provider::spec::WireFamilyId;
use broca_provider::wire::{AuthHandle, FrozenRenderConfig};
use broca_wal::WalRecord;
use clap::Parser;
use serde_json::{json, Map, Value};

const DEFAULT_QUERY: &str =
    "What is the latest stable Rust release version and its release date? Cite sources.";

#[derive(Parser)]
#[command(about = "Send one request with a server-run tool added and save the raw response")]
struct Args {
    /// A session WAL whose last `run_started` supplies the frozen render config and credential.
    #[arg(long)]
    wal: PathBuf,
    /// Directory for the output files. Created if missing.
    #[arg(long)]
    out: PathBuf,
    /// The single user turn.
    #[arg(long, default_value = DEFAULT_QUERY)]
    query: String,
    /// A tool JSON object appended to the body's `tools` array. Repeatable.
    #[arg(long)]
    inject_tool: Vec<String>,
    /// A JSON object whose keys are set on the rendered body's top level.
    #[arg(long)]
    merge: Option<String>,
    /// Send this file's bytes as the body instead of rendering one.
    #[arg(long)]
    body_file: Option<PathBuf>,
    /// Appended (comma-joined) to the Anthropic `anthropic-beta` header.
    #[arg(long)]
    extra_beta: Vec<String>,
    /// The prompt-cache scope for the ChatGPT backend (body `prompt_cache_key` and the
    /// `session-id` header), kept apart from the source session's own scope.
    #[arg(long, default_value = "native-search-probe")]
    cache_scope: String,
    /// Send to this wire model instead of the source session's (for a cheaper model on the
    /// same family). The source session's reasoning policy is kept, so pick a model that
    /// accepts it.
    #[arg(long)]
    wire_model: Option<String>,
    /// Actually send. Without it the tool renders and stops.
    #[arg(long)]
    send: bool,
    /// The subc daemon connection file.
    #[arg(long)]
    subc: Option<PathBuf>,
    /// Credentials module id on subc.
    #[arg(long, default_value = "claustrum")]
    credentials_module: String,
    /// Credential id to read (default: the WAL's frozen `auth_selection.credential_id`).
    #[arg(long)]
    credential_id: Option<String>,
    /// Hard cap on the whole exchange, in seconds.
    #[arg(long, default_value_t = 600)]
    hard_cap_secs: u64,
}

fn default_subc_path() -> PathBuf {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_default();
    home.join(".local/share/cortexkit/run/subc-connection.json")
}

/// The frozen config, model and credential id of the WAL's last episode.
fn source_config(
    wal: &Path,
) -> Result<
    (
        FrozenRenderConfig,
        broca_protocol::ModelHandle,
        Option<String>,
        Value,
    ),
    String,
> {
    let framed = broca_wal::replay_file(wal, broca_wal::DEFAULT_MAX_READ_BYTES)
        .map_err(|e| format!("read WAL {}: {e}", wal.display()))?;
    let config = framed
        .into_iter()
        .rev()
        .find_map(|f| match f.record {
            WalRecord::RunStarted { config, .. } => Some(config),
            _ => None,
        })
        .ok_or("the WAL has no run_started record")?;
    let frozen: FrozenRenderConfig = serde_json::from_value(config.frozen_render_config.clone())
        .map_err(|e| format!("decode frozen_render_config: {e}"))?;
    Ok((
        frozen,
        config.model.clone(),
        config
            .auth_selection
            .as_ref()
            .map(|a| a.credential_id.clone()),
        serde_json::to_value(&config.generation).unwrap_or(Value::Null),
    ))
}

struct Secret {
    token: String,
    account_id: Option<String>,
}

async fn fetch_secret(args: &Args, credential_id: &str) -> Result<Secret, String> {
    let id = CredentialId::parse(credential_id)
        .ok_or_else(|| format!("'{credential_id}' is not a credential id"))?;
    let handle = load_devtools_handle(&id)?;
    let subc = args.subc.clone().unwrap_or_else(default_subc_path);
    let conn = broca_subc::SubcConnection::connect(&subc)
        .await
        .map_err(|e| format!("connect subc ({}): {e}", subc.display()))?;
    // The vault authorizes by the credential handle; subc only needs an existing project root.
    let cwd = std::env::current_dir().map_err(|e| format!("current dir: {e}"))?;
    let identity =
        broca_protocol::BindIdentity::new(cwd, "native-search-probe", "native-search-probe");
    let client = broca_subc::VaultClient::new(
        Arc::new(conn),
        &args.credentials_module,
        &identity,
        Duration::from_secs(10),
    );
    client
        .get(&handle, None, false)
        .await
        .result
        .map(|t| Secret {
            token: t.token,
            account_id: t.account_id,
        })
        .map_err(|e| format!("vault credential.get: {e}"))
}

/// Write `bytes` to `path` unless they contain the secret; a hit is replaced first and counted.
fn write_checked(path: &Path, bytes: &[u8], secret: Option<&str>, redactions: &mut u64) {
    let mut out = bytes.to_vec();
    if let Some(secret) = secret.filter(|s| s.len() >= 8) {
        let text = String::from_utf8_lossy(bytes);
        if text.contains(secret) {
            out = text.replace(secret, "<REDACTED-CREDENTIAL>").into_bytes();
            *redactions += 1;
        }
    }
    if let Err(e) = std::fs::write(path, out) {
        eprintln!("write {}: {e}", path.display());
    }
}

/// The request broca's ChatGPT route sends: the codex CLI identity plus the account and
/// cache-scope headers. Mirrors `chatgpt_codex_auth` in broca-catalog (private there).
fn chatgpt_headers(secret: &Secret, scope: &str) -> Result<AuthHandle, String> {
    let account = secret
        .account_id
        .clone()
        .filter(|a| !a.trim().is_empty())
        .ok_or("the vault returned no ChatGPT account id for this credential")?;
    Ok(AuthHandle::Headers(vec![
        ("authorization".into(), format!("Bearer {}", secret.token)),
        ("chatgpt-account-id".into(), account),
        (
            "session-id".into(),
            broca_provider::hash::clamp_prompt_cache_key(scope),
        ),
        ("originator".into(), "codex_cli_rs".into()),
        (
            "user-agent".into(),
            format!(
                "codex_cli_rs/0.144.0 ({}; {})",
                std::env::consts::OS,
                std::env::consts::ARCH
            ),
        ),
    ]))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    if let Err(error) = run(Args::parse()).await {
        eprintln!("native-search-probe: {error}");
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), String> {
    std::fs::create_dir_all(&args.out)
        .map_err(|e| format!("create {}: {e}", args.out.display()))?;
    let (mut frozen, mut model, wal_credential, generation) = source_config(&args.wal)?;
    if let Some(wire_model) = &args.wire_model {
        frozen.wire_model_id = wire_model.clone();
        model = broca_protocol::ModelHandle::new(model.provider_module_id.clone(), wire_model);
    }
    if frozen.family_id == WireFamilyId::OpenAiResponsesChatgpt {
        frozen.prompt_cache_key = Some(broca_provider::hash::clamp_prompt_cache_key(
            &args.cache_scope,
        ));
    }
    let family = broca_provider::registry::wire_family(frozen.family_id)
        .ok_or("no wire family implementation for the frozen family id")?;
    println!(
        "family {:?}, wire model {}, generation {generation}",
        frozen.family_id, frozen.wire_model_id
    );

    let body: Vec<u8> = match &args.body_file {
        Some(path) => std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?,
        None => {
            let mut opts = CallOptions::new(vec![Message {
                cache_prefix_blocks: None,
                origin: None,
                ordinal: None,
                role: Role::User,
                content: vec![ContentBlock::Text {
                    text: args.query.clone(),
                }],
            }]);
            opts.step_context = Some(StepContext {
                run_id: "native-search-probe".into(),
                step_index: 1,
            });
            opts.target_model = Some(model);
            let rendered = family.render(&frozen, &opts);
            let mut value: Value = serde_json::from_slice(&rendered)
                .map_err(|e| format!("rendered body is not JSON: {e}"))?;
            let obj = value
                .as_object_mut()
                .ok_or("rendered body is not an object")?;
            for tool in &args.inject_tool {
                let tool: Value =
                    serde_json::from_str(tool).map_err(|e| format!("--inject-tool: {e}"))?;
                let tools = obj.entry("tools").or_insert_with(|| json!([]));
                tools
                    .as_array_mut()
                    .ok_or("the body's tools field is not an array")?
                    .push(tool);
            }
            if let Some(merge) = &args.merge {
                let merge: Map<String, Value> =
                    serde_json::from_str(merge).map_err(|e| format!("--merge: {e}"))?;
                obj.extend(merge);
            }
            serde_json::to_vec(&value).map_err(|e| e.to_string())?
        }
    };
    let mut redactions = 0u64;
    write_checked(
        &args.out.join("request-body.json"),
        &body,
        None,
        &mut redactions,
    );
    println!("request body: {} bytes", body.len());
    if !args.send {
        println!("render-only: pass --send to send the request");
        return Ok(());
    }

    let credential_id = args
        .credential_id
        .clone()
        .or(wal_credential)
        .ok_or("the WAL has no auth_selection; pass --credential-id")?;
    println!("credential: {credential_id} (via vault handle)");
    let secret = fetch_secret(&args, &credential_id).await?;
    let auth = if frozen.family_id == WireFamilyId::OpenAiResponsesChatgpt {
        chatgpt_headers(&secret, &args.cache_scope)?
    } else {
        AuthHandle::Bearer(secret.token.clone())
    };
    let mut request = family.endpoint(&frozen, &auth, body);
    drop(auth);
    if !args.extra_beta.is_empty() {
        let extra = args.extra_beta.join(",");
        match request
            .headers
            .iter_mut()
            .find(|(k, _)| k == "anthropic-beta")
        {
            Some((_, v)) => {
                v.push(',');
                v.push_str(&extra);
            }
            None => request.headers.push(("anthropic-beta".into(), extra)),
        }
    }
    // Header names and the non-secret values, so the report can show what was sent.
    let sent_headers: Map<String, Value> = request
        .headers
        .iter()
        .map(|(k, v)| {
            let shown = if k == "authorization" || v.contains(&secret.token) {
                json!("<credential>")
            } else if k == "chatgpt-account-id" {
                json!("<account id>")
            } else {
                json!(v)
            };
            (k.clone(), shown)
        })
        .collect();

    let client = broca_provider::build_provider_http_client();
    let mut builder = client.post(&request.url);
    for (name, value) in &request.headers {
        builder = builder.header(name, value);
    }
    println!("sending to {} ...", request.url);
    let exchange = async {
        let response = builder
            .body(request.body.clone())
            .send()
            .await
            .map_err(|e| format!("send: {e}"))?;
        let status = response.status().as_u16();
        let headers: Map<String, Value> = response
            .headers()
            .iter()
            .filter(|(k, _)| k.as_str() != "set-cookie")
            .map(|(k, v)| (k.to_string(), json!(v.to_str().unwrap_or("<non-utf8>"))))
            .collect();
        let bytes = response
            .bytes()
            .await
            .map_err(|e| format!("read body: {e}"))?;
        Ok::<_, String>((status, headers, bytes))
    };
    let (status, response_headers, bytes) =
        tokio::time::timeout(Duration::from_secs(args.hard_cap_secs), exchange)
            .await
            .map_err(|_| "hard cap reached before the response ended".to_string())??;
    println!("status {status}, {} response bytes", bytes.len());
    write_checked(
        &args.out.join("response.raw"),
        &bytes,
        Some(&secret.token),
        &mut redactions,
    );

    // What broca's own stream normalizer makes of the response.
    let mut normalizer = family.normalizer(&frozen, StepId(1));
    let mut lines = Vec::new();
    let mut normalizer_outcome = json!({"ok": true});
    if status == 200 {
        let pushed = normalizer.push(&bytes).and_then(|mut events| {
            events.extend(normalizer.finish()?);
            Ok(events)
        });
        match pushed {
            Ok(events) => {
                normalizer_outcome = json!({"ok": true, "events": events.len()});
                for event in events {
                    lines.push(format!("{event:?}"));
                }
            }
            Err(e) => normalizer_outcome = json!({"ok": false, "error": format!("{e:?}")}),
        }
    } else {
        normalizer_outcome = json!({"skipped": "non-200 status"});
    }
    write_checked(
        &args.out.join("normalized.jsonl"),
        lines.join("\n").as_bytes(),
        Some(&secret.token),
        &mut redactions,
    );
    let summary = json!({
        "status": status,
        "url": request.url,
        "sent_headers": sent_headers,
        "response_headers": response_headers,
        "request_body_len": request.body.len(),
        "response_bytes": bytes.len(),
        "normalizer": normalizer_outcome,
    });
    let pretty = serde_json::to_string_pretty(&summary).unwrap_or_default();
    write_checked(
        &args.out.join("summary.json"),
        pretty.as_bytes(),
        Some(&secret.token),
        &mut redactions,
    );
    println!(
        "{}",
        serde_json::to_string_pretty(&summary["normalizer"]).unwrap_or_default()
    );
    drop(secret);
    if redactions > 0 {
        println!("WARNING: {redactions} output file(s) contained the credential and were redacted");
    }
    Ok(())
}
