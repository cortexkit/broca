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
//! `--frozen` renders from a frozen render config JSON file instead of a WAL (the model handle
//! then names `--frozen-provider` and there is no credential id unless `--credential-id` gives
//! one). `--system-file` renders a file's text as a system message before the user turn, which
//! gives a fixed prompt-cache prefix. `--billing-block` inserts a text block as the body's first
//! `system` block, the place Claude Code puts its `x-anthropic-billing-header: ...` text.
//! `--replay-step N` (with `--wal`, optionally `--replay-run <run id>`) renders the request the
//! session made at step N, through broca's offline WAL replay, instead of a fresh turn;
//! `--reasoning-content-required` renders with the DeepSeek reasoning echo switched on.
//! `--prompt-file` renders a canonical message array instead of a fresh user turn. Completed
//! responses also write `assistant-message.json` so a second request can replay the real codec output.
//!
//! WIRE OPTIONS, all opt-in and applied after rendering, so `request-body.json` always holds
//! the identity (uncompressed) body: `--header name=value` replaces a header (the name matches
//! case-insensitively) or adds it, `--drop-header name` removes one, and `--gzip` sends the body
//! the way Claude Code 2.1.302 does: the identity JSON plus `\n` and 0 to 256 random spaces and
//! tabs, gzip-compressed at level 6, with `content-encoding: gzip`. The credential header cannot
//! be overridden or dropped. Headers are edited in the order: `--extra-beta`, `--header`,
//! `--drop-header`, then `--gzip`'s `content-encoding`.
//!
//! Output in `--out`: `request-body.json`, `response.raw` (the bytes as received),
//! `normalized.jsonl` (one line per normalizer event, or its error) and `summary.json`
//! (status, request id, prompt-cache usage, response header names and values minus cookies,
//! the identity and wire body sizes, normalizer outcome). Without `--send` the tool writes
//! `planned-request.json` instead: the url, the headers it would send (the credential shown as
//! a placeholder) and the wire body sizes, plus `request-body.wire.gz` when `--gzip` is set.
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
    #[arg(long, required_unless_present = "frozen", conflicts_with = "frozen")]
    wal: Option<PathBuf>,
    /// A frozen render config JSON file, used in place of the one a `--wal` session would supply.
    #[arg(long)]
    frozen: Option<PathBuf>,
    /// The provider module id of the model handle when rendering from `--frozen`.
    #[arg(long, default_value = "anthropic")]
    frozen_provider: String,
    /// A file whose text is rendered as a system message before the user turn.
    #[arg(long)]
    system_file: Option<PathBuf>,
    /// A text block inserted as the rendered body's first `system` block (Anthropic bodies).
    #[arg(long)]
    billing_block: Option<String>,
    /// `name=value`: replace the header of that name (case-insensitive) or add it. Repeatable.
    #[arg(long = "header")]
    header_overrides: Vec<String>,
    /// Remove the header of this name (case-insensitive). Repeatable.
    #[arg(long = "drop-header")]
    drop_headers: Vec<String>,
    /// Send the body as Claude Code 2.1.302 does: padded, gzip level 6, `content-encoding: gzip`.
    #[arg(long)]
    gzip: bool,
    /// Directory for the output files. Created if missing.
    #[arg(long)]
    out: PathBuf,
    /// The single user turn.
    #[arg(long, default_value = DEFAULT_QUERY)]
    query: String,
    /// Canonical messages to render, including producing-model origins on assistant messages.
    #[arg(long, conflicts_with_all = ["replay_step", "body_file"])]
    prompt_file: Option<PathBuf>,
    /// A tool JSON object appended to the body's `tools` array. Repeatable.
    #[arg(long)]
    inject_tool: Vec<String>,
    /// A JSON object whose keys are set on the rendered body's top level.
    #[arg(long)]
    merge: Option<String>,
    /// Send this file's bytes as the body instead of rendering one.
    #[arg(long)]
    body_file: Option<PathBuf>,
    /// Render the request the `--wal` session made at this step instead of a fresh one-turn
    /// request: the WAL is replayed up to the step's start and rendered with the run's frozen
    /// config, the same offline path `broca-session cache-dump` checks against the recorded
    /// request digests. The endpoint is built from that run's frozen config too.
    #[arg(long, requires = "wal", conflicts_with = "body_file")]
    replay_step: Option<u64>,
    /// The run whose step `--replay-step` renders (default: the WAL's last run).
    #[arg(long, requires = "replay_step")]
    replay_run: Option<String>,
    /// Render with `reasoning_content_required` set, as a newly admitted DeepSeek episode is:
    /// every assistant turn carries its stored reasoning as `reasoning_content`.
    #[arg(long, conflicts_with = "body_file")]
    reasoning_content_required: bool,
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

/// The frozen config, model, credential id and generation from `--wal` or `--frozen`.
fn load_source(
    args: &Args,
) -> Result<
    (
        FrozenRenderConfig,
        broca_protocol::ModelHandle,
        Option<String>,
        Value,
    ),
    String,
> {
    match (&args.wal, &args.frozen) {
        (Some(wal), _) => source_config(wal),
        (None, Some(path)) => {
            let text = std::fs::read_to_string(path)
                .map_err(|e| format!("read {}: {e}", path.display()))?;
            let frozen: FrozenRenderConfig = serde_json::from_str(&text)
                .map_err(|e| format!("decode {}: {e}", path.display()))?;
            let model = broca_protocol::ModelHandle::new(
                args.frozen_provider.clone(),
                frozen.wire_model_id.clone(),
            );
            Ok((frozen, model, None, Value::Null))
        }
        (None, None) => Err("pass --wal or --frozen".into()),
    }
}

/// The frozen config and rendered request body of `step` in the WAL's run `run_id` (default:
/// the last run), rendered through broca's own offline replay. With
/// `reasoning_content_required`, the run's recorded frozen config is rendered with that quirk
/// set, so the body is what the same conversation sends under the reasoning echo.
fn replay_step_body(
    wal: &Path,
    run_id: Option<&str>,
    step: u64,
    reasoning_content_required: bool,
) -> Result<(FrozenRenderConfig, Vec<u8>), String> {
    let records: Vec<WalRecord> = broca_wal::replay_file(wal, broca_wal::DEFAULT_MAX_READ_BYTES)
        .map_err(|e| format!("read WAL {}: {e}", wal.display()))?
        .into_iter()
        .map(|framed| framed.record)
        .collect();
    let starts: Vec<usize> = records
        .iter()
        .enumerate()
        .filter(|(_, record)| matches!(record, WalRecord::RunStarted { .. }))
        .map(|(index, _)| index)
        .collect();
    let start = match run_id {
        Some(wanted) => starts.iter().copied().find(|&index| {
            matches!(&records[index], WalRecord::RunStarted { run_id, .. } if run_id.0 == wanted)
        }),
        None => starts.last().copied(),
    }
    .ok_or("the WAL has no matching run_started record")?;
    let end = starts
        .iter()
        .copied()
        .find(|&index| index > start)
        .unwrap_or(records.len());
    // A retried step starts more than once; the last start is the request that was sent.
    let boundary = records[start..end]
        .iter()
        .rposition(
            |record| matches!(record, WalRecord::StepStarted { step_id } if step_id.0 == step),
        )
        .map(|offset| start + offset)
        .ok_or_else(|| format!("the run has no step {step}"))?;
    let WalRecord::RunStarted { run_id, config, .. } = &records[start] else {
        unreachable!("start indexes a run_started record")
    };
    let mut config = (**config).clone();
    if reasoning_content_required {
        config.frozen_render_config["reasoning_content_required"] = json!(true);
    }
    let (frozen, _, body) = broca_module_serve::wal_render::render_step(
        &records[..=boundary],
        &config,
        &run_id.0,
        step,
    )?;
    Ok((frozen, body))
}

/// Header names the wire options may not touch: the credential stays exactly what the family's
/// endpoint attached, so an override can never put an operator-typed secret on the command line.
const CREDENTIAL_HEADERS: [&str; 2] = ["authorization", "x-api-key"];

/// Send-time encodings and header edits, applied to the request after the family has built it.
/// Nothing here touches the rendered body bytes; `gzip_with_fresh_padding` returns new wire bytes
/// beside them.
mod wire_edit {
    /// The encoder is the production one, so the probe sends what broca sends.
    pub use broca_provider::transport::request_gzip::{gzip_with_fresh_padding, GZIP_LEVEL};

    /// Replace every header of this name (compared case-insensitively) with one `name: value`
    /// at the first one's position, or append it when absent.
    pub fn set_header(headers: &mut Vec<(String, String)>, name: &str, value: &str) {
        match headers
            .iter()
            .position(|(k, _)| k.eq_ignore_ascii_case(name))
        {
            Some(first) => {
                headers[first] = (name.to_string(), value.to_string());
                let mut index = 0;
                headers.retain(|(k, _)| {
                    let keep = index == first || !k.eq_ignore_ascii_case(name);
                    index += 1;
                    keep
                });
            }
            None => headers.push((name.to_string(), value.to_string())),
        }
    }

    /// Remove every header of this name, compared case-insensitively.
    pub fn drop_header(headers: &mut Vec<(String, String)>, name: &str) {
        headers.retain(|(k, _)| !k.eq_ignore_ascii_case(name));
    }

    /// Mark the body gzip-encoded. Claude Code's runtime sends `content-encoding` right after
    /// `x-client-request-id`, so it goes there when that header exists, otherwise last.
    pub fn set_gzip_header(headers: &mut Vec<(String, String)>) {
        drop_header(headers, "content-encoding");
        let entry = ("content-encoding".to_string(), "gzip".to_string());
        match headers
            .iter()
            .position(|(k, _)| k.eq_ignore_ascii_case("x-client-request-id"))
        {
            Some(at) => headers.insert(at + 1, entry),
            None => headers.push(entry),
        }
    }

    /// Decode a response body by its `content-encoding`. `Ok(None)` means an encoding this
    /// tool cannot decode (such as `br` or `zstd`), and the caller keeps the raw bytes.
    pub fn decode_response(encoding: Option<&str>, raw: &[u8]) -> std::io::Result<Option<Vec<u8>>> {
        use std::io::Read;
        let mut out = Vec::new();
        match encoding.map(|e| e.trim().to_ascii_lowercase()).as_deref() {
            None | Some("") | Some("identity") => return Ok(Some(raw.to_vec())),
            Some("gzip") => {
                flate2::read::MultiGzDecoder::new(raw).read_to_end(&mut out)?;
            }
            // HTTP's `deflate` is the zlib-wrapped stream.
            Some("deflate") => {
                flate2::read::ZlibDecoder::new(raw).read_to_end(&mut out)?;
            }
            Some(_) => return Ok(None),
        }
        Ok(Some(out))
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn header_edits_replace_case_insensitively_add_and_drop() {
            let mut headers = vec![
                ("User-Agent".to_string(), "old".to_string()),
                ("x-app".to_string(), "cli".to_string()),
                ("user-agent".to_string(), "duplicate".to_string()),
            ];
            set_header(&mut headers, "user-agent", "new");
            set_header(&mut headers, "x-stainless-retry-count", "0");
            assert_eq!(
                headers,
                vec![
                    ("user-agent".to_string(), "new".to_string()),
                    ("x-app".to_string(), "cli".to_string()),
                    ("x-stainless-retry-count".to_string(), "0".to_string()),
                ]
            );
            drop_header(&mut headers, "X-App");
            assert!(!headers.iter().any(|(k, _)| k == "x-app"));
        }

        #[test]
        fn gzip_header_lands_after_the_client_request_id_and_replaces_a_stale_one() {
            let mut headers = vec![
                ("Content-Encoding".to_string(), "br".to_string()),
                ("x-client-request-id".to_string(), "id".to_string()),
                ("accept-encoding".to_string(), "gzip".to_string()),
            ];
            set_gzip_header(&mut headers);
            assert_eq!(
                headers,
                vec![
                    ("x-client-request-id".to_string(), "id".to_string()),
                    ("content-encoding".to_string(), "gzip".to_string()),
                    ("accept-encoding".to_string(), "gzip".to_string()),
                ]
            );
            let mut bare = vec![("a".to_string(), "b".to_string())];
            set_gzip_header(&mut bare);
            assert_eq!(bare.last().unwrap().0, "content-encoding");
        }

        #[test]
        fn response_decoding_handles_gzip_and_refuses_unknown_encodings() {
            let gz = gzip_with_fresh_padding(b"data: {}").unwrap();
            let decoded = decode_response(Some("gzip"), &gz.wire).unwrap().unwrap();
            assert!(decoded.starts_with(b"data: {}\n"));
            assert_eq!(decode_response(None, b"x").unwrap().unwrap(), b"x");
            assert!(decode_response(Some("br"), b"x").unwrap().is_none());
        }
    }
}

/// Split a `--header name=value` argument at its first `=`.
fn parse_header_override(raw: &str) -> Result<(String, String), String> {
    let (name, value) = raw
        .split_once('=')
        .ok_or_else(|| format!("--header '{raw}' is not name=value"))?;
    let name = name.trim();
    if name.is_empty() {
        return Err(format!("--header '{raw}' has an empty name"));
    }
    Ok((name.to_string(), value.to_string()))
}

fn refuse_credential_header(name: &str, flag: &str) -> Result<(), String> {
    if CREDENTIAL_HEADERS
        .iter()
        .any(|c| c.eq_ignore_ascii_case(name.trim()))
    {
        return Err(format!(
            "{flag} may not change the credential header '{name}'"
        ));
    }
    Ok(())
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
    // Reject bad wire options before any credential is fetched.
    let header_overrides = args
        .header_overrides
        .iter()
        .map(|raw| {
            let (name, value) = parse_header_override(raw)?;
            refuse_credential_header(&name, "--header")?;
            Ok((name, value))
        })
        .collect::<Result<Vec<_>, String>>()?;
    for name in &args.drop_headers {
        refuse_credential_header(name, "--drop-header")?;
    }
    let (mut frozen, mut model, wal_credential, generation) = load_source(&args)?;
    let replayed = match (args.replay_step, &args.wal) {
        (Some(step), Some(wal)) => {
            let (replay_frozen, body) = replay_step_body(
                wal,
                args.replay_run.as_deref(),
                step,
                args.reasoning_content_required,
            )?;
            frozen = replay_frozen;
            Some(body)
        }
        _ => None,
    };
    if args.reasoning_content_required {
        frozen.reasoning_content_required = true;
    }
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
            let rendered = match replayed {
                Some(body) => body,
                None => {
                    let mut prompt = Vec::new();
                    if let Some(path) = &args.system_file {
                        let text = std::fs::read_to_string(path)
                            .map_err(|e| format!("read {}: {e}", path.display()))?;
                        prompt.push(Message {
                            cache_prefix_blocks: None,
                            origin: None,
                            ordinal: None,
                            role: Role::System,
                            content: vec![ContentBlock::Text { text }],
                        });
                    }
                    prompt.push(Message {
                        cache_prefix_blocks: None,
                        origin: None,
                        ordinal: None,
                        role: Role::User,
                        content: vec![ContentBlock::Text {
                            text: args.query.clone(),
                        }],
                    });
                    if let Some(path) = &args.prompt_file {
                        prompt = serde_json::from_slice(
                            &std::fs::read(path)
                                .map_err(|e| format!("read {}: {e}", path.display()))?,
                        )
                        .map_err(|e| format!("canonical prompt {}: {e}", path.display()))?;
                    }
                    let mut opts = CallOptions::new(prompt);
                    opts.step_context = Some(StepContext {
                        run_id: "native-search-probe".into(),
                        step_index: 1,
                    });
                    opts.target_model = Some(model);
                    family.render(&frozen, &opts)
                }
            };
            if args.inject_tool.is_empty() && args.merge.is_none() && args.billing_block.is_none() {
                // Unmodified: send the family's rendered bytes exactly as broca would.
                rendered
            } else {
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
                if let Some(text) = &args.billing_block {
                    obj.get_mut("system")
                        .and_then(Value::as_array_mut)
                        .ok_or("--billing-block needs a body with a system array")?
                        .insert(0, json!({ "type": "text", "text": text }));
                }
                // The same canonical serializer the family renders with, so the only bytes that
                // differ from an unmodified render are the edits above.
                broca_protocol::canonical_json_bytes(&value)
            }
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

    // The credential is fetched only to send. Without --send the endpoint is built with a
    // placeholder so the planned headers and wire sizes can be checked offline.
    let secret = if args.send {
        let credential_id = args
            .credential_id
            .clone()
            .or(wal_credential)
            .ok_or("the source has no auth_selection; pass --credential-id")?;
        println!("credential: {credential_id} (via vault handle)");
        Some(fetch_secret(&args, &credential_id).await?)
    } else {
        None
    };
    let token = secret
        .as_ref()
        .map(|s| s.token.clone())
        .unwrap_or_else(|| DRY_RUN_TOKEN.to_string());
    let auth = if frozen.family_id == WireFamilyId::OpenAiResponsesChatgpt {
        let placeholder = Secret {
            token: token.clone(),
            account_id: Some("dry-run-account".into()),
        };
        chatgpt_headers(secret.as_ref().unwrap_or(&placeholder), &args.cache_scope)?
    } else {
        AuthHandle::Bearer(token.clone())
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
    for (name, value) in &header_overrides {
        wire_edit::set_header(&mut request.headers, name, value);
    }
    for name in &args.drop_headers {
        wire_edit::drop_header(&mut request.headers, name);
    }
    // Gzip is a send-time encoding: `request.body` stays the identity bytes written above, and
    // the compressed bytes are a separate wire body.
    let identity_len = request.body.len();
    let gzip = if args.gzip {
        let gz =
            wire_edit::gzip_with_fresh_padding(&request.body).map_err(|e| format!("gzip: {e}"))?;
        wire_edit::set_gzip_header(&mut request.headers);
        Some(gz)
    } else {
        None
    };
    let wire_body: Vec<u8> = match &gzip {
        Some(gz) => gz.wire.clone(),
        None => request.body.clone(),
    };
    let gzip_summary = gzip.as_ref().map(|gz| {
        json!({
            "level": wire_edit::GZIP_LEVEL,
            "identity_len": gz.identity_len,
            "pad_len": gz.pad_len,
            "padded_len": gz.padded_len,
            "compressed_len": gz.wire.len(),
            "header_hex": gz.wire.iter().take(10).map(|b| format!("{b:02x}")).collect::<String>(),
        })
    });
    // Header names and the non-secret values, so the report can show what was sent.
    let sent_headers: Map<String, Value> = request
        .headers
        .iter()
        .map(|(k, v)| {
            let shown = if k.eq_ignore_ascii_case("authorization") || v.contains(&token) {
                json!("<credential>")
            } else if k == "chatgpt-account-id" {
                json!("<account id>")
            } else {
                json!(v)
            };
            (k.clone(), shown)
        })
        .collect();
    let header_order: Vec<&str> = request.headers.iter().map(|(k, _)| k.as_str()).collect();

    let Some(secret) = secret else {
        let planned = json!({
            "url": request.url,
            "headers": sent_headers,
            "header_order": header_order,
            "request_identity_len": identity_len,
            "request_wire_len": wire_body.len(),
            "gzip": gzip_summary,
        });
        let pretty = serde_json::to_string_pretty(&planned).unwrap_or_default();
        write_checked(
            &args.out.join("planned-request.json"),
            pretty.as_bytes(),
            None,
            &mut redactions,
        );
        if gzip.is_some() {
            write_checked(
                &args.out.join("request-body.wire.gz"),
                &wire_body,
                None,
                &mut redactions,
            );
        }
        println!(
            "render-only: wire body {} bytes (identity {identity_len}); pass --send to send",
            wire_body.len()
        );
        print_result_line(&json!({
            "sent": false,
            "request_identity_len": identity_len,
            "request_wire_len": wire_body.len(),
            "gzip_pad_len": gzip.as_ref().map(|gz| gz.pad_len),
            "user_agent": sent_headers.get("user-agent"),
            "anthropic_beta": sent_headers.get("anthropic-beta"),
            "content_encoding": sent_headers.get("content-encoding"),
        }));
        return Ok(());
    };

    let client = broca_provider::build_provider_http_client();
    // The client without response decoding, so `response.raw` holds the bytes exactly as they
    // arrived and the summary can report the response's own `content-encoding`.
    let mut builder = client.without_response_decoding().post(&request.url);
    for (name, value) in &request.headers {
        builder = builder.header(name, value);
    }
    println!("sending to {} ...", request.url);
    let exchange = async {
        let response = builder
            .body(wire_body.clone())
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
    let request_id = response_headers
        .get("request-id")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string();
    println!(
        "status {status}, request-id {request_id}, {} response bytes",
        bytes.len()
    );
    write_checked(
        &args.out.join("response.raw"),
        &bytes,
        Some(&secret.token),
        &mut redactions,
    );
    // The probe sends through the client that does not decode, so a compressed answer to an
    // `accept-encoding` override arrives raw and is decoded here.
    let response_encoding = response_headers
        .get("content-encoding")
        .and_then(Value::as_str)
        .map(str::to_string);
    let decoded = wire_edit::decode_response(response_encoding.as_deref(), &bytes)
        .map_err(|e| format!("decode {response_encoding:?} response: {e}"))?;

    // What broca's own stream normalizer makes of the response.
    let mut normalizer = family.normalizer(&frozen, StepId(1));
    let mut lines = Vec::new();
    let mut normalizer_outcome = json!({"ok": true});
    match (&decoded, status) {
        (Some(decoded), 200) => {
            let pushed = normalizer.push(decoded).and_then(|mut events| {
                events.extend(normalizer.finish()?);
                Ok(events)
            });
            match pushed {
                Ok(events) => {
                    normalizer_outcome = json!({"ok": true, "events": events.len()});
                    for event in events {
                        if let broca_protocol::provider::NormalizedEvent::Control(
                            broca_protocol::provider::ProviderControl::AssistantMessage { message },
                        ) = &event
                        {
                            write_checked(
                                &args.out.join("assistant-message.json"),
                                &serde_json::to_vec(message).map_err(|e| e.to_string())?,
                                Some(&secret.token),
                                &mut redactions,
                            );
                        }
                        lines.push(format!("{event:?}"));
                    }
                }
                Err(e) => normalizer_outcome = json!({"ok": false, "error": format!("{e:?}")}),
            }
        }
        (None, _) => {
            normalizer_outcome = json!({"skipped": "response content-encoding not decodable"})
        }
        _ => normalizer_outcome = json!({"skipped": "non-200 status"}),
    }
    write_checked(
        &args.out.join("normalized.jsonl"),
        lines.join("\n").as_bytes(),
        Some(&secret.token),
        &mut redactions,
    );
    let (usage_start, usage_final, message_start_keys) = decoded
        .as_deref()
        .map(sse_usage)
        .unwrap_or((Value::Null, Value::Null, Value::Null));
    let error = match (&decoded, status) {
        (Some(decoded), s) if s != 200 => {
            let text = String::from_utf8_lossy(decoded);
            let text: String = text.chars().take(2000).collect();
            json!(text.replace(&secret.token, "<REDACTED-CREDENTIAL>"))
        }
        _ => Value::Null,
    };
    let summary = json!({
        "status": status,
        "request_id": request_id,
        "url": request.url,
        "sent_headers": sent_headers,
        "header_order": header_order,
        "response_headers": response_headers,
        "response_content_encoding": response_encoding,
        "request_body_len": identity_len,
        "request_identity_len": identity_len,
        "request_wire_len": wire_body.len(),
        "gzip": gzip_summary,
        "response_bytes": bytes.len(),
        "usage_start": usage_start,
        "usage_final": usage_final,
        "message_start_keys": message_start_keys,
        "error": error,
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
        "usage {}",
        serde_json::to_string(&summary["usage_start"]).unwrap_or_default()
    );
    print_result_line(&json!({
        "sent": true,
        "status": status,
        "request_id": summary["request_id"],
        "cache_read_input_tokens": summary["usage_start"].get("cache_read_input_tokens"),
        "cache_creation_input_tokens": summary["usage_start"].get("cache_creation_input_tokens"),
        "input_tokens": summary["usage_start"].get("input_tokens"),
        "output_tokens": summary["usage_final"].get("output_tokens"),
        "request_identity_len": identity_len,
        "request_wire_len": wire_body.len(),
        "gzip_pad_len": gzip.as_ref().map(|gz| gz.pad_len),
        "response_content_encoding": summary["response_content_encoding"],
        "error_type": summary["error"].as_str()
            .and_then(|e| serde_json::from_str::<Value>(e).ok())
            .and_then(|e| e["error"]["type"].as_str().map(str::to_string)),
    }));
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

/// One `RESULT {json}` line on stdout, so a wrapper script can collect each probe's outcome
/// without parsing the other output. It never holds a header value that could be a credential.
fn print_result_line(result: &Value) {
    println!(
        "RESULT {}",
        serde_json::to_string(result).unwrap_or_default()
    );
}

/// The token attached when the tool only renders, so the planned headers show the credential
/// header's place without any credential existing in this process.
const DRY_RUN_TOKEN: &str = "dry-run-placeholder-not-a-credential";

/// From an Anthropic SSE stream: the `message_start` usage (where the prompt-cache read and
/// write counts are), the last `message_delta` usage, and the `message_start` message's keys
/// (so a field a beta adds is visible). Each is `null` when absent.
fn sse_usage(body: &[u8]) -> (Value, Value, Value) {
    let text = String::from_utf8_lossy(body);
    let (mut start, mut last, mut keys) = (Value::Null, Value::Null, Value::Null);
    for line in text.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let Ok(event) = serde_json::from_str::<Value>(data.trim()) else {
            continue;
        };
        match event.get("type").and_then(Value::as_str) {
            Some("message_start") => {
                start = event["message"]["usage"].clone();
                if let Some(message) = event["message"].as_object() {
                    keys = json!(message.keys().collect::<Vec<_>>());
                }
            }
            Some("message_delta") => last = event["usage"].clone(),
            _ => {}
        }
    }
    (start, last, keys)
}
