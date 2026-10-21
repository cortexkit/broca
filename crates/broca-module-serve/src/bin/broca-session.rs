//! `broca-session` — the management-surface CLIENT driver.
//!
//! Drives the broca module's `session.*`/`run.*` surface as a CONSUMER of a running
//! subc daemon: `route.open` the ManagementSurface for a session, then run one op
//! (`send` / `subscribe` / `status` / `cancel` / `retract`) or the combined `run` flow
//! (subscribe → send → drain the control stream to terminal). Prints greppable per-event
//! markers (or `--json-events`) so an orchestrator asserts on per-event progress, never on
//! exit alone.
//!
//! This is the neutral client binary cortexkit-e2e spawns (≥2 instances for the
//! multi-client ship-gate), and the single-turn real-module test's client. It owns no
//! daemon and no module — it's handed the daemon's connection file and drives a module's
//! session over it.
//!
//! Exit code: 0 on a clean terminal (the run reached RunFinished), 1 on a wire/startup
//! error or a non-clean terminal. `credential-audit` adds a third code — see
//! `EXIT_AUDIT_UNANSWERED` — because a check that could not answer must not be readable as a
//! check that answered "nothing wrong".
#![forbid(unsafe_code)]

use std::io::Write;
use std::path::PathBuf;

use broca_module::serve::SubscribeFrom;
use broca_module_serve::client::{decode_subscribe_event, SendOutcome, SendSpec, SubscribeEvent};
use broca_module_serve::wire::{AuthParams, CacheParams};
use broca_module_serve::wire::{VaultAuditResponse, VaultAuditRow};
use broca_module_serve::SessionClient;
use broca_protocol::caller::{ControlUnit, RunFinishReason};
use broca_protocol::request::{CacheRole, Tool};
use broca_subc::SubcConnection;
use clap::{Parser, Subcommand, ValueEnum};
use sha2::{Digest, Sha256};

#[derive(Parser, Debug)]
#[command(
    name = "broca-session",
    version,
    about = "Drive a broca module's session.*/run.* surface as a client.",
    long_about = "Drive a broca module's session.*/run.* surface as a client.\n\n\
                  NOT A DEPLOYED BINARY. Unlike ck-broca there is no placed artifact for this \
                  tool, so the copy you are running is whatever was last built from a checkout \
                  -- possibly older than the daemon it is talking to, with no placement ritual \
                  that would have told you. Compare `broca-session --version` against the \
                  daemon's and rebuild from the matching tag if they differ; a stale copy's \
                  own --help describes the behaviour of the build it came from, which is how \
                  this paragraph came to exist."
)]
struct Args {
    /// Path to the subc daemon's connection file (subc-connection.json).
    #[arg(long)]
    subc: PathBuf,
    /// The broca module id to bind (the ManagementSurface).
    #[arg(long, default_value = "broca")]
    module_id: String,
    /// The session identity: project root, harness, session name.
    #[arg(long)]
    project_root: PathBuf,
    /// An OPAQUE key component: joined with U+001F into the WAL, lease, store and idempotency
    /// keys, validated only for that separator.
    ///
    /// This default REACHES THE DAEMON as of v0.3.106. It did not before: until then
    /// `SubcConnection::route_open_target` applied `to_subc_bind` to every target, so the
    /// management leg was translated to the tool-module constant and an export fact from this
    /// CLI read `harness=runner` however this flag was set. That translation is now scoped to
    /// `RouteTarget::ToolProvider`, where the closed enum that motivated it actually lives.
    ///
    /// CONSEQUENCE FOR AN OPERATOR REOPENING AN OLD SESSION: a session this CLI created before
    /// v0.3.106 is addressed under `runner`, because harness is joined into the WAL filename.
    /// Passing `--harness runner` reopens it; the default now names a different lineage. 79
    /// such sessions exist (measured 2026-09-20), 3 of which were ever reopened.
    #[arg(long, default_value = "broca")]
    harness: String,
    #[arg(long)]
    session: String,
    /// Emit one JSON object per line instead of greppable markers.
    #[arg(long)]
    json_events: bool,
    /// How long to retry the management-surface bind while a supervised module is still
    /// coming up (HELLO-registers async after subc binds). 0 = a single attempt.
    #[arg(long, default_value_t = 30000)]
    bind_timeout_ms: u64,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(clap::Args, Debug)]
struct CacheDumpArgs {
    /// Bind session names to export. Repeat to select several sessions.
    #[arg(
        long = "session",
        required_unless_present = "since",
        conflicts_with = "since"
    )]
    sessions: Vec<String>,
    /// Export only calls whose selected attempt ended at or after RFC3339 or epoch milliseconds.
    /// Earlier WAL history is replayed to reconstruct exact request bodies.
    #[arg(long, required_unless_present = "sessions")]
    since: Option<String>,
    #[arg(long)]
    out: PathBuf,
    /// Override the state root; otherwise use the normal read-only state-root resolver.
    #[arg(long)]
    state_root: Option<PathBuf>,
    /// Address these sessions directly without consulting run-index.db.
    #[arg(long, requires_all = ["sessions", "harness"], conflicts_with = "since")]
    project_root: Option<PathBuf>,
    #[arg(long, requires_all = ["sessions", "project_root"], conflicts_with = "since")]
    harness: Option<String>,
}

#[derive(Parser)]
struct OfflineArgs {
    #[command(subcommand)]
    command: OfflineCommand,
}
#[derive(Subcommand)]
enum OfflineCommand {
    CacheDump(CacheDumpArgs),
}

fn cache_dump(args: CacheDumpArgs) -> Result<i32, String> {
    let root = match args.state_root {
        Some(root) => root,
        None => {
            broca_module_serve::state_root::resolve_state_root()
                .map_err(|_| "cannot resolve state root")?
                .path
        }
    };
    let since = args
        .since
        .as_deref()
        .map(broca_module_serve::cache_dump::parse_since)
        .transpose()?;
    let direct = match (args.project_root, args.harness) {
        (Some(project_root), Some(harness)) => Some(broca_module_serve::cache_dump::DirectBind {
            project_root,
            harness,
        }),
        (None, None) => None,
        _ => return Err("--project-root and --harness must be provided together".into()),
    };
    let summary = broca_module_serve::cache_dump::export_with_bind(
        &root,
        &args.sessions,
        since,
        &args.out,
        direct.as_ref(),
    )?;
    println!(
        "{}",
        serde_json::to_string(&summary).map_err(|_| "encode summary")?
    );
    Ok(i32::from(
        summary.calls_examined == 0 || !summary.sessions_not_found.is_empty(),
    ))
}

#[derive(Clone, Debug, ValueEnum)]
enum CliCacheRole {
    Primary,
    Ephemeral,
}

impl From<CliCacheRole> for CacheRole {
    fn from(value: CliCacheRole) -> Self {
        match value {
            CliCacheRole::Primary => CacheRole::Primary,
            CliCacheRole::Ephemeral => CacheRole::Ephemeral,
        }
    }
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Export digest-verified request bodies directly from live or archived WALs (no daemon).
    CacheDump(CacheDumpArgs),
    /// The combined single-turn flow: subscribe, send a prompt, drain the control stream to
    /// the run's terminal. This is the single-client/single-turn proof.
    Run {
        #[arg(long)]
        prompt: String,
        /// `<provider>/<model>` (the run's model handle; the module resolves the provider).
        #[arg(long)]
        model: String,
        /// Tool schemas (JSON array) the turn may call. Omit to inherit; pass `[]` for none.
        #[arg(long)]
        tools: Option<String>,
        /// Per-send tool-call budget JSON.
        #[arg(long = "budget-json")]
        budget_json: Option<String>,
        /// Admission-time cache role for this send.
        #[arg(long = "cache-role")]
        cache_role: Option<CliCacheRole>,
        /// Admission-time cache TTL preference, e.g. "5m" or "1h".
        #[arg(long = "cache-ttl")]
        cache_ttl: Option<String>,
        /// Send `cache.mode: "none"`: no prompt-cache markers, not even a provider's default
        /// ones (first-party Anthropic places them when a send states no cache preference).
        #[arg(long = "no-cache", conflicts_with_all = ["cache_role", "cache_ttl"])]
        no_cache: bool,
        /// Auth method to pin for this send, e.g. "oauth" or "apikey". Overrides the provider's
        /// configured default.
        #[arg(long = "auth-method")]
        auth_method: Option<String>,
        /// Account label to pin, when a provider holds several credentials under one method.
        /// Resolves to the vault credential `<method>:<provider>:<account>`.
        #[arg(long = "auth-account", requires = "auth_method")]
        auth_account: Option<String>,
        /// The session's context management: `none`, `compaction`, or the JSON
        /// `{"transform": "<module id>"}` (refused: broca does not serve transforms). Omit to
        /// inherit, or to take the process default on a session's first send.
        #[arg(long = "context-management")]
        context_management: Option<String>,
        /// Reasoning variant for the model, e.g. "high". Refused if the model doesn't define it.
        #[arg(long)]
        variant: Option<String>,
        /// Service tier: "flex", "default" or "priority". Refused if the model and provider
        /// don't offer it.
        #[arg(long = "service-tier")]
        service_tier: Option<String>,
    },
    /// Send one or more prompts back-to-back on a single bound connection (no re-bind between
    /// them). A 2nd send while the 1st run is still active QUEUES on the active run instead of
    /// starting a new one (a deterministic queue, with no client re-spawn between the sends).
    /// This command only sends; it does not wait for the runs to finish (a separate observer
    /// drains them).
    Send {
        #[arg(long = "prompt")]
        prompts: Vec<String>,
        #[arg(long)]
        model: String,
        #[arg(long)]
        tools: Option<String>,
        /// Per-send tool-call budget JSON.
        #[arg(long = "budget-json")]
        budget_json: Option<String>,
        /// Optional system prompt for every prompt in this invocation.
        #[arg(long)]
        system: Option<String>,
        /// Read the system prompt from a file instead of `--system`.
        #[arg(long = "system-file")]
        system_file: Option<PathBuf>,
        /// An optional logical-send idempotency key. One prompt uses it verbatim; multiple
        /// `--prompt` flags suffix it as `-1`, `-2`, ... so one invocation never reuses the
        /// same send id twice.
        #[arg(long)]
        send_id: Option<String>,
        /// Maximum output tokens for every send. Must be greater than zero.
        #[arg(long = "max-output-tokens")]
        max_output_tokens: Option<u64>,
        /// Shared provider prompt-cache key for every send.
        #[arg(long = "cache-key")]
        cache_key: Option<String>,
        /// Admission-time cache role for each send in this invocation.
        #[arg(long = "cache-role")]
        cache_role: Option<CliCacheRole>,
        /// Admission-time cache TTL preference, e.g. "5m" or "1h".
        #[arg(long = "cache-ttl")]
        cache_ttl: Option<String>,
        /// Send `cache.mode: "none"`: no prompt-cache markers, not even a provider's default
        /// ones (first-party Anthropic places them when a send states no cache preference).
        #[arg(long = "no-cache", conflicts_with_all = ["cache_role", "cache_ttl", "cache_key"])]
        no_cache: bool,
        /// Reasoning variant for the model, e.g. "high".
        #[arg(long)]
        variant: Option<String>,
        /// Service tier: "flex", "default" or "priority".
        #[arg(long = "service-tier")]
        service_tier: Option<String>,
    },
    /// A PURE OBSERVER: `session.subscribe` and drain the control stream, printing each
    /// event, until `--until-runs` RunFinished events are seen (or the stream ends). Used to
    /// prove fan-out (multiple observers on one session) and multi-turn replay (drain N
    /// episodes). Does NOT send — a separate client (or `send`) drives the run.
    Subscribe {
        /// Where to attach: "start" (from seq 1) or "live" (the current tail).
        #[arg(long, default_value = "start")]
        from: String,
        /// Resume from a durable cursor "<wal_seq>:<sub_index>" (overrides --from). The
        /// client's last-processed position — replays strictly AFTER it (resubscribe path).
        #[arg(long)]
        from_cursor: Option<String>,
        /// Stop after this many RunFinished events (one per episode). 0 = until stream end.
        #[arg(long, default_value_t = 1)]
        until_runs: u32,
        /// Also print the lossy display lane: one line per event with its arrival time, the
        /// event type and its text length (never the text itself).
        #[arg(long)]
        display: bool,
    },
    /// Import a canonical JSON message array into an empty session.
    Import {
        #[arg(long)]
        file: PathBuf,
        #[arg(long)]
        source_ref: Option<String>,
    },
    /// `run.status`: report the session state, or use `--run-id` to report one run's lifecycle
    /// status. `--raw` with `--run-id` writes the daemon's exact reply bytes to stdout and their
    /// count and SHA-256 to stderr.
    Status {
        /// Resolve this specific run's lifecycle state (via the run index), not the session.
        #[arg(long)]
        run_id: Option<String>,
        /// Write the exact run-status reply bytes to stdout; requires `--run-id`.
        #[arg(long, requires = "run_id")]
        raw: bool,
    },
    /// `role.describe`: write the running daemon's exact reply bytes to stdout and their count
    /// and SHA-256 to stderr, so a consumer can check what the deployed build advertises.
    Describe,
    /// `run.result`: the decoded state and final-message summary for one specific run.
    Result {
        #[arg(long)]
        run_id: String,
        /// Write the exact daemon reply bytes to stdout and its byte count and SHA-256 to stderr.
        #[arg(long)]
        raw: bool,
    },
    /// `run.cancel <run_id>`.
    Cancel {
        #[arg(long)]
        run_id: String,
    },
    /// `session.retract <submission_id>`.
    Retract {
        #[arg(long)]
        submission_id: String,
    },
    /// `vault.audit`: the daemon lists every vault credential its grants cover and names every
    /// credential its configuration routes to that the vault did not list. Prints one line per
    /// listed credential and per missing one, plus a summary, and exits 0 when everything broca
    /// routes to is listed and active, 1 on a finding, and 2 when the check could not answer.
    ///
    /// Binds the broca management surface like the other commands; the session identity is
    /// only the transport attachment, and the audit does not touch that session.
    CredentialAudit {
        /// Accepted and ignored. The audit runs inside the daemon now, which knows its own
        /// credentials module; kept so an existing scheduled invocation keeps running.
        #[arg(long = "credentials-module", hide = true)]
        credentials_module: Option<String>,
        /// Accepted and ignored, for the same reason.
        #[arg(long = "vault-ready-timeout-ms", hide = true)]
        vault_ready_timeout_ms: Option<u64>,
    },
    /// `session.read`: one durable transcript page (lease-free; works on idle and
    /// active sessions). Prints per-message role/ordinal/summary plus page timing.
    Read {
        /// First ordinal of the page (omit for the first page in the chosen direction).
        #[arg(long)]
        from_ordinal: Option<u64>,
        /// Max messages in the page.
        #[arg(long)]
        limit: Option<u64>,
        /// Also return the latest episode's frozen tool list (`include_tools`).
        #[arg(long)]
        include_tools: bool,
    },
}

fn split_model(model: &str) -> Result<(String, String), String> {
    model
        .split_once('/')
        .map(|(p, m)| (p.to_string(), m.to_string()))
        .ok_or_else(|| format!("--model '{model}' must be '<provider>/<model>'"))
}

fn parse_tools(tools: &Option<String>) -> Result<Option<Vec<Tool>>, String> {
    tools
        .as_ref()
        .map(|json| {
            serde_json::from_str(json).map_err(|error| format!("parse --tools json: {error}"))
        })
        .transpose()
}

/// Accept the wire JSON, or a bare word for the two unit values so a shell caller need not
/// quote a JSON string. Anything else is refused here, before a send is made.
fn parse_context_management(
    raw: Option<String>,
) -> Result<Option<broca_protocol::request::ContextManagement>, String> {
    raw.map(|raw| {
        serde_json::from_str(&raw)
            .or_else(|_| serde_json::from_value(serde_json::Value::String(raw.clone())))
            .map_err(|error| format!("invalid --context-management {raw:?}: {error}"))
    })
    .transpose()
}

fn parse_budget(raw: &Option<String>) -> Result<Option<broca_protocol::BudgetConfig>, String> {
    raw.as_ref()
        .map(|json| serde_json::from_str(json).map_err(|e| format!("invalid --budget-json: {e}")))
        .transpose()
}

fn resolve_system_prompt(
    system: Option<String>,
    system_file: Option<PathBuf>,
) -> Result<Option<String>, String> {
    match (system, system_file) {
        (Some(_), Some(_)) => Err("--system and --system-file are mutually exclusive".into()),
        (Some(text), None) => Ok(Some(text)),
        (None, Some(path)) => std::fs::read_to_string(&path)
            .map(Some)
            .map_err(|e| format!("read --system-file {}: {e}", path.display())),
        (None, None) => Ok(None),
    }
}

/// THIS EXISTS BECAUSE A SHIPPED WIRE SURFACE HAD NO EXERCISER.
///
/// `SendParams.auth` shipped in v0.3.74 and this CLI could not send it, so for three days the only
/// thing driving a caller-scoped credential pin was a peer's PRODUCTION traffic. That is the wrong
/// place to discover a defect, and it also meant I could not run the refusal probe I had recommended
/// to that peer as cheap -- I recommended it without checking the tool to run it existed.
///
/// `--auth-account` REQUIRES `--auth-method` (clap enforces it) because an account with no method is
/// not a partial selection, it is an unanswerable one: the canonical id is
/// `<method>:<provider>[:<account>]` and there is no method to build it from. Failing at parse with
/// "the following required argument was not provided" beats failing at admission with a credential
/// id the caller never typed.
fn auth_params(method: Option<String>, account: Option<String>) -> Option<AuthParams> {
    method.map(|method| AuthParams { method, account })
}

/// The admission-time knobs, grouped because they travel together and are all optional.
///
/// Clippy refused the eighth positional parameter on `run_turn`, and it was right to: the
/// fields here are all `Option`, all admission-only, and all resolved once before the loop starts.
/// A caller passing them positionally has adjacent `None`s and no way to see a transposition.
#[derive(Default)]
struct SendKnobs {
    budget: Option<broca_protocol::BudgetConfig>,
    cache: Option<CacheParams>,
    auth: Option<AuthParams>,
    context_management: Option<broca_protocol::request::ContextManagement>,
    variant: Option<String>,
    service_tier: Option<broca_protocol::request::ServiceTier>,
}

/// Parse `--service-tier` with the wire's own decoder, so the CLI accepts exactly what
/// `session.send` accepts: `fast` and any other alias are refused here too, naming the tiers.
fn parse_service_tier(
    value: Option<String>,
) -> Result<Option<broca_protocol::request::ServiceTier>, String> {
    value
        .map(|tier| {
            serde_json::from_value(serde_json::Value::String(tier.clone())).map_err(|_| {
                format!("--service-tier '{tier}' is not one of: flex, default, priority")
            })
        })
        .transpose()
}

fn cache_params(
    role: Option<CliCacheRole>,
    ttl: Option<String>,
    key: Option<String>,
    no_cache: bool,
) -> Option<CacheParams> {
    if role.is_none() && ttl.is_none() && key.is_none() && !no_cache {
        return None;
    }
    Some(CacheParams {
        role: role.map(Into::into),
        ttl,
        key,
        // This proof CLI exposes no flag for the tail declaration; a caller that needs it
        // sends `cache.tail` over the JSON-RPC surface directly.
        tail: None,
        mode: no_cache.then_some(broca_protocol::request::CacheMode::None),
    })
}

fn content_import_id(messages: &[broca_protocol::Message]) -> Result<String, String> {
    let canonical = serde_json::to_string(messages)
        .map_err(|error| format!("canonicalize import messages: {error}"))?;
    Ok(format!(
        "file-{}",
        broca_provider::hash::base62(broca_provider::hash::fnv1a128(&canonical))
    ))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() {
    if std::env::args_os()
        .nth(1)
        .is_some_and(|a| a == "cache-dump")
    {
        let OfflineCommand::CacheDump(args) = OfflineArgs::parse().command;
        std::process::exit(match cache_dump(args) {
            Ok(code) => code,
            Err(error) => {
                eprintln!("cache-dump: {error}");
                1
            }
        });
    }
    let args = Args::parse();
    let json = args.json_events;
    std::process::exit(match run(args).await {
        Ok(code) => code,
        Err(e) => {
            emit(
                json,
                &serde_json::json!({"event": "error", "msg": e}),
                || format!("ERROR {e}"),
            );
            1
        }
    });
}

fn validate_max_output_tokens(cmd: &Cmd) -> Result<(), String> {
    if matches!(
        cmd,
        Cmd::Send {
            max_output_tokens: Some(0),
            ..
        }
    ) {
        return Err("--max-output-tokens must be greater than 0".into());
    }
    Ok(())
}

async fn run(args: Args) -> Result<i32, String> {
    validate_max_output_tokens(&args.cmd)?;
    let json = args.json_events;
    // The audit is dispatched before the broca management-surface bind, and it returns its own
    // exit code rather than an error: every way it can fail to run is exit 2, and an `Err` out
    // of here would be rendered as the exit-1 "wire/startup error" every other command uses.
    if let Cmd::CredentialAudit { .. } = &args.cmd {
        return Ok(credential_audit(&args, json).await);
    }
    let session =
        broca_protocol::BindIdentity::new(&args.project_root, &args.harness, &args.session);
    session
        .validate_keyable()
        .map_err(|error| format!("invalid bind identity: {error}"))?;
    let conn = SubcConnection::connect(&args.subc)
        .await
        .map_err(|e| format!("connect subc ({}): {e}", args.subc.display()))?;
    let client = SessionClient::bind_with_timeout(
        conn,
        &args.module_id,
        session,
        std::time::Duration::from_millis(args.bind_timeout_ms),
    )
    .await
    .map_err(|e| format!("bind management surface '{}': {e}", args.module_id))?;
    // PRINT THE WHOLE BIND TRIPLE, not just the session name.
    //
    // iceteaSA's issue #22: a client-side defect made --harness inert, so every command
    // answered about a DIFFERENT session than the one named -- run.status Unknown,
    // session.read 0 messages, status Idle. All three read as broca failing to report a
    // cancelled run. What eventually gave it away was a 69-byte lineage file appearing on
    // disk during a READ-ONLY check.
    //
    // The drop is fixed (efc40996). This line is the other half and is the one that
    // generalises: the WAL name is FNV over project_root/harness/session, so any of the
    // three differing silently addresses another session. Printing one component means a
    // mismatch in the other two is invisible in the output and discoverable only by
    // noticing a file appear.
    //
    // THE ROOT IS PRINTED AS THE DAEMON WILL KEY IT, not as it was typed, and that
    // distinction cost me two probes within minutes of fixing the harness half.
    //
    // The daemon canonicalises `project_root`. On macOS /tmp is a symlink to /private/tmp,
    // so a session bound as `/tmp/x` is keyed under `/private/tmp/x`. Reproducing the
    // reporter's own diagnostic -- compute FNV over the triple, look for that file -- I used
    // the typed path and got ABSENT for BOTH candidate harnesses. Two absences is not a
    // wrong answer, it is an uninterpretable one: it reads as "neither identity was used",
    // which is impossible, and there is nothing in it to say the root was the problem.
    //
    // So printing the typed string would have reproduced the very defect this line fixes,
    // one component over. Canonicalised locally, which is not a promise that the daemon's
    // canonicalisation agrees -- only the daemon knows that. When they differ the typed form
    // is printed too, because a caller who typed a path and sees a different one needs to
    // know it is the same path and not a silently redirected bind.
    let keyed_root =
        std::fs::canonicalize(&args.project_root).unwrap_or_else(|_| args.project_root.clone());
    let root_display = if keyed_root == args.project_root {
        keyed_root.display().to_string()
    } else {
        format!(
            "{} (as typed: {})",
            keyed_root.display(),
            args.project_root.display()
        )
    };
    let raw_output = matches!(
        &args.cmd,
        Cmd::Status { raw: true, .. } | Cmd::Result { raw: true, .. } | Cmd::Describe
    );
    if !raw_output {
        emit(
            json,
            &serde_json::json!({
                "event": "bound",
                "module": args.module_id,
                "harness": args.harness,
                "project_root": keyed_root.display().to_string(),
                "project_root_as_typed": args.project_root.display().to_string(),
                "session": args.session,
            }),
            || {
                format!(
                    "BOUND module={} harness={} project_root={} session={}",
                    args.module_id, args.harness, root_display, args.session
                )
            },
        );
    }

    match args.cmd {
        Cmd::CacheDump(_) => Err("cache-dump must be invoked without daemon arguments".into()),
        Cmd::Run {
            prompt,
            model,
            tools,
            budget_json,
            cache_role,
            cache_ttl,
            no_cache,
            auth_method,
            auth_account,
            context_management,
            variant,
            service_tier,
        } => {
            run_turn(
                &client,
                json,
                &prompt,
                &model,
                &tools,
                SendKnobs {
                    budget: parse_budget(&budget_json)?,
                    cache: cache_params(cache_role, cache_ttl, None, no_cache),
                    auth: auth_params(auth_method, auth_account),
                    context_management: parse_context_management(context_management)?,
                    variant,
                    service_tier: parse_service_tier(service_tier)?,
                },
            )
            .await
        }
        Cmd::Send {
            prompts,
            model,
            tools,
            budget_json,
            system,
            system_file,
            send_id,
            max_output_tokens,
            cache_key,
            cache_role,
            cache_ttl,
            no_cache,
            variant,
            service_tier,
        } => {
            let service_tier = parse_service_tier(service_tier)?;
            if prompts.is_empty() {
                return Err("send needs at least one --prompt".into());
            }
            let system = resolve_system_prompt(system, system_file)?;
            let cache = cache_params(cache_role, cache_ttl, cache_key, no_cache);
            let (provider, model_id) = split_model(&model)?;
            let tools = parse_tools(&tools)?;
            let budget = parse_budget(&budget_json)?;
            // Send each prompt back-to-back on the SAME connection. A 2nd send while the 1st
            // run is active deterministically queues; a send to an IDLE lineage with prior
            // episodes auto-appends a new episode (the coordinator decides from run_id
            // identity — no flag needed).
            for (idx, prompt) in prompts.iter().enumerate() {
                let prompt_send_id = send_id.as_ref().map(|base| {
                    if prompts.len() == 1 {
                        base.clone()
                    } else {
                        format!("{base}-{}", idx + 1)
                    }
                });
                let outcome = client
                    .send(SendSpec {
                        prompt,
                        system: system.as_deref(),
                        provider: &provider,
                        model: &model_id,
                        variant: variant.as_deref(),
                        service_tier,
                        tools: tools.clone(),
                        send_id: prompt_send_id.as_deref(),
                        max_output_tokens,
                        cache: cache.clone(),
                        budget: budget.clone(),
                        auth: None,
                        one_shot: None,
                        context_management: None,
                    })
                    .await
                    .map_err(|e| format!("send: {e}"))?;
                emit_send_outcome(json, &outcome);
            }
            Ok(0)
        }
        Cmd::Subscribe {
            from,
            from_cursor,
            until_runs,
            display,
        } => {
            let from = match from_cursor {
                Some(c) => {
                    let (seq, idx) = c.split_once(':').ok_or_else(|| {
                        format!("--from-cursor must be '<wal_seq>:<sub_index>', got '{c}'")
                    })?;
                    SubscribeFrom::After {
                        wal_seq: seq.parse().map_err(|e| format!("bad wal_seq: {e}"))?,
                        sub_index: idx.parse().map_err(|e| format!("bad sub_index: {e}"))?,
                    }
                }
                None => match from.as_str() {
                    "start" => SubscribeFrom::Start,
                    "live" => SubscribeFrom::Live,
                    other => return Err(format!("--from must be start|live, got '{other}'")),
                },
            };
            observe(&client, json, from, until_runs, display).await
        }
        Cmd::Import { file, source_ref } => {
            // Keep full-file scratch buffers out of the future while the import request is in
            // flight. Only the parsed messages and their content-derived id cross the await.
            let (messages, import_id) = {
                let bytes = std::fs::read(&file)
                    .map_err(|error| format!("read import file {}: {error}", file.display()))?;
                let messages: Vec<broca_protocol::Message> = serde_json::from_slice(&bytes)
                    .map_err(|error| {
                        format!("parse canonical messages {}: {error}", file.display())
                    })?;
                let import_id = content_import_id(&messages)?;
                (messages, import_id)
            };
            let source_ref = source_ref.unwrap_or_else(|| format!("file:{}", file.display()));
            let response = client
                .import(messages, &import_id, &source_ref)
                .await
                .map_err(|error| format!("import: {error}"))?;
            emit(
                json,
                &serde_json::json!({"event": "import", "result": response}),
                || format!("IMPORT {response:?}"),
            );
            Ok(0)
        }
        Cmd::Status { run_id, raw } => {
            match run_id {
                None => {
                    let status = client.status().await.map_err(|e| format!("status: {e}"))?;
                    emit(
                        json,
                        &serde_json::json!({"event": "status", "status": format!("{status:?}")}),
                        || format!("STATUS {status:?}"),
                    );
                }
                Some(rid) if raw => {
                    let body = client
                        .run_status_raw(&rid)
                        .await
                        .map_err(|e| format!("run_status: {e}"))?;
                    write_raw_reply(&body)?;
                }
                Some(rid) => {
                    let status = client
                        .run_status_reply(&rid)
                        .await
                        .map_err(|e| format!("run_status: {e}"))?;
                    emit(
                        json,
                        &serde_json::json!({"event": "run_status", "run_id": rid, "status": format!("{status:?}")}),
                        || format!("RUN_STATUS run_id={rid} status={status:?}"),
                    );
                }
            }
            Ok(0)
        }
        Cmd::Describe => {
            let body = client
                .role_describe_raw()
                .await
                .map_err(|e| format!("role_describe: {e}"))?;
            write_raw_reply(&body)?;
            Ok(0)
        }
        Cmd::Result { run_id, raw } => {
            if raw {
                let body = client
                    .run_result_raw(&run_id)
                    .await
                    .map_err(|e| format!("run_result: {e}"))?;
                write_raw_reply(&body)?;
            } else {
                let result = client
                    .run_result(&run_id)
                    .await
                    .map_err(|e| format!("run_result: {e}"))?;
                emit(
                    json,
                    &serde_json::json!({"event": "run_result", "run_id": run_id, "result": format!("{result:?}")}),
                    || format!("RUN_RESULT run_id={run_id} result={result:?}"),
                );
            }
            Ok(0)
        }
        Cmd::Cancel { run_id } => {
            let resp = client
                .cancel(&run_id)
                .await
                .map_err(|e| format!("cancel: {e}"))?;
            emit(
                json,
                &serde_json::json!({"event": "cancel", "result": format!("{resp:?}")}),
                || format!("CANCEL {resp:?}"),
            );
            Ok(0)
        }
        Cmd::Retract { submission_id } => {
            let resp = client
                .retract(&submission_id)
                .await
                .map_err(|e| format!("retract: {e}"))?;
            emit(
                json,
                &serde_json::json!({"event": "retract", "result": format!("{resp:?}")}),
                || format!("RETRACT {resp:?}"),
            );
            Ok(0)
        }
        // Dispatched before the bind above, so this arm is unreachable. Kept rather than
        // folded into a catch-all so adding a command still fails to compile until it is
        // dispatched somewhere.
        Cmd::CredentialAudit { .. } => {
            unreachable!("credential-audit is dispatched before the management-surface bind")
        }
        Cmd::Read {
            from_ordinal,
            limit,
            include_tools,
        } => {
            let started = std::time::Instant::now();
            let page = client
                .read_with_tools(from_ordinal, limit, include_tools)
                .await
                .map_err(|e| format!("read: {e}"))?;
            let elapsed_us = started.elapsed().as_micros();
            if json {
                // SERIALIZE THE RESPONSE WHOLESALE, then layer CLI metadata over it. The
                // previous shape rebuilt this object from a hand-written field list, which
                // silently dropped every field added to SessionReadResponse afterwards --
                // `head` (the cursor for a gapless read-to-subscribe handoff) and
                // `lineage_id` (recreation detection) were both missing, so a caller got
                // exit 0 and a complete-looking page that could do neither.
                //
                // An allowlist here is a promise to remember; flattening the response is the
                // same guarantee without the remembering. Keep it that way: a new wire field
                // must reach this surface without anyone editing this function.
                let event = read_page_event(&page, elapsed_us)?;
                emit(json, &event, String::new);
            } else {
                for message in &page.messages {
                    let text: String = message
                        .message
                        .content
                        .iter()
                        .filter_map(|block| match block {
                            broca_protocol::content::ContentBlock::Text { text }
                            | broca_protocol::content::ContentBlock::TextWithState {
                                text, ..
                            } => Some(text.as_str()),
                            _ => None,
                        })
                        .collect::<Vec<_>>()
                        .join(" ");
                    let preview: String = text.chars().take(80).collect();
                    println!(
                        "[{}] {:?} {} block(s) {}",
                        message.ordinal,
                        message.message.role,
                        message.message.content.len(),
                        preview
                    );
                }
                // Print `absent` rather than zeroes when the last run reported no usage. The
                // two are different claims -- zeroes say the run was measured and spent
                // nothing, absence says nothing was measured -- and they arrive here as
                // `Some(Usage::default())` and `None`, which a Debug print would render almost
                // identically. A consumer of this line is usually checking which one it got.
                let usage = match page.lineage_state.usage {
                    Some(usage) => format!(
                        "in={} cached={} out={}",
                        usage
                            .input_tokens
                            .map_or_else(|| "unreported".into(), |n| n.to_string()),
                        usage
                            .cached_input_tokens
                            .map_or_else(|| "unreported".into(), |n| n.to_string()),
                        usage
                            .output_tokens
                            .map_or_else(|| "unreported".into(), |n| n.to_string())
                    ),
                    None => "absent".to_string(),
                };
                println!(
                    "READ_PAGE messages={} next_from_ordinal={:?} elapsed={}us last_run_usage={}",
                    page.messages.len(),
                    page.next_from_ordinal,
                    elapsed_us,
                    usage
                );
                if let Some(tools) = &page.tools {
                    let names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
                    println!(
                        "TOOLS run={} count={} names={}",
                        page.tools_run_id.as_deref().unwrap_or("?"),
                        tools.len(),
                        names.join(",")
                    );
                } else if include_tools {
                    println!("TOOLS absent");
                }
            }
            Ok(0)
        }
    }
}

// -- credential-audit ------------------------------------------------------------------------
//
// A standing check for the one credential fault the vault cannot see from its own side: a
// credential broca still routes to that has DISAPPEARED from the vault, or that broca's grants
// no longer cover. Inside the vault, a credential nobody reads looks the same whether or not a
// consumer still depends on it, so the detector has to sit on the consumer's side.
//
// The check runs INSIDE the daemon (`vault.audit`), under the same supervised identity runs
// read credentials as, so it answers "can broca read this" rather than a proxy for it. This
// command only asks for it and prints the answer. It reads no handle file.

/// Every credential broca's configuration routes to was listed, and every one it routes to is
/// active.
const EXIT_AUDIT_CLEAN: i32 = 0;
/// A finding: a credential broca routes to was not listed (vanished or no longer granted), or
/// was listed in a state other than `active`.
const EXIT_AUDIT_FINDING: i32 = 1;
/// The check could not answer: no broca route, no vault listing, or a daemon that does not
/// serve `vault.audit`. Deliberately distinct from `EXIT_AUDIT_CLEAN`, because an empty result
/// from a check that never ran reads exactly like a clean one and so fails in the reassuring
/// direction.
const EXIT_AUDIT_UNANSWERED: i32 = 2;

/// The vault's word for a credential that serves normally.
const ACTIVE_STATE: &str = "active";

/// The exit code for what the audit got back. `Err` is a request that got no answer at all.
///
/// A missing credential and a referenced credential in any state but `active` are findings.
/// A listed row broca does not route to is reported and never a finding: the grant covers
/// more than broca uses, which is expected.
fn audit_exit_code(answer: &Result<VaultAuditResponse, String>) -> i32 {
    match answer {
        Err(_) | Ok(VaultAuditResponse::Unanswered { .. }) => EXIT_AUDIT_UNANSWERED,
        Ok(VaultAuditResponse::Checked { rows, missing, .. }) => {
            let inactive = rows
                .iter()
                .any(|row| !row.referenced_by.is_empty() && row.state != ACTIVE_STATE);
            if !missing.is_empty() || inactive {
                EXIT_AUDIT_FINDING
            } else {
                EXIT_AUDIT_CLEAN
            }
        }
    }
}

/// The class of one listed row, shared by the human and JSON renderings.
fn row_class(row: &VaultAuditRow) -> &'static str {
    match (row.referenced_by.is_empty(), row.state == ACTIVE_STATE) {
        (true, _) => "unreferenced",
        (false, true) => "ok",
        (false, false) => "not_active",
    }
}

/// The events one audit answer prints, in order: one per listed row, one per missing
/// credential, then the summary. Each is the JSON form and its human line.
fn audit_events(answer: &Result<VaultAuditResponse, String>) -> Vec<(serde_json::Value, String)> {
    let exit = audit_exit_code(answer);
    let (principal, view, rows, missing) = match answer {
        Err(reason) | Ok(VaultAuditResponse::Unanswered { reason }) => {
            return vec![(
                serde_json::json!({
                    "event": "credential_audit_did_not_run",
                    "msg": reason,
                    "exit": exit,
                }),
                format!("CREDENTIAL_AUDIT_DID_NOT_RUN exit={exit} msg={reason}"),
            )]
        }
        Ok(VaultAuditResponse::Checked {
            principal,
            view,
            rows,
            missing,
        }) => (principal, view, rows, missing),
    };
    let mut events = Vec::with_capacity(rows.len() + missing.len() + 1);
    for row in rows {
        let class = row_class(row);
        let account = row.account_id.as_deref().unwrap_or("-");
        events.push((
            serde_json::json!({
                "event": "credential_audit",
                "credential_id": row.credential_id,
                "type": row.credential_type,
                "state": row.state,
                "account_id": row.account_id,
                "referenced_by": row.referenced_by,
                "result": class,
            }),
            format!(
                "CREDENTIAL_AUDIT id={} result={class} type={} state={} account={account} referenced_by={}",
                row.credential_id,
                row.credential_type,
                row.state,
                row.referenced_by.join(","),
            ),
        ));
    }
    for gone in missing {
        events.push((
            serde_json::json!({
                "event": "credential_audit",
                "credential_id": gone.credential_id,
                "referenced_by": gone.referenced_by,
                "result": "missing",
            }),
            format!(
                "CREDENTIAL_AUDIT id={} result=missing referenced_by={}",
                gone.credential_id,
                gone.referenced_by.join(","),
            ),
        ));
    }
    let referenced = rows
        .iter()
        .filter(|row| !row.referenced_by.is_empty())
        .count();
    let not_active = rows
        .iter()
        .filter(|row| row_class(row) == "not_active")
        .count();
    events.push((
        serde_json::json!({
            "event": "credential_audit_summary",
            "principal": principal,
            "view": view,
            "listed": rows.len(),
            "referenced": referenced,
            "not_active": not_active,
            "missing": missing.len(),
            "exit": exit,
        }),
        format!(
            "CREDENTIAL_AUDIT_SUMMARY principal={principal} view={view} listed={} referenced={referenced} not_active={not_active} missing={} exit={exit}",
            rows.len(),
            missing.len(),
        ),
    ));
    events
}

/// Ask the daemon for `vault.audit`. A request that got no reply at all (no connection, no
/// bind, a refused op) is an `Err`; the daemon's own `Unanswered` comes back as `Ok`.
async fn request_vault_audit(args: &Args) -> Result<VaultAuditResponse, String> {
    let session =
        broca_protocol::BindIdentity::new(&args.project_root, &args.harness, &args.session);
    session
        .validate_keyable()
        .map_err(|error| format!("invalid bind identity: {error}"))?;
    let conn = SubcConnection::connect(&args.subc)
        .await
        .map_err(|error| format!("connect subc ({}): {error}", args.subc.display()))?;
    let client = SessionClient::bind_with_timeout(
        conn,
        &args.module_id,
        session,
        std::time::Duration::from_millis(args.bind_timeout_ms),
    )
    .await
    .map_err(|error| format!("bind management surface '{}': {error}", args.module_id))?;
    client
        .vault_audit()
        .await
        .map_err(|error| format!("vault.audit: {error}"))
}

/// Run the daemon's credential audit, print it, and return this process's exit code.
async fn credential_audit(args: &Args, json: bool) -> i32 {
    let answer = request_vault_audit(args).await;
    for (event, line) in audit_events(&answer) {
        emit(json, &event, || line);
    }
    audit_exit_code(&answer)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunTurnAction {
    Ignore,
    Continue,
    Finished,
    Paused,
}

struct RunTurnTracker<'a> {
    target_run_id: &'a str,
    started: bool,
    final_text: String,
    /// Assistant text seen before this run was identified, held back from rendering.
    ///
    /// A resumed run emits its answer with no `RunStarted` to precede it, so the text
    /// arrives while the client still cannot prove the run is its own. Rendering it then
    /// would risk printing a PRIOR run's reply as this turn's; discarding it loses the
    /// answer entirely. Holding it and promoting it only when a terminal names this run
    /// keeps both guarantees.
    provisional_text: String,
    finish_reason: Option<RunFinishReason>,
}

impl<'a> RunTurnTracker<'a> {
    fn new(target_run_id: &'a str) -> Self {
        Self {
            target_run_id,
            started: false,
            final_text: String::new(),
            provisional_text: String::new(),
            finish_reason: None,
        }
    }

    fn consume(&mut self, unit: &ControlUnit) -> RunTurnAction {
        // ATTRIBUTE BY RUN WHEN THE EVENT SAYS WHICH RUN IT IS.
        //
        // Waiting for `RunStarted` was the only way to know whose terminal this was, and
        // it fails on the case that matters most: a RESUMED run writes no new
        // `RunStarted` (same episode), and a Live attach excludes pre-attach history. So a
        // client identifying its run that way waits through its own successful
        // completion, prints nothing, and exits 1 on a run that finished cleanly.
        // EVERY RUN-ADDRESSED ENDING IS DECIDED HERE, ABOVE THE START GATE.
        //
        // A resumed episode emits no new `RunStarted`, so anything that waits for one to
        // identify its run waits through its own ending. That was fixed for `RunFinished`
        // and NOT for `Paused` in the same edit -- and `Paused` has always carried its run
        // id, so it was hoistable the whole time. A resumed run that pauses again (its
        // credential resolves at admission, then the provider answers 401) published a
        // `Paused` the client ignored, and the attach hung with no local deadline.
        //
        // So the rule is structural rather than a second instance fixed: ANY control unit
        // that names a run AND ends this client's interest in it belongs above this gate.
        // Adding one below it reintroduces the hang, which is why they are grouped.
        if let ControlUnit::Paused { run_id, .. } = unit {
            if run_id.0.as_str() == self.target_run_id {
                return RunTurnAction::Paused;
            }
            self.provisional_text.clear();
            return RunTurnAction::Ignore;
        }
        if let ControlUnit::RunFinished {
            run_id: Some(id),
            reason,
            ..
        } = unit
        {
            if id.0.as_str() == self.target_run_id {
                if !self.started {
                    // Retroactively ours: everything since the attach belonged to this
                    // run after all, so promote the text held back from rendering.
                    self.final_text = std::mem::take(&mut self.provisional_text);
                }
                self.finish_reason = Some(*reason);
                return RunTurnAction::Finished;
            }
            // An EARLIER run draining into this stream: the CLI subscribes before it
            // sends, so a prior run's tail can arrive first.
            self.provisional_text.clear();
            return RunTurnAction::Ignore;
        }

        if !self.started {
            // STILL SUPPRESSED FROM RENDERING, deliberately. Text here is either a prior
            // run's (which must not print as this turn's answer) or a resumed run's, and
            // nothing distinguishes them until a terminal names its run. Holding it
            // unrendered keeps the old guarantee and still lets the answer be recovered.
            return match unit {
                ControlUnit::RunStarted {
                    run_id,
                    submission_id: _,
                } if run_id.0.as_str() == self.target_run_id => {
                    self.started = true;
                    self.provisional_text.clear();
                    RunTurnAction::Continue
                }
                ControlUnit::RunStarted { .. } => {
                    self.provisional_text.clear();
                    RunTurnAction::Ignore
                }
                _ => {
                    if let Some(text) = assistant_text(unit) {
                        self.provisional_text = text;
                    }
                    RunTurnAction::Ignore
                }
            };
        }

        if let Some(text) = assistant_text(unit) {
            self.final_text = text;
        }

        match unit {
            // No run on the terminal means a producer older than that field. Fall back to
            // the start gate, which is what this client did for every terminal before it.
            ControlUnit::RunFinished { reason, .. } => {
                self.finish_reason = Some(*reason);
                RunTurnAction::Finished
            }
            _ => RunTurnAction::Continue,
        }
    }

    fn exit_code(&self) -> i32 {
        match self.finish_reason {
            Some(
                RunFinishReason::Completed
                | RunFinishReason::MaxSteps
                | RunFinishReason::BudgetExhausted
                | RunFinishReason::ContextCeiling,
            ) => 0,
            Some(
                RunFinishReason::Interrupted
                | RunFinishReason::Cancelled
                | RunFinishReason::Error
                | RunFinishReason::TransformUnavailable,
            )
            | None => 1,
        }
    }
}

/// Build the `--json-events` read_page event by SERIALIZING THE RESPONSE WHOLESALE, then
/// layering CLI-only metadata over it.
///
/// The previous shape rebuilt this object from a hand-written field list, which silently
/// dropped every field added to `SessionReadResponse` afterwards: `head` (the cursor for a
/// gapless read-to-subscribe handoff) and `lineage_id` (recreation detection) were both
/// missing, so a caller got exit 0 and a complete-looking page that could do neither.
///
/// An allowlist here is a promise to remember; flattening the response is the same guarantee
/// without the remembering. `read_page_event_carries_every_response_field` fails if anyone
/// reverts to enumerating fields, so the guard does not depend on this comment being read.
fn read_page_event(
    page: &broca_wire::SessionReadResponse,
    elapsed_us: u128,
) -> Result<serde_json::Value, String> {
    let mut event = serde_json::to_value(page).map_err(|e| format!("serialize read page: {e}"))?;
    let object = event
        .as_object_mut()
        .ok_or("read page must serialize to a JSON object")?;
    object.insert("event".into(), "read_page".into());
    // u128 has no serde_json::Value conversion. A read page's elapsed time cannot approach u64
    // range, so this narrow is exact in practice and saturates rather than wrapping if that
    // ever stops being true.
    let elapsed_us = u64::try_from(elapsed_us).unwrap_or(u64::MAX);
    object.insert("elapsed_us".into(), elapsed_us.into());
    object.insert("message_count".into(), page.messages.len().into());
    Ok(event)
}

fn assistant_text(unit: &ControlUnit) -> Option<String> {
    let ControlUnit::AssistantMessage { message } = unit else {
        return None;
    };
    let text = message
        .content
        .iter()
        .filter_map(|block| match block {
            broca_protocol::content::ContentBlock::Text { text }
            | broca_protocol::content::ContentBlock::TextWithState { text, .. } => {
                Some(text.as_str())
            }
            _ => None,
        })
        .collect::<String>();
    Some(text)
}

/// Why the combined turn declined to follow a send through to its terminal.
///
/// The queued case is a deliberate limit rather than an impossibility: the coordinator does
/// derive the submission id from the run id, so a client COULD reconstruct the mapping. Doing
/// so would bake a coordinator-internal naming convention into a separate process, and the
/// convention has already changed once -- the drain still carries a fallback for records that
/// predate it. A client that guessed wrong would wait forever on a run that started under a
/// different id, so this waits for the id to arrive on the wire instead of inferring it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum RunTurnRefusal {
    Pending { submission_id: String },
}

impl std::fmt::Display for RunTurnRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending { submission_id } => write!(
                formatter,
                "not waiting for queued submission '{submission_id}': the session is busy, and the control stream carries run ids rather than submission ids, so this client would have to guess which started run is yours; use the send-only command, observe RunStarted to learn the run id, then poll run.status with 'status --run-id <run_id>'"
            ),
        }
    }
}

fn run_id_for_combined_turn(outcome: &SendOutcome) -> Result<&str, RunTurnRefusal> {
    match outcome {
        SendOutcome::Active { run_id }
        | SendOutcome::Finished { run_id, .. }
        | SendOutcome::Paused { run_id, .. } => Ok(run_id),
        SendOutcome::Pending { submission_id } => Err(RunTurnRefusal::Pending {
            submission_id: submission_id.clone(),
        }),
    }
}

/// The combined single-turn flow: subscribe live, send, drain the control stream to this run's
/// terminal, and print its control units. Returns 0 for Completed/MaxSteps, else 1.
async fn run_turn(
    client: &SessionClient,
    json: bool,
    prompt: &str,
    model: &str,
    tools: &Option<String>,
    knobs: SendKnobs,
) -> Result<i32, String> {
    let SendKnobs {
        budget,
        cache,
        auth,
        context_management,
        variant,
        service_tier,
    } = knobs;
    let (provider, model_id) = split_model(model)?;
    let tools = parse_tools(tools)?;

    // Subscribe BEFORE sending so the attach barrier registers this receiver before the send can
    // write. Attach live because durable history belongs to earlier runs and must not be mistaken
    // for this turn's control events or final text.
    let mut stream = client
        .subscribe(SubscribeFrom::Live)
        .await
        .map_err(|e| format!("subscribe: {e}"))?;

    let outcome = client
        .send(SendSpec {
            prompt,
            system: None,
            provider: &provider,
            model: &model_id,
            variant: variant.as_deref(),
            service_tier,
            tools,
            send_id: None,
            max_output_tokens: None,
            cache,
            budget,
            auth,
            one_shot: None,
            context_management,
        })
        .await
        .map_err(|e| format!("send: {e}"))?;
    emit_send_outcome(json, &outcome);
    if let SendOutcome::Finished { reason, .. } = &outcome {
        return Ok(
            if matches!(
                reason,
                RunFinishReason::Completed
                    | RunFinishReason::MaxSteps
                    | RunFinishReason::BudgetExhausted
            ) {
                0
            } else {
                1
            },
        );
    }
    let target_run_id = run_id_for_combined_turn(&outcome).map_err(|error| error.to_string())?;
    let mut tracker = RunTurnTracker::new(target_run_id);

    // Drain the control stream until this run's terminal (RunFinished) or the stream ends.
    loop {
        let event = match stream.next().await {
            Ok(Some(body)) => body,
            Ok(None) => break, // the stream ended cleanly (or its underlying connection closed)
            Err(e) => return Err(format!("subscribe stream: {e}")),
        };
        match decode_subscribe_event(&event) {
            Ok(SubscribeEvent::Control { wal_seq, unit, .. }) => {
                let action = tracker.consume(&unit);
                if action == RunTurnAction::Ignore {
                    continue;
                }
                emit_control(ControlRenderMode::Run, json, wal_seq, &unit);
                // RunFinished has no run id, so it is ours only after the matching RunStarted.
                // Paused carries a run id and is accepted only when it names this run.
                match action {
                    RunTurnAction::Finished => break,
                    RunTurnAction::Paused => break, // not a clean finish (exit 1)
                    RunTurnAction::Ignore | RunTurnAction::Continue => {}
                }
            }
            Ok(SubscribeEvent::Display(_)) => {} // lossy display, not asserted on
            Err(e) => return Err(format!("decode subscribe event: {e}")),
        }
    }

    emit_run_final_text(json, &tracker.final_text);
    // Preserve the finish-reason classification here: reporting success for a failed run silently
    // turns an automation failure into a passing step, which is worse than crashing.
    Ok(tracker.exit_code())
}

/// One display-lane event for an operator: its arrival time, type and text length. The text
/// itself is left out, since a reasoning or answer delta is conversation content.
fn display_line(event: &broca_protocol::DisplayEvent) -> serde_json::Value {
    use broca_protocol::DisplayEvent as D;
    let (kind, chars) = match event {
        D::TextDelta { delta, .. } => ("text_delta", Some(delta.chars().count())),
        D::ReasoningDelta { delta, .. } => ("reasoning_delta", Some(delta.chars().count())),
        D::ToolInputDelta { delta, .. } => ("tool_input_delta", Some(delta.chars().count())),
        D::DisplayGap { .. } => ("display_gap", None),
        D::DisplayReset => ("display_reset", None),
    };
    let at_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64);
    serde_json::json!({"event": "display", "type": kind, "chars": chars, "at_ms": at_ms})
}

/// A pure observer: subscribe and drain the control stream, printing each event, until
/// `until_runs` RunFinished events are seen (or the stream ends). Exit 0 if the target run
/// count was reached. Proves fan-out (run this alongside a `run`/`send` client) and
/// multi-turn replay (until_runs=2 drains two episodes).
async fn observe(
    client: &SessionClient,
    json: bool,
    from: SubscribeFrom,
    until_runs: u32,
    display: bool,
) -> Result<i32, String> {
    let mut stream = client
        .subscribe(from)
        .await
        .map_err(|e| format!("subscribe: {e}"))?;
    let mut finished = 0u32;
    // The last durable cursor this observer PROCESSED — emitted at the end so a caller can
    // resubscribe strictly after it (the client-owned last-processed position).
    let mut last_cursor: Option<(u64, u32)> = None;
    loop {
        let event = match stream.next().await {
            Ok(Some(body)) => body,
            Ok(None) => break, // the stream ended cleanly (or its underlying connection closed)
            Err(e) => return Err(format!("subscribe stream: {e}")),
        };
        match decode_subscribe_event(&event) {
            Ok(SubscribeEvent::Control {
                wal_seq,
                sub_index,
                unit,
            }) => {
                last_cursor = Some((wal_seq, sub_index));
                emit_control(ControlRenderMode::Subscribe, json, wal_seq, &unit);
                if matches!(unit, ControlUnit::RunFinished { .. }) {
                    finished += 1;
                    if until_runs != 0 && finished >= until_runs {
                        break;
                    }
                }
            }
            Ok(SubscribeEvent::Display(event)) => {
                if display {
                    let line = display_line(&event);
                    emit(json, &line, || line.to_string());
                }
            }
            Err(e) => return Err(format!("decode subscribe event: {e}")),
        }
    }
    let (cseq, cidx) = last_cursor.unwrap_or((0, 0));
    emit(
        json,
        &serde_json::json!({"event": "observed", "runs_finished": finished, "cursor": format!("{cseq}:{cidx}")}),
        || format!("OBSERVED runs_finished={finished} cursor={cseq}:{cidx}"),
    );
    Ok(if until_runs == 0 || finished >= until_runs {
        0
    } else {
        1
    })
}

fn emit_send_outcome(json: bool, outcome: &SendOutcome) {
    match outcome {
        SendOutcome::Paused {
            run_id,
            pause_reason,
        } => emit(
            json,
            &serde_json::json!({"event":"send", "state":"paused", "run_id":run_id, "pause_reason":pause_reason}),
            || {
                format!(
                    "SEND state=paused run_id={run_id} pause_reason={}",
                    pause_reason.as_str()
                )
            },
        ),
        SendOutcome::Active { run_id } => emit(
            json,
            &serde_json::json!({"event": "send", "state": "active", "run_id": run_id}),
            || format!("SEND state=active run_id={run_id}"),
        ),
        SendOutcome::Finished { run_id, reason } => emit(
            json,
            &serde_json::json!({"event": "send", "state": "finished", "run_id": run_id, "reason": reason}),
            || {
                format!(
                    "FINISHED run={run_id} reason={}",
                    serde_json::to_value(reason).unwrap().as_str().unwrap()
                )
            },
        ),
        SendOutcome::Pending { submission_id } => emit(
            json,
            &serde_json::json!({"event": "send", "state": "pending", "submission_id": submission_id}),
            || format!("SEND state=pending submission_id={submission_id}"),
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ControlRenderMode {
    Run,
    Subscribe,
}

fn emit_control(mode: ControlRenderMode, json: bool, wal_seq: u64, unit: &ControlUnit) {
    emit_control_to(mode, json, wal_seq, unit, &mut |line| println!("{line}"));
}

fn emit_control_to(
    mode: ControlRenderMode,
    json: bool,
    wal_seq: u64,
    unit: &ControlUnit,
    output: &mut impl FnMut(String),
) {
    match unit {
        ControlUnit::RunStarted {
            run_id,
            submission_id,
        } => {
            let event = if let Some(submission_id) = submission_id {
                serde_json::json!({
                    "event": "run_started",
                    "seq": wal_seq,
                    "run_id": run_id.0,
                    "submission_id": submission_id.0,
                })
            } else {
                serde_json::json!({"event": "run_started", "seq": wal_seq, "run_id": run_id.0})
            };
            emit_to(
                json,
                &event,
                || match submission_id {
                    Some(submission_id) => format!(
                        "RUN_STARTED seq={wal_seq} run_id={} submission_id={}",
                        run_id.0, submission_id.0
                    ),
                    None => format!("RUN_STARTED seq={wal_seq} run_id={}", run_id.0),
                },
                output,
            );
        }
        ControlUnit::StepStarted { step_id } => emit_to(
            json,
            &serde_json::json!({"event": "step_started", "seq": wal_seq, "step": step_id.0}),
            || format!("STEP_STARTED seq={wal_seq} step={}", step_id.0),
            output,
        ),
        ControlUnit::BudgetFired {
            run_id,
            kind,
            threshold,
            ordinal,
        } => emit_to(
            json,
            &serde_json::json!({
                "event": "budget_fired",
                "seq": wal_seq,
                "run_id": run_id.0,
                "kind": kind,
                "threshold": threshold,
                "ordinal": ordinal,
            }),
            || {
                format!(
                    "BUDGET_FIRED seq={wal_seq} run_id={} kind={kind:?} threshold={threshold} ordinal={ordinal}",
                    run_id.0
                )
            },
            output,
        ),
        ControlUnit::ToolCall { call } => emit_to(
            json,
            &serde_json::json!({"event": "tool_call", "seq": wal_seq, "name": call.tool_name, "id": call.tool_call_id.0}),
            || {
                format!(
                    "TOOL_CALL seq={wal_seq} name={} id={}",
                    call.tool_name, call.tool_call_id.0
                )
            },
            output,
        ),
        ControlUnit::ToolResult { result } => {
            let text = match &result.output {
                broca_protocol::content::ToolOutput::Text { text } => text.clone(),
                broca_protocol::content::ToolOutput::Error { message } => message.clone(),
                _ => String::new(),
            };
            emit_to(
                json,
                &serde_json::json!({"event": "tool_result", "seq": wal_seq, "id": result.tool_call_id.0, "is_error": result.is_error, "text": text}),
                || {
                    format!(
                        "TOOL_RESULT seq={wal_seq} id={} is_error={} text={}",
                        result.tool_call_id.0,
                        result.is_error,
                        text.replace('\n', "\\n")
                    )
                },
                output,
            );
        }
        // Usage is rendered rather than skipped because the CURSOR ADVANCES PAST THIS UNIT.
        // The control lane is the reliable one: a consumer persists the cursor, and anything
        // this arm declines to emit is unrecoverable by resubscribing from it. Dropping a
        // field here is silent to the consumer, who cannot tell an absent field from a step
        // that reported nothing. Same defect as the assistant-message arm below, which used
        // to be a bare no-op for the same reason.
        ControlUnit::StepFinished {
            step_id,
            finish_reason,
            usage,
            ..
        } => emit_to(
            json,
            &serde_json::json!({"event": "step_finished", "seq": wal_seq, "step": step_id.0, "reason": format!("{finish_reason:?}"), "usage": usage}),
            || {
                format!(
                    "STEP_FINISHED seq={wal_seq} step={} reason={finish_reason:?} in={} out={} cached_in={}",
                    step_id.0,
                    usage.input_tokens.map_or_else(|| "unreported".into(), |n| n.to_string()),
                    usage.output_tokens.map_or_else(|| "unreported".into(), |n| n.to_string()),
                    usage.cached_input_tokens.map_or_else(|| "unreported".into(), |n| n.to_string())
                )
            },
            output,
        ),
        ControlUnit::AssistantMessage { message } => {
            if mode == ControlRenderMode::Subscribe {
                // The observer advances its durable cursor for every processed control event. If
                // this reliable event is silent, resuming after that cursor skips the reply forever.
                emit_to(
                    json,
                    &serde_json::json!({
                        "event": "assistant_message",
                        "seq": wal_seq,
                        "message": message,
                    }),
                    || {
                        format!(
                            "ASSISTANT_MESSAGE seq={wal_seq} message_id={} content={:?}",
                            message.message_id.0, message.content
                        )
                    },
                    output,
                );
            }
        }
        // NO ARM IN THIS MATCH MAY USE `..`. The cursor advances past every unit rendered
        // here, so a field this function declines to emit is unrecoverable by resubscribing
        // and is indistinguishable, to the consumer, from a field the engine never set. This
        // arm was the fourth instance of that: assistant messages (r121), per-step usage
        // (r133), and now the terminal's usage, provider finish reason and retry count. If a
        // new field appears on a ControlUnit, name it here or decide deliberately not to.
        ControlUnit::RunFinished {
            run_id,
            reason,
            usage,
            final_step_finish_reason,
            indeterminate_tool_calls,
            retries_used,
        } => emit_to(
            json,
            &serde_json::json!({"event": "run_finished", "seq": wal_seq, "reason": format!("{reason:?}"), "usage": usage, "final_step_finish_reason": final_step_finish_reason.map(|r| format!("{r:?}")), "indeterminate_tool_calls": indeterminate_tool_calls, "retries_used": retries_used}),
            || {
                let totals = usage
                    .as_ref()
                    .map(|u| {
                        format!(
                            " in={} out={} cached_in={}",
                            u.input_tokens
                                .map_or_else(|| "unreported".into(), |n| n.to_string()),
                            u.output_tokens
                                .map_or_else(|| "unreported".into(), |n| n.to_string()),
                            u.cached_input_tokens
                                .map_or_else(|| "unreported".into(), |n| n.to_string())
                        )
                    })
                    .unwrap_or_default();
                let final_step = final_step_finish_reason
                    .map(|r| format!(" final_step={r:?}"))
                    .unwrap_or_default();
                // Absent on payloads from a producer before the terminal named its run;
                // printing nothing is right there, because a fabricated id would be worse
                // than an admitted gap.
                let run = run_id
                    .as_ref()
                    .map(|id| format!(" run={}", id.0))
                    .unwrap_or_default();
                let indeterminate = indeterminate_tool_calls
                    .as_ref()
                    .map(|calls| {
                        let ids = calls
                            .iter()
                            .map(|id| id.0.as_str())
                            .collect::<Vec<_>>()
                            .join(",");
                        format!(" indeterminate=[{ids}]")
                    })
                    .unwrap_or_default();
                format!(
                    "RUN_FINISHED seq={wal_seq} reason={reason:?}{run}{totals}{final_step}{indeterminate} retries={retries_used}"
                )
            },
            output,
        ),
        ControlUnit::Error { error } => emit_to(
            json,
            &serde_json::json!({"event": "loop_error", "seq": wal_seq, "class": format!("{:?}", error.class), "msg": error.message}),
            || {
                format!(
                    "LOOP_ERROR seq={wal_seq} class={:?} msg={}",
                    error.class,
                    error.message.replace('\n', "\\n")
                )
            },
            output,
        ),
        // A pause is terminal for this attach and breaks the run loop, so with no arm here the
        // client exits non-zero having printed nothing since RUN_STARTED: a run waiting on
        // re-authentication reads exactly like one that died. It is also the only terminal that
        // is RESUMABLE under the same run id, which is the part an operator most needs told.
        ControlUnit::Paused {
            run_id,
            reason,
            error,
        } => emit_to(
            json,
            &serde_json::json!({
                "event": "run_paused",
                "wal_seq": wal_seq,
                "run_id": run_id.0,
                "reason": format!("{reason:?}"),
                "error": error.as_ref().map(|e| e.message.clone()),
            }),
            || {
                format!(
                    "RUN_PAUSED seq={wal_seq} run_id={} reason={reason:?}{}",
                    run_id.0,
                    error
                        .as_ref()
                        .map(|e| format!(" msg={}", e.message.replace('\n', "\\n")))
                        .unwrap_or_default()
                )
            },
            output,
        ),
        _ => {}
    }
}

fn emit_run_final_text(json: bool, text: &str) {
    emit_run_final_text_to(json, text, &mut |line| println!("{line}"));
}

fn emit_run_final_text_to(json: bool, text: &str, output: &mut impl FnMut(String)) {
    if !text.trim().is_empty() {
        emit_to(
            json,
            &serde_json::json!({"event": "final_text", "text": text}),
            || format!("FINAL_TEXT {}", text.replace('\n', "\\n")),
            output,
        );
    }
}

fn write_raw_reply(body: &[u8]) -> Result<(), String> {
    let mut stdout = std::io::stdout().lock();
    stdout
        .write_all(body)
        .and_then(|()| stdout.flush())
        .map_err(|error| format!("write raw daemon reply to stdout: {error}"))?;
    eprintln!("RAW bytes={} sha256={:x}", body.len(), Sha256::digest(body));
    Ok(())
}

/// Emit one event: a JSON line (`--json-events`) or a greppable human marker.
fn emit(json: bool, obj: &serde_json::Value, human: impl FnOnce() -> String) {
    emit_to(json, obj, human, &mut |line| println!("{line}"));
}

fn emit_to(
    json: bool,
    obj: &serde_json::Value,
    human: impl FnOnce() -> String,
    output: &mut impl FnMut(String),
) {
    output(if json { obj.to_string() } else { human() });
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::{cache_params, read_page_event, run, Args, Cmd};

    #[test]
    fn cache_dump_reads_all_compaction_records_live_and_archived() {
        use broca_core::wal_writer::session_addr;
        use broca_protocol::BindIdentity;
        use broca_wal::{framing, WalRecord};
        use serde_json::json;
        use sha2::{Digest, Sha256};

        let root = broca_test_support::scratch_root().join("cache-dump-compaction-reader");
        std::fs::create_dir_all(root.join("wal")).unwrap();
        let bind = BindIdentity::new(root.join("project"), "reader-test", "compaction");
        let address = session_addr(&bind);
        let decode = |value| serde_json::from_value::<WalRecord>(value).unwrap();
        let started = decode(json!({
            "type": "run_started", "run_id": "reader-run", "session": bind,
            "config": {
                "model": {"provider_module_id": "p", "model_id": "m"},
                "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
                "response_format": {"type": "text"}, "context_epoch": 0, "renderer_version": 1,
                "frozen_render_config": broca_provider::families::anthropic_frozen_config("m")
            },
            "input": [{"role": "user", "content": [{"type": "text", "text": "reader fixture"}]}]
        }));
        let step = decode(json!({"type": "step_started", "step_id": 1}));
        // The records below replace the input with the user text "summary".
        // Define that request here instead of calling render_step: if render_step
        // ignored compaction, generating the fixture with it would hide the error.
        let body = broca_protocol::canonical_json_bytes(&json!({
            "max_tokens":2048, "model":"m", "stream":true,
            "messages":[{"role":"user", "content":[{"type":"text", "text":"summary"}]}],
            "system":[{"type":"text", "text":"You are Claude Code, Anthropic's official CLI for Claude."}],
            "thinking":{"type":"enabled", "budget_tokens":1024}
        }));
        let mut records = vec![started];
        let compaction: Vec<WalRecord> =
            include_str!("../../../broca-wal/tests/fixtures/compaction_records.jsonl")
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
        assert_eq!(compaction.len(), 7);
        assert_eq!(
            compaction
                .iter()
                .map(|record| serde_json::to_value(record).unwrap()["type"].clone())
                .collect::<std::collections::HashSet<_>>()
                .len(),
            4,
        );
        records.extend(compaction);
        records.push(step);
        records.push(decode(json!({
            "type": "model_attempt_finished", "step_id": 1, "attempt": 0,
            "request_body_len": body.len(), "request_body_sha256": format!("{:x}", Sha256::digest(&body)),
            "response_body_len": 1, "response_body_sha256": "unused", "outcome": "complete", "ts_ms": 1000
        })));
        records.push(decode(json!({
            "type": "model_step_finished", "step_id": 1,
            "assistant_message": {"message_id": "reader-message", "content": [{"type": "text", "text": "done"}]},
            "usage": {"input_tokens": 11, "output_tokens": 5}
        })));
        let bytes: Vec<u8> = records
            .iter()
            .enumerate()
            .flat_map(|(index, record)| framing::encode(index as u64 + 1, 7, record).unwrap())
            .collect();
        let live = root.join("wal").join(format!("{address}.wal"));
        std::fs::write(&live, &bytes).unwrap();
        for archived in [false, true] {
            if archived {
                std::fs::create_dir(root.join("wal-archive")).unwrap();
                let mut writer =
                    broca_wal::archive::ArchiveWriter::create(root.join("wal-archive/fold-1.ark"))
                        .unwrap()
                        .without_feature_floor();
                writer.append(&address, &bytes).unwrap();
                writer.seal().unwrap();
                std::fs::remove_file(&live).unwrap();
            }
            let decoded = broca_core::FileWalStore::new(root.join("wal"))
                .read_address_records(&address)
                .unwrap();
            assert_eq!(
                decoded
                    .into_iter()
                    .map(|frame| frame.record)
                    .collect::<Vec<_>>(),
                records,
            );
            let out = root.join(if archived { "archived" } else { "live" });
            assert_eq!(
                super::cache_dump(super::CacheDumpArgs {
                    sessions: vec![bind.session.clone()],
                    since: None,
                    out: out.clone(),
                    state_root: Some(root.clone()),
                    project_root: Some(bind.project_root.clone()),
                    harness: Some(bind.harness.clone()),
                }),
                Ok(0),
            );
            let body_path = out.join(
                broca_provider::request_dump::request_dump_filename(None, "reader-run", 1)
                    .replace(".json", ".body.json"),
            );
            assert_eq!(std::fs::read(body_path).unwrap(), body);
            assert!(std::fs::read_dir(&out).unwrap().any(|entry| entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .ends_with(".response.json")));
            assert_eq!(
                live.exists(),
                !archived,
                "cache-dump must not extract or repair its source"
            );
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    /// A rendered control unit must carry every field the cursor is about to advance past.
    /// This asserts USAGE specifically because it was silently dropped: the arm destructured
    /// with `..`, so a consumer persisting cursors lost per-step usage with no way to notice,
    /// and no way to recover it by resubscribing from the cursor it had stored.
    /// The terminal carries usage, the provider's final-step reason and the retry count, and
    /// the cursor advances past it. NON-DEFAULT values throughout on purpose: an expectation
    /// built from zeros and Nones cannot distinguish a field being RENDERED from a field being
    /// DROPPED, which is the whole failure this guards.
    /// Every field the wire response carries must reach the `--json-events` surface.
    ///
    /// This compares the event against the SERIALIZED RESPONSE rather than a named list, so it
    /// covers fields that do not exist yet. A named list would need editing for each new field
    /// — which is exactly the maintenance promise that failed and dropped `head` and
    /// `lineage_id` in the first place, and a test that repeats the bug's own shape cannot
    /// catch the bug.
    #[test]
    fn read_page_event_carries_every_response_field() {
        let page = broca_wire::SessionReadResponse {
            next_late_after: None,
            deferred_calls: None,
            late_approvals: None,
            open_batch: None,
            messages: Vec::new(),
            next_from_ordinal: Some(9),
            head: Some(broca_wire::CursorWire {
                wal_seq: 4,
                sub_index: 1,
            }),
            lineage_id: Some("11111111-2222-4333-8444-555555555555".into()),
            lineage_state: broca_wire::SessionReadLineageState {
                last_run_id: Some("run-x".into()),
                state: "completed".into(),
                reason: None,
                error: None,
                usage: None,
            },
            tools: Some(vec![broca_wire::SessionReadTool {
                name: "lookup".into(),
                description: "look something up".into(),
                input_schema: serde_json::json!({"type": "object"}),
                module: Some("aft".into()),
            }]),
            tools_run_id: Some("run-x".into()),
        };

        let event = read_page_event(&page, 1234).expect("build read_page event");
        let event = event.as_object().expect("event is an object");
        let response = serde_json::to_value(&page).expect("serialize response");
        let response = response.as_object().expect("response is an object");

        assert!(
            !response.is_empty(),
            "fixture must populate fields or this test passes vacuously"
        );
        for (field, value) in response {
            assert_eq!(
                event.get(field),
                Some(value),
                "response field {field:?} is missing or altered on the json-events surface"
            );
        }

        // CLI-only metadata rides alongside rather than replacing anything.
        assert_eq!(
            event.get("event").and_then(|v| v.as_str()),
            Some("read_page")
        );
        assert_eq!(event.get("elapsed_us").and_then(|v| v.as_u64()), Some(1234));
    }

    #[test]
    fn run_finished_renders_terminal_metadata_before_the_cursor_advances() {
        let unit = ControlUnit::RunFinished {
            run_id: None,
            reason: RunFinishReason::Completed,
            usage: Some(Usage {
                input_tokens: Some(4321),
                output_tokens: Some(99),
                cached_input_tokens: Some(1000),
                ..Default::default()
            }),
            final_step_finish_reason: Some(broca_protocol::content::FinishReason::Length),
            indeterminate_tool_calls: Some(vec![broca_protocol::ToolCallId("call-1".into())]),
            retries_used: 3,
        };
        let mut human = String::new();
        emit_control_to(
            ControlRenderMode::Subscribe,
            false,
            11,
            &unit,
            &mut |line| human.push_str(&line),
        );
        assert!(human.contains("in=4321"), "input tokens dropped: {human}");
        assert!(human.contains("out=99"), "output tokens dropped: {human}");
        assert!(
            human.contains("final_step=Length"),
            "provider finish reason dropped: {human}"
        );
        assert!(
            human.contains("indeterminate=[call-1]"),
            "indeterminate call ids dropped: {human}"
        );
        assert!(human.contains("retries=3"), "retry count dropped: {human}");

        let mut json_out = String::new();
        emit_control_to(ControlRenderMode::Subscribe, true, 11, &unit, &mut |line| {
            json_out.push_str(&line)
        });
        for needle in ["4321", "Length", "call-1", "retries_used"] {
            assert!(
                json_out.contains(needle),
                "json render dropped {needle}: {json_out}"
            );
        }
    }

    #[test]
    fn step_finished_renders_usage_before_the_cursor_advances() {
        let unit = ControlUnit::StepFinished {
            step_id: broca_protocol::StepId(2),
            finish_reason: broca_protocol::content::FinishReason::Stop,
            usage: Usage {
                input_tokens: Some(1234),
                output_tokens: Some(56),
                cached_input_tokens: Some(789),
                ..Default::default()
            },
        };
        let mut human = String::new();
        emit_control_to(ControlRenderMode::Subscribe, false, 7, &unit, &mut |line| {
            human.push_str(&line)
        });
        assert!(human.contains("in=1234"), "input tokens dropped: {human}");
        assert!(human.contains("out=56"), "output tokens dropped: {human}");
        assert!(
            human.contains("cached_in=789"),
            "cached input dropped: {human}"
        );

        let mut json_out = String::new();
        emit_control_to(ControlRenderMode::Subscribe, true, 7, &unit, &mut |line| {
            json_out.push_str(&line)
        });
        assert!(
            json_out.contains("1234") && json_out.contains("789"),
            "json render dropped usage: {json_out}"
        );
    }

    use super::{
        content_import_id, emit_control_to, emit_run_final_text_to, parse_tools,
        run_id_for_combined_turn, ControlRenderMode, RunTurnAction, RunTurnRefusal, RunTurnTracker,
    };
    use broca_module_serve::client::SendOutcome;
    use broca_protocol::caller::{ControlUnit, PauseReason, RunFinishReason};
    use broca_protocol::content::{AssistantMessage, ContentBlock};
    use broca_protocol::usage::Usage;
    use broca_protocol::{MessageId, RunId};
    use clap::Parser;

    fn run_started(run_id: &str) -> ControlUnit {
        ControlUnit::RunStarted {
            run_id: RunId(run_id.into()),
            submission_id: None,
        }
    }

    fn assistant(text: &str) -> ControlUnit {
        ControlUnit::AssistantMessage {
            message: Arc::new(AssistantMessage {
                message_id: MessageId("message".into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            }),
        }
    }

    fn run_finished() -> ControlUnit {
        run_finished_with(RunFinishReason::Completed)
    }

    /// A pause naming its run. `Paused` has always carried one.
    fn paused_for(run_id: &str) -> ControlUnit {
        ControlUnit::Paused {
            run_id: RunId(run_id.into()),
            reason: broca_protocol::PauseReason::AuthRequired,
            error: None,
        }
    }

    /// A terminal that names its run, which is what a current producer emits.
    fn run_finished_for(run_id: &str) -> ControlUnit {
        ControlUnit::RunFinished {
            run_id: Some(RunId(run_id.into())),
            reason: RunFinishReason::Completed,
            usage: None,
            final_step_finish_reason: None,
            indeterminate_tool_calls: None,
            retries_used: 0,
        }
    }

    fn run_finished_with(reason: RunFinishReason) -> ControlUnit {
        ControlUnit::RunFinished {
            run_id: None,
            reason,
            usage: None,
            final_step_finish_reason: None,
            indeterminate_tool_calls: None,
            retries_used: 0,
        }
    }

    fn exit_code_for(reason: RunFinishReason) -> i32 {
        let mut tracker = RunTurnTracker::new("ours");
        assert_eq!(
            tracker.consume(&run_started("ours")),
            RunTurnAction::Continue
        );
        assert_eq!(
            tracker.consume(&run_finished_with(reason)),
            RunTurnAction::Finished
        );
        tracker.exit_code()
    }

    fn render_combined_turn(json: bool, controls: &[ControlUnit]) -> Vec<String> {
        let mut tracker = RunTurnTracker::new("ours");
        let mut lines = Vec::new();

        for (wal_seq, unit) in controls.iter().enumerate() {
            let action = tracker.consume(unit);
            if action == RunTurnAction::Ignore {
                continue;
            }
            emit_control_to(
                ControlRenderMode::Run,
                json,
                wal_seq as u64,
                unit,
                &mut |line| lines.push(line),
            );
            if matches!(action, RunTurnAction::Finished | RunTurnAction::Paused) {
                emit_run_final_text_to(json, &tracker.final_text, &mut |line| lines.push(line));
                break;
            }
        }

        lines
    }

    #[test]
    fn combined_run_completed_exits_zero() {
        assert_eq!(exit_code_for(RunFinishReason::Completed), 0);
    }

    #[test]
    fn combined_run_max_steps_exits_zero() {
        assert_eq!(exit_code_for(RunFinishReason::MaxSteps), 0);
    }

    #[test]
    fn combined_run_error_exits_nonzero() {
        assert_ne!(exit_code_for(RunFinishReason::Error), 0);
    }

    #[test]
    fn combined_run_interrupted_exits_nonzero() {
        assert_ne!(exit_code_for(RunFinishReason::Interrupted), 0);
    }

    #[test]
    fn combined_run_cancelled_exits_nonzero() {
        assert_ne!(exit_code_for(RunFinishReason::Cancelled), 0);
    }

    #[test]
    fn combined_run_transform_unavailable_exits_nonzero() {
        assert_ne!(exit_code_for(RunFinishReason::TransformUnavailable), 0);
    }

    #[test]
    fn content_import_id_matches_legacy_canonical_digest() {
        let messages = vec![broca_protocol::Message::new(
            broca_protocol::Role::User,
            vec![ContentBlock::Text {
                text: "canonical import".into(),
            }],
        )];
        let canonical = serde_json::to_string(&messages).unwrap();
        let legacy = format!(
            "file-{}",
            broca_provider::hash::base62(broca_provider::hash::fnv1a128(&canonical))
        );

        assert_eq!(content_import_id(&messages).unwrap(), legacy);
    }

    #[test]
    fn send_flags_build_cache_key_with_role_and_ttl() {
        let args = Args::try_parse_from([
            "broca-session",
            "--subc",
            "/tmp/subc.json",
            "--project-root",
            "/tmp/project",
            "--session",
            "s1",
            "send",
            "--prompt",
            "hello",
            "--model",
            "provider/model",
            "--cache-key",
            "shared-key/東京",
            "--cache-role",
            "primary",
            "--cache-ttl",
            "1h",
            "--max-output-tokens",
            "137",
        ])
        .expect("parse send command");
        let Cmd::Send {
            max_output_tokens,
            cache_key,
            cache_role,
            cache_ttl,
            no_cache,
            ..
        } = args.cmd
        else {
            panic!("expected send command");
        };

        assert_eq!(max_output_tokens, Some(137));
        let cache = cache_params(cache_role, cache_ttl, cache_key, no_cache).unwrap();
        assert_eq!(cache.key.as_deref(), Some("shared-key/東京"));
        assert_eq!(cache.ttl.as_deref(), Some("1h"));
        assert!(matches!(
            cache.role,
            Some(broca_protocol::request::CacheRole::Primary)
        ));
    }

    #[tokio::test]
    async fn send_max_output_tokens_zero_is_refused_locally() {
        let args = Args::try_parse_from([
            "broca-session",
            "--subc",
            "/tmp/subc.json",
            "--project-root",
            "/tmp/project",
            "--session",
            "s1",
            "send",
            "--prompt",
            "hello",
            "--model",
            "provider/model",
            "--max-output-tokens",
            "0",
        ])
        .expect("zero is parsed so the CLI can explain its refusal");

        assert_eq!(
            run(args).await.unwrap_err(),
            "--max-output-tokens must be greater than 0"
        );
    }

    #[test]
    fn send_cache_key_conflicts_with_no_cache() {
        let result = Args::try_parse_from([
            "broca-session",
            "--subc",
            "/tmp/subc.json",
            "--project-root",
            "/tmp/project",
            "--session",
            "s1",
            "send",
            "--prompt",
            "hello",
            "--model",
            "provider/model",
            "--cache-key",
            "shared-key",
            "--no-cache",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn import_subcommand_accepts_canonical_file_and_source_ref() {
        let args = Args::try_parse_from([
            "broca-session",
            "--subc",
            "/tmp/subc.json",
            "--project-root",
            "/tmp/project",
            "--session",
            "s1",
            "import",
            "--file",
            "/tmp/canonical.json",
            "--source-ref",
            "cc:session-1",
        ])
        .expect("parse import command");
        let Cmd::Import { file, source_ref } = args.cmd else {
            panic!("expected import command");
        };
        assert_eq!(file, std::path::PathBuf::from("/tmp/canonical.json"));
        assert_eq!(source_ref.as_deref(), Some("cc:session-1"));
    }

    #[test]
    fn absent_tools_flag_omits_surface_while_empty_json_is_explicit() {
        assert_eq!(parse_tools(&None).unwrap(), None);
        assert_eq!(parse_tools(&Some("[]".into())).unwrap(), Some(Vec::new()));
    }

    #[test]
    fn subscribe_emits_assistant_message_in_human_and_json_modes() {
        let reply = assistant("model reply");
        let mut human_lines = Vec::new();
        emit_control_to(
            ControlRenderMode::Subscribe,
            false,
            41,
            &reply,
            &mut |line| human_lines.push(line),
        );
        assert_eq!(
            human_lines,
            ["ASSISTANT_MESSAGE seq=41 message_id=message content=[Text { text: \"model reply\" }]"],
        );

        let mut json_lines = Vec::new();
        emit_control_to(
            ControlRenderMode::Subscribe,
            true,
            41,
            &reply,
            &mut |line| json_lines.push(line),
        );
        assert_eq!(json_lines.len(), 1);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&json_lines[0]).unwrap(),
            serde_json::json!({
                "event": "assistant_message",
                "seq": 41,
                "message": {
                    "message_id": "message",
                    "content": [{"type": "text", "text": "model reply"}],
                },
            }),
        );
    }

    #[test]
    fn combined_run_renders_model_output_once_as_final_text() {
        let reply = assistant("model reply");
        let mut tracker = RunTurnTracker::new("ours");
        assert_eq!(
            tracker.consume(&run_started("ours")),
            RunTurnAction::Continue
        );
        assert_eq!(tracker.consume(&reply), RunTurnAction::Continue);

        let mut human_lines = Vec::new();
        emit_control_to(ControlRenderMode::Run, false, 41, &reply, &mut |line| {
            human_lines.push(line)
        });
        emit_run_final_text_to(false, &tracker.final_text, &mut |line| {
            human_lines.push(line)
        });
        assert_eq!(human_lines, ["FINAL_TEXT model reply"]);

        let mut json_lines = Vec::new();
        emit_control_to(ControlRenderMode::Run, true, 41, &reply, &mut |line| {
            json_lines.push(line)
        });
        emit_run_final_text_to(true, &tracker.final_text, &mut |line| json_lines.push(line));
        assert_eq!(json_lines.len(), 1);
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&json_lines[0]).unwrap(),
            serde_json::json!({"event": "final_text", "text": "model reply"}),
        );
    }

    #[test]
    fn combined_run_clears_earlier_text_when_final_assistant_message_is_empty() {
        let controls = [
            run_started("ours"),
            assistant("earlier tool-step commentary"),
            assistant(""),
            run_finished(),
        ];

        let human_lines = render_combined_turn(false, &controls);
        assert_eq!(
            human_lines,
            [
                "RUN_STARTED seq=0 run_id=ours",
                "RUN_FINISHED seq=3 reason=Completed retries=0",
            ]
        );
        assert!(!human_lines
            .iter()
            .any(|line| line.starts_with("FINAL_TEXT")));

        let json_events = render_combined_turn(true, &controls)
            .into_iter()
            .map(|line| serde_json::from_str::<serde_json::Value>(&line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            json_events,
            [
                serde_json::json!({"event": "run_started", "seq": 0, "run_id": "ours"}),
                serde_json::json!({"event": "run_finished", "seq": 3, "reason": "Completed", "usage": null, "final_step_finish_reason": null, "indeterminate_tool_calls": null, "retries_used": 0}),
            ]
        );
        assert!(!json_events
            .iter()
            .any(|event| event["event"] == "final_text"));
    }

    #[test]
    fn combined_run_emits_non_empty_final_assistant_text_once_after_run_finished() {
        let controls = [
            run_started("ours"),
            assistant("model reply"),
            run_finished(),
        ];

        let human_lines = render_combined_turn(false, &controls);
        assert_eq!(
            human_lines,
            [
                "RUN_STARTED seq=0 run_id=ours",
                "RUN_FINISHED seq=2 reason=Completed retries=0",
                "FINAL_TEXT model reply",
            ]
        );
        assert_eq!(
            human_lines
                .iter()
                .filter(|line| line.starts_with("FINAL_TEXT"))
                .count(),
            1
        );

        let json_events = render_combined_turn(true, &controls)
            .into_iter()
            .map(|line| serde_json::from_str::<serde_json::Value>(&line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            json_events,
            [
                serde_json::json!({"event": "run_started", "seq": 0, "run_id": "ours"}),
                serde_json::json!({"event": "run_finished", "seq": 2, "reason": "Completed", "usage": null, "final_step_finish_reason": null, "indeterminate_tool_calls": null, "retries_used": 0}),
                serde_json::json!({"event": "final_text", "text": "model reply"}),
            ]
        );
        assert_eq!(
            json_events
                .iter()
                .filter(|event| event["event"] == "final_text")
                .count(),
            1
        );
    }

    /// A RESUMED run that PAUSES AGAIN must end the attach, not hang it.
    ///
    /// The twin of the case below, and it survived that fix: `RunFinished` was hoisted
    /// above the start gate and `Paused` was left behind it, though `Paused` has always
    /// carried its run id. Trigger: a durably auth-paused episode is re-sent, the
    /// credential resolves at admission so the run RESUMES (no new `RunStarted`), and the
    /// provider then answers 401. The daemon publishes `Paused` with the original run id;
    /// the client ignored it and waited on a stream with no local deadline.
    #[test]
    fn combined_turn_stops_on_a_resumed_run_that_pauses_again() {
        let mut tracker = RunTurnTracker::new("ours");
        // Exactly what a resumed-then-paused episode puts on the wire after the attach:
        // no RunStarted precedes it.
        assert_eq!(
            tracker.consume(&paused_for("ours")),
            RunTurnAction::Paused,
            "the client must end the attach on its own pause, with or without a RunStarted"
        );
    }

    /// And another run's pause is still not ours, which the hoist must not trade away.
    #[test]
    fn combined_turn_ignores_a_foreign_pause_before_its_run_starts() {
        let mut tracker = RunTurnTracker::new("ours");
        assert_eq!(
            tracker.consume(&paused_for("foreign")),
            RunTurnAction::Ignore
        );
        assert_eq!(
            tracker.consume(&run_finished_for("ours")),
            RunTurnAction::Finished,
            "a foreign pause must not end this turn, nor prevent it ending properly"
        );
    }

    /// A RESUMED run completes with no `RunStarted`, and the client must still see it.
    ///
    /// Resume continues the same episode and deliberately writes no new `RunStarted`,
    /// while a Live attach excludes pre-attach history. A client that waits for one waits
    /// through its own successful completion: no answer printed, no finish reason, exit 1
    /// on a run that finished cleanly. The terminal naming its run is what closes that.
    #[test]
    fn combined_turn_accepts_a_resumed_run_that_never_emits_run_started() {
        let mut tracker = RunTurnTracker::new("ours");
        // Exactly what a resumed run puts on the wire after the attach.
        assert_eq!(
            tracker.consume(&assistant("resumed answer")),
            RunTurnAction::Ignore,
            "still unattributed, so it must not RENDER as this turn's reply yet"
        );
        assert!(
            tracker.final_text.is_empty(),
            "nothing is this turn's answer until a terminal says so"
        );
        assert_eq!(
            tracker.consume(&run_finished_for("ours")),
            RunTurnAction::Finished
        );
        assert_eq!(
            tracker.final_text, "resumed answer",
            "the held text is promoted once the terminal names this run"
        );
        assert_eq!(tracker.finish_reason, Some(RunFinishReason::Completed));
    }

    /// And a PRIOR run's answer is never promoted, which is the guarantee the
    /// hold-then-promote path must not trade away.
    #[test]
    fn combined_turn_never_promotes_an_earlier_runs_answer() {
        let mut tracker = RunTurnTracker::new("ours");
        assert_eq!(
            tracker.consume(&assistant("foreign answer")),
            RunTurnAction::Ignore
        );
        assert_eq!(
            tracker.consume(&run_finished_for("foreign")),
            RunTurnAction::Ignore,
            "a terminal naming another run is not this turn's"
        );
        assert_eq!(
            tracker.consume(&run_finished_for("ours")),
            RunTurnAction::Finished
        );
        assert!(
            tracker.final_text.is_empty(),
            "the earlier run's text must not survive its own terminal"
        );
    }

    #[test]
    fn combined_turn_ignores_foreign_terminal_and_text_before_its_run_starts() {
        let controls = [
            run_started("foreign"),
            assistant("foreign answer"),
            run_finished(),
            run_started("ours"),
            assistant("our answer"),
            run_finished(),
        ];
        let mut tracker = RunTurnTracker::new("ours");

        for unit in &controls[..3] {
            assert_eq!(tracker.consume(unit), RunTurnAction::Ignore);
        }
        assert!(tracker.final_text.is_empty());
        assert_eq!(tracker.consume(&controls[3]), RunTurnAction::Continue);
        assert_eq!(tracker.consume(&controls[4]), RunTurnAction::Continue);
        assert_eq!(tracker.final_text, "our answer");
        assert_eq!(tracker.consume(&controls[5]), RunTurnAction::Finished);
    }

    #[test]
    fn combined_turn_accepts_only_its_own_pause() {
        let mut tracker = RunTurnTracker::new("ours");
        assert_eq!(
            tracker.consume(&run_started("ours")),
            RunTurnAction::Continue
        );
        assert_eq!(
            tracker.consume(&ControlUnit::Paused {
                run_id: RunId("foreign".into()),
                reason: PauseReason::AuthRequired,
                error: None,
            }),
            RunTurnAction::Ignore
        );
        assert_eq!(
            tracker.consume(&ControlUnit::Paused {
                run_id: RunId("ours".into()),
                reason: PauseReason::AuthRequired,
                error: None,
            }),
            RunTurnAction::Paused
        );
    }

    #[test]
    fn combined_turn_declines_to_wait_for_a_queued_submission() {
        let outcome = SendOutcome::Pending {
            submission_id: "submission-7".into(),
        };
        let refusal = run_id_for_combined_turn(&outcome).unwrap_err();
        assert_eq!(
            refusal,
            RunTurnRefusal::Pending {
                submission_id: "submission-7".into(),
            }
        );
        let message = refusal.to_string();
        assert!(message.contains("send-only command"));
        assert!(message.contains("run.status"));
        assert!(message.contains("submission-7"));
    }
}

#[cfg(test)]
mod credential_audit_tests {
    use broca_module_serve::wire::{VaultAuditMissing, VaultAuditResponse, VaultAuditRow};

    use super::{
        audit_events, audit_exit_code, EXIT_AUDIT_CLEAN, EXIT_AUDIT_FINDING, EXIT_AUDIT_UNANSWERED,
    };

    fn row(id: &str, state: &str, referenced_by: &[&str]) -> VaultAuditRow {
        VaultAuditRow {
            credential_id: id.into(),
            credential_type: "oauth".into(),
            state: state.into(),
            account_id: None,
            referenced_by: referenced_by.iter().map(|key| key.to_string()).collect(),
        }
    }

    fn checked(rows: Vec<VaultAuditRow>, missing: Vec<VaultAuditMissing>) -> VaultAuditResponse {
        VaultAuditResponse::Checked {
            principal: "reserved:broca".into(),
            view: "digest".into(),
            rows,
            missing,
        }
    }

    /// THE FINDING THE AUDIT EXISTS FOR: a credential broca routes to that the vault did not
    /// list exits 1 and is printed by id with the keys that depend on it.
    #[test]
    fn a_referenced_credential_the_vault_did_not_list_exits_one() {
        let answer = Ok(checked(
            vec![row("oauth:anthropic", "active", &["oauth:anthropic"])],
            vec![VaultAuditMissing {
                credential_id: "apikey:kimi-for-coding".into(),
                referenced_by: vec!["apikey:kimi-code-plan-global".into()],
            }],
        ));
        assert_eq!(audit_exit_code(&answer), EXIT_AUDIT_FINDING);
        let lines: Vec<String> = audit_events(&answer)
            .into_iter()
            .map(|(_, line)| line)
            .collect();
        assert!(lines.contains(
            &"CREDENTIAL_AUDIT id=apikey:kimi-for-coding result=missing referenced_by=apikey:kimi-code-plan-global"
                .to_string()
        ), "{lines:?}");
        assert!(
            lines.last().unwrap().contains("missing=1 exit=1"),
            "{lines:?}"
        );
    }

    /// A referenced credential listed in any state but active is a finding; an unreferenced
    /// one is reported and is not.
    #[test]
    fn only_a_referenced_inactive_row_is_a_finding() {
        let clean = Ok(checked(
            vec![
                row("oauth:anthropic", "active", &["oauth:anthropic"]),
                row("apikey:openrouter", "needs_reauth", &[]),
            ],
            Vec::new(),
        ));
        assert_eq!(audit_exit_code(&clean), EXIT_AUDIT_CLEAN);
        let events = audit_events(&clean);
        assert_eq!(
            events.len(),
            3,
            "every listed row is printed, plus the summary"
        );
        assert_eq!(events[1].0["result"], "unreferenced");
        assert_eq!(events[1].0["state"], "needs_reauth");

        let finding = Ok(checked(
            vec![row("oauth:anthropic", "needs_reauth", &["oauth:anthropic"])],
            Vec::new(),
        ));
        assert_eq!(audit_exit_code(&finding), EXIT_AUDIT_FINDING);
        assert_eq!(audit_events(&finding)[0].0["result"], "not_active");
    }

    /// ABSENCE IS NOT ZERO. A listing the daemon could not get, or a request that got no
    /// answer at all, exits 2 and says the check did not run; it never reads as "nothing
    /// vanished".
    #[test]
    fn an_unanswered_audit_exits_two_and_says_it_did_not_run() {
        for answer in [
            Ok(VaultAuditResponse::Unanswered {
                reason: "credential.list_scoped: vault transport failure".into(),
            }),
            Err("vault.audit: route rejected".to_string()),
        ] {
            assert_eq!(audit_exit_code(&answer), EXIT_AUDIT_UNANSWERED);
            let events = audit_events(&answer);
            assert_eq!(events.len(), 1);
            assert_eq!(events[0].0["event"], "credential_audit_did_not_run");
            assert_eq!(events[0].0["exit"], EXIT_AUDIT_UNANSWERED);
        }
    }

    #[test]
    fn an_empty_but_answered_audit_is_clean() {
        let answer = Ok(checked(Vec::new(), Vec::new()));
        assert_eq!(audit_exit_code(&answer), EXIT_AUDIT_CLEAN);
        assert_eq!(audit_events(&answer)[0].0["listed"], 0);
    }
}
