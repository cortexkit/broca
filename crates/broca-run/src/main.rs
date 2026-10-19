//! `broca-run` — the harness CLI seed.
//!
//! Drives the agentic `run_loop` as a CONSUMER of an already-running subc daemon and
//! an OpenAI-compatible chat provider (e.g. a mock, or any real openai-compatible
//! endpoint). It connects to subc, discovers its tools from the live catalog (so the
//! harness uses the manifest the module actually registered, not hand-authored
//! schemas), drives the loop, and prints greppable per-turn markers to stdout so an
//! orchestrator can assert on per-turn progress (never on session-exit alone).
//!
//! This is the neutral binary the cortexkit-e2e harness orchestrates: it owns no
//! daemon and no provider — it's handed their endpoints and drives the loop between
//! them. It's also the embryonic harness CLI (a harness discovers its tools and runs
//! the loop; it isn't hand-fed them).
//!
//! Exit code: 0 if the run reached a clean terminal (Completed/MaxSteps), 1 on a
//! provider/loop error, an ambiguous-dispatch Interrupt, or a startup failure.

#![forbid(unsafe_code)]

use std::path::PathBuf;
use std::time::Duration;

use broca_catalog::live::{AuthMode, ProviderResolution};
use broca_core::{run_loop, AllowAll, FileLeaseStore, FileWalStore, OutputEvent, RunRequest};
use broca_protocol::{
    caller::{ControlUnit, RunFinishReason},
    content::{ContentBlock, Message, Role, ToolOutput},
    request::{ModelHandle, StopCondition, Tool, ToolChoice},
    BindIdentity, GenerationConfig, RunId,
};
use broca_subc::{SubcConnection, SubcToolPlane};
use clap::Parser;
use tokio::sync::mpsc;

#[derive(Parser, Debug)]
#[command(
    name = "broca-run",
    version,
    about = "Drive the run_loop through a running subc daemon + an openai-compatible provider."
)]
struct Args {
    /// Path to the subc daemon's connection file (subc-connection.json).
    #[arg(long)]
    subc: PathBuf,

    /// Base URL of an openai-compatible provider (e.g. a mock or any
    /// openai-compatible endpoint). POSTs go to `<base_url>/chat/completions`. When
    /// set, this is an explicit override: the provider is OpenAI-Chat at this URL and
    /// `--model` is sent verbatim as the wire model id (the catalog is NOT consulted).
    /// Omit it to resolve the provider/family/base_url from the catalog by `--model`.
    #[arg(long)]
    base_url: Option<String>,

    /// The model to run. With `--base-url`, this is the wire model id sent verbatim.
    /// Without `--base-url`, this is a catalog id `<provider>/<model>` (e.g.
    /// `anthropic/claude-opus-5` or `deepseek/deepseek-v4-flash`) that resolves the wire
    /// family, base URL, and auth kind from the bundled catalog.
    #[arg(long)]
    model: String,

    /// The user prompt to start the run.
    #[arg(long)]
    prompt: String,

    /// Optional JSON array of tools to use INSTEAD of catalog discovery — each
    /// `{name, description, input_schema, execution_mode}`. Omit to derive the tool
    /// set from what the target module registers in subc's catalog.
    #[arg(long)]
    tools: Option<String>,

    /// The subc module id that provides the tools (the tool-provider to route to).
    #[arg(long, default_value = "aft")]
    module_id: String,

    /// The harness identity for the bind: an OPAQUE key component, joined with U+001F into
    /// the WAL, lease, store and idempotency keys. Broca validates nothing about it beyond
    /// rejecting that separator.
    ///
    /// It used to say "runner" because tool modules run a closed enum that has no broca
    /// variant, and this default was chosen when the caller's value was FORWARDED to them.
    /// It is not any more: `to_subc_bind` TRANSLATES to `TOOL_MODULE_HARNESS` at the
    /// tool-route edge, so the enum never sees this string and the caller's choice is free.
    #[arg(long, default_value = "broca")]
    harness: String,

    /// The session id for this run (single-writer lease key).
    #[arg(long, default_value = "broca-run")]
    session: String,

    /// The project root the run binds to (AFT sandboxes file tools to this root).
    #[arg(long, default_value = ".")]
    project_root: PathBuf,

    /// If set, sent as `Authorization: Bearer <token>`. Omit for no auth header.
    /// An explicit override; takes precedence over `--auth-from-opencode`.
    #[arg(long)]
    auth_bearer: Option<String>,

    /// Resolve the credential from the OpenCode auth file
    /// (`~/.local/share/opencode/auth.json`) for the resolved provider — its OAuth
    /// access token or api key, placed in the header the provider's auth kind requires.
    /// No token refresh (uses whatever is on disk). Ignored if `--auth-bearer` is set.
    #[arg(long)]
    auth_from_opencode: bool,

    /// Max model steps before a structural stop.
    #[arg(long, default_value_t = 8)]
    max_steps: u64,

    /// Max output tokens to request.
    #[arg(long, default_value_t = 2048)]
    max_tokens: u32,

    /// How long to wait for the first route bind (AFT's cold configure — index /
    /// callgraph / semantic-model build — can be slow on a large project root).
    /// Generous by default because a real harness binding a real repo must tolerate
    /// AFT's first-configure cost.
    #[arg(long, default_value_t = 30_000)]
    bind_timeout_ms: u64,

    /// Emit one JSON object per line instead of the human-readable markers — for an
    /// orchestrator that asserts on structured events (stable keys, robust to wording
    /// changes). The default human markers stay the default.
    #[arg(long)]
    json_events: bool,
}

/// Emit one event line: a JSON object when `json` is set, else the human-readable
/// form. Both carry the same data, so a structured consumer and a human reader never
/// diverge on what happened.
fn emit(json: bool, event: serde_json::Value, human: impl FnOnce() -> String) {
    if json {
        println!("{}", serde_json::to_string(&event).unwrap_or_default());
    } else {
        println!("{}", human());
    }
}

/// Resolve the live provider from the CLI args, via the shared catalog→live resolver
/// (so this CLI and the `broca-module-serve` binary can't drift on how a model resolves).
/// `--auth-bearer` (explicit) wins; else `--auth-from-opencode`; else no auth.
fn resolve_provider(
    args: &Args,
) -> Result<ProviderResolution, broca_catalog::live::ResolutionError> {
    let auth = if let Some(t) = args.auth_bearer.as_ref().filter(|s| !s.is_empty()) {
        AuthMode::Bearer(t.clone())
    } else if args.auth_from_opencode {
        AuthMode::FromOpencode
    } else {
        AuthMode::None
    };
    broca_catalog::live::resolve_provider(
        &args.model,
        args.base_url.as_deref(),
        args.max_tokens,
        &auth,
        // The CLI doesn't drive the vault auth-method routing, so no family override or
        // antigravity project — a plain catalog-or-base_url resolution.
        &broca_catalog::live::ResolveOptions::default(),
    )
}

/// The per-process nonce baked into fresh broca-run ids. `{pid}{start-nanos}` makes each
/// CLI invocation greppable while still making a restarted process mint a different suffix.
fn launch_nonce() -> &'static str {
    use std::sync::OnceLock;

    static NONCE: OnceLock<String> = OnceLock::new();
    NONCE.get_or_init(|| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{}{nanos:x}", std::process::id())
    })
}

/// Mint a fresh run_id for each broca-run send. The `broca-run-` prefix stays greppable, while
/// the launch nonce plus sequence ensure a new invocation never reuses an earlier episode id.
fn mint_run_id(session: &str) -> RunId {
    use std::sync::atomic::{AtomicU64, Ordering};

    static SEQ: AtomicU64 = AtomicU64::new(0);
    let seq = SEQ.fetch_add(1, Ordering::SeqCst);
    RunId(format!("broca-run-{session}-{}-{seq}", launch_nonce()))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 4)]
async fn main() {
    let args = Args::parse();
    let json = args.json_events;
    std::process::exit(match run(args).await {
        Ok(code) => code,
        Err(e) => {
            // A startup failure (couldn't connect / no tools / bind timed out) is a
            // greppable error line + exit 1, never a silent hang.
            emit(
                json,
                serde_json::json!({"event": "error", "msg": e}),
                || format!("ERROR {e}"),
            );
            1
        }
    });
}

/// Where this CLI keeps its fusiform last-known-good catalog.
///
/// Resolves the state root through the same function the daemon calls, so the CLI and the
/// daemon share one LKG instead of the CLI keeping a private copy. Two copies would drift, and
/// the one that drifted would be the one nobody was looking at. That includes the daemon's
/// refusals: with no resolvable home, or with state stranded at the old connection-relative
/// default, the CLI refuses for the same reason and with the same message.
fn catalog_lkg_path(args: &Args) -> Result<std::path::PathBuf, String> {
    let state_root = broca_module_serve::state_root::resolve_for_launch(&args.subc)
        .map_err(|e| e.to_string())?;
    Ok(state_root.path.join("catalog/fusiform-lkg.json"))
}

async fn run(args: Args) -> Result<i32, String> {
    let json = args.json_events;
    let bind = BindIdentity::new(&args.project_root, &args.harness, &args.session);
    bind.validate_keyable()
        .map_err(|error| format!("invalid bind identity: {error}"))?;

    // 1. Connect to the running subc daemon as a consumer.
    let conn = SubcConnection::connect(&args.subc)
        .await
        .map_err(|e| format!("connect subc ({}): {e}", args.subc.display()))?;
    let plane = SubcToolPlane::new(conn, &args.module_id);

    // 1b. Fill the catalog before resolving a model.
    //
    // Resolution reads the process-global cache and no longer falls back to a compile-time
    // snapshot, so without this every model id fails with "model catalog unavailable". The
    // daemon fills it from a detached 30-minute poll and can serve from the last-known-good
    // meanwhile; a one-shot CLI has neither a loop nor a prior LKG to fall back on, so it polls
    // once and waits. That makes fusiform a hard startup dependency HERE and only here -- the
    // opposite of the daemon's posture, and correct for a foreground command that would
    // otherwise fail with a confusing error a few lines later.
    let lkg_path = catalog_lkg_path(&args)?;
    let catalog_cache = std::sync::Arc::new(broca_catalog::CatalogCache::open(lkg_path.clone()));
    broca_catalog::install_global_catalog_cache(std::sync::Arc::clone(&catalog_cache));
    let catalog_conn = std::sync::Arc::new(
        SubcConnection::connect(&args.subc)
            .await
            .map_err(|e| format!("catalog consumer connect: {e}"))?,
    );
    let poller = broca_module_serve::CatalogPoller::new(
        catalog_conn,
        BindIdentity::new(&args.project_root, "broca", "catalog-refresh"),
        std::sync::Arc::clone(&catalog_cache),
    );
    if let Err(error) = poller.poll().await {
        // Serve from a stored LKG if one exists; refuse only when there is no catalog at all,
        // and say which of the two failed so the operator knows whether to look at fusiform or
        // at the cache path.
        if catalog_cache.snapshot().is_err() {
            return Err(format!(
                "fetch model catalog from fusiform: {error} (and no last-known-good catalog at {})",
                lkg_path.display()
            ));
        }
        eprintln!("broca-run: catalog poll failed, serving the last-known-good: {error}");
    }

    // 2. Discover tools: explicit override, else the live catalog manifest.
    let tools: Vec<Tool> = match &args.tools {
        Some(json) => serde_json::from_str(json).map_err(|e| format!("parse --tools json: {e}"))?,
        None => plane
            .tools_from_catalog()
            .await
            .map_err(|e| format!("read catalog tools: {e}"))?,
    };
    if tools.is_empty() {
        return Err(format!(
            "no tools available for module '{}' (catalog empty + no --tools)",
            args.module_id
        ));
    }
    let tool_names: Vec<&str> = tools.iter().map(|t| t.name.as_str()).collect();
    emit(
        json,
        serde_json::json!({"event": "tools", "module": args.module_id, "count": tools.len(), "names": tool_names}),
        || {
            format!(
                "TOOLS module={} count={} names={}",
                args.module_id,
                tools.len(),
                tool_names.join(",")
            )
        },
    );

    // 3. Warm the route bind ahead of the timed loop so AFT's cold configure can't
    //    look like a loop stall.
    plane
        .warm_bind(&bind, Duration::from_millis(args.bind_timeout_ms))
        .await
        .map_err(|e| format!("route bind: {e}"))?;
    emit(
        json,
        serde_json::json!({"event": "bound", "module": args.module_id, "root": args.project_root.display().to_string()}),
        || {
            format!(
                "BOUND module={} root={}",
                args.module_id,
                args.project_root.display()
            )
        },
    );

    // 4. Build the live provider. Two modes:
    //    - explicit --base-url: OpenAI-Chat at that URL, --model sent verbatim (the
    //      mock / openai-compatible path; the catalog is not consulted);
    //    - else: resolve provider/family/base_url/auth-kind from the catalog by --model.
    let resolved = resolve_provider(&args).map_err(|e| format!("resolve provider: {e}"))?;
    emit(
        json,
        serde_json::json!({"event": "provider", "family": resolved.family, "base_url": resolved.base_url, "wire_model": resolved.wire_model, "auth": resolved.auth_desc}),
        || {
            format!(
                "PROVIDER family={} base_url={} wire_model={} auth={}",
                resolved.family, resolved.base_url, resolved.wire_model, resolved.auth_desc
            )
        },
    );
    let provider = resolved.provider;
    let provider_tag = resolved.provider_tag;

    // 5. The run request.
    let store = FileLeaseStore::new(args.project_root.join(".broca").join("leases"));
    let wal = FileWalStore::new(args.project_root.join(".broca").join("wal"));
    let policy_allowed = tools.iter().map(|t| t.name.clone()).collect();
    let req = RunRequest {
        session_class: None,
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: mint_run_id(&args.session),
        session: bind,
        model: ModelHandle::new(&provider_tag, &resolved.wire_model),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: args.prompt.clone(),
            }],
        }],
        tools,
        policy_allowed,
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: args.max_steps }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
        structured_output: None,
    };

    // 6. Drive the loop, printing greppable per-turn markers as events arrive (so a
    //    hung loop shows progress up to where it stalled — never a false "completed").
    let (tx, mut rx) = mpsc::channel::<OutputEvent>(2048);
    let printer = tokio::spawn(async move {
        let mut final_text = String::new();
        while let Some(ev) = rx.recv().await {
            if let OutputEvent::Control(unit) = ev {
                match unit {
                    ControlUnit::RunStarted {
                        run_id,
                        submission_id,
                    } => {
                        let event = if let Some(submission_id) = &submission_id {
                            serde_json::json!({
                                "event": "run_started",
                                "run_id": run_id.0,
                                "submission_id": submission_id.0,
                            })
                        } else {
                            serde_json::json!({"event": "run_started", "run_id": run_id.0})
                        };
                        emit(json, event, || match submission_id {
                            Some(submission_id) => format!(
                                "RUN_STARTED run_id={} submission_id={}",
                                run_id.0, submission_id.0
                            ),
                            None => format!("RUN_STARTED run_id={}", run_id.0),
                        });
                    }
                    ControlUnit::StepStarted { step_id } => emit(
                        json,
                        serde_json::json!({"event": "step_started", "step": step_id.0}),
                        || format!("STEP_STARTED step={}", step_id.0),
                    ),
                    ControlUnit::ToolCall { call } => emit(
                        json,
                        serde_json::json!({"event": "tool_call", "name": call.tool_name, "id": call.tool_call_id.0}),
                        || {
                            format!(
                                "TOOL_CALL name={} id={}",
                                call.tool_name, call.tool_call_id.0
                            )
                        },
                    ),
                    ControlUnit::ToolResult { result } => {
                        let text = match &result.output {
                            ToolOutput::Text { text } => text.clone(),
                            ToolOutput::Error { message } => message.clone(),
                            _ => String::new(),
                        };
                        emit(
                            json,
                            serde_json::json!({"event": "tool_result", "id": result.tool_call_id.0, "is_error": result.is_error, "text": text}),
                            || {
                                format!(
                                    "TOOL_RESULT id={} is_error={} text={}",
                                    result.tool_call_id.0,
                                    result.is_error,
                                    text.replace('\n', "\\n")
                                )
                            },
                        );
                    }
                    ControlUnit::StepFinished {
                        step_id,
                        finish_reason,
                        ..
                    } => emit(
                        // The canonical "one turn served" marker.
                        json,
                        serde_json::json!({"event": "step_finished", "step": step_id.0, "reason": format!("{finish_reason:?}")}),
                        || {
                            format!(
                                "STEP_FINISHED step={} reason={:?}",
                                step_id.0, finish_reason
                            )
                        },
                    ),
                    ControlUnit::AssistantMessage { message } => {
                        let t: String = message
                            .content
                            .iter()
                            .filter_map(|b| match b {
                                ContentBlock::Text { text }
                                | ContentBlock::TextWithState { text, .. } => Some(text.as_str()),
                                _ => None,
                            })
                            .collect();
                        if !t.trim().is_empty() {
                            final_text = t;
                        }
                    }
                    ControlUnit::Error { error } => emit(
                        json,
                        serde_json::json!({"event": "loop_error", "class": format!("{:?}", error.class), "msg": error.message}),
                        || {
                            format!(
                                "LOOP_ERROR class={:?} msg={}",
                                error.class,
                                error.message.replace('\n', "\\n")
                            )
                        },
                    ),
                    _ => {}
                }
            }
        }
        final_text
    });

    let summary = run_loop(req, &provider, &plane, &store, &AllowAll, &wal, &tx)
        .await
        .map_err(|e| format!("run_loop: {e}"))?;
    drop(tx);
    let final_text = printer.await.unwrap_or_default();

    emit(
        json,
        serde_json::json!({"event": "run_finished", "reason": format!("{:?}", summary.reason), "steps": summary.steps}),
        || {
            format!(
                "RUN_FINISHED reason={:?} steps={}",
                summary.reason, summary.steps
            )
        },
    );
    emit(
        json,
        {
            let mut event = serde_json::json!({"event": "usage"});
            for (key, count) in [
                ("input", summary.usage.input_tokens),
                ("output", summary.usage.output_tokens),
                ("reasoning", summary.usage.reasoning_tokens),
            ] {
                if let Some(count) = count {
                    event[key] = count.into();
                }
            }
            event
        },
        || {
            format!(
                "USAGE input={} output={} reasoning={}",
                summary
                    .usage
                    .input_tokens
                    .map_or_else(|| "unreported".into(), |n| n.to_string()),
                summary
                    .usage
                    .output_tokens
                    .map_or_else(|| "unreported".into(), |n| n.to_string()),
                summary
                    .usage
                    .reasoning_tokens
                    .map_or_else(|| "unreported".into(), |n| n.to_string())
            )
        },
    );
    if !final_text.trim().is_empty() {
        emit(
            json,
            serde_json::json!({"event": "final_text", "text": final_text}),
            || format!("FINAL_TEXT {}", final_text.replace('\n', "\\n")),
        );
    }

    // A clean terminal is Completed or the structural MaxSteps; an Interrupt
    // (ambiguous dispatch) or Error is a non-zero exit for the orchestrator.
    let code = match summary.reason {
        RunFinishReason::Completed
        | RunFinishReason::MaxSteps
        | RunFinishReason::BudgetExhausted => 0,
        RunFinishReason::Interrupted
        | RunFinishReason::Cancelled
        | RunFinishReason::Error
        | RunFinishReason::TransformUnavailable => 1,
    };
    Ok(code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::request::CallOptions;
    use broca_provider::families::{openai_chat_frozen_config, OpenAiChat};
    use broca_provider::wire::WireFamily;

    /// Seed the process-global catalog cache from the bundled snapshot.
    ///
    /// These tests call `resolve_provider` directly, so they bypass the poll that `run` does and
    /// would otherwise fail with "model catalog unavailable" rather than exercising what they are
    /// about. The snapshot stands in for a fusiform response because the assertions are about
    /// family selection and rendering, not about catalog contents.
    fn seed_catalog() {
        use std::sync::Once;
        static SEEDED: Once = Once::new();
        SEEDED.call_once(|| {
            let catalog = broca_catalog::Catalog::from_json_str(
                include_str!("../../broca-catalog/data/models.dev.json"),
                include_str!("../../broca-catalog/data/overlay.json"),
            )
            .expect("bundled snapshot is well formed");
            let cache = std::sync::Arc::new(broca_catalog::CatalogCache::empty());
            cache.install_parsed_for_test(&catalog, 0, broca_catalog::CatalogSource::Fusiform);
            broca_catalog::install_global_catalog_cache(cache);
        });
    }

    /// Parse a minimal arg vector into `Args` (exercises the real clap parsing, so a
    /// flag rename or a wrong `required` shows up here). Extra flags are appended.
    fn args(extra: &[&str]) -> Args {
        let mut v = vec![
            "broca-run",
            "--subc",
            "/tmp/x.json",
            "--model",
            "PLACEHOLDER",
            "--prompt",
            "hi",
        ];
        v.extend_from_slice(extra);
        Args::parse_from(v)
    }

    /// `resolve_provider` Ok-or-panic — `ProviderResolution` holds a `Box<dyn Provider>`
    /// (not `Debug`), so the tests can't use `.expect()` on the Result directly.
    fn resolved_ok(a: &Args) -> ProviderResolution {
        match resolve_provider(a) {
            Ok(r) => r,
            Err(e) => panic!("resolve_provider should succeed: {e}"),
        }
    }

    fn fixed_opts() -> CallOptions {
        let mut o = CallOptions::new(vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            tags: Default::default(),
            role: Role::User,
            content: vec![ContentBlock::Text { text: "hi".into() }],
        }]);
        o.tool_choice = ToolChoice::Auto.into();
        o
    }

    /// Fresh broca-run sends must mint distinct run_ids so a new prompt against an idle
    /// lineage appends a new episode instead of reading as a crash-recovery re-drive.
    #[test]
    fn minted_run_ids_are_unique_and_keep_the_broca_run_prefix() {
        let a = mint_run_id("session-a");
        let b = mint_run_id("session-a");
        assert_ne!(a.0, b.0, "two sends should not reuse the same run_id");
        let nonce = launch_nonce();
        assert!(
            a.0.contains(nonce) && b.0.contains(nonce),
            "minted ids should include the launch nonce: {} / {}",
            a.0,
            b.0
        );
        assert!(
            a.0.starts_with("broca-run-session-a-"),
            "ids keep the broca-run session prefix: {}",
            a.0
        );
        assert!(
            b.0.starts_with("broca-run-session-a-"),
            "ids keep the broca-run session prefix: {}",
            b.0
        );
    }

    /// The cortexkit-e2e mock path is UNCHANGED: `--base-url <url> --model <id>` builds
    /// an OpenAI-Chat provider at that URL with the model verbatim, rendering exactly
    /// the bytes the old hardcoded `LiveProvider<OpenAiChat>` construction did.
    #[test]
    fn base_url_override_is_the_unchanged_openai_chat_mock_path() {
        let mut a = args(&[
            "--base-url",
            "http://127.0.0.1:9999",
            "--max-tokens",
            "2048",
        ]);
        a.model = "mock-model".to_string();
        let resolved = resolved_ok(&a);

        assert_eq!(resolved.family, "OpenAiChat");
        assert_eq!(resolved.base_url, "http://127.0.0.1:9999");
        assert_eq!(resolved.wire_model, "mock-model");
        assert_eq!(resolved.provider_tag, "openai-compatible");

        // The reference: the exact construction the CLI used before this change.
        let mut frozen = openai_chat_frozen_config("mock-model");
        frozen.base_url = Some("http://127.0.0.1:9999".to_string());
        frozen.max_output_tokens = 2048;
        let reference = OpenAiChat.render(&frozen, &fixed_opts());

        assert_eq!(
            resolved.provider.render_request(&fixed_opts()),
            reference,
            "the --base-url mock path must render byte-identical to the pre-change \
             LiveProvider<OpenAiChat> construction (cortexkit-e2e depends on it)"
        );
    }

    /// No `--auth-bearer`, no `--auth-from-opencode` → no auth header (the mock needs
    /// none). Same as the old default.
    #[test]
    fn base_url_path_defaults_to_no_auth() {
        let a = args(&["--base-url", "http://127.0.0.1:9999"]);
        let resolved = resolved_ok(&a);
        assert_eq!(resolved.auth_desc, "none");
    }

    /// Catalog mode resolves a native family correctly: `anthropic/<model>` → the
    /// AnthropicMessages family (not OpenAI-Chat), and the resolved provider renders
    /// the required "You are Claude Code" identity-lead block — proving the CLI drives
    /// the corrected catalog path, not the old OpenAI-chat-shape default.
    #[test]
    fn catalog_mode_resolves_anthropic_to_its_native_family() {
        seed_catalog();
        let mut a = args(&[]);
        a.model = "anthropic/claude-opus-5".to_string();
        let resolved = resolved_ok(&a);

        assert_eq!(resolved.family, "AnthropicMessages");
        assert_eq!(resolved.wire_model, "claude-opus-5");
        assert_eq!(resolved.provider_tag, "anthropic");

        let rendered =
            String::from_utf8(resolved.provider.render_request(&fixed_opts())).expect("utf8 body");
        assert!(
            rendered.contains("You are Claude Code"),
            "a catalog-resolved Anthropic must render the required identity-lead block \
             (the bug this whole change fixes); body was: {rendered}"
        );
    }

    /// Without `--base-url`, a `--model` that is not a `<provider>/<model>` catalog id
    /// is a clear error, not a silent fallback to some default endpoint.
    #[test]
    fn catalog_mode_rejects_a_non_catalog_model() {
        let mut a = args(&[]);
        a.model = "just-a-bare-name".to_string();
        let err = match resolve_provider(&a) {
            Err(e) => e,
            Ok(_) => panic!("a bare model id without --base-url must error"),
        };
        assert!(
            err.to_string().contains("not a catalog id"),
            "error should explain the catalog-id requirement, got: {err}"
        );
    }

    /// An unknown provider/model under catalog mode errors clearly (no silent default).
    #[test]
    fn catalog_mode_rejects_unknown_catalog_model() {
        seed_catalog();
        let mut a = args(&[]);
        a.model = "anthropic/no-such-model-xyz".to_string();
        let err = match resolve_provider(&a) {
            Err(e) => e,
            Ok(_) => panic!("unknown model must error"),
        };
        assert!(
            err.to_string().contains("no served model"),
            "error should name the missing model, got: {err}"
        );
    }
}
