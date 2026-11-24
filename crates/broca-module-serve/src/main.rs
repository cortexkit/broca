//! The broca subc module serve binary.
//!
//! Lifecycle (the canonical module handshake): read `--subc <connection-file>`,
//! HMAC-authenticate as a client to subc, HELLO-register the manifest, then serve the
//! frame loop (route.bind → ack, session.*/run.* route requests → the coordinator).
#![forbid(unsafe_code)]

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use broca_core::FileLeaseStore;
use broca_module::LineageRegistry;
use broca_module_serve::{
    spawn_catalog_poll, CatalogPoller, ChargeBasisTable, ConsumerRoutes, ServeEnd, ServeIoError,
};
use broca_subc::{SubcConnection, SubcToolPlane};
use subc_protocol::SUBC_MODULE_ID_ENV;
use subc_transport::{authenticate_client, connection_file};
use tokio::net::TcpStream;

mod state_format;

const DEFAULT_MODULE_ID: &str = "broca";
const AUTH_DEADLINE: Duration = Duration::from_secs(2);
const EMBEDDED_ENGRAM_CATALOG: &[u8] = include_bytes!("../../../engram-catalog.json");
/// Maximum time a credential-resolving call keeps STARTING work toward the credentials module:
/// waiting for it to register, and retrying its temporary route-open refusals. Boot never spends
/// this budget, and it stays below the 10-second response deadline a caller of broca ordinarily
/// waits, so an absent or restarting credentials module reaches that caller as a typed vault
/// error instead of a transport timeout.
///
/// It does not cut off a route open already in flight: that one may run to
/// `broca_subc::ROUTE_OPEN_ATTEMPT_TIMEOUT` (15s), because the daemon can legitimately take up to
/// 12s to relay the bind. Shrinking this budget would not shorten that wait, only start fewer
/// opens. When a bind is that slow the caller may time out first, but the route that opens is
/// cached, so the caller's next attempt finds it ready rather than binding again.
const VAULT_READY_TIMEOUT: Duration = Duration::from_secs(5);

/// Whether System One serves OpenAI's Decisions model (`openai/gpt-6-luna-decisions`). Off
/// until a live request has confirmed OpenAI's answer shapes: the adapter is tested only
/// against a stand-in server, and while it's off the model is neither advertised nor served,
/// even though its catalog row exists.
const OPENAI_DECISIONS_SERVED: bool = false;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    broca_module::diagnostics::initialize();
    broca_module_serve::serve::initialize_session_readers();
    run_runtime(serve_main())?
}

fn run_runtime<F: std::future::Future>(work: F) -> std::io::Result<F::Output> {
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    // The heap profiler walks the stack inside the allocator, on top of whatever a worker was
    // doing, and a debug build's workers overflowed their 2 MiB under it; give them room.
    #[cfg(feature = "heap-profile")]
    builder.thread_stack_size(8 * 1024 * 1024);
    let runtime = builder.build()?;
    let result = runtime.block_on(work);
    // The main future explicitly joins durability work before returning. Startup export
    // catch-up is replayable from the durable outbox, and archive prewarm is only a cache;
    // neither may prolong process exit through Tokio's implicit blocking-pool join.
    runtime.shutdown_background();
    Ok(result)
}

async fn serve_main() -> Result<(), Box<dyn std::error::Error>> {
    // READ THE LAUNCH NONCE BEFORE ANYTHING ELSE. The daemon hands it over as an inherited pipe
    // (named by SUBC_LAUNCH_NONCE_FD), and that descriptor stays inheritable until it is read:
    // a process spawned before this line would inherit the pipe and could read broca's secret.
    // The accessor reads it once, closes it and caches the answer; HELLO and every route open
    // later ask the same accessor and get the cached value. An error is reported below, once the
    // logger exists, rather than here, so the probes that follow keep working in a shell that
    // inherited the variable without the pipe.
    let launch_nonce = subc_os::launch_nonce();
    // TAKE THE REGISTRATION CHANNEL NOW TOO, for the same reason: the daemon's socket for broca
    // stays inheritable until it is taken. broca registers over it when the daemon gave one; see
    // `broca_subc::registration`. Its errors are reported once the logger exists, below.
    let registration_channel = broca_subc::registration::adopt();
    // Reject stray invocations before opening logs or reading config: discovery probes are
    // not daemon boots and must not leave records in the operator's production log.
    let args = parse_launch_args(std::env::args_os().skip(1)).unwrap_or_else(|error| {
        eprintln!("ck-broca: {error}\nvalid arguments: --subc <connection-file>, --version (-V), --known-features");
        std::process::exit(2);
    });
    let conn_path = match args {
        LaunchArgs::Version => {
            println!("ck-broca {}", env!("CARGO_PKG_VERSION"));
            return Ok(());
        }
        LaunchArgs::WalKnownFeatures => {
            // scripts/place-release.sh compares these sorted names with the features in
            // existing WALs to avoid installing a build that cannot read them.
            let mut names = broca_records::floor::KNOWN_FEATURES.to_vec();
            names.sort_unstable();
            for name in names {
                println!("{name}");
            }
            return Ok(());
        }
        LaunchArgs::Daemon(path) => path,
    };
    // BROCA RESOLVES ITS OWN MODULE ID AND THE LOGGER USES THAT ANSWER. Reading the variable
    // here rather than letting the logging crate read it again is the whole point: the crate's
    // zero-argument constructor REFUSES when `SUBC_MODULE_ID` is absent, which would replace
    // the documented fallback below with a hard failure. An operator running this binary by
    // hand to check a config -- the case the provider-override boot test stands for -- would
    // then be told the module id is missing instead of what is wrong with their config. One
    // variable, one reader, and the fallback stays where it is documented.
    let module_id = std::env::var(SUBC_MODULE_ID_ENV)
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| DEFAULT_MODULE_ID.to_string());
    // INSTALL THE LOGGER BEFORE ANYTHING THAT CAN LOG, which is everything below this line.
    // The next statement is one of them: loading the archive config warns when the retention
    // knob is set below the safe floor, and a subscriber installed any later would drop that
    // record with nothing to show it had been written. Checked rather than assumed -- the
    // provider-override load only returns errors, but
    // `WalArchiveConfig::parse` really does warn.
    //
    // It goes after argument validation and the fast probes: neither a usage refusal nor
    // a provenance query should create a log segment.
    //
    // KNOWN GAP: this constructor does not read the daemon's retention knobs
    // (`CK_LOG_MAX_AGE_DAYS`, `CK_LOG_ALARM_SEGMENT_MB`), so segments keep cortexkit-log's
    // default age window and oversize alarm. Only `Config::from_env` reads them, and that is
    // the same constructor that insists on naming the module itself. cortexkit-log 0.3.2 adds
    // `with_env_retention`, which applies those knobs to a config the caller has already
    // named; adopt it there and this paragraph goes away.
    //
    // The guard is bound for the whole of `main`: dropping it stops logging, so this must not
    // become a `let _ =`.
    let _log = cortexkit_log::init(cortexkit_log::Config::for_module(&module_id))?;
    // A descriptor that was named but could not be read (empty, closed, not the named pipe) means
    // the handoff went wrong. Refuse to start, with the accessor's own message, and never fall
    // back to the environment copy: that would hide the broken handoff, and in a process that
    // inherited the variable from a module it would let that process act as broca.
    if let Err(error) = &launch_nonce {
        tracing::error!(target: "lifecycle", "refusing to start: launch nonce: {error}");
        return Err(ServeIoError(format!("launch nonce: {error}")).into());
    }
    // A channel the daemon handed over that can't be used stops the start, as a broken nonce
    // handoff does: registering over the connection file instead would hide it.
    let registration_channel = match registration_channel {
        Ok(channel) => channel,
        Err(error) => {
            tracing::error!(target: "lifecycle", "refusing to start: {error}");
            return Err(ServeIoError(error).into());
        }
    };
    if let Some(refusal) = broca_subc::registration::refusal() {
        tracing::warn!(target: "lifecycle", %refusal, "registration channel not adopted; registering with the launch nonce");
    }
    // Before any broca.jsonc loader: each of them reads a missing file as "no overrides".
    broca_module_serve::config_home::require_explicit_file(
        broca_module_serve::cache_routing::CONFIG_PATH_ENV,
        "broca.jsonc",
    )
    .map_err(ServeIoError)?;
    // Where the session store, leases and run index live. Resolved before any connection is
    // made so a machine that cannot place its durable state refuses to start by name instead
    // of first registering and then failing. See `state_root` for the rule, and for why the
    // location is a contract with astrocyte rather than a private choice. Resolving writes
    // nothing.
    let state_root = broca_module_serve::state_root::resolve_state_root()
        .map_err(|e| ServeIoError(e.to_string()))?;
    // A profiling build answers heap snapshot requests from a folder in its state root.
    #[cfg(feature = "heap-profile")]
    broca_heap_profile::start(state_root.join("heap-profile"));
    // broca's settings, read once: every component they shape reads its own section from here.
    // broca registers first, and takes the daemon's settings when the daemon manages them;
    // then it never reads the file. Under a daemon that doesn't, it reads broca.jsonc.
    let mock_provider_path =
        std::env::var("BROCA_PROVIDER_BASE_URL").is_ok_and(|value| !value.trim().is_empty());
    // The start writes into the state root (the old settings file's saved copy) before the
    // settings arrive, so it takes the lock first.
    lock_state_root(&state_root)?;
    let (settings, gated) = match start_gated(
        &conn_path,
        registration_channel,
        &module_id,
        mock_provider_path,
    )
    .await?
    {
        Some(started) => started,
        None => return Ok(()),
    };
    settings.log_conversion();
    let service_tier = broca_module_serve::config::ServiceTierConfig::from_settings(&settings)
        .map_err(ServeIoError)?;
    let classifier_routing =
        broca_module_serve::config::ClassifierRoutingConfig::from_settings(&settings)
            .map_err(ServeIoError)?;
    broca_protocol::configure_service_tier(service_tier.enabled)
        .map_err(|error| ServeIoError(error.into()))?;
    // Before any session opens: every sync the session store makes follows it.
    broca_session_store::durable::configure_full_flush(
        broca_module_serve::config::StorageSyncConfig::from_settings(&settings)
            .map_err(ServeIoError)?
            .full_flush,
    );
    let bus_event_config =
        broca_module_serve::bus_events::Config::from_settings(&settings).map_err(ServeIoError)?;
    let backup_classes = broca_module_serve::session_class::BackupClasses::from_settings(&settings)
        .map_err(ServeIoError)?;
    let provider_overrides =
        broca_module_serve::provider_overrides::ProviderOverrides::from_settings(&settings)
            .map_err(ServeIoError)?;
    // System One's provider endpoints honour the same boot override a chat
    // provider does.
    let typesafe_base_url = provider_overrides.base_url("typesafe").map(str::to_owned);
    let openai_decisions_base_url = provider_overrides
        .base_url(broca_module_serve::system_one::openai_decisions::PROVIDER_ID)
        .map(str::to_owned);
    let workers_ai_base_url = provider_overrides
        .base_url(broca_module_serve::system_one::workers_ai::PROVIDER_ID)
        .map(str::to_owned);
    let stop_signal = shutdown_signal()?;
    // Refuse unsupported markers before the stall log or any durable store is opened.
    state_format::prepare(&state_root).map_err(ServeIoError)?;
    let _stall_watchdog = broca_module_serve::stall::start(&state_root)?;
    // An optional provider base_url override (the mock/openai-compatible path; e.g. aimock).
    // When unset, the run's model id is resolved through the catalog.
    let provider_base_url = std::env::var("BROCA_PROVIDER_BASE_URL")
        .ok()
        .filter(|v| !v.trim().is_empty());
    // `mock_provider_path` (read at start): whether a mock/openai-compatible base_url override is
    // set. On that path every run authenticates with AuthMode::None, so the vault is never
    // consulted (and a mock rig runs no credentials module to open it against). Gates the vault
    // connection below.

    // Real provider calls use a second outbound consumer connection for handle-keyed vault
    // credentials. Mock providers need no credential route.

    // EVERY OUTBOUND MODULE ID COMES FROM HERE, and so does the manifest's `consumes`
    // declaration, which is handed the same struct below. Routes below read these fields
    // rather than re-reading the environment, so what broca declares and what it dials
    // cannot diverge.
    //
    // - tool_module: the default provider the SubcToolPlane dispatches tools to.
    // - vault_module: present only when a real provider call will resolve credentials from
    //   the vault.
    // Per-session context-management defaults. A malformed section refuses boot by name, like
    // the provider overrides: a default that silently fell back to "none" would let sessions
    // reach the window their operator believed was managed.
    let context_management =
        broca_module_serve::context_management::ContextManagementConfig::from_settings(&settings)
            .map_err(ServeIoError)?;
    // The `account_selection` section: which routing module new runs ask for their provider
    // account, and the switch that bypasses it. A malformed section refuses boot by name.
    let account_selection =
        broca_module_serve::account_routing::AccountSelectionConfig::from_settings(&settings)
            .map_err(ServeIoError)?;
    // The operator switches for content-filter fallback, both layers under one key. A malformed
    // block refuses boot by name, like the sections above; without one, both stay off.
    let anthropic_fallbacks =
        broca_module_serve::config::AnthropicFallbackConfig::from_settings(&settings)
            .map_err(ServeIoError)?;
    let tool_name_aliases =
        broca_module_serve::config::ToolNameAliasesConfig::from_settings(&settings)
            .map_err(ServeIoError)?;
    for (route, aliases) in &tool_name_aliases.0 {
        match aliases {
            broca_module_serve::config::ToolAliasRoute::Names { names } if !names.is_empty() => {
                tracing::info!(credential_route = %route, alias_count = names.len(), "tool name aliases configured");
            }
            broca_module_serve::config::ToolAliasRoute::PrefixAll { prefix } => {
                tracing::info!(credential_route = %route, alias_count = "all", prefix = %prefix, "tool name aliases configured; concrete count resolved at admission");
            }
            _ => {}
        }
    }
    let account_limit_pause =
        broca_module_serve::config::AccountLimitPauseConfig::from_settings(&settings)
            .map_err(ServeIoError)?;
    // HELLO already fixed what broca consumes, before the settings were known.
    let consumer_routes = gated.consumer_routes.clone();
    // How long an idle lineage holds its lease before the coordinator closes it. A real
    // operational parameter: a server hosting many sessions wants a short value to free leases
    // quickly; a desktop wants longer to avoid the cost of restarting a closed session.
    // Default 30s. Read from the environment for now (same pattern as BROCA_PROVIDER_BASE_URL);
    // the long-term home is the module's config file.
    // A NON-EMPTY but unparseable value is a misconfiguration — fail loud at startup rather
    // than silently falling back to the default (which would hide the operator's mistake).
    let idle_ttl = match std::env::var("BROCA_IDLE_TTL_MS") {
        Ok(v) if v.trim().is_empty() => None,
        Ok(v) => Some(Duration::from_millis(v.trim().parse::<u64>().map_err(
            |_| {
                ServeIoError(format!(
                    "BROCA_IDLE_TTL_MS must be a non-negative integer (ms), got {v:?}"
                ))
            },
        )?)),
        Err(_) => None,
    };

    // The module holds TWO subc roles in this one process: it serves session.* as a MODULE
    // (the inbound connection below), AND dispatches tools as a CONSUMER (this outbound
    // connection). The LLM provider is NOT subc — it's a direct HTTP/SSE call the executor
    // makes. One SHARED consumer connection carries per-session tool routes; the executor
    // frees a session's route on close (so the shared connection doesn't leak channels).
    // Every long-lived consumer connection follows the daemon's catalog generation, so the
    // catalog is read again only when it changed (`SubcConnection::follow_catalog`).
    let tool_conn = Arc::new(
        SubcConnection::connect(&conn_path)
            .await
            .map_err(|e| ServeIoError(format!("tool-plane consumer connect: {e}")))?,
    );
    tool_conn.follow_catalog();
    // Whether the daemon admits routes under scopes (`scopes/v1`) decides how an unstamped
    // send into a scoped session is refused. When the question cannot be asked, serve falls
    // back to the same list in its own connection's HELLO_ACK.
    let daemon_capabilities = match tool_conn.server_capabilities().await {
        Ok(capabilities) => Some(capabilities),
        Err(error) => {
            tracing::warn!(
                target: "scope",
                "server.describe failed, reading scopes/v1 from HELLO_ACK instead: {error}"
            );
            None
        }
    };
    let plane = SubcToolPlane::new(tool_conn, consumer_routes.tool_module.clone());

    // The coordinator: a registry over the session store, the file leases and the run index.
    // The module owns its state_root: the leases and the session store are its durability
    // domain, and the session store is the system of record. The run index holds projections
    // and export facts beside it.
    //
    // `state_root` was resolved at startup: BROCA_STATE_ROOT when set, otherwise the durable
    // data home, `<data home>/cortexkit/broca`, and never the daemon's runtime directory, whose
    // files cleanup removes. Astrocyte reads `run-index.db` at that data-home path, so the
    // default location is a contract with another module, not a local preference.
    // Create the state root up front so the SQLite run index can open its file (the file
    // stores create their own subdirs lazily, but SqliteStore::open needs the dir to exist).
    create_state_directories(&state_root)?;
    // The catalog only guides external backup capture; a filesystem refusal must not take
    // session service offline.
    if let Err(error) = write_engram_catalog(&state_root) {
        tracing::error!(
            target: "engram",
            path = %state_root.join("engram-catalog.json").display(),
            "could not publish the backup catalog; continuing to serve: {error}"
        );
    }
    let lease_store = Arc::new(FileLeaseStore::new(state_root.join("leases")));
    // The session classes whose new sessions start in the backed-up root.
    let backup_class_names = backup_classes.0.clone();

    // The provider-held runtime: the slots a held call waits in, and the intake that pulls each
    // provider's late results under broca's own principal and hands them to the run's batch.
    // Its feeds ride a consumer connection of their own, apart from the tool routes. Installing
    // this runtime declares held-reply support on dispatch routes in every build.
    let feed_conn = Arc::new(
        SubcConnection::connect(&conn_path)
            .await
            .map_err(|e| ServeIoError(format!("late-results consumer connect: {e}")))?,
    );
    feed_conn.follow_catalog();
    let held_resumer = Arc::new(broca_module_serve::held_runtime::LateBoundResumer::default());
    let held_routes = Arc::new(broca_module_serve::held_runtime::FeedRoutes::new(
        Arc::clone(&feed_conn),
        broca_module::late_results_puller::LateResultsIntake::feed_identity(&state_root),
    ));
    let held_runtime = broca_module_serve::held_runtime::HeldRuntime::build(
        &state_root,
        feed_conn
            .consumer_module_id()
            .map(str::to_owned)
            .unwrap_or_default(),
        held_routes.clone(),
        held_routes,
        held_resumer.clone(),
    )
    .map_err(|e| ServeIoError(format!("open late-results store: {e}")))?;
    held_runtime.publish_for_health();
    let plane =
        plane.with_provider_held_support(held_runtime.store.clone(), held_runtime.slots.clone());
    let plane = Arc::new(plane);
    // The plane asks the daemon about a scope (`scope.describe`) over broca's own MODULE
    // connection, because the daemon answers it only on a registered module's connection. The
    // serve loop below attaches the describer once that connection has registered.
    let scope_describer = Arc::new(broca_subc::ModuleScopeDescriber::new());
    plane.set_scope_describer(scope_describer.clone());
    let index = Arc::new(
        broca_store::SqliteStore::open(state_root.join("run-index.db"))
            .map_err(|error| ServeIoError(format!("open run index: {error}")))?,
    );
    let run_index = Some(index);
    // Load the last-known-good before any admission is possible. The fetch leg gets its own
    // consumer connection, but construction performs no fusiform route.open; only the detached
    // poll below can dial that sibling module.
    broca_module_serve::diagnostics::install(state_root.clone());
    let catalog_cache = Arc::new(broca_catalog::CatalogCache::open(
        state_root.join("catalog/fusiform-lkg.json"),
    ));
    if let Some(error) = catalog_cache.lkg_load_error() {
        tracing::warn!(
            target: "catalog",
            "catalog LKG could not be loaded; admission will refuse until a poll succeeds: {error}"
        );
    }
    broca_catalog::install_global_catalog_cache(Arc::clone(&catalog_cache));
    let catalog_conn = Arc::new(
        SubcConnection::connect(&conn_path)
            .await
            .map_err(|e| ServeIoError(format!("catalog consumer connect: {e}")))?,
    );
    catalog_conn.follow_catalog();
    let catalog_identity = broca_protocol::BindIdentity::new(
        state_root.to_string_lossy().to_string(),
        "broca",
        "catalog-refresh",
    );
    // Overrides the catalog does not serve are named once, against a loaded catalog: now when
    // a last-known-good copy is loaded, otherwise after the first successful refresh.
    let catalog_poller = provider_overrides.report_unserved(
        &catalog_cache,
        CatalogPoller::new(catalog_conn, catalog_identity, Arc::clone(&catalog_cache)),
        |notice| tracing::warn!(target: "catalog", "{notice}"),
    );
    let _catalog_poll = spawn_catalog_poll(Arc::new(catalog_poller));

    // The retired method file is only checked for presence, never opened.
    let _obsolete_methods = method_config_for_launch(mock_provider_path, || {
        broca_module_serve::MethodConfig::load_resolved(
            broca_module_serve::MethodConfig::default_path(),
        )
    })
    .map_err(ServeIoError)?;

    // Admission and execution use the same role adapter and consumer connection. Hook routes
    // are cached by caller session, independently of the tool plane's dispatch routes.
    // Compaction shares this connection but owns a separate role-route cache and channels.
    let plan_conn = Arc::new(
        SubcConnection::connect(&conn_path)
            .await
            .map_err(|e| ServeIoError(format!("fetch-plan consumer connect: {e}")))?,
    );
    plan_conn.follow_catalog();
    let tool_route_health_plane = Arc::clone(&plane);
    let executor_impl =
        executor::LiveLoopExecutor::new(state_root.clone(), plane, provider_base_url);
    let mut executor_impl = executor_impl
    .with_provider_held_lifecycle(held_runtime.slots.clone())
    .with_hook_plane(Arc::new(broca_subc::SubcHookPlane::new(plan_conn.clone())))
    .with_provider_overrides(provider_overrides)
    .with_claude_code_install_id(
        match broca_module_serve::claude_code_identity::load_or_create_install_id(&state_root) {
            Ok(install_id) => Some(install_id),
            Err(error) => {
                // Without an install id there is no stable device id, so OAuth episodes are
                // admitted without the Claude Code persona rather than with an unstable one.
                tracing::warn!(%error, "Claude Code install id unavailable; OAuth runs omit the persona");
                None
            }
        },
    );
    let cache_table =
        broca_module_serve::CacheTtlTable::from_settings(&settings).map_err(ServeIoError)?;
    executor_impl = executor_impl.with_cache_table(cache_table);
    // Both content-filter fallback switches, read above with the other early config so the
    // refusal mapping can take its own switch from the same block.
    executor_impl = executor_impl.with_anthropic_fallbacks(anthropic_fallbacks);
    executor_impl = executor_impl.with_tool_name_aliases(tool_name_aliases);
    executor_impl =
        executor_impl.with_max_same_account_wait(account_limit_pause.max_same_account_wait);
    let charge_basis_table =
        Arc::new(ChargeBasisTable::from_settings(&settings).map_err(ServeIoError)?);
    // Local override rows no longer stop startup; they are named here once and recorded on
    // every fact as charge_basis_overrides. Silent when there are none.
    if let Some(warning) = charge_basis_table.override_warning() {
        tracing::warn!(target: "billing", "{warning}");
    }
    charge_basis_table.publish_for_health();
    let mut vault_gauge = broca_module_serve::serve::VaultGauge::not_in_use();
    let mut classifier_router = None;
    // Where the session store's deployment key is read from: the vault, by grant. `None`
    // when the module reads no vault, so sessions kept in the session store stay refused.
    let mut store_key_source: Option<Arc<dyn broca_module_serve::store_key::DeploymentKeySource>> =
        None;
    // System One's provider credentials. Without a vault (the mock provider path) their
    // requests carry none, as every run's do there.
    let mut system_one_credential: Arc<
        dyn broca_module_serve::system_one::credential::CredentialSource,
    > = Arc::new(broca_module_serve::system_one::credential::NoCredential);
    let mut workers_ai_credential: Arc<
        dyn broca_module_serve::system_one::credential::CredentialSource,
    > = Arc::new(broca_module_serve::system_one::credential::NoCredential);
    let mut openai_decisions_credential: Arc<
        dyn broca_module_serve::system_one::credential::CredentialSource,
    > = Arc::new(broca_module_serve::system_one::credential::NoCredential);
    // Open the vault only when a real provider call will actually be made. A mock base_url
    // path authenticates with AuthMode::None for every run (see `decide_auth`), so the vault
    // is unused there — skip it. The default is now the vault, but the mock path never needs
    // it, which keeps the mock-based e2e rigs (no credentials module) working unchanged.
    if let Some(vault_module) = consumer_routes.vault_module.clone() {
        // Every credential is read by grant, by credential id. The daemon deliberately never
        // opens the capability-handle file: the vault revokes this module's handles once no
        // placed build reads them, so a handle read here would fail for every run.
        let listing_cache = Arc::new(broca_module_serve::account_routing::ListingCache::default());
        let addressing = broca_module_serve::VaultAddressing::new(Arc::clone(&listing_cache));
        // A process-global identity for broca's vault calls, not a conversation identity.
        // Only harness/session travel in the wire bind; the local storage-root field is not sent.
        let vault_bind = broca_protocol::BindIdentity::new(
            state_root.to_string_lossy().to_string(),
            "broca",
            "vault",
        );
        let vault_conn = Arc::new(
            SubcConnection::connect(&conn_path)
                .await
                .map_err(|e| ServeIoError(format!("vault consumer connect: {e}")))?,
        );
        vault_conn.follow_catalog();
        vault_gauge = broca_module_serve::serve::VaultGauge::disconnected();
        // A detached catalog watch keeps health truthful without delaying this module's own
        // HELLO: it polls until the credentials module is catalog-live, stamps the gauge, and
        // exits. Without the stamp an idle server would report a degraded vault forever, since
        // only real credential calls otherwise update the gauge. Actual vault calls overwrite
        // this reachability signal with call-outcome truth either way.
        let readiness_conn = Arc::clone(&vault_conn);
        let readiness_gauge = vault_gauge.clone();
        let readiness_module = vault_module.clone();
        tokio::spawn(async move {
            let mut warned = false;
            loop {
                let live = readiness_conn
                    .catalog_list()
                    .await
                    .map(|modules| {
                        modules
                            .iter()
                            .any(|module| module.module_id == readiness_module)
                    })
                    .unwrap_or(false);
                if live {
                    // Deliberately one-shot, so the retained CONNECTED state is STALE-ABLE: if
                    // claustrum later disappears nothing here notices, and health keeps saying
                    // connected until a real vault call stamps a failure. On a busy daemon that
                    // is the next admission needing credentials; on an idle one it is unbounded.
                    //
                    // Not fixed by polling forever: this gauge exists to keep health truthful
                    // WITHOUT idle IO, and a permanent catalog poll buys freshness for a window
                    // in which, by construction, no work is failing. What the reader gets
                    // instead is provenance. This stamp moves the state word only and never
                    // touches last_ok_ms, so health emits vault=connected with NO vault_ok_age_s.
                    // Connected WITHOUT an age means catalog-live was seen once and no call has
                    // ever succeeded; connected WITH a growing age means calls used to work and
                    // none has run since. Reading those two as the same green is the misreading.
                    readiness_gauge.stamp_ok_if_disconnected();
                    return;
                }
                if !warned {
                    tracing::warn!(
                        target: "credentials",
                        "credentials module '{readiness_module}' is not catalog-live; \
                         serving with degraded health until it registers"
                    );
                    warned = true;
                }
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });
        // Routing is reached over the vault's consumer connection rather than one of its own:
        // like the vault route it is one route for the whole process, and routing authorizes
        // a request by the module identity that connection presents, as the vault does.
        let routing = Arc::new(broca_module_serve::account_routing::RoutingClient::new(
            Arc::new(
                broca_module_serve::account_routing::SubcRoutingTransport::new(
                    Arc::clone(&vault_conn),
                    account_selection.routing_module.clone(),
                    broca_protocol::BindIdentity::new(
                        state_root.to_string_lossy().to_string(),
                        "broca",
                        "accounts",
                    ),
                ),
            ),
            account_selection,
        ));
        broca_module_serve::account_routing::publish_counters(routing.counters());
        broca_module_serve::usage_observe::start(
            Arc::clone(&vault_conn),
            broca_protocol::BindIdentity::new(
                state_root.to_string_lossy().to_string(),
                "broca",
                "quota",
            ),
        );
        // System One reads its provider keys by grant like a run does, on a vault route of its
        // own so its credential reads never queue behind a session's. The providers share it.
        let system_one_vault = Arc::new(broca_subc::VaultClient::new(
            Arc::clone(&vault_conn),
            &vault_module,
            &vault_bind,
            VAULT_READY_TIMEOUT,
        ));
        // Owed reports still describe real runs after routed admission is switched off.
        classifier_router = Some(broca_module_serve::system_one::Routing {
            client: routing.clone(),
            credentials: Arc::new(broca_module_serve::system_one::VaultRoutingCredentials(
                system_one_vault.clone(),
            )),
            module_id: "broca".into(),
        });
        let system_one_vault_credential = |credential_id: &str| {
            Arc::new(
                broca_module_serve::system_one::credential::VaultCredential {
                    client: Arc::clone(&system_one_vault),
                    credential_id: credential_id.to_owned(),
                    addressing: Some(addressing.clone()),
                },
            )
        };
        system_one_credential =
            system_one_vault_credential(broca_module_serve::system_one::typesafe::CREDENTIAL_ID);
        workers_ai_credential =
            system_one_vault_credential(broca_module_serve::system_one::workers_ai::CREDENTIAL_ID);
        openai_decisions_credential = system_one_vault_credential(
            broca_module_serve::system_one::openai_decisions::CREDENTIAL_ID,
        );
        // The deployment key is read on a route of its own, so its retries while the record is
        // missing never queue behind a run's credential read.
        store_key_source = Some(Arc::new(broca_subc::VaultClient::new(
            Arc::clone(&vault_conn),
            &vault_module,
            &vault_bind,
            VAULT_READY_TIMEOUT,
        )));
        let client = broca_subc::VaultClient::new(
            vault_conn,
            &vault_module,
            &vault_bind,
            VAULT_READY_TIMEOUT,
        );
        // INFO: the startup census. It answers "which credentials does this process hold",
        // which is a question asked when something is already wrong -- so it must be IN the log,
        // and it is not itself a warning. No per-call log line is needed for the same purpose:
        // this line plus the durable resolved_credential_id on every export fact answers it.
        tracing::info!(
            target: "credentials",
            "auth source = vault (every credential is read by grant as {}; credentials resolved from listed rows)",
            client
                .scoped_principal()
                .unwrap_or_else(|| "nobody (no supervised consumer identity)".to_string())
        );
        broca_module_serve::account_routing::publish_unmapped(listing_cache.unmapped());
        executor_impl = executor_impl.with_vault(executor::VaultAuth {
            client,
            addressing,
            methods: broca_module_serve::MethodConfig::from_listing(Arc::clone(&listing_cache)),
            gauge: vault_gauge.clone(),
            cache: executor::VaultLkgCache::default(),
            listing: listing_cache,
        });
        executor_impl = executor_impl.with_routing(routing);
        executor_impl.prime_account_listing();
    }

    executor_impl = executor_impl.with_context_management(context_management);
    let vault_auditor = executor_impl.vault_auditor();
    let store_reader = run_index
        .clone()
        .map(|store| store as Arc<dyn broca_core::RunEventBodies>);
    let retention = Arc::new(
        broca_module::retention::RetentionService::new(&state_root, lease_store.clone())
            .map_err(|error| ServeIoError(format!("retention clock: {error}")))?,
    );
    executor_impl = executor_impl.with_compaction_plane(Arc::new(
        broca_subc::SubcCompactionPlane::new(plan_conn.clone()),
    ));
    let executor = Arc::new(executor_impl.with_clock(retention.clock.clone()));
    // The session store. Its runtime opens here, without the deployment key, and serves plain sessions from the
    // start; encrypted sessions are served once the key has loaded from the vault (see
    // `store_key`). `session_storage.encryption` chooses the mode of the sessions it creates.
    let session_storage =
        broca_module_serve::config::SessionStorageConfig::from_settings(&settings)
            .map_err(ServeIoError)?;
    let store_slot = Arc::new(
        broca_module::store_open::StoreSlot::awaiting_key(
            &state_root,
            session_storage.deployment_key_id,
        )
        .creating(session_storage.new_sessions()),
    );
    broca_module_serve::store_key::publish(Arc::clone(&store_slot));
    // With run events on, a session in the session store hands the events of its durable records
    // to the outbox the publisher drains.
    let store_run_events = bus_event_config.enabled.then(|| {
        broca_module_serve::bus_events::StoreRunEvents::new(run_index.as_ref().unwrap().clone())
    });
    let store_setup = broca_module::store_open::StoreRuntimeSetup {
        state_root: state_root.clone(),
        executor: executor.clone(),
        idle_ttl: idle_ttl.unwrap_or(broca_module::actor::IDLE_TTL),
        import_roots: broca_module_serve::config::ImportConfig::from_settings(&settings)
            .map_err(ServeIoError)?
            .roots,
        // A run that ends on a session in the session store is billed into the usage export
        // under the charge-basis table.
        billing: run_index
            .as_ref()
            .map(|legacy| broca_module::store_billing::StoreBilling {
                index: legacy.clone(),
                resolver: charge_basis_table.clone(),
            }),
        run_events: store_run_events
            .clone()
            .map(|events| events as Arc<dyn broca_module::store_run_events::RunEventSink>),
        send_states: store_run_events
            .clone()
            .map(|events| events as Arc<dyn broca_module::store_send_states::SendStateSink>),
    };
    if let Err(error) = store_slot.open_without_key(&store_setup).await {
        // Plain sessions stay unserved until the key loads, which opens the runtime again.
        tracing::warn!(
            target: "session_storage",
            "the new storage could not be opened without its deployment key: {error}"
        );
    }
    let store_move_leases: Arc<dyn broca_core::LeaseStore> = lease_store.clone();
    // Both live hints and durable run events share this process's credential and broker link.
    let bus_connector = Arc::new(broca_module_serve::bus_events_nats::NatsConnector::new(
        broca_module_serve::bus_events_nats::CredentialHolder::new(plan_conn.clone()),
    ));
    let step_events = Arc::new(broca_module_serve::step_events::StepEvents::start_on_bus(
        bus_connector.clone(),
    ));
    // Account observations also go on the bus as route facts (routing 0.7.36 §8.1). A run's end
    // also stores its `run_finished` fact in the outbox, and routing's answer settles it there.
    if let Some(routing) = &classifier_router {
        routing.client.set_route_facts(bus_connector.clone());
        if let Some(events) = &store_run_events {
            routing.client.set_route_fact_outbox(events.clone());
        }
    }
    let mut opener_builder =
        broca_module::DefaultOpener::new(lease_store, executor, Arc::clone(&store_slot))
            .with_backup_classes(backup_class_names)
            .with_charge_basis_resolver(charge_basis_table.clone())
            .with_step_events(step_events.clone());
    if let Some(ttl) = idle_ttl {
        opener_builder = opener_builder.with_idle_ttl(ttl);
    }
    let registry = LineageRegistry::new_with_run_index(Arc::new(opener_builder), store_reader);
    registry.set_session_address_inventory(run_index.as_ref().unwrap().clone());
    let registry = Arc::new(registry);
    // `session.restore_from_backup` repairs sessions kept in the session store after their files
    // came back from a backup, holding each one it writes as a move does.
    registry.set_store_restorer(Arc::new(
        broca_module_serve::store_restore::StoreRestore::new(
            registry.as_ref().clone(),
            &state_root,
            Arc::clone(&store_slot),
            Arc::clone(&store_move_leases),
        ),
    ));
    // Deletes what sealed changes left behind in sessions kept in the session store that no actor
    // holds, such as finished workers' sessions (see `store_collect`). An actor collects the
    // session it serves itself.
    let _store_collect = tokio::spawn(
        Arc::new(
            broca_module::store_collect::StoreCollectSweep::for_registry(
                state_root.clone(),
                Arc::clone(&store_slot),
                Arc::clone(&store_move_leases),
                registry.as_ref().clone(),
            ),
        )
        .run(registry.stopping_signal()),
    );
    held_runtime.bind_stopping_signal(registry.stopping_signal());
    registry.set_tool_route_health(Arc::new(move |open| {
        tool_route_health_plane.tool_routes_health(open)
    }));
    registry.set_retention_service(retention);
    // System One: decision models served in this process with no session. Its batch record
    // is its own file; `run-index.db` is a rebuildable projection and cannot hold an authority.
    let llm_tools_config = broca_module_serve::config::LlmToolsConfig::from_settings(&settings)
        .map_err(ServeIoError)?;
    let llm_tools_identity = broca_protocol::BindIdentity::new(
        state_root.to_string_lossy().to_string(),
        "broca",
        "llm-tools",
    );
    let cerebellum = Arc::new(broca_subc::PrivateContentClient::new(
        plan_conn.clone(),
        llm_tools_identity.clone(),
    ));
    let system_one = {
        let store = Arc::new(
            broca_store::SystemOneStore::open(state_root.join("system-one.db"))
                .map_err(|e| ServeIoError(format!("open System One batch store: {e}")))?,
        );
        let config = broca_module_serve::system_one::Config {
            routed_models: classifier_routing.enabled,
            ..Default::default()
        };
        let openai_decisions =
            broca_module_serve::system_one::openai_decisions::OpenAiDecisions::new(
                openai_decisions_base_url.as_deref(),
                openai_decisions_credential,
                config.attempt_timeout,
            )
            .map_err(ServeIoError)?;
        let typesafe = broca_module_serve::system_one::typesafe::TypeSafe::new(
            typesafe_base_url.as_deref(),
            Arc::clone(&system_one_credential),
            config.attempt_timeout,
        )
        .map_err(ServeIoError)?;
        let workers_ai = |model_path: &'static str| {
            broca_module_serve::system_one::workers_ai::WorkersAi::new(
                workers_ai_base_url.as_deref(),
                model_path,
                Arc::clone(&workers_ai_credential),
                config.attempt_timeout,
            )
            .map(Arc::new)
            .map_err(ServeIoError)
        };
        let clef = workers_ai(broca_module_serve::system_one::workers_ai::CLEF_MODEL_PATH)?;
        let clef_flash =
            workers_ai(broca_module_serve::system_one::workers_ai::CLEF_FLASH_MODEL_PATH)?;
        let service = broca_module_serve::system_one::SystemOne::new(
            store,
            broca_module_serve::serve::process_launch_id(),
            Arc::new(broca_module_serve::system_one::LiveCatalog),
            {
                let mut pins = vec![
                    broca_module_serve::system_one::ModelPin::jev(Arc::new(typesafe)),
                    broca_module_serve::system_one::ModelPin::clef(clef),
                    broca_module_serve::system_one::ModelPin::clef_flash(clef_flash),
                ];
                if OPENAI_DECISIONS_SERVED {
                    pins.push(broca_module_serve::system_one::ModelPin::openai_decisions(
                        Arc::new(openai_decisions),
                    ));
                }
                pins
            },
            config,
            registry.batches_in_flight_gauge(),
        );
        let service = if let Some(routing) = classifier_router {
            service.with_routing(routing)
        } else {
            service
        };
        let service = if llm_tools_config.image_references {
            service.with_image_resolver(cerebellum.clone())
        } else {
            service
        };
        let service =
            Arc::new(service.with_export(run_index.as_ref().unwrap().clone(), charge_basis_table));
        let released = service
            .release_abandoned()
            .map_err(|e| ServeIoError(format!("release System One batches left running: {e}")))?;
        if released > 0 {
            tracing::info!(target: "system_one", released,
                "released batches a previous process left running; their unanswered items are asked again on re-send");
        }
        service
    };
    let _routing_recovery = system_one.spawn_routing_recovery(registry.stopping_signal());
    let _system_one_sweep = system_one.spawn_sweeper(registry.stopping_signal());
    // Private-content admission asks cerebellum whether a scope read the person's private
    // content and Basal which providers a flow's approved version allows. Both questions ride
    // the module connection `plan_conn` presents under broca's own principal; cerebellum and
    // Basal admit only `reserved:broca`.
    let llm_tools_asks = Arc::new(broca_module_serve::llm_tools::SubcAsks::new(
        cerebellum,
        Arc::new(broca_subc::FlowProvidersClient::new(
            plan_conn.clone(),
            llm_tools_identity.clone(),
        )),
    ));
    let held_calls = Arc::new(broca_module_serve::llm_tools::held::HeldCalls::new(
        Arc::new(
            broca_store::grants::GrantsStore::open(state_root.join("grants.db"))
                .map_err(|error| ServeIoError(format!("open grants store: {error}")))?,
        ),
        broca_module_serve::serve::process_launch_id().into(),
    ));
    held_calls
        .expire_due(broca_module_serve::llm_tools::held::epoch_ms())
        .map_err(|error| ServeIoError(format!("recover held deadlines: {error}")))?;
    let _held_timers = held_calls.spawn_timers(registry.stopping_signal());
    let llm_tools = Arc::new(
        broca_module_serve::llm_tools::LlmTools::new(
            Arc::new(system_one.fork_for_tools()),
            llm_tools_config,
            llm_tools_asks,
        )
        .with_held(held_calls.clone())
        .with_consent(
            Arc::new(broca_module_serve::llm_tools::consent::Consent::new(
                held_calls.store.clone(),
                Arc::new(broca_module_serve::llm_tools::consent::SubcConsentIo::new(
                    plan_conn.clone(),
                    llm_tools_identity,
                    scope_describer.clone(),
                )),
            )),
            registry.stopping_signal(),
        ),
    );
    llm_tools
        .recover_authorized()
        .map_err(|error| ServeIoError(format!("recover authorized held calls: {error}")))?;
    llm_tools
        .recover_consent()
        .map_err(|error| ServeIoError(format!("recover consent cards: {error}")))?;
    let door = Arc::new(broca_subc::session_door::SessionDoorClient::new(
        plan_conn.clone(),
        broca_protocol::BindIdentity::new(state_root.clone(), "broca", "agent-data"),
    ));
    let agent_data = Arc::new({
        let legacy = run_index.as_ref().unwrap();
        broca_module_serve::agent_data::AgentDataService::new(
            state_root.clone(),
            registry.as_ref().clone(),
            legacy.clone(),
            door,
        )
    });
    let phase_started = std::time::Instant::now();
    agent_data
        .reconcile_boot()
        .await
        .map_err(|error| ServeIoError(format!("agent-data boot recovery: {}", error.message)))?;
    tracing::info!(target: "lifecycle", elapsed_ms = phase_started.elapsed().as_millis() as u64,
        "agent-data boot recovery complete");
    let retention_sweep = Some({
        let registry = registry.clone();
        let ready = scope_describer.clone();
        tokio::spawn(async move {
            ready.wait_registered().await;
            if registry.run_retention_sweeper().await.is_err() {
                tracing::warn!(target: "retention", "retention sweep stopped; durable recovery required");
            }
        })
    });
    let bus_stop = tokio_util::sync::CancellationToken::new();
    let _bus_stop_guard = bus_stop.clone().drop_guard();
    // Compaction v2 uses session-bound calls. Legacy sessions no longer consume
    // provider announcements or replay the retired compaction bus outbox.
    let event_store: Arc<dyn broca_store::bus_events::RunEventStore> =
        run_index.as_ref().unwrap().clone();
    let _bus_events = broca_module_serve::bus_events::Service {
        config: bus_event_config,
        store: event_store,
        registry: registry.as_ref().clone(),
        connector: Some(bus_connector),
        store_events: store_run_events,
        stop: bus_stop,
    }
    .spawn();
    // Fetch-plan admission uses a consumer connection of its own, keeping its short-lived fetch
    // routes apart from the long-lived routes the model's tool calls are dispatched on.
    registry.set_plan_admitter(Arc::new(broca_subc::SubcPlanAdmitter::new(plan_conn)));
    // What each session's frozen config declares about its prompt cache, for `session.warm`,
    // the admission reply and `session.baseline`.
    registry.set_cache_declarer(Arc::new(
        broca_module_serve::cache_warm::ProviderCacheDeclarer,
    ));
    // Store recovery has one owner: plain sessions open now, encrypted candidates wait for
    // the deployment key. Neither pass waits for discovery of the session's log.
    let _store_key = {
        let registry = registry.as_ref().clone();
        let store_slot = Arc::clone(&store_slot);
        let ready = scope_describer.clone();
        let agent_data = agent_data.clone();
        tokio::spawn(async move {
            ready.wait_registered().await;
            registry.enable_restart_resumes();
            broca_module_serve::store_key::KeyLoader {
                source: store_key_source,
                slot: Arc::clone(&store_slot),
                setup: store_setup,
                backoff: Default::default(),
            }
            .run(registry)
            .await;
            // A move into this computer that stopped mid-way recovers at boot, except for an
            // encrypted session, which can't be read until the key has loaded.
            if store_slot.key_loaded() {
                if let Err(error) = agent_data.reconcile_pending_cuts().await {
                    tracing::warn!(target: "lifecycle", error = %error.message,
                        "agent-data move recovery after the storage key loaded failed; a retry of the move finishes it");
                }
            }
        })
    };
    // Late results reach a run through its session's actor, so delivery waits for the registry.
    // The intake reads an open session through its actor too, from memory.
    held_resumer.bind(registry.clone());
    held_runtime.intake.set_registry(registry.clone());
    // Boot recovery for provider-held calls runs off the startup path, once the dependency
    // routes are registered: a lost intake store is rebuilt from the live WALs first, every
    // registered session is opened so its open-time recovery runs, and then each module that
    // still needs its feed gets its puller.
    let _held_recovery = {
        let registry = registry.clone();
        let describer = scope_describer.clone();
        let runtime = held_runtime.clone();
        tokio::spawn(async move {
            describer.wait_registered().await;
            runtime.recover(registry).await;
        })
    };
    // Return freed heap pages to the system after a peak. The hook wakes once per
    // `MIN_INTERVAL` and does nothing unless a run finished or a session closed since its
    // last release, so an idle daemon pays one timer wake a minute.
    let _memory_relief = {
        let stopping = registry.stopping_signal();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(broca_memory_relief::MIN_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    biased;
                    _ = stopping.cancelled() => return,
                    _ = tick.tick() => {}
                }
                let released =
                    tokio::task::spawn_blocking(broca_memory_relief::release_if_due).await;
                if let Ok(Some(released)) = released {
                    tracing::info!(target: "lifecycle", "returned freed heap pages: {released:?}");
                }
            }
        })
    };
    let (read_half, write_half) = gated.connection.hand_over().await;
    let registered = Some(gated.registration);
    let serving = broca_module_serve::serve::serve_connection_with_llm_tools(
        read_half,
        write_half,
        &module_id,
        registry.clone(),
        registry.clone(),
        vault_gauge,
        Some(catalog_cache),
        consumer_routes,
        vault_auditor,
        daemon_capabilities,
        Some(scope_describer),
        Some(agent_data),
        Some(system_one),
        Some(llm_tools),
        registered,
    );
    tokio::pin!(serving);
    tokio::select! {
        biased;
        _ = stop_signal => {
            // Read BEFORE stopping: stop_and_seal cancels admission first, so a count taken after
            // it would report the drain's own effect rather than what the signal interrupted.
            let in_flight_at_signal = registry.slot_counts().runs_in_flight;
            let stopping = registry.stop_and_seal();
            tokio::pin!(stopping);
            // Keep the wire loop live for health probes and typed admission refusals while
            // actors drain. Losing the connection must not abandon the durable session jobs.
            // Actor sealing owns final log syncs and lease release; transport loss cannot
            // abandon those durability jobs before the bounded seal attempt completes.
            let sealed = tokio::select! {
                biased;
                sealed = &mut stopping => sealed,
                _ = &mut serving => stopping.await,
            };
            let report = registry.shutdown_report().expect("stop records its outcome");
            // LOG BOTH ARMS. Logging only the failure made SUCCESS AND NEVER-RAN IDENTICAL: a
            // quiet-window stop seals nothing, returns true, and said nothing -- exactly what a
            // handler that never fired would produce. Since every placement happens on a quiet
            // window by design, that silence was the permanent reading, and the FIRST live
            // measurement of this handler was spent arguing about which of the two it meant.
            //
            // The ambiguity ran in the reassuring direction, which is why it had to go: a
            // decorative handler and a working one were indistinguishable from outside forever.
            if sealed {
                tracing::warn!(
                    target: "lifecycle",
                    "shutdown seal complete; {} session(s) were in flight; {}",
                    in_flight_at_signal,
                    report
                );
            } else {
                tracing::warn!(
                    target: "lifecycle",
                    "shutdown incomplete; unsealed WALs require replay; {}",
                    report
                );
            }
            // Stop the sweep supervisor so it cannot start another deletion. Interrupted
            // blocking deletions are recoverable from their durable tombstones on next boot;
            // joining this supervisor is not a blocking-pool drain.
            if let Some(sweep) = retention_sweep {
                let _ = sweep.await;
            }
            // Tokio runtime destruction waits for blocking filesystem tasks. A stuck fsync must
            // not turn the bounded seal attempt into an unbounded shutdown -- so the process ends
            // here rather than by returning. HOW it ends is the part that matters:
            die_as_signalled();
        }
        // A `ck module restart` drains routes, sends `module.draining` (starting the step-boundary
        // stop), then GOODBYE on channel 0, then waits for child exit before SIGKILL. Serve returning
        // starts the final grace and seal. A daemon cut that SIGTERMs the module reaches the signal
        // arm above instead. Both call the same stop_and_seal, so which arm fires changes only the
        // log line. Errors seal too: transport death leaves runs executing just as GOODBYE does.
        result = &mut serving => {
            // WHY serve returned goes on both lines, appended after the fixed prefix that
            // placement greps match: `goodbye` is a planned stop the daemon announced on channel
            // 0, `eof` and `reset` are the connection ending without one, `error` is a fault.
            let cause = ServeEnd::cause_of(&result);
            let in_flight_at_goodbye = registry.slot_counts().runs_in_flight;
            // Wait for final actor log syncs and lease release before dropping the runtime.
            let sealed = registry.stop_and_seal().await;
            let report = registry.shutdown_report().expect("stop records its outcome");
            // Stop the sweep supervisor; incomplete deletions recover from durable tombstones.
            if let Some(sweep) = retention_sweep {
                let _ = sweep.await;
            }
            if sealed {
                // STAYS AT WARN, and it looks exactly like the boot statements demoted to info
                // in this same commit: true on every clean shutdown, naming no fault. The
                // difference is that ITS ABSENCE IS A FINDING. subc greps this line at every
                // placement, and a missing line means the trigger never fired -- a real outcome,
                // measured once already when the seal hung off a SIGTERM the daemon never sends.
                // At info, an operator filtering to `warn` sees silence, and silence would then
                // mean either "no seal" or "you filtered it" -- exactly the ambiguity this line
                // exists to remove. Do not demote it while that grep is part of the placement.
                tracing::warn!(
                    target: "lifecycle",
                    "shutdown seal complete on serve return (cause: {}); {} session(s) were in flight; {}",
                    cause,
                    in_flight_at_goodbye,
                    report
                );
            } else {
                tracing::warn!(
                    target: "lifecycle",
                    "shutdown incomplete on serve return (cause: {}); unsealed WALs require replay; {}",
                    cause,
                    report
                );
            }
            let end = result?;
            // A registration channel can't be dialled again, so a broca that loses it must be
            // started anew, and the daemon's supervisor treats a clean exit it didn't ask for as a
            // stop. Only the daemon's own GOODBYE ends such a broca cleanly.
            if broca_subc::registration::adopted() && !matches!(end, ServeEnd::Goodbye) {
                return Err(ServeIoError(format!(
                    "the daemon ended broca's registration channel without GOODBYE (cause: {cause})"
                ))
                .into());
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod runtime_shutdown_tests {
    use super::run_runtime;

    #[test]
    fn runtime_exit_joins_durability_but_not_unrelated_blocking_jobs() {
        let (durability_started, durability_entered) = std::sync::mpsc::channel();
        let (release_durability, durability_gate) = std::sync::mpsc::channel();
        let (unrelated_started, unrelated_entered) = std::sync::mpsc::channel();
        let (release_unrelated, unrelated_gate) = std::sync::mpsc::channel();
        let (finished, exit) = std::sync::mpsc::channel();
        let process = std::thread::spawn(move || {
            run_runtime(async move {
                tokio::task::spawn_blocking(move || {
                    unrelated_started.send(()).unwrap();
                    unrelated_gate.recv().unwrap();
                });
                let durability = tokio::task::spawn_blocking(move || {
                    durability_started.send(()).unwrap();
                    durability_gate.recv().unwrap();
                });
                durability.await.unwrap();
            })
            .unwrap();
            finished.send(()).unwrap();
        });
        durability_entered.recv().unwrap();
        unrelated_entered.recv().unwrap();
        assert!(
            matches!(exit.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)),
            "exit must wait for the durability join"
        );
        release_durability.send(()).unwrap();
        // The timeout is a deadlock watchdog, not the ordering assertion: the unrelated
        // job remains behind its gate until after exit has been observed.
        let result = exit.recv_timeout(std::time::Duration::from_secs(5));
        release_unrelated.send(()).unwrap();
        process.join().unwrap();
        result.expect("an unrelated blocking job delayed runtime exit");
    }
}

/// The SIGTERM trigger for the seal. Not a backstop: until subc gives modules their own process
/// groups, it is how every DAEMON CUT reaches broca, because launchd SIGTERMs the daemon's whole
/// group (measured: 3 of 3 cuts on 2026-09-23 logged this arm). A module restart reaches the
/// other arm, via `serve` returning.
///
/// The non-unix arm below stays pending because SIGTERM has no equivalent there; a module stop
/// on Windows is the channel-0 GOODBYE, which the serve-return arm handles.
/// End the process AS IF THE SIGTERM HAD NOT BEEN HANDLED, after the seal has run.
///
/// The supervisor's contract (subc, stated 2026-09-23): an unrequested EXIT 0 means "I stopped on
/// purpose" and is left down, while a death BY SIGNAL is a crash and is respawned. This arm used
/// to end in `exit(0)`, which turned an operator's `kill -TERM` from "respawned" into "left
/// down" -- the defect callosum and astrocyte both shipped and found the same afternoon. The
/// seal is the handler's whole job; how the process then ends has to be what the parent would
/// have seen with no handler at all.
///
/// So: restore the default disposition and re-raise. The log record is already in the kernel --
/// cortexkit-log writes each line with an unbuffered `write_all` -- so dying by signal loses
/// nothing the seal logged.
///
/// Only this arm re-raises. The serve-return arm ends with the ordinary return from `main`: the
/// daemon went away or asked us to stop, and exit 0 is the true answer there.
/// ONE BROCA PER STATE ROOT, decided before anything touches the root: the settings gate's copy
/// of the old settings file, the state-format decision, the stall log, store opens and every
/// boot recovery step (fence-index rebuild, cut reconcile, crash-orphan seal, System One release)
/// assume no other process is doing the same work on the same files. A held lock exits at once,
/// non-zero, so the daemon counts a failed start and backs off; see `state_root_lock`.
fn lock_state_root(state_root: &std::path::Path) -> Result<(), ServeIoError> {
    match broca_module_serve::state_root_lock::acquire(state_root) {
        Ok(lock) => {
            lock.hold_until_exit();
            Ok(())
        }
        Err(error) => {
            tracing::error!(target: "lifecycle", root = %state_root.display(), "{error}");
            Err(ServeIoError(error.to_string()))
        }
    }
}

/// The module connection registered unready, waiting to serve once broca is built.
struct Gated {
    connection: Arc<broca_module_serve::module_connection::ModuleConnection>,
    registration: broca_module_serve::serve::GatedRegistration,
    /// What HELLO declared broca consumes: the settings can't change it any more.
    consumer_routes: ConsumerRoutes,
}

/// Puts broca's settings file where the settings gate's daemon reads it,
/// `<config home>/cortexkit/broca.jsonc`, rewritten into the schema's shapes (see
/// `settings::rewrite`), and returns that path. `old` is the file broca read before the gate; it
/// is left as it was. `Err` stops the start: with the file not moved, the daemon would serve broca
/// the defaults in place of the settings the old file holds.
fn move_settings_file(old: Option<&std::path::Path>) -> Result<std::path::PathBuf, ServeIoError> {
    use broca_module_serve::settings::rewrite;
    let new = rewrite::daemon_settings_file().map_err(ServeIoError)?;
    let Some(old) = old else {
        return Ok(new);
    };
    let data_dir = broca_module_serve::state_root::resolve_state_root().map_err(|error| {
        ServeIoError(format!(
            "broca.jsonc not moved: no state root for its old copy: {error}"
        ))
    })?;
    let moved = rewrite::move_old_file(old, &new, &data_dir).map_err(|error| {
        ServeIoError(format!(
            "broca.jsonc at {} could not be moved to {}: {error}",
            old.display(),
            new.display()
        ))
    })?;
    let (notes, saved, stale_old) = match moved {
        rewrite::Moved::Nothing => (Vec::new(), None, None),
        rewrite::Moved::Moved { notes, saved } => {
            tracing::warn!(target: "config", from = %old.display(), to = %new.display(), "broca.jsonc copied to the path the settings gate reads; the old file is kept for older builds");
            (notes, Some(saved), None)
        }
        rewrite::Moved::InPlace {
            rewritten,
            stale_old,
        } => match rewritten {
            Some(done) => (done.notes, Some(done.saved), stale_old),
            None => (Vec::new(), None, stale_old),
        },
    };
    for note in &notes {
        tracing::warn!(target: "config", "broca.jsonc rewritten: {note}");
    }
    if let Some(saved) = saved {
        tracing::warn!(target: "config", saved = %saved.display(), "broca.jsonc's previous bytes are saved");
    }
    if let Some(stale) = stale_old {
        tracing::warn!(target: "config", stale = %stale.display(), file = %new.display(), "an older broca.jsonc is still in broca's own folder; broca no longer reads it, so edit the other file");
    }
    Ok(new)
}

/// Registers broca unready and gets its settings: the daemon's view when it has a settings gate,
/// else `broca.jsonc`. `None` when the daemon stopped broca before any view arrived.
async fn start_gated(
    conn_path: &PathBuf,
    channel: Option<Box<dyn broca_subc::registration::DaemonStream>>,
    module_id: &str,
    mock_provider_path: bool,
) -> Result<Option<(broca_module_serve::settings::Settings, Gated)>, ServeIoError> {
    use broca_module_serve::settings::{gate, Settings};
    // The routes broca consumes are named before its settings are known; no setting changes them.
    let consumer_routes = ConsumerRoutes::resolve(
        std::env::var(broca_module_serve::consumer_routes::TOOL_MODULE_ENV)
            .ok()
            .as_deref(),
        !mock_provider_path,
    )
    .with_bus_events(true);
    const CONFIG_PATH_ENV: &str = broca_module_serve::cache_routing::CONFIG_PATH_ENV;
    let (old_file, explicit) =
        match broca_module_serve::config_home::config_file(CONFIG_PATH_ENV, "broca.jsonc") {
            broca_module_serve::config_home::ConfigHome::Resolved { path, explicit } => {
                (Some(path), explicit)
            }
            broca_module_serve::config_home::ConfigHome::Unresolvable => (None, false),
        };
    // A file named by BROCA_CONFIG_PATH is read where it is and never copied into the config
    // home, so a test or a one-off start pointed at its own file can't replace the user's
    // settings. The daemon's gate reads only its own path, so such a start refuses under it.
    let user_file = match (&old_file, explicit) {
        (Some(path), true) => path.clone(),
        _ => move_settings_file(old_file.as_deref())?,
    };
    let (mut read_half, mut write_half) =
        tokio::io::split(registered_stream(conn_path, channel).await?);
    let ack = broca_module_serve::serve::register_unready(
        &mut read_half,
        &mut write_half,
        module_id,
        &consumer_routes,
    )
    .await?;
    let gated = ack
        .subc_capabilities
        .iter()
        .any(|capability| capability == subc_protocol::config::CONFIG_GATE_CAPABILITY);
    let (settings, gate) = if gated {
        if explicit {
            return Err(ServeIoError(format!(
                "{CONFIG_PATH_ENV} names {}, but the daemon manages broca's settings and reads \
                 them only from {}; unset {CONFIG_PATH_ENV}",
                user_file.display(),
                broca_module_serve::settings::rewrite::daemon_settings_file()
                    .map(|path| path.display().to_string())
                    .unwrap_or_else(|error| error),
            )));
        }
        let user_file = user_file.display().to_string();
        match gate::wait_for_settings(
            &mut read_half,
            &mut write_half,
            ack.negotiated_ver,
            &user_file,
        )
        .await?
        {
            gate::GateStart::Settings(settings, gate) => (settings, gate),
            gate::GateStart::Stopped(end) => {
                tracing::info!(target: "lifecycle", ?end, "stopped before broca's settings arrived");
                return Ok(None);
            }
        }
    } else {
        // A daemon without the gate: the moved file is read as broca read the old one.
        broca_module_serve::transport_config::validate_default().map_err(ServeIoError)?;
        let settings = Settings::read(&user_file).map_err(ServeIoError)?;
        (settings, gate::Gate::ungated(ack.negotiated_ver))
    };
    let connection = Arc::new(
        broca_module_serve::module_connection::ModuleConnection::new(
            Box::new(read_half.unsplit(write_half)),
            gate.clone(),
        ),
    );
    if broca_subc::registration::adopted() {
        broca_subc::registration::install_minter(connection.clone());
        tracing::info!(target: "lifecycle", "registered over the daemon's registration channel");
    }
    let registration = broca_module_serve::serve::GatedRegistration { ack, gate };
    Ok(Some((
        settings,
        Gated {
            connection,
            registration,
            consumer_routes,
        },
    )))
}

fn die_as_signalled() -> ! {
    #[cfg(unix)]
    {
        let _ = signal_hook::low_level::emulate_default_handler(signal_hook::consts::SIGTERM);
    }
    // Unreachable on unix unless re-raising itself failed. 128 + SIGTERM is the conventional
    // "terminated by signal" status, so even that fallback does not read as a deliberate stop.
    std::process::exit(143)
}

fn shutdown_signal() -> std::io::Result<impl std::future::Future<Output = ()>> {
    #[cfg(unix)]
    {
        let mut signal = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        Ok(async move {
            signal.recv().await;
        })
    }
    #[cfg(not(unix))]
    {
        tracing::warn!(
            target: "lifecycle",
            "graceful-stop trigger unavailable: awaiting channel-0 route.closing protocol"
        );
        Ok(std::future::pending())
    }
}

/// Launches only check whether the obsolete method file exists so the operator can delete it.
/// Mock launches may warn too, but neither path reads method defaults.
fn method_config_for_launch<F>(
    _mock_provider_path: bool,
    load: F,
) -> Result<broca_module_serve::MethodConfig, String>
where
    F: FnOnce() -> Result<broca_module_serve::MethodConfig, String>,
{
    // Checking for an obsolete file is safe even for mocks: no credential values are read.
    load()
}

/// The stream broca registers on, authenticated: the registration channel when the daemon gave
/// one, else a socket dialled from the connection file. The handshake is the same on both.
async fn registered_stream(
    conn_path: &PathBuf,
    channel: Option<Box<dyn broca_subc::registration::DaemonStream>>,
) -> Result<Box<dyn broca_subc::registration::DaemonStream>, ServeIoError> {
    let Some(mut channel) = channel else {
        return Ok(Box::new(connect(conn_path).await?));
    };
    let conn = connection_file::read(conn_path).map_err(|e| ServeIoError(e.to_string()))?;
    authenticate_client(&mut channel, &conn, AUTH_DEADLINE)
        .await
        .map_err(|e| ServeIoError(format!("authenticate on the registration channel: {e}")))?;
    Ok(channel)
}

async fn connect(conn_path: &PathBuf) -> Result<TcpStream, ServeIoError> {
    let conn = connection_file::read(conn_path).map_err(|e| ServeIoError(e.to_string()))?;
    let mut stream = broca_subc::tcp_connect::connect_endpoints(
        conn.endpoints
            .iter()
            .map(|endpoint| (endpoint.host.as_str(), endpoint.port)),
    )
    .await
    .map_err(|e| ServeIoError(format!("connect: {e}")))?;
    authenticate_client(&mut stream, &conn, AUTH_DEADLINE)
        .await
        .map_err(|e| ServeIoError(format!("authenticate: {e}")))?;
    Ok(stream)
}

#[cfg(test)]
mod endpoint_connect_tests {
    use super::*;
    use subc_transport::connection_file::{ConnectionInfo, Endpoint, SCHEMA_VERSION};

    #[tokio::test]
    async fn ipv6_endpoint_connects_after_failed_first() {
        // The first endpoint must refuse every connect for the whole test. A socket that is
        // bound but never listening does that, and keeps the port reserved. Closing a listener
        // instead frees its port, and a parallel test in this process can be handed that
        // port: the client then connects to the other test's listener and fails the handshake
        // with "Connection reset by peer" rather than falling back to IPv6.
        let first = tokio::net::TcpSocket::new_v4().unwrap();
        first
            .bind(std::net::SocketAddr::from(([127, 0, 0, 1], 0)))
            .unwrap();
        let listener = tokio::net::TcpListener::bind(("::1", 0)).await.unwrap();
        let key = vec![0x5c; 32];
        let path = broca_test_support::scratch_root()
            .join(format!("module-connect-{}.json", std::process::id()));
        connection_file::write_atomic(
            &path,
            &ConnectionInfo {
                schema: SCHEMA_VERSION,
                wire_version: None,
                endpoints: vec![
                    Endpoint {
                        host: "127.0.0.1".into(),
                        port: first.local_addr().unwrap().port(),
                    },
                    Endpoint {
                        host: "::1".into(),
                        port: listener.local_addr().unwrap().port(),
                    },
                ],
                key: key.clone(),
                daemon_id: [0xc5; 16],
                pid: std::process::id(),
                daemon_ver: "test".into(),
            },
        )
        .unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            subc_transport::authenticate_server(
                &mut stream,
                &key,
                &[0xc5; 16],
                "test",
                broca_test_support::STAND_IN_AUTH_DEADLINE,
            )
            .await
            .unwrap();
            stream
        });
        let stream = connect(&path)
            .await
            .expect("module socket must fall back to raw IPv6");
        assert!(stream.peer_addr().unwrap().is_ipv6());
        server.await.unwrap();
        drop(first);
        std::fs::remove_file(path).unwrap();
    }
}

enum LaunchArgs {
    Daemon(PathBuf),
    Version,
    WalKnownFeatures,
}

fn parse_launch_args(args: impl IntoIterator<Item = OsString>) -> Result<LaunchArgs, String> {
    let mut args = args.into_iter();
    let mut conn_path = None;
    let mut version = false;
    let mut known_features = false;
    while let Some(arg) = args.next() {
        if arg == "--subc" {
            if conn_path.is_some() {
                return Err("duplicate --subc argument".into());
            }
            let value = args.next().ok_or("missing value for --subc")?;
            if value.is_empty() || value.to_string_lossy().starts_with('-') {
                return Err(format!("missing value for --subc; got {value:?}"));
            }
            conn_path = Some(PathBuf::from(value));
        } else if arg == "--version" || arg == "-V" {
            version = true;
        } else if arg == "--known-features" {
            known_features = true;
        } else {
            return Err(format!("unknown argument {arg:?}"));
        }
    }
    if version {
        return Ok(LaunchArgs::Version);
    }
    if known_features {
        return Ok(LaunchArgs::WalKnownFeatures);
    }
    conn_path
        .map(LaunchArgs::Daemon)
        .ok_or_else(|| "missing required --subc <connection-file>".into())
}

/// The real-loop executor: drives core's admitted writer loop with a live provider resolved
/// from the catalog plus the real `SubcToolPlane`. Kept in a submodule so the binary's
/// wiring is the only place that builds a live provider.
mod executor;

fn create_state_directories(state_root: &std::path::Path) -> Result<(), ServeIoError> {
    broca_owner_only::create_dir_all(state_root)
        .map_err(|e| ServeIoError(format!("create state root {}: {e}", state_root.display())))?;
    broca_owner_only::create_dir(state_root.join("leases"))
        .or_else(|error| {
            if error.kind() == std::io::ErrorKind::AlreadyExists {
                Ok(())
            } else {
                Err(error)
            }
        })
        .map_err(|e| ServeIoError(format!("create lease directory: {e}")))?;

    // Storage folders are created here, not on their first write, because the backup
    // descriptor names them, and engram refuses a capture whose declared path is missing.
    for name in [
        broca_records::session_storage::SESSION_STORAGE_DURABLE_DIR,
        broca_records::SESSION_STORAGE_DIR,
        broca_records::session_storage::SESSION_STORAGE_EPHEMERAL_DIR,
        broca_records::session_storage::SESSION_STORAGE_MOVES_DIR,
    ] {
        let path = state_root.join(name);
        match broca_owner_only::create_dir(&path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists && path.is_dir() => {}
            Err(error) => {
                return Err(ServeIoError(format!(
                    "create state directory {}: {error}",
                    path.display()
                )));
            }
        }
    }

    broca_module::store_open::recovery::prepare_directories(state_root)
        .map_err(|error| ServeIoError(format!("create recovery candidate directories: {error}")))?;
    broca_core::media_uploads::create_upload_directories(state_root)
        .map_err(|error| ServeIoError(error.to_string()))?;
    Ok(())
}

fn write_engram_catalog(state_root: &Path) -> std::io::Result<()> {
    let path = state_root.join("engram-catalog.json");
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if !metadata.file_type().is_file() => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{} is not a regular file", path.display()),
            ));
        }
        Ok(_) => {
            if std::fs::read(&path)? == EMBEDDED_ENGRAM_CATALOG {
                ensure_engram_catalog_owner_only(&path)?;
                return Ok(());
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }

    broca_records::move_fence::atomic_json_bytes(state_root, &path, EMBEDDED_ENGRAM_CATALOG)
}

fn ensure_engram_catalog_owner_only(path: &Path) -> std::io::Result<()> {
    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{} is not a regular file", path.display()),
        ));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if metadata.permissions().mode() & 0o7777 != 0o600 {
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
    }
    #[cfg(windows)]
    {
        if broca_owner_only::validate(path).is_err() {
            broca_owner_only::repair(path)?;
        }
    }

    Ok(())
}

#[cfg(all(test, windows))]
mod windows_acl_tests {
    use super::*;
    use broca_owner_only::test_support::{assert_protected_private as assert_private, BroadParent};
    #[test]
    fn windows_state_and_lease_directories_are_private() {
        let fixture = BroadParent::new("state");
        let root = fixture.path.join("state");
        create_state_directories(&root).unwrap();
        write_engram_catalog(&root).unwrap();
        assert_private(&root);
        assert_private(&root.join("leases"));
        assert_private(&root.join("session-store-durable"));
        assert_private(&root.join("session-store"));
        assert_private(&root.join("session-store-ephemeral"));
        assert_private(&root.join("session-store-moves"));
        assert_private(&root.join("engram-catalog.json"));
    }
}

#[cfg(test)]
mod state_directory_tests {
    use super::{create_state_directories, write_engram_catalog, EMBEDDED_ENGRAM_CATALOG};
    use std::path::PathBuf;

    // broca's owner-only scratch root, not the system temp directory: on Windows the owner-only
    // checks refuse state under a shared temp folder.
    fn fresh_root(tag: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("system clock after Unix epoch")
            .as_nanos();
        broca_test_support::scratch_root().join(format!(
            "broca-state-directory-{tag}-{}-{nonce}",
            std::process::id()
        ))
    }

    #[test]
    fn first_boot_writes_the_tracked_engram_catalog_bytes() {
        let state_root = fresh_root("catalog-first-boot");
        create_state_directories(&state_root).expect("create startup state directories");

        write_engram_catalog(&state_root).expect("write the embedded backup catalog");

        let catalog_path = state_root.join("engram-catalog.json");
        assert_eq!(
            std::fs::read(&catalog_path).expect("read the written backup catalog"),
            EMBEDDED_ENGRAM_CATALOG,
            "the state-root catalog must exactly match the tracked source"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&catalog_path)
                    .expect("read catalog permissions")
                    .permissions()
                    .mode()
                    & 0o777,
                0o600,
                "the backup catalog must be owner-only"
            );
        }
        #[cfg(windows)]
        broca_owner_only::validate(&catalog_path)
            .expect("the backup catalog ACL must be owner-only");
        let _ = std::fs::remove_dir_all(&state_root);
    }

    #[test]
    fn identical_engram_catalog_is_not_rewritten() {
        let state_root = fresh_root("catalog-identical");
        create_state_directories(&state_root).expect("create startup state directories");
        let catalog_path = state_root.join("engram-catalog.json");
        let mut options = broca_owner_only::OpenOptions::new();
        options.write(true).create_new(true);
        use std::io::Write;
        options
            .open(&catalog_path)
            .expect("create the existing catalog")
            .write_all(EMBEDDED_ENGRAM_CATALOG)
            .expect("write the existing catalog");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&catalog_path, std::fs::Permissions::from_mode(0o644))
                .expect("make the existing catalog readable to others");
        }
        let before = std::fs::metadata(&catalog_path).expect("read existing catalog metadata");
        let modified_before = before.modified().expect("read existing catalog mtime");
        #[cfg(unix)]
        let identity_before = {
            use std::os::unix::fs::MetadataExt;
            (before.dev(), before.ino())
        };

        write_engram_catalog(&state_root).expect("leave the identical catalog in place");

        let after = std::fs::metadata(&catalog_path).expect("read catalog metadata after boot");
        assert_eq!(
            after.modified().expect("read catalog mtime after boot"),
            modified_before,
            "an identical catalog must keep its mtime"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            use std::os::unix::fs::PermissionsExt;
            assert_eq!((after.dev(), after.ino()), identity_before);
            assert_eq!(
                after.permissions().mode() & 0o777,
                0o600,
                "an existing matching catalog must remain owner-only"
            );
        }
        let _ = std::fs::remove_dir_all(&state_root);
    }

    #[test]
    fn differing_engram_catalog_is_replaced_with_the_embedded_file() {
        let state_root = fresh_root("catalog-replaced");
        create_state_directories(&state_root).expect("create startup state directories");
        let catalog_path = state_root.join("engram-catalog.json");
        std::fs::write(&catalog_path, b"stale catalog").expect("write stale catalog");
        #[cfg(unix)]
        let identity_before = {
            use std::os::unix::fs::MetadataExt;
            let metadata = std::fs::metadata(&catalog_path).expect("read stale catalog metadata");
            (metadata.dev(), metadata.ino())
        };

        write_engram_catalog(&state_root).expect("replace the stale catalog");

        assert_eq!(
            std::fs::read(&catalog_path).expect("read replaced catalog"),
            EMBEDDED_ENGRAM_CATALOG
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata =
                std::fs::metadata(&catalog_path).expect("read replaced catalog metadata");
            assert_ne!((metadata.dev(), metadata.ino()), identity_before);
        }
        assert!(
            std::fs::read_dir(&state_root)
                .expect("list state root")
                .all(|entry| !entry
                    .expect("read state root entry")
                    .file_name()
                    .to_string_lossy()
                    .contains("engram-catalog.json.tmp")),
            "atomic publication must leave no temporary catalog file"
        );
        let _ = std::fs::remove_dir_all(&state_root);
    }

    #[test]
    fn portable_descriptor_directories_are_created_before_any_session() {
        let state_root = fresh_root("portable");
        create_state_directories(&state_root).expect("create startup state directories");
        let descriptor_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .join("engram-catalog.json");
        let placed_descriptor = state_root.join("engram-catalog.json");
        std::fs::copy(&descriptor_path, &placed_descriptor)
            .expect("place the tracked backup descriptor in the scratch state root");
        let descriptor: serde_json::Value = serde_json::from_slice(
            &std::fs::read(&placed_descriptor).expect("read placed backup descriptor"),
        )
        .expect("placed descriptor is JSON");

        let entries = descriptor["entries"]
            .as_array()
            .expect("placed descriptor entries are a list");
        let mut portable_entries = 0;
        for entry in entries.iter().filter(|entry| entry["class"] == "portable") {
            portable_entries += 1;
            let entry_id = entry["entry_id"]
                .as_str()
                .expect("portable entry has an id");
            assert_eq!(
                entry["mechanism"], "filetree",
                "portable entry {entry_id} must name a boot-created directory"
            );
            let relative_path = entry["path"]
                .as_str()
                .unwrap_or_else(|| panic!("portable entry {entry_id} has no path"));
            let path = state_root.join(relative_path);
            assert!(
                path.is_dir(),
                "portable entry {entry_id} names {}, but startup did not create that directory",
                path.display()
            );
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = std::fs::metadata(&path)
                    .expect("read portable directory metadata")
                    .permissions()
                    .mode()
                    & 0o777;
                assert_eq!(mode, 0o700, "portable entry {entry_id} must be owner-only");
            }
            let mut actual: Vec<_> = std::fs::read_dir(&path)
                .expect("read newly created portable directory")
                .map(|entry| entry.unwrap().file_name())
                .collect();
            actual.sort();
            let expected: &[&str] = match relative_path {
                broca_records::session_storage::SESSION_STORAGE_DURABLE_DIR => {
                    &["recovery-candidates"]
                }
                _ => &[],
            };
            assert_eq!(
                actual, expected.iter().map(|name| std::ffi::OsString::from(*name)).collect::<Vec<_>>(),
                "boot must create {entry_id} and only its structural subdirectories before any session writes a log"
            );
        }
        assert!(
            portable_entries > 0,
            "placed descriptor has no portable entries"
        );
        let _ = std::fs::remove_dir_all(&state_root);
    }

    /// The class roots contain only their empty recovery-candidate directories before any
    /// session is placed. They and the empty moves folder are owner-only, and the descriptor
    /// names each with its backup class: only the durable root is captured.
    #[test]
    fn session_store_class_roots_exist_before_catalog_and_match_backup_classes() {
        let state_root = fresh_root("session-store-classes");
        create_state_directories(&state_root).unwrap();
        write_engram_catalog(&state_root).unwrap();
        let catalog: serde_json::Value =
            serde_json::from_slice(&std::fs::read(state_root.join("engram-catalog.json")).unwrap())
                .unwrap();
        let entries = catalog["entries"].as_array().unwrap();
        for (path, class) in [
            (
                broca_records::session_storage::SESSION_STORAGE_DURABLE_DIR,
                "portable",
            ),
            (broca_records::SESSION_STORAGE_DIR, "excluded"),
            (
                broca_records::session_storage::SESSION_STORAGE_EPHEMERAL_DIR,
                "excluded",
            ),
            (
                broca_records::session_storage::SESSION_STORAGE_MOVES_DIR,
                "device-local",
            ),
        ] {
            let folder = state_root.join(path);
            assert!(folder.is_dir(), "startup must create {}", folder.display());
            broca_owner_only::validate(&folder).unwrap();
            if path == broca_records::session_storage::SESSION_STORAGE_MOVES_DIR {
                assert_eq!(std::fs::read_dir(&folder).unwrap().count(), 0, "{path}");
            } else {
                let children: Vec<_> = std::fs::read_dir(&folder)
                    .unwrap()
                    .map(|entry| entry.unwrap().file_name())
                    .collect();
                assert_eq!(
                    children,
                    vec![std::ffi::OsString::from("recovery-candidates")]
                );
                let candidates = folder.join("recovery-candidates");
                broca_owner_only::validate(&candidates).unwrap();
                assert_eq!(std::fs::read_dir(candidates).unwrap().count(), 0);
            }
            let matching: Vec<_> = entries
                .iter()
                .filter(|entry| entry["path"] == path)
                .collect();
            assert_eq!(matching.len(), 1, "one catalog entry for {path}");
            assert_eq!(matching[0]["class"], class, "backup class for {path}");
            if class == "excluded" {
                assert!(!matching[0]["reason"].as_str().unwrap().is_empty());
            }
        }
        // The placement rule's classes name exactly these roots.
        for class in broca_records::session_storage::StorageClass::ALL {
            assert!(class.store_root(&state_root).is_dir(), "{class}");
        }
        let _ = std::fs::remove_dir_all(state_root);
    }

    #[test]
    fn the_upload_tree_exists_before_catalog_and_is_excluded() {
        let state_root = fresh_root("media-catalog");
        create_state_directories(&state_root).unwrap();
        assert!(!state_root.join("engram-catalog.json").exists());
        for child in ["objects", "refs"] {
            let path = state_root.join("media-uploads").join(child);
            assert!(path.is_dir(), "startup must create {}", path.display());
            broca_owner_only::validate(&path).unwrap();
        }
        write_engram_catalog(&state_root).unwrap();
        let catalog: serde_json::Value =
            serde_json::from_slice(&std::fs::read(state_root.join("engram-catalog.json")).unwrap())
                .unwrap();
        let entries = catalog["entries"].as_array().unwrap();
        let matching: Vec<_> = entries
            .iter()
            .filter(|entry| entry["path"] == "media-uploads")
            .collect();
        assert_eq!(matching.len(), 1, "one catalog entry for media-uploads");
        assert_eq!(matching[0]["class"], "excluded");
        assert!(!matching[0]["reason"].as_str().unwrap().is_empty());
        let _ = std::fs::remove_dir_all(state_root);
    }

    #[cfg(unix)]
    #[test]
    fn startup_does_not_change_modes_of_existing_durable_directories() {
        use std::os::unix::fs::PermissionsExt;

        let state_root = fresh_root("existing");
        std::fs::create_dir_all(&state_root).expect("create existing state root");
        let expected = [
            (
                broca_records::session_storage::SESSION_STORAGE_DURABLE_DIR,
                0o750,
            ),
            (
                broca_records::session_storage::SESSION_STORAGE_MOVES_DIR,
                0o755,
            ),
        ];
        for (name, mode) in expected {
            let path = state_root.join(name);
            std::fs::create_dir(&path).expect("create existing durable directory");
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode))
                .expect("set existing directory mode");
        }

        create_state_directories(&state_root).expect("leave existing directories in place");

        for (name, mode) in expected {
            let path = state_root.join(name);
            let actual = std::fs::metadata(path)
                .expect("read existing directory metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(actual, mode, "startup changed existing {name} permissions");
        }
        let _ = std::fs::remove_dir_all(&state_root);
    }

    #[test]
    fn startup_directory_errors_name_the_path() {
        let state_root = fresh_root("blocked");
        std::fs::create_dir_all(&state_root).expect("create blocked state root");
        let blocked = state_root.join(broca_records::session_storage::SESSION_STORAGE_DURABLE_DIR);
        std::fs::write(&blocked, "not a directory").expect("occupy durable directory path");

        let error =
            create_state_directories(&state_root).expect_err("file blocks directory creation");

        assert!(
            error.to_string().contains(&blocked.display().to_string()),
            "startup error must name the path {}: {error}",
            blocked.display()
        );
        let _ = std::fs::remove_dir_all(&state_root);
    }
}
