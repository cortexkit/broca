//! The broca subc module serve binary.
//!
//! Lifecycle (the canonical module handshake): read `--subc <connection-file>`,
//! HMAC-authenticate as a client to subc, HELLO-register the manifest, then serve the
//! frame loop (route.bind → ack, session.*/run.* route requests → the coordinator).
#![forbid(unsafe_code)]

use broca_diagnostics::{spawn_blocking, JobKind};

use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use broca_core::{FileLeaseStore, FileWalStore, RunIndex};
use broca_module::{DefaultOpener, LineageRegistry};
use broca_module_serve::{
    spawn_catalog_poll, CatalogPoller, ChargeBasisTable, ConsumerRoutes, ServeEnd, ServeIoError,
};
use broca_subc::{SubcConnection, SubcToolPlane};
use subc_protocol::SUBC_MODULE_ID_ENV;
use subc_transport::{authenticate_client, connection_file};
use tokio::net::TcpStream;

const DEFAULT_MODULE_ID: &str = "broca";
const AUTH_DEADLINE: Duration = Duration::from_secs(2);
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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    run_runtime(serve_main())?
}

fn run_runtime<F: std::future::Future>(work: F) -> std::io::Result<F::Output> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
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
    // Reject stray invocations before opening logs or reading config: discovery probes are
    // not daemon boots and must not leave records in the operator's production log.
    let args = parse_launch_args(std::env::args_os().skip(1)).unwrap_or_else(|error| {
        eprintln!("ck-broca: {error}\nvalid arguments: --subc <connection-file>, --version (-V), --wal-known-features");
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
            let mut names = broca_wal::KNOWN_FEATURES.to_vec();
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
    // The startup crash-orphan pass treats a WAL last written before this instant as a crashed
    // process's leftover; anything written later belongs to a live writer.
    let process_started = std::time::SystemTime::now();
    broca_module_serve::transport_config::validate_default().map_err(ServeIoError)?;
    let archive_config = broca_module_serve::wal_archive::WalArchiveConfig::load_default_path()
        .map_err(ServeIoError)?;
    let backup_classes = broca_module_serve::session_class::BackupClasses::load_default_path()
        .map_err(ServeIoError)?;
    let provider_overrides =
        broca_module_serve::provider_overrides::ProviderOverrides::load_default_path()
            .map_err(ServeIoError)?;
    let stop_signal = shutdown_signal()?;
    // Where the WAL, leases and run index live. Resolved before any connection is made so a
    // machine that cannot place its durable state refuses to start by name instead of first
    // registering and then failing. See `state_root` for the rule, and for why the location is a
    // contract with astrocyte rather than a private choice.
    let state_root = broca_module_serve::state_root::resolve_for_launch(&conn_path)
        .map_err(|e| ServeIoError(e.to_string()))?
        .path;
    let _stall_watchdog = broca_module_serve::stall::start(&state_root)?;
    // An optional provider base_url override (the mock/openai-compatible path; e.g. aimock).
    // When unset, the run's model id is resolved through the catalog.
    let provider_base_url = std::env::var("BROCA_PROVIDER_BASE_URL")
        .ok()
        .filter(|v| !v.trim().is_empty());
    // Whether a mock/openai-compatible base_url override is set. On that path every run
    // authenticates with AuthMode::None, so the vault is never consulted (and a mock rig runs
    // no credentials module to open it against). Gates the vault connection below.
    let mock_provider_path = provider_base_url.is_some();

    // Credential source: default the credential vault, or the pre-vault OpenCode auth file
    // when BROCA_AUTH_SOURCE=opencode. Selecting the vault opens a SECOND outbound consumer
    // connection to the same daemon (a process-global credentials route; credentials are
    // handle-keyed, not session-keyed) and loads the provider→handle map. A misconfiguration
    // here fails loud at startup (no silent fallback to the old source).
    let auth_source =
        executor::AuthSource::from_env(std::env::var("BROCA_AUTH_SOURCE").ok().as_deref())
            .map_err(ServeIoError)?;

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
        broca_module_serve::context_management::ContextManagementConfig::load_default_path()
            .map_err(ServeIoError)?;
    // The `account_selection` section: which routing module new runs ask for their provider
    // account, and the switch that bypasses it. A malformed section refuses boot by name.
    let account_selection =
        broca_module_serve::account_routing::AccountSelectionConfig::load_default_path()
            .map_err(ServeIoError)?;
    let consumer_routes = ConsumerRoutes::resolve(
        std::env::var(broca_module_serve::consumer_routes::TOOL_MODULE_ENV)
            .ok()
            .as_deref(),
        auth_source == executor::AuthSource::Vault && !mock_provider_path,
    )
    .with_account_selection(&account_selection);
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
    let tool_conn = SubcConnection::connect(&conn_path)
        .await
        .map_err(|e| ServeIoError(format!("tool-plane consumer connect: {e}")))?;
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
    let plane = Arc::new(SubcToolPlane::new(
        tool_conn,
        consumer_routes.tool_module.clone(),
    ));
    // The plane asks the daemon about a scope (`scope.describe`) over broca's own MODULE
    // connection, because the daemon answers it only on a registered module's connection. The
    // serve loop below attaches the describer once that connection has registered.
    let scope_describer = Arc::new(broca_subc::ModuleScopeDescriber::new());
    plane.set_scope_describer(scope_describer.clone());

    // The coordinator: a registry over file-backed lease + WAL stores + the durable run
    // index, driving the real agentic loop. The module owns its state_root -- the lease, WAL,
    // and store partition are its durability domain, and the WAL is the system of record: the
    // store is a projection that can be rebuilt from it, while nothing can rebuild the WAL.
    //
    // `state_root` was resolved at startup: BROCA_STATE_ROOT when set, otherwise the durable
    // data home, `<data home>/cortexkit/broca`, and never the daemon's runtime directory, whose
    // files cleanup removes. Astrocyte reads `run-index.db` at that data-home path, so the
    // default location is a contract with another module, not a local preference.
    // Create the state root up front so the SQLite run index can open its file (the file
    // stores create their own subdirs lazily, but SqliteStore::open needs the dir to exist).
    create_state_directories(&state_root)?;
    let _archive_timer = archive_config.spawn(state_root.clone());
    let lease_store = Arc::new(FileLeaseStore::new(state_root.join("leases")));
    let wal_store =
        Arc::new(FileWalStore::new(state_root.join("wal")).with_backup_classes(backup_classes.0));
    let crash_orphan_wal = wal_store.clone();
    let run_index = Arc::new(
        broca_store::SqliteStore::open(state_root.join("run-index.db"))
            .map_err(|e| ServeIoError(format!("open run index: {e}")))?,
    );
    // Budget caps have their own durability domain. Keeping this database separate
    // prevents session WAL and run-index maintenance from changing admission state.
    let cap_store = Arc::new(
        broca_store::CapsStore::open(state_root.join("caps.db"))
            .map_err(|e| ServeIoError(format!("open cap table: {e}")))?,
    );

    // Load the last-known-good before any admission is possible. The fetch leg gets its own
    // consumer connection, but construction performs no fusiform route.open; only the detached
    // poll below can dial that sibling module.
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
    for notice in provider_overrides.notices(broca_catalog::catalog().ok().as_ref()) {
        tracing::warn!(target: "catalog", "{notice}");
    }
    let catalog_conn = Arc::new(
        SubcConnection::connect(&conn_path)
            .await
            .map_err(|e| ServeIoError(format!("catalog consumer connect: {e}")))?,
    );
    let catalog_identity = broca_protocol::BindIdentity::new(
        state_root.to_string_lossy().to_string(),
        "broca",
        "catalog-refresh",
    );
    let _catalog_poll = spawn_catalog_poll(Arc::new(CatalogPoller::new(
        catalog_conn,
        catalog_identity,
        Arc::clone(&catalog_cache),
    )));

    // Both vault and OpenCode use the same provider→method defaults to name the credential
    // selected for a run. The mock path deliberately skips this load; see the rule on
    // `method_config_for_launch`.
    let auth_methods = method_config_for_launch(mock_provider_path, || {
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
    let mut executor_impl = executor::LiveLoopExecutor::new(
        wal_store.clone(),
        run_index.clone(),
        plane,
        provider_base_url,
    )
    .with_hook_plane(Arc::new(broca_subc::SubcHookPlane::new(plan_conn.clone())))
    .with_compaction_plane(Arc::new(broca_subc::SubcCompactionPlane::new(
        plan_conn.clone(),
    )))
    .with_elicitation_plane(Arc::new(broca_subc::consent::ScopedConsentPlane::new(
        plan_conn.clone(),
        broca_subc::RouteOpenRetry::default_patience(),
    )))
    .with_methods(auth_methods.clone())
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
        broca_module_serve::CacheTtlTable::load_default_path().map_err(ServeIoError)?;
    executor_impl = executor_impl.with_cache_table(cache_table);
    let charge_basis_table = Arc::new(ChargeBasisTable::load_default_path().map_err(ServeIoError)?);
    if let Some(notice) = charge_basis_table.ignored_version_notice() {
        tracing::warn!(target: "billing", "{notice}");
    }
    // Local override rows no longer stop startup; they are named here once and recorded on
    // every fact as charge_basis_overrides. Silent when there are none.
    if let Some(warning) = charge_basis_table.override_warning() {
        tracing::warn!(target: "billing", "{warning}");
    }
    charge_basis_table.publish_for_health();
    // The terminal run index is the durable outbox for a crash after its commit but before live
    // export emission. Catch up off the runtime so HELLO remains non-blocking;
    // repeated work races safely with the live path because export facts are immutable by fact_id.
    let catch_up_index = run_index.clone();
    let catch_up_wal = wal_store.clone();
    let catch_up_resolver = charge_basis_table.clone();
    let _export_catch_up = spawn_blocking(JobKind::Recovery, move || {
        let started = std::time::Instant::now();
        match broca_core::catch_up_usage_exports(
            catch_up_index.as_ref(),
            catch_up_wal.as_ref(),
            catch_up_resolver.as_ref(),
        ) {
            // SAY IT FINISHED, AND HOW LONG IT TOOK, even when there was nothing to do. This
            // arm used to log nothing, so "finished in 50 ms" and "still running 20 s after
            // boot" were the same observation from outside. This pass shares the store's single
            // connection with admission's snapshot read, so it was the first suspect for the
            // first send after a respawn that was never admitted. Measured at 2-5.5 s, it had
            // always finished before that send; the stall was the archive membership build,
            // prewarmed below.
            Ok(0) => tracing::info!(
                target: "export",
                "usage-export startup catch-up finished in {} ms; nothing missed",
                started.elapsed().as_millis()
            ),
            Ok(count) => tracing::warn!(
                target: "export",
                "caught up {count} missed usage-export run(s) in {} ms",
                started.elapsed().as_millis()
            ),
            Err(error) => {
                tracing::warn!(target: "export", "usage-export startup catch-up failed: {error}")
            }
        }
    });
    // Build the archive membership cache now, off the send path. Otherwise the first NEW
    // session after a restart builds it inside admission: 10,821 ms cold on production
    // (2026-09-25 18:46Z), past the client's 10 s send deadline, so that send was never
    // admitted. See FileWalStore::prewarm_archive_membership. The build logs its own time.
    let prewarm_wal = wal_store.clone();
    let _archive_prewarm = spawn_blocking(JobKind::Recovery, move || {
        if let Err(error) = prewarm_wal.prewarm_archive_membership() {
            tracing::warn!(
                target: "archive",
                "archive membership prewarm failed; the first new session will build it instead: {error}"
            );
        }
    });
    let mut vault_gauge = broca_module_serve::serve::VaultGauge::not_in_use();
    // Open the vault only when a real provider call will actually be made. A mock base_url
    // path authenticates with AuthMode::None for every run (see `decide_auth`), so the vault
    // is unused there — skip it. The default is now the vault, but the mock path never needs
    // it, which keeps the mock-based e2e rigs (no credentials module) working unchanged.
    if let Some(vault_module) = consumer_routes.vault_module.clone() {
        // Every credential is read by grant, by credential id. The daemon deliberately never
        // opens the capability-handle file: the vault revokes this module's handles once no
        // placed build reads them, so a handle read here would fail for every run.
        let credential_ids =
            broca_module_serve::VaultCredentialIds::load_default_path().map_err(ServeIoError)?;
        let addressing = broca_module_serve::VaultAddressing::new(credential_ids);
        // A process-global bind for the vault route. The vault ignores the bind identity
        // (reads are authorized by this module's grant, not by bind), but subc's daemon canonicalizes
        // and REJECTS a non-existent project_root — so use the state root, which exists.
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
        if account_selection.legacy_static {
            tracing::warn!(
                target: "accounts",
                "account_selection.legacy_static is on: every run takes the statically \
                 configured provider account, and routing is neither asked nor told"
            );
        }
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
        let client = broca_subc::VaultClient::new(
            vault_conn,
            &vault_module,
            &vault_bind,
            VAULT_READY_TIMEOUT,
        );
        // INFO: the startup census. It answers "which credentials does this process hold",
        // which is a question asked when something is already wrong -- so it must be IN the log,
        // and it is not itself a warning. A contributor proposed a per-call log line for the
        // same purpose (issue #19); this line plus the durable resolved_credential_id on every
        // export fact is what made that unnecessary.
        tracing::info!(
            target: "credentials",
            "auth source = vault (every credential is read by grant as {}; \
             vault.credential_ids aliases: {:?})",
            client
                .scoped_principal()
                .unwrap_or_else(|| "nobody (no supervised consumer identity)".to_string()),
            addressing.aliases()
        );
        executor_impl = executor_impl.with_vault(executor::VaultAuth {
            client,
            addressing,
            methods: auth_methods,
            gauge: vault_gauge.clone(),
            cache: executor::VaultLkgCache::default(),
            listing: Default::default(),
        });
        executor_impl = executor_impl.with_routing(routing);
        executor_impl.prime_account_listing();
    }

    executor_impl = executor_impl.with_context_management(context_management);
    let vault_auditor = executor_impl.vault_auditor();
    let wal_reader: Arc<dyn broca_core::WalStore> = wal_store.clone();
    let store_reader: Arc<dyn broca_core::MaterializedStore> = run_index.clone();
    let retention = Arc::new(
        broca_module::retention::RetentionService::new(
            wal_store.clone(),
            run_index.clone(),
            lease_store.clone(),
            run_index.clone(),
            charge_basis_table.clone(),
        )
        .map_err(|error| ServeIoError(format!("retention clock: {error}")))?,
    );
    let executor = Arc::new(executor_impl.with_clock(retention.clock.clone()));
    let move_wal = wal_store.clone();
    let mut opener_builder = DefaultOpener::new(lease_store, wal_store, executor)
        .with_run_index(run_index.clone())
        .with_materialized_store(run_index.clone())
        .with_export_index(run_index.clone())
        .with_charge_basis_resolver(charge_basis_table)
        .with_cap_store(cap_store.clone());
    if let Some(ttl) = idle_ttl {
        opener_builder = opener_builder.with_idle_ttl(ttl);
    }
    let registry = Arc::new(LineageRegistry::new_with_reader_and_caps(
        Arc::new(opener_builder),
        Some(wal_reader),
        Some(store_reader),
        Some(cap_store),
    ));
    registry.set_retention_service(retention);
    let agent_data = Arc::new(broca_module_serve::agent_data::AgentDataService::new(
        state_root.clone(),
        registry.as_ref().clone(),
        move_wal,
        run_index.clone(),
        Arc::new(broca_subc::session_door::SessionDoorClient::new(
            plan_conn.clone(),
            broca_protocol::BindIdentity::new(state_root.clone(), "broca", "agent-data"),
        )),
    ));
    agent_data
        .reconcile_boot()
        .await
        .map_err(|error| ServeIoError(format!("agent-data boot recovery: {}", error.message)))?;
    registry
        .boot_retention()
        .await
        .map_err(|error| ServeIoError(format!("retention boot recovery: {error}")))?;
    let retention_sweep = {
        let registry = registry.clone();
        tokio::spawn(async move {
            if registry.run_retention_sweeper().await.is_err() {
                tracing::warn!(target: "retention", "retention sweep stopped; durable recovery required");
            }
        })
    };
    // Fetch-plan admission uses a consumer connection of its own, keeping its short-lived fetch
    // routes apart from the long-lived routes the model's tool calls are dispatched on.
    registry.set_plan_admitter(Arc::new(broca_subc::SubcPlanAdmitter::new(plan_conn)));
    // What each session's frozen config declares about its prompt cache, for `session.warm`,
    // the admission reply and `session.baseline`.
    registry.set_cache_declarer(Arc::new(
        broca_module_serve::cache_warm::ProviderCacheDeclarer,
    ));
    let crash_orphans = mark_crash_orphans(
        &registry,
        Arc::clone(&run_index),
        Arc::clone(&crash_orphan_wal),
        process_started,
    )
    .await;
    let _crash_orphan_seal = {
        let registry = Arc::clone(&registry);
        tokio::spawn(async move {
            let started = std::time::Instant::now();
            let sweep = registry.seal_marked_crash_orphans(crash_orphans).await;
            // One line per pass, at WARN like the shutdown seal's, and on every boot: a
            // silent pass would read the same as a pass that never ran.
            tracing::warn!(
                target: "lifecycle",
                "crash-orphan seal complete in {} ms; {} run(s) sealed interrupted, {} index \
                 row(s) repaired from the WAL ({} active index row(s); {} session(s) marked, {} archived, {} written since \
                 boot, {} unreadable, {} held by another process, {} failed to open)",
                started.elapsed().as_millis(),
                sweep.sealed,
                sweep.index_repaired,
                sweep.candidate_runs,
                sweep.marked,
                sweep.not_live,
                sweep.written_since_boot,
                sweep.unreadable,
                sweep.lease_held,
                sweep.open_failed,
            );
        })
    };

    let _restart_resume = {
        let registry = registry.clone();
        let index = run_index.clone();
        let wal = crash_orphan_wal.clone();
        let describer = scope_describer.clone();
        tokio::spawn(async move {
            describer.wait_registered().await;
            let candidates =
                spawn_blocking(JobKind::Recovery, move || index.restart_resume_candidates()).await;
            match candidates {
                Ok(Ok(candidates)) => {
                    registry.resume_restart_pauses(wal, candidates).await;
                }
                error => {
                    tracing::warn!(target: "lifecycle", "restart resume: cannot list candidate runs: {error:?}")
                }
            }
        })
    };
    // Finished runs are not restart candidates, but their actors may still need to
    // retract unanswered approval questions. The query excludes sessions whose content
    // retention has expired: the retention service retracts those using its ledger of
    // question identifiers, without reopening the deleted conversation.
    let _withdrawal_debt = {
        let registry = registry.clone();
        let index = run_index.clone();
        let describer = scope_describer.clone();
        tokio::spawn(async move {
            let stopping = registry.stopping_signal();
            tokio::select! {
                biased;
                _ = stopping.cancelled() => return,
                _ = describer.wait_registered() => {}
            }
            let candidates = tokio::select! {
                biased;
                _ = stopping.cancelled() => return,
                rows = spawn_blocking(JobKind::Recovery, move || index.runs_with_withdrawals_owed()) => rows,
            };
            let candidates = match candidates {
                Ok(Ok(rows)) => rows,
                error => {
                    tracing::warn!(target: "lifecycle", "withdrawal debt: cannot list candidate runs: {error:?}");
                    return;
                }
            };
            let mut seen = std::collections::HashSet::new();
            for row in candidates {
                if !seen.insert(row.session.clone()) {
                    continue;
                }
                tokio::select! {
                    biased;
                    _ = stopping.cancelled() => return,
                    opened = registry.actor_for(&row.session) => {
                        if let Err(error) = opened {
                            tracing::warn!(target: "lifecycle", "withdrawal debt: cannot open session '{}': {error}", row.session.session);
                        }
                    }
                }
            }
        })
    };
    let stream = connect(&conn_path).await?;
    let (read_half, write_half) = tokio::io::split(stream);
    let serving = broca_module_serve::serve::serve_connection_with_agent_data(
        read_half,
        write_half,
        &module_id,
        registry.clone(),
        registry.clone(),
        vault_gauge,
        Some(catalog_cache),
        archive_config,
        consumer_routes,
        vault_auditor,
        daemon_capabilities,
        Some(scope_describer),
        Some(agent_data),
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
            // Actor sealing owns final WAL syncs and lease release; transport loss cannot
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
            let _ = retention_sweep.await;
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
            // Wait for final actor WAL syncs and lease release before dropping the runtime.
            let sealed = registry.stop_and_seal().await;
            let report = registry.shutdown_report().expect("stop records its outcome");
            // Stop the sweep supervisor; incomplete deletions recover from durable tombstones.
            let _ = retention_sweep.await;
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
            result?;
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

/// First half of the startup crash-orphan pass, run BEFORE serving begins.
///
/// A process that dies without its shutdown seal (SIGKILL, panic, power loss) leaves each
/// in-flight run with a `RunStarted`, no terminal, and an `active` index row. This marks the
/// sessions that may hold one; the registry's actors then seal them `Interrupted` through the
/// shutdown seal's own code when they open. Marking before the first request is what lets the
/// sealing half run in the background while serving: a caller's send that reaches a marked
/// session first takes the mark in its own open and seals before its send is handled.
///
/// Candidates come from the run index's active rows, not from replaying every live WAL, so the
/// cost is one stat per candidate session. A failure here costs only the pass: the orphans stay
/// as they are and ordinary recovery handles them when the session is next opened.
async fn mark_crash_orphans(
    registry: &LineageRegistry,
    run_index: Arc<broca_store::SqliteStore>,
    wal_store: Arc<FileWalStore>,
    process_started: std::time::SystemTime,
) -> broca_module::registry::CrashOrphanSweep {
    let candidates = spawn_blocking(JobKind::Recovery, move || run_index.active_runs())
        .await
        .map_err(|error| error.to_string())
        .and_then(|result| result.map_err(|error| error.to_string()));
    let candidates = match candidates {
        Ok(rows) => rows
            .into_iter()
            .map(|(run_id, key)| (broca_protocol::RunId(run_id), key))
            .collect(),
        Err(error) => {
            tracing::warn!(
                target: "lifecycle",
                "crash-orphan pass could not list active runs; skipping it this boot: {error}"
            );
            Vec::new()
        }
    };
    let registry = registry.clone();
    spawn_blocking(JobKind::Recovery, move || {
        registry.mark_crash_orphans(wal_store.as_ref(), candidates, process_started)
    })
    .await
    .unwrap_or_default()
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

/// A mock/base-url launch must not load the developer's real auth-methods config.
///
/// Extracted from `main` so the rule is reachable by a test. Inline in `async fn main` it was
/// stated in a comment and enforced by nothing, which is the shape where a refactor can drop the
/// branch and every test still passes.
///
/// WHY IT MATTERS: on the mock path every run authenticates `AuthMode::None`, so there is no
/// credential to name and `auth_selection` must stay unset. Loading the real config would give
/// those runs a provider-method default they cannot honour, and the WAL would then freeze an
/// `auth_selection` describing a credential the run never used -- durable, wrong, and carried
/// into billing export as an account attribution for a run that authenticated with nothing.
///
/// It is also the reason a mock rig needs no config file on disk: reading one would make the
/// e2e suites depend on developer state.
fn method_config_for_launch<F>(
    mock_provider_path: bool,
    load: F,
) -> Result<broca_module_serve::MethodConfig, String>
where
    F: FnOnce() -> Result<broca_module_serve::MethodConfig, String>,
{
    if mock_provider_path {
        return Ok(broca_module_serve::MethodConfig::empty());
    }
    load()
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
        let first = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
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
        drop(first);
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
    let mut wal_known_features = false;
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
        } else if arg == "--wal-known-features" {
            wal_known_features = true;
        } else {
            return Err(format!("unknown argument {arg:?}"));
        }
    }
    if version {
        return Ok(LaunchArgs::Version);
    }
    if wal_known_features {
        return Ok(LaunchArgs::WalKnownFeatures);
    }
    conn_path
        .map(LaunchArgs::Daemon)
        .ok_or_else(|| "missing required --subc <connection-file>".into())
}

/// The real-loop executor: drives core's admitted writer loop with a live provider resolved
/// from the catalog plus the real `SubcToolPlane`. Kept in a submodule so the binary's
/// wiring is the only place that builds a live provider.
mod executor {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use async_trait::async_trait;
    use broca_catalog::live::{AuthMode, CachePolicy};
    use broca_core::{
        run_loop_on_writer_admitted_with_prior_snapshot, AllowAll, FileWalStore, OutputEvent,
        RunError, RunSummary, WalStore,
    };
    use broca_module::executor::{RunExecutor, RunJob, RunKind};
    use broca_protocol::caller::{PauseReason, RunFinishReason};
    use broca_protocol::error::{ErrorClass, ProviderError};
    use broca_protocol::{AuthRequest, BindIdentity};
    use broca_provider::credential_refresh::{
        RefreshFuture, RefreshedSecret, SecretRefresher, SharedSecretRefresher,
    };
    use broca_provider::{build_provider_http_client, ProviderHttpClient};
    use broca_subc::{SubcToolPlane, VaultError, VaultGet, VaultToken};
    use tokio::sync::mpsc;

    /// How long to wait for the tool route to bind before the timed loop (gives the tool
    /// provider time for its one-time setup on the first route open for a session — e.g. AFT
    /// cold-starts its code index, which on a large project can take seconds).
    const BIND_TIMEOUT: Duration = Duration::from_secs(30);

    /// Where the executor gets a provider credential. Selected by `BROCA_AUTH_SOURCE`
    /// (default `vault`). The vault is the authoritative source for every credential kind
    /// (api-key included), resolving a fresh, refresh-on-expiry token per run; `opencode`
    /// opts back into reading the static pre-vault auth file.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum AuthSource {
        /// Read the OpenCode auth file (the pre-vault behavior; opt-in via
        /// `BROCA_AUTH_SOURCE=opencode`).
        Opencode,
        /// Resolve a fresh, refresh-on-expiry token from the credential vault (the default).
        Vault,
    }

    impl AuthSource {
        /// Parse `BROCA_AUTH_SOURCE`. Unset/empty ⇒ `Vault` (the default — the credential
        /// vault is now the authoritative source for all credential kinds). `opencode`
        /// selects the pre-vault auth-file behavior. A non-empty unrecognized value is a
        /// MISCONFIGURATION — fail loud rather than silently fall back (which would hide an
        /// operator's typo and quietly use a different auth source than they asked for).
        pub fn from_env(value: Option<&str>) -> Result<Self, String> {
            match value.map(str::trim) {
                None | Some("") | Some("vault") => Ok(AuthSource::Vault),
                Some("opencode") => Ok(AuthSource::Opencode),
                Some(other) => Err(format!(
                    "BROCA_AUTH_SOURCE must be 'opencode' or 'vault', got {other:?}"
                )),
            }
        }
    }

    use broca_catalog::live::WireFamilyId;
    use broca_module_serve::account_routing::{AccountChoice, RoutingClient};
    use broca_module_serve::{AuthMethod, CacheTtlTable, CredentialId};

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum EpisodeAuthContext {
        FreshSession,
        NewEpisode,
        Resume,
    }

    fn episode_auth_context(
        fresh_episode: bool,
        session_has_prior_episode: bool,
    ) -> EpisodeAuthContext {
        if !fresh_episode {
            EpisodeAuthContext::Resume
        } else if session_has_prior_episode {
            EpisodeAuthContext::NewEpisode
        } else {
            EpisodeAuthContext::FreshSession
        }
    }

    fn caller_auth_for_episode(
        auth: Option<&AuthRequest>,
        context: EpisodeAuthContext,
    ) -> Result<(Option<AuthMethod>, Option<&str>), RunError> {
        let Some(auth) = auth else {
            return Ok((None, None));
        };
        if context == EpisodeAuthContext::Resume {
            return Err(RunError::Config(
                "auth pin refused: this send is a genuine resume of an unfinished episode; \
                 a fresh run (a new session or a new episode after the prior episode finishes) \
                 would honor the pin"
                    .into(),
            ));
        }
        let method = AuthMethod::from_token(&auth.method).ok_or_else(|| {
            RunError::Config(format!(
                "unknown auth method '{}'; accepted methods are 'apikey', 'oauth', \
                 'antigravity', and 'chatgpt'",
                auth.method
            ))
        })?;
        Ok((Some(method), auth.account.as_deref()))
    }

    /// The wire family a method IMPLIES, when the method selects a distinct wire grammar.
    /// This is the SINGLE authority mapping method → family override, consulted by BOTH the
    /// family DRIVER (the catalog resolver's `family_override`, which builds the run with this
    /// family) AND the run-start consistency check below — so the family the run renders with
    /// and the family the guard asserts can never come from two sources, hence can never
    /// drift to a mismatched (family, credential) pair.
    ///
    /// A same-family method (`ApiKey`/`Oauth`) returns `None` → the catalog's family is used
    /// unchanged. The catalog still receives the method separately because Anthropic API key
    /// changes frozen quirks and auth headers without changing the family. A wire-changing method
    /// returns its family: `Antigravity` renders the Code-Assist envelope (`GoogleCodeAssist`),
    /// `Chatgpt` renders the codex `/responses` (`OpenAiResponsesChatgpt`).
    pub use broca_module_serve::family_for_method;

    /// The one OpenCode method/selection decision used by family routing, the consistency guard,
    /// and durable metadata.
    struct OpencodeEpisodeAuth {
        selection: Option<broca_core::AuthSelection>,
        method: Option<AuthMethod>,
    }

    /// Resolve OpenCode auth metadata before provider construction. Fresh runs prefer a caller
    /// pin, then the configured provider method, then the auth-file entry type. Resumes ignore all
    /// three and recover the method from the frozen credential id so drift cannot change the family.
    fn opencode_auth_for_episode(
        fresh_episode: bool,
        frozen_credential_id: Option<&str>,
        discovered_method: Option<AuthMethod>,
        pinned_method: Option<AuthMethod>,
        methods: &broca_module_serve::MethodConfig,
        provider_id: &str,
    ) -> OpencodeEpisodeAuth {
        if fresh_episode {
            let method = pinned_method
                .or_else(|| methods.method_for(provider_id))
                .or(discovered_method);
            let selection = method.map(|method| broca_core::AuthSelection {
                credential_id: broca_module_serve::CredentialId::new(method, provider_id, None)
                    .to_id_string(),
                // OpenCode auth entries carry no vault account id.
                account_ref: None,
                resolved_credential_id: None,
                // OpenCode auth never asks the vault for anything.
                vault_request: None,
                // Nor routing: the OpenCode file has no accounts to choose between.
                account_decision: None,
            });
            OpencodeEpisodeAuth { selection, method }
        } else {
            let selection = frozen_credential_id.map(|id| broca_core::AuthSelection {
                credential_id: id.to_string(),
                // A resumed OpenCode run keeps no vault account id.
                account_ref: None,
                resolved_credential_id: None,
                vault_request: None,
                account_decision: None,
            });
            let method = frozen_credential_id
                .and_then(broca_module_serve::CredentialId::parse)
                .map(|credential| credential.method);
            OpencodeEpisodeAuth { selection, method }
        }
    }

    /// OpenCode auth entries expose only an access token/API key and their generic type. Unlike
    /// the vault payload, they do not provide Antigravity's required cloud project, so selecting
    /// that body grammar must fail before admission rather than silently falling back to Gemini.
    fn validate_opencode_method_metadata(
        provider_id: &str,
        method: Option<AuthMethod>,
    ) -> Result<(), RunError> {
        if method == Some(AuthMethod::Antigravity) {
            return Err(RunError::Config(format!(
                "auth method 'antigravity' for provider '{provider_id}' requires a project_id, \
                 but BROCA_AUTH_SOURCE=opencode provides no project metadata"
            )));
        }
        Ok(())
    }

    /// The full auth resolution for a run: how to authenticate, whether to pause for re-auth,
    /// what to freeze, and (for the consistency guard) which method was chosen. NO `Debug`
    /// derive — `mode` can carry a secret token.
    pub struct AuthResolution {
        /// How the provider authenticates (the shaped credential, or none/opencode).
        pub mode: AuthMode,
        /// Set when the credential needs re-auth: admit then pause (no provider call).
        pub pause: Option<ProviderError>,
        /// The credential id to FREEZE into a fresh run's RunConfig. `None` on the mock path;
        /// OpenCode resolves its configured-or-discovered method before provider construction.
        /// On a resume this is the already-frozen value (re-frozen is a no-op — the loop only
        /// writes it on a fresh RunStarted).
        pub selection: Option<broca_core::AuthSelection>,
        /// The resolved method used by provider-family selection and the consistency guard.
        /// OpenCode resolves it before provider construction from config/auth-file metadata on a
        /// fresh run or from the frozen credential id on resume.
        pub method: Option<AuthMethod>,
        /// The cloud project the credential carries — `Some` only for an Antigravity
        /// credential (a body render input the google-codeassist family freezes). `None`
        /// otherwise. Off the secret path (it's a project name, not a token).
        pub project_id: Option<String>,
        /// The provider-side account the credential belongs to — `Some` only for a ChatGPT
        /// credential (the vault parses it from the token at refresh time, so it tracks
        /// re-logins). The authoritative `ChatGPT-Account-Id` header source; the catalog's
        /// local JWT decode is the fallback. Off the secret path (an account id, not a token).
        pub account_id: Option<String>,
        /// The record_version of the vault credential THIS run was served (from
        /// `GetResult.record_version`); `None` off the vault path or when no token was
        /// served. Echoed on a wire-auth-failure report so the vault CAS-invalidates only
        /// the version the provider actually rejected — a stale report cannot kill an
        /// already-refreshed credential. Off the secret path (a counter, not a token).
        pub record_version: Option<u64>,
        /// How this run gets a fresh token for its credential after a mid-run 401. Present when
        /// the vault served a token; `None` keeps a 401 pausing the run straight away.
        pub in_run_refresh: Option<InRunRefresh>,
    }

    /// A run's in-step token refresh, plus the record_version of the token the run is using
    /// NOW. That version starts as the admission-time one and moves when a refresh serves a new
    /// token, so the terminal wire-auth report names the token the provider last rejected.
    pub struct InRunRefresh {
        pub refresher: SharedSecretRefresher,
        pub served_version: Arc<AtomicU64>,
        /// When the token the run is using NOW expires (`None` for a token that never does,
        /// such as an API key). Moves together with `served_version`, so the pre-send check
        /// always judges the token the next request would carry. Only the refresher reads it
        /// in production; it is exposed here, like `served_version`, so tests can see it move.
        #[cfg_attr(not(test), allow(dead_code))]
        pub served_expires_at_ms: Arc<std::sync::Mutex<Option<i64>>>,
        /// The record_version the in-step refresh already reported to the vault, if it ran.
        /// The run-end report skips that version, so one rejected token is reported once.
        pub reported_version: Arc<std::sync::Mutex<Option<u64>>>,
    }

    /// Gets a fresh token for the ONE credential a run was admitted with, after the provider
    /// rejected the token with a 401. Never reads another credential.
    ///
    /// A scoped read cannot force a refresh (`credential.get_scoped` has no `force_refresh`, on
    /// purpose: it would let any grant holder spend an upstream mint on demand). The vault's
    /// route to a fresh token is REPORT, THEN RE-READ: the failure report marks the served
    /// version stale if it is still current, and the next scoped read of a stale record
    /// refreshes it and comes back at a higher record_version. If another reader already
    /// refreshed, the report names a superseded version and changes nothing, and the re-read
    /// simply returns the newer token; concurrent readers share one refresh.
    ///
    /// The report sent here IS the refresh trigger, so it is the only report for the rejected
    /// version: it is recorded in `reported_version` and the run-end report skips it.
    ///
    /// It also replaces a token that is about to expire before each attempt, with a plain
    /// re-read and no report (see `refresh_expiring_secret`).
    struct VaultTokenRefresher {
        vault: Arc<VaultAuth>,
        credential_key: String,
        vault_id: String,
        served_version: Arc<AtomicU64>,
        served_expires_at_ms: Arc<std::sync::Mutex<Option<i64>>>,
        reported_version: Arc<std::sync::Mutex<Option<u64>>>,
        /// Set by the first failed pre-send re-read, so a vault outage logs one warning per
        /// run rather than one per attempt.
        presend_failure_logged: std::sync::atomic::AtomicBool,
        no_expiry_logged: std::sync::atomic::AtomicBool,
    }

    /// How close to its expiry a held token may get before the next attempt replaces it.
    ///
    /// The request and its whole stream must fit inside the token's remaining life. Providers
    /// (Anthropic and Google, measured) check the token when a request arrives, not while it
    /// streams, so a token that expires mid-stream fails only the NEXT request. Five minutes
    /// covers any single request comfortably. It is half the lifetime demanded on every read,
    /// so a token the vault just served is never immediately judged to be expiring.
    const PRESEND_REFRESH_MARGIN_MS: i64 = VAULT_MIN_TTL_MS / 2;

    /// Whether a token expiring at `expires_at_ms` should be replaced before a send at
    /// `now_ms`. A token without an expiry (an API key) never is.
    fn held_token_is_expiring(expires_at_ms: Option<i64>, now_ms: i64) -> bool {
        expires_at_ms.is_some_and(|expires_at_ms| {
            now_ms.saturating_add(PRESEND_REFRESH_MARGIN_MS) > expires_at_ms
        })
    }

    impl VaultTokenRefresher {
        fn served_expiry(&self) -> Option<i64> {
            *self
                .served_expires_at_ms
                .lock()
                .unwrap_or_else(|poison| poison.into_inner())
        }

        fn set_served_expiry(&self, expires_at_ms: Option<i64>) {
            *self
                .served_expires_at_ms
                .lock()
                .unwrap_or_else(|poison| poison.into_inner()) = expires_at_ms;
        }

        /// Accounts for a failed vault read in the cache and the health gauge, the same way for
        /// both refresh paths.
        fn note_read_failure(&self, error: &VaultError) {
            let error_class = lkg_error_class(error);
            evict_if_vault_authoritative(&self.vault.cache, &self.credential_key, error_class);
            if error_class == LkgErrorClass::Connectivity {
                self.vault.gauge.stamp_error();
            } else {
                self.vault.gauge.stamp_ok();
            }
        }
    }

    impl SecretRefresher for VaultTokenRefresher {
        fn refresh_rejected_secret(&self) -> RefreshFuture<'_, RefreshedSecret> {
            Box::pin(async move {
                let rejected = self.served_version.load(Ordering::SeqCst);
                // The provider's 401 is authoritative for the version it rejected: never let
                // this process's cache serve it again, whatever the vault answers next.
                evict_for_provider_rejection(
                    &self.vault.cache,
                    &self.credential_key,
                    401,
                    rejected,
                );
                // Recorded before sending, so the run-end report is skipped even when this one
                // is lost in transit: the rule is at most one report per rejected version.
                *self
                    .reported_version
                    .lock()
                    .unwrap_or_else(|poison| poison.into_inner()) = Some(rejected);
                if let Err(error) = self
                    .vault
                    .client
                    .report_auth_failure_at(
                        broca_subc::VaultAddress::CredentialId(&self.vault_id),
                        401,
                        rejected,
                    )
                    .await
                {
                    // The re-read below still runs: a newer token may already exist.
                    tracing::warn!(
                        target: "credentials",
                        "in-run 401 report for '{}' v{rejected} failed: {error}",
                        self.vault_id
                    );
                }
                let min_ttl_ms = Some(VAULT_MIN_TTL_MS);
                let got = self
                    .vault
                    .client
                    .get_scoped(&self.vault_id, min_ttl_ms)
                    .await;
                match got.result {
                    Ok(token) => {
                        self.vault.gauge.stamp_ok();
                        // RE-SEND ONLY FOR A NEWER TOKEN. An unchanged version means the vault
                        // did not refresh: the token broca holds is the current one, so the 401
                        // is not staleness and sending the same token again cannot succeed.
                        // The run pauses as it did before in-step refresh existed.
                        if token.record_version <= rejected {
                            tracing::warn!(target: "credentials", "credential reread outcome=same_version vault_id='{}' rejected_version={rejected} served_version={} expires_at_ms={}", self.vault_id, token.record_version, token.expires_at_ms.map_or_else(|| "none".to_string(), |value| value.to_string()));
                            return None;
                        }
                        tracing::warn!(target: "credentials", "credential reread outcome=refreshed vault_id='{}' old_version={rejected} new_version={}", self.vault_id, token.record_version);
                        self.vault
                            .cache
                            .insert(self.credential_key.clone(), token.clone());
                        self.served_version
                            .store(token.record_version, Ordering::SeqCst);
                        self.set_served_expiry(token.expires_at_ms);
                        Some(RefreshedSecret {
                            evidence: broca_protocol::CredentialRefresh {
                                vault_op: broca_core::VaultReadOp::GetScoped.as_str().to_string(),
                                min_ttl_ms,
                                // A scoped read carries no force flag; the refresh came from
                                // the failure report marking the rejected version stale.
                                force_refresh: false,
                                record_version: Some(token.record_version),
                                reason: Some(
                                    broca_protocol::CredentialRefresh::REASON_REJECTED.into(),
                                ),
                            },
                            account_ref: token.account_id,
                            secret: token.token,
                        })
                    }
                    // needs_reauth, a refusal, or no reply: no fresh token, so the 401 stands and
                    // the run pauses exactly as it did before in-step refresh existed.
                    Err(error) => {
                        self.note_read_failure(&error);
                        tracing::warn!(target: "credentials", "credential reread outcome=reread_failed vault_id='{}' rejected_version={rejected} error_class={:?} vault_replied={}", self.vault_id, lkg_error_class(&error), got.vault_replied);
                        None
                    }
                }
            })
        }

        /// A plain scoped re-read when the held token is close to expiry. The vault refreshes a
        /// record that cannot meet `min_ttl_ms`, so the read returns a token with at least that
        /// much life left. NO failure report is sent: the token was not rejected, and a report
        /// would make the vault count an ordinary expiry as a provider rejection.
        fn refresh_expiring_secret(&self) -> RefreshFuture<'_, RefreshedSecret> {
            Box::pin(async move {
                let expires_at_ms = self.served_expiry();
                if expires_at_ms.is_none() {
                    if !self
                        .no_expiry_logged
                        .swap(true, std::sync::atomic::Ordering::SeqCst)
                    {
                        tracing::info!(target: "credentials", "held credential for '{}' has no expiry; pre-send refresh cannot fire", self.vault_id);
                    }
                    return None;
                }
                if !held_token_is_expiring(expires_at_ms, epoch_millis()) {
                    return None;
                }
                let held = self.served_version.load(Ordering::SeqCst);
                let min_ttl_ms = Some(VAULT_MIN_TTL_MS);
                let got = self
                    .vault
                    .client
                    .get_scoped(&self.vault_id, min_ttl_ms)
                    .await;
                match got.result {
                    Ok(token) => {
                        self.vault.gauge.stamp_ok();
                        if token.record_version < held {
                            // Older than what this run already holds: never step backwards.
                            return None;
                        }
                        self.vault
                            .cache
                            .insert(self.credential_key.clone(), token.clone());
                        self.set_served_expiry(token.expires_at_ms);
                        // The same version is the token already held. Its expiry is taken from
                        // the vault's answer above, so the check is not repeated on stale data,
                        // but there is nothing new to send.
                        if token.record_version == held {
                            tracing::info!(target: "credentials", "credential reread outcome=same_version vault_id='{}' served_version={} expires_at_ms={}", self.vault_id, token.record_version, token.expires_at_ms.map_or_else(|| "none".to_string(), |value| value.to_string()));
                            return None;
                        }
                        self.served_version
                            .store(token.record_version, Ordering::SeqCst);
                        Some(RefreshedSecret {
                            evidence: broca_protocol::CredentialRefresh {
                                vault_op: broca_core::VaultReadOp::GetScoped.as_str().to_string(),
                                min_ttl_ms,
                                force_refresh: false,
                                record_version: Some(token.record_version),
                                reason: Some(
                                    broca_protocol::CredentialRefresh::REASON_EXPIRING.into(),
                                ),
                            },
                            account_ref: token.account_id,
                            secret: token.token,
                        })
                    }
                    // The held token may still be valid, and if it is not, the provider's 401
                    // still reaches the rejection path. So the attempt goes out as it is.
                    Err(error) => {
                        self.note_read_failure(&error);
                        if !self
                            .presend_failure_logged
                            .swap(true, std::sync::atomic::Ordering::SeqCst)
                        {
                            tracing::warn!(
                                target: "credentials",
                                "pre-send refresh of expiring token for '{}' v{held} failed; \
                                 sending with the held token: {error}",
                                self.vault_id
                            );
                        }
                        None
                    }
                }
            })
        }
    }

    fn resolve_provider_for_episode(
        model_id: &str,
        provider_base_url: Option<&str>,
        max_output_tokens: Option<u32>,
        resolution: &AuthResolution,
        options: &broca_catalog::live::ResolveOptions<'_>,
        client: ProviderHttpClient,
        render: broca_catalog::live::EpisodeRenderConfig<'_>,
    ) -> Result<broca_catalog::live::ProviderResolution, broca_catalog::live::ResolutionError> {
        if resolution.pause.is_some()
            && options.family_override == Some(WireFamilyId::OpenAiResponsesChatgpt)
        {
            broca_catalog::live::resolve_chatgpt_pause_episode_with_client(
                model_id,
                max_output_tokens,
                options,
                client,
                render,
            )
        } else if let Some(base_url) = provider_base_url.filter(|url| !url.is_empty()) {
            broca_catalog::live::resolve_catalog_episode_at_base_url(
                model_id,
                base_url,
                max_output_tokens,
                &resolution.mode,
                options,
                client,
                render,
            )
        } else {
            broca_catalog::live::resolve_catalog_episode_with_client(
                model_id,
                max_output_tokens,
                &resolution.mode,
                options,
                client,
                render,
            )
        }
    }

    /// What the executor does about auth for THIS run, decided from the credential source's
    /// result before the loop. Kept as a typed enum so the mapping is exhaustive + unit-
    /// tested in isolation from the live vault connection. Deliberately NO `Debug` derive —
    /// `Proceed` can carry a secret token, which must never reach a log line.
    pub enum AuthDecision {
        /// Resolve the provider with this auth mode and run normally.
        Proceed(AuthMode),
        /// The credential needs re-auth (authoritative, before any provider call): admit the
        /// run then pause it for re-auth (carrying this error), making NO provider call. The
        /// run is resumable under the same id once the user re-auths.
        PauseForReauth(ProviderError),
        /// Fail admission permanently (no RunStarted, no ghost run) because retrying cannot
        /// repair the credential configuration.
        FailAdmission(String),
        /// Fail admission transiently (no RunStarted, no ghost run). There is no durable run to
        /// pause yet, so the caller retries through its normal transient-error policy.
        FailAdmissionTransient(String),
    }

    /// Map a credential.get result into the run's auth decision. ONLY `needs_reauth` is a
    /// resumable pause (the refresh token is dead → the user must re-auth, and the run waits
    /// for them). Connectivity and lock failures reject admission as transient because no run
    /// exists to pause yet; malformed/missing credentials remain permanent configuration faults.
    pub fn vault_outcome_to_decision(result: Result<VaultToken, VaultError>) -> AuthDecision {
        match result {
            Ok(token) => AuthDecision::Proceed(AuthMode::Secret(token.token)),
            Err(VaultError::NeedsReauth) => AuthDecision::PauseForReauth(ProviderError {
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::Auth,
                message: "credential needs re-authentication (vault: refresh token expired \
                          or revoked)"
                    .to_string(),
                status: None,
                retry_after_secs: None,
                provider_code: Some("needs_reauth".to_string()),
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
            }),
            Err(e @ VaultError::Transport(_)) => AuthDecision::FailAdmissionTransient(format!(
                "credentials service unreachable (transient — retry): {e}"
            )),
            Err(VaultError::VaultLocked) => AuthDecision::FailAdmissionTransient(
                "credentials service locked (transient — retry)".to_string(),
            ),
            Err(e @ VaultError::RefreshFailed) => AuthDecision::FailAdmissionTransient(format!(
                "credential refresh failed (transient — retry): {e}"
            )),
            Err(e) => {
                AuthDecision::FailAdmission(format!("credential not configured or unusable: {e}"))
            }
        }
    }

    /// A minimum token lifetime to demand from the vault on `credential.get`, so a token
    /// stays valid for the whole run (the vault refreshes if it would expire sooner). A run
    /// is a single turn (model steps + tool calls) — minutes at most — so a generous budget
    /// avoids a mid-run expiry without forcing needless refreshes.
    const VAULT_MIN_TTL_MS: i64 = 10 * 60 * 1000;

    /// Error classes relevant to deciding whether a cached credential may stand in for the
    /// vault. Authoritative credential replies never lose to an older cached token.
    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum LkgErrorClass {
        Connectivity,
        /// The vault ANSWERED, and the answer makes this credential unusable.
        ///
        /// Renamed from `NeedsReauth`, which named ONE INSTANCE of the class and invited the
        /// mapping to carry only that instance -- which is exactly what happened. `NotFound` fell
        /// through to `Other`, so a revoked handle left its cached token in place, and a LATER
        /// VAULT OUTAGE served it.
        ///
        /// The incoherence is what makes that a defect rather than a liberal cache: the same
        /// credential was REFUSED while the vault was reachable and ACCEPTED once it was not.
        CredentialUnusable,
        Other,
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum LkgDecision {
        Serve,
        Refuse,
    }

    /// The minimum information needed to decide cache eligibility without touching token bytes.
    struct CachedCredentialMeta {
        expires_at_ms: Option<i64>,
    }

    /// Decide whether an in-memory token remains safe to serve during a vault outage. Tokens
    /// without an expiry are API keys and remain eligible; expiring tokens must clear the same
    /// minimum-TTL margin requested from the normal vault path.
    fn lkg_decision(
        cached: Option<&CachedCredentialMeta>,
        now_ms: i64,
        min_ttl_ms: i64,
        error_class: LkgErrorClass,
    ) -> LkgDecision {
        if error_class != LkgErrorClass::Connectivity {
            return LkgDecision::Refuse;
        }
        let Some(cached) = cached else {
            return LkgDecision::Refuse;
        };
        match cached.expires_at_ms {
            None => LkgDecision::Serve,
            Some(expires_at_ms) => {
                if now_ms.saturating_add(min_ttl_ms) <= expires_at_ms {
                    LkgDecision::Serve
                } else {
                    LkgDecision::Refuse
                }
            }
        }
    }

    /// Process-only token cache. It deliberately has no `Debug` implementation and is never
    /// persisted or logged; entries are cloned only into a run's existing secret auth path.
    #[derive(Default)]
    pub struct VaultLkgCache {
        entries: Mutex<HashMap<String, VaultToken>>,
    }

    impl VaultLkgCache {
        fn insert(&self, credential_id: String, token: VaultToken) {
            let mut entries = self.entries.lock().unwrap();
            // Reads and refreshes from different runs can finish out of order. Compare
            // while holding the insertion lock so a late reply cannot roll back the cache.
            if entries
                .get(&credential_id)
                .is_some_and(|current| current.record_version > token.record_version)
            {
                return;
            }
            entries.insert(credential_id, token);
        }

        fn get(&self, credential_id: &str) -> Option<VaultToken> {
            self.entries.lock().unwrap().get(credential_id).cloned()
        }

        fn evict(&self, credential_id: &str) {
            self.entries.lock().unwrap().remove(credential_id);
        }

        fn evict_version(&self, credential_id: &str, record_version: u64) {
            // A completed run can report after another run refreshes this entry, so the
            // version check and removal must happen while holding the same lock.
            let mut entries = self.entries.lock().unwrap();
            if entries
                .get(credential_id)
                .is_some_and(|token| token.record_version == record_version)
            {
                entries.remove(credential_id);
            }
        }
    }

    fn evict_if_vault_authoritative(
        cache: &VaultLkgCache,
        credential_id: &str,
        error_class: LkgErrorClass,
    ) {
        if error_class == LkgErrorClass::CredentialUnusable {
            cache.evict(credential_id);
        }
    }

    fn evict_for_provider_rejection(
        cache: &VaultLkgCache,
        credential_id: &str,
        status: u16,
        record_version: u64,
    ) {
        if matches!(status, 401 | 403) {
            cache.evict_version(credential_id, record_version);
        }
    }

    fn epoch_millis() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis() as i64)
            .unwrap_or(0)
    }

    fn lkg_error_class(error: &VaultError) -> LkgErrorClass {
        match error {
            VaultError::Transport(_) | VaultError::Protocol(_) | VaultError::VaultLocked => {
                LkgErrorClass::Connectivity
            }
            // AUTHORITATIVE NEGATIVES: the vault replied, and the reply says this credential
            // cannot be used -- the grant needs re-auth, or the handle does not exist at all.
            // Both evict, because the doc on LkgErrorClass promises an authoritative reply never
            // loses to an older cached token.
            //
            // A scoped NotFound is byte-identical for "no grant", "credential absent" and "the
            // vault's store failed during the grant lookup" (claustrum pins the last equal to the
            // first, so the reply can't be used to probe which credentials exist). Evicting on it
            // is still right, per claustrum's owner: revoking a grant or removing a credential
            // means "stop using it now", and a cache that served until expiry would turn that into
            // "within the hour", or never for static keys with no expiry. The store-fault case
            // needs the vault's own SQLite to be failing, when the next fetch would fail anyway;
            // none was recorded in auth_events from 2026-08-11 to 2026-09-30.
            VaultError::NeedsReauth | VaultError::NotFound => LkgErrorClass::CredentialUnusable,
            // EVERYTHING ELSE STAYS `Other`, AND THE RESTRAINT IS DELIBERATE. These are refusals
            // about the REQUEST or the VAULT, not verdicts on the credential, so evicting on them
            // would discard a token that is still good:
            //   RefreshFailed       transient by its own definition
            //   TtlUnsatisfiable    about the min_ttl_ms THIS CODE chose, not the credential
            //   TooManyItems        a request-shape refusal
            //   RefreshUnsupported  a cached non-refreshable token is valid until it expires
            //   Corrupt             the record is unreadable NOW; the token already minted from
            //                       it may still work, and eviction removes the only copy
            _ => LkgErrorClass::Other,
        }
    }

    /// The request to freeze on the run, decided from the vault's RAW answer -- before the
    /// cache fallback in `resolve_vault_outcome` can turn a failure into a served token.
    ///
    /// PRESENT IFF THE VAULT'S REPLY WAS RECEIVED. `vault_request` exists so a vault's own
    /// record of a credential.get can be joined against the run that made it, so it must mean
    /// "the vault received this request and answered it". Every reply qualifies, including an
    /// authoritative refusal (NotFound, NeedsReauth), a locked vault, or a reply broca could
    /// not decode: in each of those the vault did get the request.
    ///
    /// Everything else is recorded as None: a route open the daemon refused or that failed, a
    /// daemon rejection of the request before delivery, a transport failure, a bad frame on
    /// the connection. In those the vault either never saw the request or broca cannot tell,
    /// so it claims nothing. Recording the request there was wrong: when the vault was
    /// unreachable and a cached token was served, the export fact claimed a request the vault
    /// never saw, and the join found a broca row with no vault row behind it.
    ///
    /// Read from `VaultGet::vault_replied`, which the vault client sets from whether a reply
    /// body arrived, NEVER from the error variant: a daemon refusal and an undecodable vault
    /// reply are both `VaultError::Protocol`, so the variant cannot say who answered.
    fn recorded_vault_request(
        outcome: &VaultGet,
        request: broca_core::VaultRequest,
    ) -> Option<broca_core::VaultRequest> {
        outcome.vault_replied.then_some(request)
    }

    fn resolve_vault_outcome(
        cache: &VaultLkgCache,
        gauge: &broca_module_serve::serve::VaultGauge,
        credential_id: &str,
        outcome: Result<VaultToken, VaultError>,
        now_ms: i64,
        min_ttl_ms: i64,
    ) -> Result<VaultToken, VaultError> {
        match outcome {
            Ok(token) => {
                gauge.stamp_ok();
                cache.insert(credential_id.to_string(), token.clone());
                Ok(token)
            }
            // Refused in this process before anything was sent: it says nothing about the vault,
            // so the gauge is left alone, and nothing about the credential, so the cache is too.
            Err(error @ VaultError::NoConsumerIdentity { .. }) => Err(error),
            Err(error) => {
                let error_class = lkg_error_class(&error);
                // Authoritative re-auth state must invalidate older process memory.
                evict_if_vault_authoritative(cache, credential_id, error_class);
                if error_class != LkgErrorClass::Connectivity {
                    gauge.stamp_ok();
                    return Err(error);
                }

                let cached = cache.get(credential_id);
                let cached_meta = cached.as_ref().map(|token| CachedCredentialMeta {
                    expires_at_ms: token.expires_at_ms,
                });
                if lkg_decision(cached_meta.as_ref(), now_ms, min_ttl_ms, error_class)
                    == LkgDecision::Serve
                {
                    gauge.stamp_serving_cached();
                    Ok(cached.expect("Serve requires a cached credential"))
                } else {
                    gauge.stamp_error();
                    Err(error)
                }
            }
        }
    }

    fn validate_routed_account(
        token: &VaultToken,
        routed_vault_id: &str,
        routed_account: &str,
    ) -> Result<(), RunError> {
        if token
            .account_id
            .as_deref()
            .is_some_and(|served| served != routed_account)
        {
            return Err(RunError::Config(format!(
                "account_identity_mismatch: routing chose account {routed_account} \
                 for credential {routed_vault_id}, but the vault served account {}",
                token.account_id.as_deref().unwrap_or_default()
            )));
        }
        Ok(())
    }

    /// The credential the run actually USED, paired with the record_version of the token it
    /// was served — the identity a post-run wire-auth-failure report needs. `None` when the
    /// run never got a vault token: the mock path, a frozen OpenCode selection, a pre-loop
    /// `needs_reauth` pause (the vault already knows; no provider call was made), or a
    /// version-less resolution (nothing served, nothing to CAS against). Requiring BOTH pieces
    /// structurally prevents a report that could invalidate a version the run never saw.
    fn used_credential_for_report(
        pause: &Option<ProviderError>,
        selection: Option<broca_core::AuthSelection>,
        record_version: Option<u64>,
    ) -> Option<(String, u64)> {
        if pause.is_some() {
            return None;
        }
        match (selection, record_version) {
            (Some(s), Some(version)) => Some((s.credential_id, version)),
            _ => None,
        }
    }

    fn should_check_wire_auth(outcome: &Result<RunSummary, RunError>) -> bool {
        matches!(
            outcome,
            Ok(summary)
                if summary.paused == Some(PauseReason::AuthRequired)
                    || summary.reason == RunFinishReason::Error
        )
    }

    async fn post_run_wire_auth_status(
        outcome: &Result<RunSummary, RunError>,
        wal_store: Arc<dyn WalStore>,
        session: BindIdentity,
    ) -> Option<u16> {
        // A provider 401/403 reaches the loop only as an AuthRequired pause or a wire-error
        // terminal. Completed, limited, interrupted, and infrastructure-error outcomes cannot
        // carry a rejected-credential status, so do not replay their entire lineage WAL.
        if !should_check_wire_auth(outcome) {
            return None;
        }
        terminal_wire_auth_status(wal_store, session).await
    }

    /// The HTTP status of a run's terminal AUTH error, but ONLY for an Auth-class 401 or 403 —
    /// the statuses that can mean a credential the provider rejected (so the vault should
    /// invalidate it). Reads the session's last episode terminal from the durable WAL: an
    /// Auth-class failure pauses the run, so the status lives on the terminal
    /// `RunPaused.error.status` (or `RunFinished.error.status` if a future path finalizes instead
    /// of pausing). Returns `None` for any other terminal, including a Permanent 403, so the
    /// caller reports ONLY a genuine wire auth rejection, never a forbidden or transient error.
    async fn terminal_wire_auth_status(
        wal_store: Arc<dyn WalStore>,
        session: BindIdentity,
    ) -> Option<u16> {
        let records =
            broca_diagnostics::spawn_blocking(broca_diagnostics::JobKind::WalRead, move || {
                wal_store.read_records(&session)
            })
            .await
            .ok()?
            .ok()?;
        latest_terminal_error(&records).and_then(wire_auth_status)
    }

    /// Return the newest pause/finish record's error without crossing that terminal boundary.
    /// An error-free terminal deliberately produces `None`, even when an older terminal failed.
    fn latest_terminal_error(records: &[broca_wal::FramedRecord]) -> Option<&ProviderError> {
        records
            .iter()
            .rev()
            .find_map(|frame| match &frame.record {
                broca_wal::WalRecord::RunPaused { error, .. }
                | broca_wal::WalRecord::RunFinished { error, .. } => Some(error.as_ref()),
                _ => None,
            })
            .flatten()
    }

    /// The reportable wire-auth status of a terminal error: an Auth-class `401` or `403` only.
    /// A Permanent 403 is an authorization denial, not a dead credential, and must not be
    /// reported to the vault. Pure, so the class and status filter is unit-tested without WAL
    /// plumbing.
    fn wire_auth_status(err: &ProviderError) -> Option<u16> {
        if err.class != ErrorClass::Auth {
            return None;
        }
        match err.status {
            Some(s @ (401 | 403)) => Some(s),
            _ => None,
        }
    }

    struct PriorRunView {
        run_id: broca_protocol::RunId,
        config: broca_wal::RunConfig,
        idle: bool,
    }

    fn prior_run_view_from_checkpoint(
        checkpoint: &broca_core::AdmissionCheckpointV1,
    ) -> Option<PriorRunView> {
        checkpoint
            .last_episode
            .as_ref()
            .map(|episode| PriorRunView {
                run_id: episode.run_id.clone(),
                config: episode.config.clone(),
                idle: episode.disposition == broca_core::EpisodeDisposition::Finished,
            })
    }

    fn frozen_render_config(
        config: &broca_wal::RunConfig,
    ) -> Option<broca_catalog::live::FrozenRenderConfig> {
        serde_json::from_value(config.frozen_render_config.clone()).ok()
    }

    fn frozen_cache_policy(config: &broca_wal::RunConfig) -> Option<CachePolicy> {
        frozen_render_config(config).and_then(|frozen| frozen.cache)
    }

    fn frozen_prompt_cache_key(config: &broca_wal::RunConfig) -> Option<String> {
        frozen_render_config(config).and_then(|frozen| frozen.prompt_cache_key)
    }

    /// The cache policy and caller cache key an episode takes as frozen instead of resolving its
    /// own. `Some` for a new episode (not a resume) whose request has `inherit_frozen_cache` set,
    /// which a steer or interrupt that omitted `cache` gets: the values come from `last`, the
    /// config of the session's latest episode. `None` otherwise, including when the session has
    /// no episode yet.
    fn inherited_cache(
        req: &broca_core::RunRequest,
        fresh_episode: bool,
        last: Option<&broca_wal::RunConfig>,
    ) -> Option<(Option<CachePolicy>, Option<String>)> {
        if !(req.inherit_frozen_cache && fresh_episode) {
            return None;
        }
        let config = last?;
        Some((frozen_cache_policy(config), frozen_prompt_cache_key(config)))
    }

    /// The output cap the episode froze at start. Read straight off the blob, like the recorded
    /// host, so a blob the typed config cannot parse still restores the number it carries.
    fn frozen_max_output_tokens(config: &broca_wal::RunConfig) -> Option<u32> {
        config
            .frozen_render_config
            .get("max_output_tokens")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| u32::try_from(value).ok())
    }

    /// The reasoning variant the episode froze at start, re-applied as recorded on resume. Read
    /// off the blob like the output cap, so a resume never re-derives it from the catalog.
    fn frozen_variant(config: &broca_wal::RunConfig) -> Option<broca_catalog::live::FrozenVariant> {
        config
            .frozen_render_config
            .get("variant")
            .and_then(|variant| serde_json::from_value(variant.clone()).ok())
    }

    /// The Anthropic thinking-echo scope the episode froze at start, re-applied on resume. A blob
    /// recorded before the scope existed has no `echo_scope` key and reads back as the legacy
    /// last-assistant-message scope, which is exactly what that episode rendered with.
    fn frozen_thinking_echo_scope(
        config: &broca_wal::RunConfig,
    ) -> Option<broca_catalog::live::ThinkingEchoScope> {
        let reasoning = config.frozen_render_config.get("reasoning")?;
        match serde_json::from_value(reasoning.clone()).ok()? {
            broca_provider::policy::ReasoningPolicy::AnthropicInterleaved {
                echo_scope, ..
            } => Some(echo_scope),
            _ => None,
        }
    }

    /// The Anthropic cross-model thinking rule the episode froze at start, re-applied on resume.
    /// A blob recorded before the rule existed has no `cross_model` key and reads back as the
    /// legacy same-model-only rule, which is exactly what that episode rendered with.
    fn frozen_thinking_cross_model(
        config: &broca_wal::RunConfig,
    ) -> Option<broca_catalog::live::CrossModelThinking> {
        let reasoning = config.frozen_render_config.get("reasoning")?;
        match serde_json::from_value(reasoning.clone()).ok()? {
            broca_provider::policy::ReasoningPolicy::AnthropicInterleaved {
                cross_model, ..
            } => Some(cross_model),
            _ => None,
        }
    }

    /// The Claude Code persona the episode froze at start, re-applied on resume. A blob recorded
    /// before the persona existed has no `claude_code_persona` key and reads back as `None`,
    /// the shape that episode rendered with.
    fn frozen_claude_code_persona(
        config: &broca_wal::RunConfig,
    ) -> Option<broca_catalog::live::ClaudeCodePersona> {
        config
            .frozen_render_config
            .get("claude_code_persona")
            .and_then(|persona| serde_json::from_value(persona.clone()).ok())
    }

    fn frozen_responses_tools_non_strict(config: &broca_wal::RunConfig) -> bool {
        config
            .frozen_render_config
            .get("responses_tools_non_strict")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    }

    /// Whether the episode froze the DeepSeek reasoning echo. A config recorded before DeepSeek
    /// was opted in has no field and reads as false, so its resume keeps the bytes it was sending.
    fn frozen_reasoning_content_required(config: &broca_wal::RunConfig) -> bool {
        config
            .frozen_render_config
            .get("reasoning_content_required")
            .and_then(serde_json::Value::as_bool)
            .unwrap_or(false)
    }

    fn frozen_reasoning_echo(
        config: &broca_wal::RunConfig,
    ) -> Option<broca_provider::policy::ReasoningEchoPolicy> {
        config
            .frozen_render_config
            .get("reasoning_echo")
            .and_then(|v| serde_json::from_value(v.clone()).ok())
    }

    /// The service tier the episode was admitted with, re-applied on resume from the durable
    /// `RunConfig` rather than the resuming send, and without re-checking the catalog's rates.
    fn frozen_service_tier(
        config: &broca_wal::RunConfig,
    ) -> Option<broca_catalog::live::ServiceTier> {
        config.service_tier
    }

    fn context_limit_for_episode(
        fresh_episode: bool,
        caller_context_limit: Option<u64>,
        catalog_context_limit: Option<u64>,
        frozen_context_limit: Option<u64>,
    ) -> Option<u64> {
        if fresh_episode {
            caller_context_limit.or(catalog_context_limit)
        } else {
            frozen_context_limit
        }
    }

    /// The opened credential-vault consumer plus how each credential is addressed. Present only
    /// when the auth source is the vault AND the connection opened at startup; absent for the
    /// OpenCode default. Process-global (credentials are not session-keyed), so one client
    /// serves every session and provider.
    pub struct VaultAuth {
        pub client: broca_subc::VaultClient,
        /// Per credential key: the vault credential id to read by grant.
        pub addressing: broca_module_serve::VaultAddressing,
        /// The per-provider default auth method (config-home). Resolves a run's
        /// `(provider[, pinned method])` → the credential id whose handle to look up. A
        /// provider absent here uses [`FALLBACK_METHOD`] (api-key, the long tail).
        pub methods: broca_module_serve::MethodConfig,
        /// The health gauge this executor stamps from vault-call outcomes, so the
        /// `health.check` reply reports live vault reachability without probing anything.
        pub gauge: std::sync::Arc<broca_module_serve::serve::VaultGauge>,
        /// Process-only last-known-good credentials keyed by canonical credential id.
        pub cache: VaultLkgCache,
        /// The latest `credential.list_scoped` answer: the candidate accounts broca offers
        /// routing when it asks which account a run should use.
        pub listing: broca_module_serve::account_routing::ListingCache,
    }

    /// List the vault's credentials now and keep the answer as the latest listing.
    pub async fn refresh_listing(vault: &VaultAuth) -> Option<Arc<broca_subc::vault::ScopedList>> {
        match vault.client.list_scoped().await {
            Ok(listing) => Some(vault.listing.store(listing)),
            Err(error) => {
                tracing::warn!(
                    target: "accounts",
                    "credential.list_scoped failed; account candidates keep the previous listing: {error}"
                );
                None
            }
        }
    }

    /// The latest listing. A stale one is returned at once while one refresh runs behind it;
    /// with none cached yet, this lists now.
    async fn latest_listing(vault: Arc<VaultAuth>) -> Option<Arc<broca_subc::vault::ScopedList>> {
        match vault.listing.get() {
            Some((listing, stale)) => {
                if stale && vault.listing.begin_refresh() {
                    tokio::spawn(async move {
                        refresh_listing(&vault).await;
                        vault.listing.end_refresh();
                    });
                }
                Some(listing)
            }
            None => refresh_listing(&vault).await,
        }
    }

    impl broca_module_serve::vault_audit::VaultAuditor for VaultAuth {
        fn audit(
            &self,
        ) -> std::pin::Pin<
            Box<
                dyn std::future::Future<Output = broca_module_serve::wire::VaultAuditResponse>
                    + Send
                    + '_,
            >,
        > {
            Box::pin(async move {
                let referenced = broca_module_serve::vault_audit::referenced_credentials(
                    &self.methods,
                    &self.addressing,
                );
                let listing = self.client.list_scoped().await;
                broca_module_serve::vault_audit::audit_listing(
                    self.client.scoped_principal(),
                    listing,
                    &referenced,
                )
            })
        }
    }

    /// Report a provider's rejection of a served token to the vault, by the same credential id
    /// the read used, under the same grant. A key that does not parse was never read, so there
    /// is nothing to report.
    async fn report_wire_auth_failure(
        vault: &VaultAuth,
        credential_key: &str,
        status: u16,
        record_version: u64,
    ) -> Result<(), VaultError> {
        let Some(key) = broca_module_serve::CredentialId::parse(credential_key) else {
            return Ok(());
        };
        let vault_id = vault.addressing.vault_id_for(&key);
        vault
            .client
            .report_auth_failure_at(
                broca_subc::VaultAddress::CredentialId(&vault_id),
                status,
                record_version,
            )
            .await
    }

    /// Whether the run-end failure report for `record_version` would repeat the report the
    /// in-step refresh already sent for that same version.
    fn run_end_report_is_duplicate(
        reported_version: Option<&std::sync::Mutex<Option<u64>>>,
        record_version: u64,
    ) -> bool {
        reported_version.is_some_and(|reported| {
            *reported.lock().unwrap_or_else(|poison| poison.into_inner()) == Some(record_version)
        })
    }

    /// The auth method for a provider with no configured default — the openai-compatible
    /// long tail (deepseek/cerebras/...) is api-key, so api-key is the safe fallback.
    const FALLBACK_METHOD: broca_module_serve::AuthMethod = broca_module_serve::AuthMethod::ApiKey;

    /// Report that THIS RUN pays a marker write premium it cannot itself read back.
    ///
    /// Markers cost tokens to write and repay on a later request that re-sends the same prefix.
    /// A run declaring no tools makes exactly one provider call, so nothing later in THIS run
    /// can read what it wrote.
    ///
    /// IT DOES NOT FOLLOW THAT THE WRITE IS WASTED, and the earlier wording of this warning said
    /// it was. Anthropic's prompt cache is server-side and keyed on the CONTENT PREFIX -- not on
    /// a session, a run, or a connection -- so a later run re-sending the same leading bytes hits
    /// the cache this run wrote, within its TTL. ALF's spec aggregator is exactly that shape: a
    /// fold header re-sent across rounds, each round a separate tool-less run, every round after
    /// the first reading the first one's write. The old text told them their intended use was
    /// impossible (2026-09-05).
    ///
    /// So the honest scope is one run, and broca cannot see past it: whether a later request
    /// re-sends this prefix is the caller's plan, not a fact available here. Report the premium,
    /// name the condition that repays it, and let the caller decide.
    ///
    /// This only reports. The cache shape is the caller's decision, and refusing the request
    /// here would trade a rare, recoverable waste for a permanent ambiguity about who decides
    /// it on a path where byte-stability matters. Hint policies are exempt -- they carry a
    /// cache key rather than paid breakpoints, so a single-step run costs nothing extra.
    fn single_call_cache_marker_warning(
        policy: &Option<broca_provider::wire::CachePolicy>,
        tools: &[broca_protocol::request::Tool],
    ) -> Option<String> {
        let policy = policy.as_ref()?;
        if !tools.is_empty() {
            return None;
        }
        let broca_provider::wire::CacheClass::Markers { ttl, omit_ttl, .. } = &policy.class else {
            return None;
        };
        // Only the no-request Anthropic default sets `omit_ttl`. That policy is broca's own
        // choice, applied to every such send, so a warning addressed to a caller about markers
        // they "requested" would be both wrong and on nearly every tool-less send.
        if *omit_ttl {
            return None;
        }
        Some(format!(
            "cache markers requested on a run with no tools (ttl={ttl:?}); this run makes \
             one provider call, so it pays the write premium without reading it back -- expected \
             if a later request re-sends the same prefix within the ttl, wasted if not"
        ))
    }

    fn warn_single_call_cache_markers(
        policy: &Option<broca_provider::wire::CachePolicy>,
        tools: &[broca_protocol::request::Tool],
    ) {
        if let Some(message) = single_call_cache_marker_warning(policy, tools) {
            tracing::warn!(target: "cache", "{message}");
        }
    }

    /// Report that the caller's `cache.tail` declaration was resolved away and will not reach
    /// any request.
    ///
    /// `cache.tail` is a statement of fact the caller has and broca does not -- that their final
    /// message differs next request. Two resolutions discard it:
    ///
    ///  - a request carrying only `cache.key` resolves to NO policy at all (key alone fixes a
    ///    cache scope, it does not turn breakpoints on), so there is no marker plan to change.
    ///    First-party Anthropic is the exception: a key-only request there gets the default
    ///    marker policy, and that policy does honour `cache.tail`;
    ///  - a hint-family provider gets a cache key rather than placed breakpoints, so there is no
    ///    message anchor to withhold in the first place.
    ///
    /// Neither costs the caller anything, which is exactly why this must be said out loud. The
    /// value of a declaration is that broca acts on it; a declaration silently dropped looks
    /// identical to one honoured, and the caller has no way to tell which they got. Inert is not
    /// the property that matters here -- undetectable is.
    ///
    /// A WARNING rather than a refusal, on purpose. The declaration is MEANINGLESS on these two
    /// paths, not harmful, and an admission error for a case that costs nothing would reject
    /// working callers to make a point. (Contrast the tool-bearing conflict, refused in
    /// `request_config`: there honouring the declaration would actively degrade caching.)
    fn dropped_cache_tail_warning(
        request: Option<&broca_protocol::request::CacheRequest>,
        policy: &Option<broca_provider::wire::CachePolicy>,
        provider_id: &str,
    ) -> Option<String> {
        request?.tail?;
        match policy.as_ref().map(|policy| &policy.class) {
            Some(broca_provider::wire::CacheClass::Markers { .. }) => None,
            Some(broca_provider::wire::CacheClass::Hints { .. }) => Some(format!(
                "cache.tail declared for provider '{provider_id}', whose wire family uses \
                 prompt-cache hints instead of placed breakpoints; there is no message anchor to \
                 withhold, so the declaration has no effect -- drop cache.tail for this provider"
            )),
            Some(broca_provider::wire::CacheClass::None) | None => Some(
                "cache.tail declared but this request resolved no marker policy, so there \
                 is no breakpoint plan for it to change and the declaration has no effect; broca \
                 plans markers only when the request also carries cache.role or cache.ttl \
                 (cache.key alone fixes a cache scope without enabling breakpoints), except on \
                 first-party anthropic, which places them by default"
                    .to_string(),
            ),
        }
    }

    fn warn_dropped_cache_tail(
        request: Option<&broca_protocol::request::CacheRequest>,
        policy: &Option<broca_provider::wire::CachePolicy>,
        provider_id: &str,
    ) {
        if let Some(message) = dropped_cache_tail_warning(request, policy, provider_id) {
            tracing::warn!(target: "cache", "{message}");
        }
    }

    /// Drives the real agentic loop for each run: resolves the provider from the catalog
    /// (or a base_url override), dispatches tools through the shared `SubcToolPlane`, and
    /// threads the admission ack so the coordinator admits a run only once it's durable.
    pub struct LiveLoopExecutor {
        wal_store: Arc<FileWalStore>,
        /// The SQLite session store is both the durable run index and the materialized
        /// transcript prefix store used by resume and `session.read`.
        session_store: Arc<broca_store::SqliteStore>,
        /// The SHARED tool plane (one consumer connection, per-session routes). Shared so
        /// every session's tool dispatch rides one outbound subc connection.
        plane: Arc<SubcToolPlane>,
        /// The actor uses this adapter before accepting sends; core uses the same adapter for
        /// the remaining phases and resumed work against the frozen declarations.
        hooks: Option<Arc<broca_subc::SubcHookPlane>>,
        compaction_plane: Option<Arc<broca_subc::SubcCompactionPlane>>,
        elicitation_plane: Option<Arc<broca_subc::consent::ScopedConsentPlane>>,
        /// One process-long HTTP client. Every per-run provider gets a cheap clone so sequential
        /// episodes retain the same DNS, TCP, TLS, and HTTP connection pool.
        provider_client: ProviderHttpClient,
        /// The same clock the retention sweep uses for time-dependent decisions.
        clock: broca_core::clock::Clock,
        /// An optional provider base_url override (the aimock / openai-compatible path).
        /// `None` ⇒ resolve the run's model id through the catalog.
        provider_base_url: Option<String>,
        /// Configured hosts change only the endpoint. Keeping them out of provider_base_url
        /// prevents them from disabling credentials and cache planning like the mock path.
        provider_overrides: broca_module_serve::provider_overrides::ProviderOverrides,
        /// The credential source. `None` ⇒ the OpenCode default; `Some` ⇒ resolve a fresh
        /// token from the vault per run (the opt-in cutover, off until an operator selects it).
        /// Shared so a run's in-step token refresher can reach the vault from the provider's
        /// transport task.
        vault: Option<Arc<VaultAuth>>,
        /// Provider→method defaults used to name a credential on the OpenCode path after its
        /// reader identifies the entry type. The value contains no secrets and is also held by
        /// the vault path for handle lookup.
        methods: broca_module_serve::MethodConfig,
        /// The per-session context-management defaults, used when an episode is admitted or
        /// resumed to decide (or refuse) the value it runs under.
        context_management: broca_module_serve::context_management::ContextManagementConfig,
        /// Prompt-cache defaults loaded once at boot. Resolution is pure and runs only when a
        /// fresh episode is admitted; resumes reuse the policy already frozen in the WAL.
        cache_table: CacheTtlTable,
        /// Where the OpenCode credential source reads its auth file. `None` ⇒ the HOME-derived
        /// OpenCode path, which is what every production launch uses. Only tests set it, so a
        /// unit test reads a fixture instead of the operator's real credential file.
        opencode_auth_path: Option<std::path::PathBuf>,
        /// The routing module broca asks which provider account a new run uses, and reports
        /// each run's account evidence to. `None` without the vault credential source.
        routing: Option<Arc<RoutingClient>>,
        /// This installation's id, the root of the Claude Code persona's device id (see
        /// `broca_module_serve::claude_code_identity`). `None` admits Anthropic OAuth episodes
        /// without a persona, in the shape they had before it existed.
        claude_code_install_id: Option<String>,
        /// Each session's pinned `cc_version` suffix once computed in this process (`None` when
        /// its WAL could not be read), so the WAL is read for it at most once per session.
        cc_suffix_memo: Arc<std::sync::Mutex<std::collections::HashMap<String, Option<String>>>>,
        /// WAL bytes read to pin `cc_version` suffixes, for tests that prove a read happens at
        /// most once.
        cc_suffix_wal_bytes: Arc<std::sync::atomic::AtomicU64>,
    }

    impl LiveLoopExecutor {
        pub fn new(
            wal_store: Arc<FileWalStore>,
            session_store: Arc<broca_store::SqliteStore>,
            plane: Arc<SubcToolPlane>,
            provider_base_url: Option<String>,
        ) -> Self {
            Self {
                wal_store,
                session_store,
                plane,
                hooks: None,
                compaction_plane: None,
                elicitation_plane: None,
                provider_client: build_provider_http_client(),
                clock: broca_core::clock::Clock::default(),
                provider_base_url,
                provider_overrides: Default::default(),
                vault: None,
                methods: broca_module_serve::MethodConfig::empty(),
                context_management: Default::default(),
                cache_table: CacheTtlTable::shipped_defaults(),
                opencode_auth_path: None,
                routing: None,
                claude_code_install_id: None,
                cc_suffix_memo: Default::default(),
                cc_suffix_wal_bytes: Default::default(),
            }
        }

        /// The `cc_version` suffix pinned for a new episode's session: the one the session's
        /// latest episode froze, or else one computed from the session's first durable user
        /// message, read from the WAL only as far as the record that carries it (never a
        /// compaction view). The result is remembered per session for this process, so a session
        /// whose latest episode recorded no suffix (it used another provider) does not read the
        /// WAL again. `None` when the WAL cannot be read: logged once per session at WARN, and
        /// the renderer then computes the suffix from each request as before.
        async fn session_cc_version_suffix(
            &self,
            req: &broca_core::RunRequest,
            last_episode_frozen_config: Option<&serde_json::Value>,
            durable_head: u64,
        ) -> Option<String> {
            use broca_core::WalStore as _;
            use broca_module_serve::claude_code_identity::{
                first_durable_cc_version_suffix, inherited_cc_version_suffix,
            };
            if let Some(inherited) = inherited_cc_version_suffix(last_episode_frozen_config) {
                return Some(inherited);
            }
            let key = broca_core::session_addr(&req.session);
            if let Some(remembered) = self.cc_suffix_memo.lock().unwrap().get(&key) {
                return remembered.clone();
            }
            // An empty WAL holds no earlier message, so the new input's is the first.
            let durable = if durable_head == 0 {
                None
            } else {
                let wal_store = Arc::clone(&self.wal_store);
                let session = req.session.clone();
                let read = broca_diagnostics::spawn_blocking(
                    broca_diagnostics::JobKind::WalRead,
                    move || wal_store.first_user_message(&session),
                )
                .await;
                match read {
                    Ok(Ok((message, bytes))) => {
                        self.cc_suffix_wal_bytes
                            .fetch_add(bytes, std::sync::atomic::Ordering::Relaxed);
                        message
                    }
                    Ok(Err(_)) | Err(_) => {
                        tracing::warn!(
                            session = %req.session.session,
                            "the session's first user message could not be read; its Claude Code cc_version suffix is not pinned"
                        );
                        self.remember_cc_suffix(key, None);
                        return None;
                    }
                }
            };
            let suffix = first_durable_cc_version_suffix(durable.as_slice(), &req.input);
            self.remember_cc_suffix(key, suffix.clone());
            suffix
        }

        /// Remember a session's pinned suffix (or that its WAL could not be read) for this
        /// process. The map is cleared when it grows past a bound, which costs at most one more
        /// bounded read per session.
        fn remember_cc_suffix(&self, key: String, suffix: Option<String>) {
            const MAX_REMEMBERED_SESSIONS: usize = 16_384;
            let mut memo = self.cc_suffix_memo.lock().unwrap();
            if memo.len() >= MAX_REMEMBERED_SESSIONS {
                memo.clear();
            }
            memo.insert(key, suffix);
        }

        /// Install the id the Claude Code persona's device id derives from.
        pub fn with_claude_code_install_id(mut self, install_id: Option<String>) -> Self {
            self.claude_code_install_id = install_id;
            self
        }

        pub fn with_hook_plane(mut self, hooks: Arc<broca_subc::SubcHookPlane>) -> Self {
            self.hooks = Some(hooks);
            self
        }

        pub fn with_compaction_plane(
            mut self,
            compaction: Arc<broca_subc::SubcCompactionPlane>,
        ) -> Self {
            self.compaction_plane = Some(compaction);
            self
        }

        pub fn with_elicitation_plane(
            mut self,
            elicitation: Arc<broca_subc::consent::ScopedConsentPlane>,
        ) -> Self {
            self.elicitation_plane = Some(elicitation);
            self
        }

        /// Ask `routing` which account each new subscription-account run uses.
        pub fn with_routing(mut self, routing: Arc<RoutingClient>) -> Self {
            self.routing = Some(routing);
            self
        }

        /// List the vault's credentials once at boot, in the background, so the first
        /// admission finds its candidates ready instead of listing inside its budget.
        pub fn prime_account_listing(&self) {
            let (Some(vault), Some(routing)) = (self.vault.clone(), self.routing.as_ref()) else {
                return;
            };
            if routing.config().legacy_static {
                return;
            }
            tokio::spawn(async move {
                refresh_listing(&vault).await;
            });
        }

        /// How a fresh run's account is chosen. Only a fresh episode on the vault path with a
        /// routing client asks; a resume reuses its frozen decision and never asks again.
        async fn choose_account_for_episode(
            &self,
            req: &broca_core::RunRequest,
            fresh_episode: bool,
            pinned_method: Option<AuthMethod>,
            pinned_account: Option<&str>,
            conversation_key: Option<&str>,
            inherited_conversation_key: Option<&str>,
        ) -> Result<AccountChoice, broca_core::AccountSelectionRefusal> {
            if !fresh_episode || self.provider_base_url.is_some() {
                return Ok(AccountChoice::NotApplicable);
            }
            let (Some(vault), Some(routing)) = (self.vault.as_ref(), self.routing.as_ref()) else {
                return Ok(AccountChoice::NotApplicable);
            };
            let static_credential = broca_module_serve::auth_routing::resolve_credential_id(
                req.model.provider_module_id.as_str(),
                pinned_method,
                pinned_account,
                &vault.methods,
                FALLBACK_METHOD,
            );
            let initial_input_bytes = serde_json::to_vec(&req.input)
                .map(|bytes| bytes.len() as u64)
                .unwrap_or_default();
            let deadline =
                tokio::time::Instant::now() + broca_module_serve::account_routing::ADMISSION_BUDGET;
            broca_module_serve::account_routing::choose_account(
                routing,
                broca_module_serve::account_routing::ChoiceInputs {
                    conversation_key,
                    inherited_conversation_key,
                    static_credential: &static_credential,
                    caller_named_account: pinned_account.is_some(),
                    model: req.model.model_id.as_str(),
                    session: &req.session,
                    run_id: &req.run_id.0,
                    initial_input_bytes,
                },
                latest_listing(Arc::clone(vault)),
                deadline,
            )
            .await
        }

        /// Read OpenCode credentials from this file instead of the HOME-derived one. Test-only:
        /// production has no reason to read another file, and HOME cannot be changed per test
        /// because every test in the process shares it.
        #[cfg(test)]
        pub fn with_opencode_auth_path(mut self, path: std::path::PathBuf) -> Self {
            self.opencode_auth_path = Some(path);
            self
        }

        /// Select the credential vault as the auth source (the cutover). Without this the
        /// executor uses the OpenCode default — so the vault is opt-in by construction.
        pub fn with_vault(mut self, vault: VaultAuth) -> Self {
            self.vault = Some(Arc::new(vault));
            self
        }

        /// What answers `vault.audit`: the same vault client, identity and routing
        /// configuration runs use. `None` without the vault auth source.
        pub fn vault_auditor(
            &self,
        ) -> Option<Arc<dyn broca_module_serve::vault_audit::VaultAuditor>> {
            self.vault
                .clone()
                .map(|vault| vault as Arc<dyn broca_module_serve::vault_audit::VaultAuditor>)
        }

        pub fn with_provider_overrides(
            mut self,
            overrides: broca_module_serve::provider_overrides::ProviderOverrides,
        ) -> Self {
            self.provider_overrides = overrides;
            self
        }

        /// Install the auth-method defaults loaded during process boot.
        pub fn with_methods(mut self, methods: broca_module_serve::MethodConfig) -> Self {
            self.methods = methods;
            self
        }

        /// Install the prompt-cache defaults loaded during process boot.
        pub fn with_cache_table(mut self, table: CacheTtlTable) -> Self {
            self.cache_table = table;
            self
        }

        pub fn with_clock(mut self, clock: broca_core::clock::Clock) -> Self {
            self.clock = clock;
            self
        }

        /// Install the context-management defaults loaded at boot.
        pub fn with_context_management(
            mut self,
            config: broca_module_serve::context_management::ContextManagementConfig,
        ) -> Self {
            self.context_management = config;
            self
        }

        fn cache_policy_for_episode(
            &self,
            fresh_episode: bool,
            family_override: Option<WireFamilyId>,
            req: &broca_core::RunRequest,
            prior_run: Option<&PriorRunView>,
        ) -> Result<Option<CachePolicy>, String> {
            Ok(if self.provider_base_url.is_some() {
                None
            } else if fresh_episode {
                let family_for_cache = family_override.or_else(|| {
                    broca_catalog::catalog().ok().and_then(|catalog| {
                        catalog.wire_family(
                            req.model.provider_module_id.as_str(),
                            req.model.model_id.as_str(),
                        )
                    })
                });
                match family_for_cache {
                    Some(family) => {
                        let policy = broca_module_serve::resolve_cache_policy(
                            family,
                            req.model.provider_module_id.as_str(),
                            req.cache.as_ref(),
                            &self.cache_table,
                            &req.session,
                        )?;
                        warn_single_call_cache_markers(&policy, &req.tools);
                        warn_dropped_cache_tail(
                            req.cache.as_ref(),
                            &policy,
                            req.model.provider_module_id.as_str(),
                        );
                        policy
                    }
                    None => None,
                }
            } else {
                prior_run.and_then(|prior| frozen_cache_policy(&prior.config))
            })
        }

        /// The prompt-cache key a fresh episode freezes: the caller's `cache.key`, else the
        /// session-derived default for a provider the cache table lists. A resume never calls
        /// this; it re-applies the key frozen in the episode's render config. The
        /// `BROCA_PROVIDER_BASE_URL` mock endpoint is opaque, so like the cache policy it gets
        /// no provider default, only a key the caller named.
        fn prompt_cache_key_for_fresh_episode(
            &self,
            req: &broca_core::RunRequest,
        ) -> Option<String> {
            if self.provider_base_url.is_some() {
                return broca_module_serve::resolve_caller_cache_key(req.cache.as_ref());
            }
            broca_module_serve::resolve_prompt_cache_key(
                req.model.provider_module_id.as_str(),
                req.cache.as_ref(),
                &self.cache_table,
                &req.session,
            )
        }

        /// Decide this run's auth: the mock path needs none; the vault path resolves the
        /// provider's method → credential id → handle → a fresh token (mapping the outcome);
        /// otherwise the OpenCode default. Returns the resolution auth mode plus an optional
        /// pause-for-reauth error (set only when the vault is authoritative that the
        /// credential is dead — the run admits then pauses, making no provider call).
        ///
        /// A configured provider always resolves (api-key credentials included, because the
        /// vault is the single authority for all credential kinds). The only FailAdmission
        /// case is a genuinely unconfigured credential: a clear "no `<method>` credential for
        /// `<provider>`", never a silent fallback to a different method.
        ///
        /// `frozen_credential_id` is the prior run's frozen selection on a RESUME (read from
        /// the durable RunConfig). When present it is RE-USED verbatim — the method is never
        /// re-resolved from config (config/router could have drifted → a different method → a
        /// different family → a cache bust). `None` ⇒ a fresh run, which resolves a caller pin
        /// before config and freezes the result. `fresh_episode` distinguishes a fresh run from a
        /// resume of an old WAL that has no selection; the latter must not be retro-stamped.
        // Tests drive auth resolution without a routing decision through this form.
        #[cfg(test)]
        async fn decide_auth(
            &self,
            provider_id: &str,
            frozen_credential_id: Option<&str>,
            fresh_episode: bool,
            pinned_method: Option<AuthMethod>,
            pinned_account: Option<&str>,
        ) -> Result<AuthResolution, RunError> {
            self.decide_auth_with_account(
                provider_id,
                frozen_credential_id,
                fresh_episode,
                pinned_method,
                pinned_account,
                &AccountChoice::NotApplicable,
                None,
            )
            .await
        }

        /// Does what [`Self::decide_auth`] does, except that when `account` says routing chose
        /// a credential, that credential is read instead of the statically configured one, and
        /// the selection records how the account was chosen. `frozen_decision` is the prior
        /// run's recorded decision on a resume, carried unchanged so a resume's selection equals
        /// the one frozen at admission.
        #[allow(clippy::too_many_arguments)]
        async fn decide_auth_with_account(
            &self,
            provider_id: &str,
            frozen_credential_id: Option<&str>,
            fresh_episode: bool,
            pinned_method: Option<AuthMethod>,
            pinned_account: Option<&str>,
            account: &AccountChoice,
            frozen_decision: Option<&broca_core::AccountDecision>,
        ) -> Result<AuthResolution, RunError> {
            if self.provider_base_url.is_some() {
                // The mock path needs no credential (and nothing to freeze/guard).
                return Ok(AuthResolution {
                    mode: AuthMode::None,
                    pause: None,
                    selection: None,
                    method: None,
                    project_id: None,
                    account_id: None,
                    record_version: None,
                    in_run_refresh: None,
                });
            }
            let Some(vault) = self.vault.as_ref() else {
                if let Some(account) = pinned_account {
                    return Err(RunError::Config(format!(
                        "auth account pin '{account}' requires the vault credential source; \
                         BROCA_AUTH_SOURCE=opencode cannot select account-scoped credentials"
                    )));
                }
                // OpenCode's file records only generic api/oauth entry types. Resolve the
                // configured/frozen effective method before choosing a provider family; otherwise
                // a special method would be stamped only after the plain catalog family was built.
                let discovered_method = if fresh_episode {
                    match self.opencode_auth_path.as_deref() {
                        Some(path) => {
                            broca_catalog::live::opencode_auth_method_at(path, provider_id)
                        }
                        None => broca_catalog::live::opencode_auth_method(provider_id),
                    }
                } else {
                    None
                };
                let episode_auth = opencode_auth_for_episode(
                    fresh_episode,
                    frozen_credential_id,
                    discovered_method,
                    pinned_method,
                    &self.methods,
                    provider_id,
                );
                validate_opencode_method_metadata(provider_id, episode_auth.method)?;
                return Ok(AuthResolution {
                    // The provider resolver reads the secret later; an injected file travels in
                    // the mode so that read uses the same file as the method lookup above.
                    mode: match &self.opencode_auth_path {
                        Some(path) => AuthMode::FromOpencodeFile(path.clone()),
                        None => AuthMode::FromOpencode,
                    },
                    pause: None,
                    selection: episode_auth.selection,
                    method: episode_auth.method,
                    project_id: None,
                    account_id: None,
                    record_version: None,
                    in_run_refresh: None,
                });
            };
            // Resume re-uses the frozen credential id verbatim; a fresh run resolves the caller
            // pin before config (api-key fallback for the long tail). `select_credential_id`
            // owns that branch so the resume invariant is unit-tested in isolation.
            // A credential routing chose is read by the vault id routing returned, verbatim;
            // `choose_account` has already checked it parses as this run's method and provider.
            let routed = match account {
                AccountChoice::Routed {
                    credential_id,
                    account_id,
                    decision,
                    view_digest,
                } => CredentialId::parse(credential_id)
                    .map(|parsed| (parsed, credential_id, account_id, decision, view_digest)),
                _ => None,
            };
            let credential_id = match &routed {
                Some((parsed, ..)) => parsed.clone(),
                None => broca_module_serve::auth_routing::select_credential_id(
                    frozen_credential_id,
                    provider_id,
                    pinned_method,
                    pinned_account,
                    &vault.methods,
                    FALLBACK_METHOD,
                ),
            };
            let credential_key = credential_id.to_id_string();
            // Every credential is read by grant, by its vault credential id. A scoped not_found
            // is final: nothing retries it another way, which would hide the misconfiguration
            // the operator needs to see.
            let vault_id = match &routed {
                Some((_, vault_id, ..)) => (*vault_id).clone(),
                None => vault.addressing.vault_id_for(&credential_id).into_owned(),
            };
            // The request is built once and the call reads its arguments from it, so what the
            // selection records below is exactly what the vault was sent. get_scoped has no
            // force_refresh parameter, so none is recorded.
            let vault_request = broca_core::VaultRequest {
                min_ttl_ms: Some(VAULT_MIN_TTL_MS),
                force_refresh: None,
                op: Some(broca_core::VaultReadOp::GetScoped),
            };
            let (recorded_request, outcome) = if matches!(account, AccountChoice::AuthRequired(_)) {
                // Routing found every remaining candidate needing a fresh login. The run is
                // admitted (RunStarted is written) and paused for re-authentication, the same
                // outcome as the vault itself answering `needs_reauth`; no credential is read,
                // so no vault request is recorded.
                (None, Err(VaultError::NeedsReauth))
            } else {
                let got = vault
                    .client
                    .get_scoped(&vault_id, vault_request.min_ttl_ms)
                    .await;
                // Decided from the vault's own answer, before the cache fallback below can
                // replace a failure with a token the vault did not serve for this request.
                let recorded_request = recorded_vault_request(&got, vault_request);
                // Reject a different account before it can enter the shared cache: static
                // admissions using the same credential key do not have a routed-account check.
                if let (Some((_, routed_vault_id, routed_account, ..)), Ok(token)) =
                    (&routed, &got.result)
                {
                    vault.gauge.stamp_ok();
                    validate_routed_account(token, routed_vault_id, routed_account)?;
                }
                let outcome = resolve_vault_outcome(
                    &vault.cache,
                    vault.gauge.as_ref(),
                    &credential_key,
                    got.result,
                    epoch_millis(),
                    VAULT_MIN_TTL_MS,
                );
                (recorded_request, outcome)
            };
            if let Some((_, routed_vault_id, routed_account, decision, view_digest)) = &routed {
                match &outcome {
                    // The vault does not serve the account routing chose to this principal. That
                    // excludes the account for this admission only: routing is told, broca
                    // re-lists, and the send is refused rather than spent on another account.
                    Err(VaultError::NotFound) => {
                        if let Some(routing) = self.routing.as_ref() {
                            let relist_vault = Arc::clone(vault);
                            broca_module_serve::account_routing::report_scoped_not_found(
                                Arc::clone(routing),
                                decision.admission_id.clone().unwrap_or_default(),
                                (*routed_vault_id).clone(),
                                (*view_digest).clone(),
                                async move { refresh_listing(&relist_vault).await },
                            );
                        }
                        return Err(RunError::AccountSelectionRefused(
                            broca_core::AccountSelectionRefusal {
                                routing_code:
                                    broca_module_serve::account_routing::ACCOUNT_NOT_AUTHORIZED
                                        .to_string(),
                                retry_after_s: None,
                                decision_id: decision.decision_id.clone(),
                                class: ErrorClass::Permanent,
                            },
                        ));
                    }
                    // A credential that resolves to another account than the one routing chose
                    // would spend on an account nobody selected.
                    Ok(token) => validate_routed_account(token, routed_vault_id, routed_account)?,
                    _ => {}
                }
            }
            // The Antigravity cloud project travels alongside the token (Some only for an
            // antigravity credential); capture it before the outcome is consumed so it can be
            // frozen as a body render input.
            let project_id = outcome.as_ref().ok().and_then(|t| t.project_id.clone());
            let account_id = outcome.as_ref().ok().and_then(|t| t.account_id.clone());
            let record_version = outcome.as_ref().ok().map(|t| t.record_version);
            let expires_at_ms = outcome.as_ref().ok().and_then(|t| t.expires_at_ms);
            // Freeze the resolved id on a fresh run (the loop writes it only on a fresh
            // RunStarted). A resume with a frozen selection reuses it; an older resume without
            // one keeps None rather than retro-stamping its WAL.
            let selection = (fresh_episode || frozen_credential_id.is_some()).then(|| {
                broca_core::AuthSelection {
                    credential_id: credential_id.to_id_string(),
                    // Freeze the vault-served account id verbatim alongside the credential id so
                    // usage export labels spend with the account this run authenticated with.
                    account_ref: account_id.clone(),
                    resolved_credential_id: outcome
                        .as_ref()
                        .ok()
                        .and_then(|token| token.resolved_credential_id.clone()),
                    // Present only if the vault replied; see `recorded_vault_request`.
                    vault_request: recorded_request,
                    // How the account was chosen; a resume carries the decision frozen at
                    // admission.
                    account_decision: match account {
                        AccountChoice::NotApplicable => frozen_decision.cloned(),
                        chosen => chosen.decision().cloned(),
                    },
                }
            });
            let method = Some(credential_id.method);
            let in_run_refresh = record_version.map(|version| {
                let served_version = Arc::new(AtomicU64::new(version));
                let served_expires_at_ms = Arc::new(std::sync::Mutex::new(expires_at_ms));
                let reported_version = Arc::new(std::sync::Mutex::new(None));
                InRunRefresh {
                    refresher: SharedSecretRefresher(Arc::new(VaultTokenRefresher {
                        vault: Arc::clone(vault),
                        credential_key: credential_key.clone(),
                        vault_id: vault_id.clone(),
                        served_version: Arc::clone(&served_version),
                        served_expires_at_ms: Arc::clone(&served_expires_at_ms),
                        reported_version: Arc::clone(&reported_version),
                        presend_failure_logged: std::sync::atomic::AtomicBool::new(false),
                        no_expiry_logged: std::sync::atomic::AtomicBool::new(false),
                    })),
                    served_version,
                    served_expires_at_ms,
                    reported_version,
                }
            });
            // A scoped not_found keeps not_found's class (a permanent admission failure) but
            // says what it can mean; see `scoped_not_found_message`.
            let decision = match &outcome {
                Err(VaultError::NotFound) => {
                    AuthDecision::FailAdmission(broca_module_serve::scoped_not_found_message(
                        &vault
                            .client
                            .scoped_principal()
                            .unwrap_or_else(|| "this process's principal".to_string()),
                        &credential_key,
                        &vault_id,
                    ))
                }
                _ => vault_outcome_to_decision(outcome),
            };
            match decision {
                // A pause resolves a provider with no auth only to admit the resumable run.
                // Credential-bound render fields may be absent (Antigravity's project_id), so
                // this initial frozen config is not trusted for a call. On successful re-auth,
                // core replaces it durably before the first provider call; a mid-run pause keeps
                // the ordinary strict drift guard because a call already used its config.
                AuthDecision::PauseForReauth(err) => Ok(AuthResolution {
                    mode: AuthMode::None,
                    pause: Some(err),
                    selection,
                    method,
                    project_id,
                    account_id,
                    record_version,
                    in_run_refresh: None,
                }),
                AuthDecision::Proceed(mode) => Ok(AuthResolution {
                    mode,
                    pause: None,
                    selection,
                    method,
                    project_id,
                    account_id,
                    record_version,
                    in_run_refresh,
                }),
                AuthDecision::FailAdmission(msg) => Err(RunError::Config(msg)),
                AuthDecision::FailAdmissionTransient(msg) => Err(RunError::Transient(msg)),
            }
        }
    }

    #[async_trait]
    impl RunExecutor for LiveLoopExecutor {
        fn elicitation_plane(&self) -> Option<Arc<dyn broca_core::tool_plane::ElicitationPlane>> {
            self.elicitation_plane
                .as_ref()
                .map(|plane| plane.clone() as Arc<dyn broca_core::tool_plane::ElicitationPlane>)
        }

        fn hook_plane(&self) -> Option<Arc<dyn broca_core::HookPlane>> {
            self.hooks
                .as_ref()
                .map(|hooks| hooks.clone() as Arc<dyn broca_core::HookPlane>)
        }

        fn compaction_plane(&self) -> Option<Arc<dyn broca_core::compaction::CompactionPlane>> {
            self.compaction_plane
                .as_ref()
                .map(|plane| plane.clone() as Arc<dyn broca_core::compaction::CompactionPlane>)
        }

        fn warm_after_send_ms(&self, config: &broca_wal::RunConfig) -> Option<u64> {
            broca_module_serve::cache_warm::warm_after_send_ms(config)
        }

        fn warm_costs(
            &self,
            config: &broca_wal::RunConfig,
            prefix_tokens: u64,
            ttl_ms: u64,
        ) -> Result<broca_core::keep_warm::WarmCosts, String> {
            broca_module_serve::cache_warm::automatic_warm_prices(config, prefix_tokens, ttl_ms)
        }

        fn responses_warm_cost(
            &self,
            config: &broca_wal::RunConfig,
            usage: broca_protocol::Usage,
        ) -> Result<f64, String> {
            broca_module_serve::cache_warm::responses_warm_cost(config, usage)
        }

        async fn warm_quota(
            &self,
            config: &broca_wal::RunConfig,
            headers: &std::collections::BTreeMap<String, String>,
        ) -> Result<(), String> {
            let credential = config
                .auth_selection
                .as_ref()
                .and_then(|selection| CredentialId::parse(&selection.credential_id))
                .ok_or("account_unknown")?;
            if !matches!(credential.method, AuthMethod::Oauth | AuthMethod::Chatgpt) {
                return Ok(());
            }
            let routing = self.routing.as_deref().unwrap_or_else(|| {
                broca_module_serve::keep_warm_quota::unavailable_routing_client()
            });
            broca_module_serve::cache_warm::automatic_warm_quota_for_config(
                routing, config, headers,
            )
            .await
        }
        /// A warm builds a provider over the config the warmed request was sent with, holding a
        /// credential read now exactly as a resume of the episode reads it, and sends one
        /// attempt. Nothing about the config is re-derived, so a recorded request renders to the
        /// bytes its step sent or the warm is refused.
        async fn warm(
            &self,
            job: broca_module::WarmJob,
        ) -> Result<broca_core::WarmSent, broca_module::WarmError> {
            use broca_module::WarmError;
            let config = &job.config;
            let frozen = broca_module_serve::cache_warm::warmable_config(config)?;
            let provider_id = config.model.provider_module_id.as_str();
            let selection = config.auth_selection.as_ref();
            // The credential the episode froze is read again exactly as a resume reads it: no
            // account is chosen afresh, and the frozen decision is carried unchanged.
            let resolution = self
                .decide_auth_with_account(
                    provider_id,
                    selection.map(|selection| selection.credential_id.as_str()),
                    false,
                    None,
                    None,
                    &AccountChoice::NotApplicable,
                    selection.and_then(|selection| selection.account_decision.as_ref()),
                )
                .await
                .map_err(|error| WarmError::Unavailable(error.to_string()))?;
            if let Some(pause) = resolution.pause {
                return Err(WarmError::Unavailable(format!(
                    "the session's credential needs re-authentication: {}",
                    pause.message
                )));
            }
            let provider = broca_catalog::live::provider_for_recorded_config(
                provider_id,
                &config.model.model_id,
                frozen,
                &resolution.mode,
                resolution.method,
                selection.and_then(|selection| selection.account_ref.as_deref()),
                resolution.account_id.as_deref(),
                self.provider_client.clone(),
                &self.clock,
            )
            .map_err(WarmError::Unavailable)?;
            broca_module::executor::send_warm(provider.as_ref(), job.base).await
        }

        async fn execute(
            &self,
            mut job: RunJob,
            out: mpsc::Sender<OutputEvent>,
        ) -> Result<RunSummary, RunError> {
            // DECIDE auth + RESOLVE the provider BEFORE the loop (so a resolution failure is
            // a typed error via the admission ack, never a ghost run that wrote RunStarted
            // then died). The run's model id is `<provider>/<model>` from the wire request; a
            // base_url override (aimock) sends the model verbatim as openai-compatible.
            let model_id = format!(
                "{}/{}",
                job.req.model.provider_module_id, job.req.model.model_id
            );
            // Admission, auth/cache selection, and core reconstruction share the same
            // checkpoint-plus-tail view; a RunStarted may already be behind its watermark.
            let prior_snapshot = &job.prior_snapshot;
            let prior_run = if matches!(job.kind, RunKind::AppendEpisode) {
                None
            } else {
                prior_run_view_from_checkpoint(&prior_snapshot.checkpoint_at_head)
            };
            let fresh_episode = matches!(job.kind, RunKind::AppendEpisode)
                || prior_run
                    .as_ref()
                    .is_none_or(|prior| prior.idle && prior.run_id != job.req.run_id);
            // Context management is decided before auth so a refused value costs no credential
            // read. The last episode's config is read from the snapshot directly because an
            // appended episode has no `prior_run` view, yet is exactly the case that inherits.
            let episode_context_management = match self.context_management.for_episode(
                fresh_episode,
                job.req.context_management.as_ref(),
                prior_snapshot
                    .checkpoint_at_head
                    .last_episode
                    .as_ref()
                    .map(|episode| &episode.config),
                &job.req.session.project_root.to_string_lossy(),
            ) {
                Ok(decided) => decided,
                Err(error) => {
                    if let Some(ack) = job.on_admitted {
                        let _ = ack.send(Err(error.clone()));
                    }
                    return Err(error);
                }
            };
            let auth_context = episode_auth_context(fresh_episode, prior_run.is_some());
            let (pinned_method, pinned_account) =
                match caller_auth_for_episode(job.auth.as_ref(), auth_context) {
                    Ok(pin) => pin,
                    Err(error) => {
                        if let Some(ack) = job.on_admitted {
                            let _ = ack.send(Err(error.clone()));
                        }
                        return Err(error);
                    }
                };
            // On a RESUME, re-use the credential the prior run FROZE (read from the durable
            // RunConfig of the last episode) — never re-resolve the method from config/router
            // (a drift would change the wire family and bust the cache). A NEW episode
            // (append) resolves fresh — it's a new run with its own method. A fresh run / an
            // opencode-prior run reads None and resolves from config.
            let frozen_credential_id: Option<String> = if fresh_episode {
                None
            } else {
                prior_run
                    .as_ref()
                    .and_then(|prior| prior.config.auth_selection.clone())
                    .map(|s| s.credential_id)
            };

            // Authentication comes from no credential for mock, a fresh vault token, or the
            // opencode default. If the vault says the credential needs re-authentication, we
            // still resolve the provider without auth to record the correct RunStarted, then
            // pause before any provider call. Auth attaches after rendering and is excluded
            // from the config hash, so omitting it does not change the frozen config.
            // A fresh subscription-account run asks routing to choose an account within the
            // routing time limit set by ADMISSION_BUDGET; a resume reuses the account decision
            // saved in that episode's RunConfig. This session's persisted checkpoint contains
            // its latest episode's conversation key. A new episode inherits that key, or uses
            // the legacy session-derived fallback if the latest episode predates key recording,
            // without scanning older WAL history. If this job executes a previously queued
            // send, its saved key arrives in job.conversation_key and takes precedence.
            let inherited_key = prior_snapshot
                .checkpoint_at_head
                .last_episode
                .as_ref()
                .and_then(|episode| episode.config.auth_selection.as_ref())
                .and_then(|selection| selection.account_decision.as_ref())
                .and_then(|decision| decision.conversation_key.as_deref());
            let account_choice = match self
                .choose_account_for_episode(
                    &job.req,
                    fresh_episode,
                    pinned_method,
                    pinned_account,
                    job.conversation_key.as_deref(),
                    inherited_key,
                )
                .await
            {
                Ok(choice) => choice,
                Err(refusal) => {
                    let error = RunError::AccountSelectionRefused(refusal);
                    if let Some(ack) = job.on_admitted {
                        let _ = ack.send(Err(error.clone()));
                    }
                    return Err(error);
                }
            };
            let frozen_decision = if fresh_episode {
                None
            } else {
                prior_run
                    .as_ref()
                    .and_then(|prior| prior.config.auth_selection.as_ref())
                    .and_then(|selection| selection.account_decision.clone())
            };
            let resolution = match self
                .decide_auth_with_account(
                    job.req.model.provider_module_id.as_str(),
                    frozen_credential_id.as_deref(),
                    fresh_episode,
                    pinned_method,
                    pinned_account,
                    &account_choice,
                    frozen_decision.as_ref(),
                )
                .await
            {
                Ok(v) => v,
                Err(e) => {
                    if let Some(ack) = job.on_admitted {
                        let _ = ack.send(Err(e.clone()));
                    }
                    return Err(e);
                }
            };
            // Public sends enter as FreshOrResume because the wire cannot know whether the
            // credential has been repaired. Once auth succeeds, mark the core attempt as
            // auth-resolved so an AuthRequired replay continues; core ignores that signal for
            // fresh runs and every other replay resolution.
            let mode =
                broca_module_serve::start_mode_after_auth(job.kind, resolution.pause.is_none());
            // DRIVE the wire family from the resolved auth method: a method that selects a
            // distinct grammar (Antigravity → google-codeassist, ChatGPT → chatgpt-responses)
            // overrides the catalog provider's family. `family_for_method` is the single
            // authority — the same one the consistency guard below consults — so the family
            // and the credential can't drift to a mismatched pair. The Antigravity project
            // (a body render input) is frozen here too.
            let family_override = resolution.method.and_then(family_for_method);
            // A steer or interrupt that omitted `cache` keeps the session's cache: the policy and
            // key the latest episode froze, exactly as a resume of that episode would render.
            let inherited = inherited_cache(
                &job.req,
                fresh_episode,
                prior_snapshot
                    .checkpoint_at_head
                    .last_episode
                    .as_ref()
                    .map(|episode| &episode.config),
            );
            let cache_policy = match &inherited {
                Some((policy, _)) => Ok(policy.clone()),
                None => self.cache_policy_for_episode(
                    fresh_episode,
                    family_override,
                    &job.req,
                    prior_run.as_ref(),
                ),
            };
            let cache_policy = match cache_policy {
                Ok(policy) => policy,
                Err(error) => {
                    if let Some(ack) = job.on_admitted {
                        let _ = ack.send(Err(RunError::Config(error.clone())));
                    }
                    return Err(RunError::Config(error));
                }
            };
            let prompt_cache_key = if let Some((_, key)) = inherited {
                key
            } else if fresh_episode {
                self.prompt_cache_key_for_fresh_episode(&job.req)
            } else {
                prior_run
                    .as_ref()
                    .and_then(|prior| frozen_prompt_cache_key(&prior.config))
            };
            // The Claude Code persona a fresh episode freezes; the catalog keeps it only when
            // the episode resolves to the Anthropic OAuth shape. A resume restores the persona
            // its episode recorded, or its recorded absence, below.
            let speaks_as_claude_code = job.req.model.provider_module_id.as_str() == "anthropic"
                && resolution.method != Some(AuthMethod::ApiKey);
            let fresh_persona = match (&self.claude_code_install_id, fresh_episode) {
                (Some(install_id), true) if speaks_as_claude_code => {
                    let cc_version_suffix = self
                        .session_cc_version_suffix(
                            &job.req,
                            prior_snapshot
                                .checkpoint_at_head
                                .last_episode
                                .as_ref()
                                .map(|episode| &episode.config.frozen_render_config),
                            prior_snapshot.head_seq,
                        )
                        .await;
                    match broca_module_serve::claude_code_identity::fresh_persona(
                        install_id,
                        resolution.account_id.as_deref(),
                        resolution
                            .selection
                            .as_ref()
                            .map_or(job.req.model.provider_module_id.as_str(), |selection| {
                                selection.credential_id.as_str()
                            }),
                        &job.req.session,
                        cc_version_suffix,
                    ) {
                        Ok(persona) => Some(persona),
                        Err(error) => {
                            tracing::warn!(%error, "Claude Code persona unavailable for this episode");
                            None
                        }
                    }
                }
                _ => None,
            };
            let restored_persona = prior_run
                .as_ref()
                .filter(|_| !fresh_episode)
                .and_then(|prior| frozen_claude_code_persona(&prior.config));
            let codeassist_identity = match broca_module_serve::codeassist_identity::for_episode(
                &job.req.session,
                prior_run
                    .as_ref()
                    .filter(|_| !fresh_episode)
                    .map(|prior| &prior.config.frozen_render_config),
                prior_snapshot.head_seq,
                epoch_millis().max(0) as u64,
            ) {
                Ok(identity) => identity,
                Err(error) => {
                    let error = format!("invalid recorded Code Assist identity: {error}");
                    if let Some(ack) = job.on_admitted {
                        let _ = ack.send(Err(RunError::Config(error.clone())));
                    }
                    return Err(RunError::Config(error));
                }
            };
            let resolve_options = broca_catalog::live::ResolveOptions {
                claude_code_persona: fresh_persona.as_ref(),
                auth_method: resolution.method,
                family_override,
                project_id: resolution.project_id.as_deref(),
                // Use the codex session-id header when no request-level caller scope is fixed for this run.
                session_id: Some(job.req.session.session.as_str()),
                account_id: resolution.account_id.as_deref(),
                cache_policy: cache_policy.clone(),
                prompt_cache_key: prompt_cache_key.as_deref(),
                // Only a fresh episode resolves the caller's requested variant. A resume ignores
                // the resuming send's variant and re-applies the one frozen in the episode's
                // render config (`restored_variant` below), matching core, which keeps the
                // recorded `RunConfig.variant` on resume.
                variant: job.req.variant.as_deref().filter(|_| fresh_episode),
                // The same rule for the service tier: only a fresh episode checks and freezes
                // the caller's tier; a resume re-applies the recorded one (below).
                service_tier: job.req.service_tier.filter(|_| fresh_episode),
                clock: Some(&self.clock),
                frozen_account_ref: if fresh_episode {
                    resolution
                        .selection
                        .as_ref()
                        .and_then(|s| s.account_ref.as_deref())
                } else {
                    prior_run
                        .as_ref()
                        .and_then(|p| p.config.auth_selection.as_ref())
                        .and_then(|s| s.account_ref.as_deref())
                },
                secret_refresher: resolution
                    .in_run_refresh
                    .as_ref()
                    .map(|refresh| refresh.refresher.clone()),
                // Structured output is per run: only a fresh episode decides it from its own
                // send. A resume re-applies the decision frozen in the episode (below).
                structured_output: job.req.structured_output.as_ref().filter(|_| fresh_episode),
            };
            let restored_structured_output = prior_run
                .as_ref()
                .filter(|_| !fresh_episode)
                .and_then(|prior| {
                    broca_protocol::structured_output::FrozenStructuredOutput::from_frozen_render_config(
                        Some(&prior.config.frozen_render_config),
                    )
                });
            let restored_variant = prior_run
                .as_ref()
                .filter(|_| !fresh_episode)
                .and_then(|prior| frozen_variant(&prior.config));
            // For a ChatGPT auth-at-start pause, this provider exists solely to satisfy the
            // core's provider type until resume rebuilds real credentials and runs the pre-call
            // refreeze check. The core durably writes RunStarted and RunPaused without rendering or
            // calling the pause-only provider; every non-pause attempt, including repaired resume,
            // takes the strict auth path.
            let resolved = match resolve_provider_for_episode(
                &model_id,
                self.provider_base_url.as_deref(),
                job.req.generation.max_output_tokens,
                &resolution,
                &resolve_options,
                self.provider_client.clone(),
                broca_catalog::live::EpisodeRenderConfig {
                    codeassist_identity: codeassist_identity.as_ref(),
                    // Only the host and the output cap are restored. Re-resolve other catalog
                    // fields so core's frozen_render_config_drift guard remains a real
                    // comparison on resume. Changing the operator override must not move an
                    // existing episode.
                    base_url: if let Some(prior) = prior_run.as_ref().filter(|_| !fresh_episode) {
                        prior
                            .config
                            .frozen_render_config
                            .get("base_url")
                            .and_then(|value| value.as_str())
                    } else {
                        self.provider_overrides
                            .base_url(job.req.model.provider_module_id.as_str())
                    },
                    // A send with no cap takes its default from the catalog's limit.output,
                    // which moves with ordinary upstream churn. The default is an episode input
                    // frozen once: a resume reuses the recorded number instead of re-deriving it,
                    // so a moved catalog value cannot refuse the resume. A caller's explicit cap
                    // still wins over this inside the resolver.
                    max_output_tokens: prior_run
                        .as_ref()
                        .filter(|_| !fresh_episode)
                        .and_then(|prior| frozen_max_output_tokens(&prior.config)),
                    // The frozen variant is restored as recorded, never re-derived from a
                    // catalog that may have changed since the episode was admitted.
                    variant: restored_variant.as_ref(),
                    // The admitted tier, from the durable RunConfig, never the resuming send.
                    service_tier: prior_run
                        .as_ref()
                        .filter(|_| !fresh_episode)
                        .and_then(|prior| frozen_service_tier(&prior.config)),
                    // The thinking-echo scope is restored as recorded, so an episode admitted
                    // before the tool-loop scope became the default resumes with its old bytes.
                    thinking_echo_scope: prior_run
                        .as_ref()
                        .filter(|_| !fresh_episode)
                        .and_then(|prior| frozen_thinking_echo_scope(&prior.config)),
                    // The cross-model thinking rule is restored as recorded, so an episode
                    // admitted while signed thinking was echoed only to the model that produced
                    // it resumes with its old bytes.
                    thinking_cross_model: prior_run
                        .as_ref()
                        .filter(|_| !fresh_episode)
                        .and_then(|prior| frozen_thinking_cross_model(&prior.config)),
                    chatgpt_effort_updates: prior_run.as_ref().filter(|_| !fresh_episode).map(
                        |prior| {
                            prior.config.frozen_render_config["chatgpt_effort_updates"]
                                .as_bool()
                                .unwrap_or(false)
                        },
                    ),
                    // Restored as recorded: an absent field (an episode admitted before DeepSeek
                    // was opted into the reasoning echo) reads back as false and keeps its bytes.
                    reasoning_content_required: prior_run
                        .as_ref()
                        .filter(|_| !fresh_episode)
                        .map(|prior| frozen_reasoning_content_required(&prior.config)),
                    reasoning_echo: prior_run
                        .as_ref()
                        .filter(|_| !fresh_episode)
                        .map(|prior| frozen_reasoning_echo(&prior.config)),
                    // Older episodes omitted strict; only a fresh episode takes the new default.
                    responses_tools_non_strict: prior_run
                        .as_ref()
                        .filter(|_| !fresh_episode)
                        .map(|prior| frozen_responses_tools_non_strict(&prior.config)),
                    // Restored as recorded, never re-decided from a catalog that may have moved.
                    structured_output: restored_structured_output.as_ref(),
                    // Restored as recorded, including a recorded absence: an OAuth episode
                    // admitted before the persona existed resumes with its old bytes.
                    claude_code_persona: if fresh_episode {
                        broca_catalog::live::EpisodePersona::Fresh
                    } else {
                        broca_catalog::live::EpisodePersona::Recorded(restored_persona.as_ref())
                    },
                },
            ) {
                Ok(r) => r,
                Err(e) => {
                    // Pre-loop resolution failure → fire the admission ack with the error and
                    // return WITHOUT touching the WAL (no RunStarted, no ghost run).
                    if let Some(ack) = job.on_admitted {
                        let _ = ack.send(Err(e.clone().into()));
                    }
                    return Err(e.into());
                }
            };

            // (provider, auth-method) CONSISTENCY GUARD: a method that IMPLIES a wire family
            // (the wire-changing methods) must match the family the provider actually
            // resolved — else a frozen mismatch (e.g. a code-assist token against the plain
            // Gemini endpoint) would render a token through the wrong wire shape and 400.
            // `family_for_method` is the single authority both this guard and the future
            // family driver consult, so the pair can never drift. Same-family method differences
            // are frozen into the render config by catalog resolution; fail admission (no
            // RunStarted is written, so no half-started run) if a family-changing method resolves
            // an inconsistent family.
            if let Some(method) = resolution.method {
                if let Some(implied) = family_for_method(method) {
                    if resolved.family_id != implied {
                        let msg = format!(
                            "auth method '{}' implies wire family {:?} but provider resolved \
                             {:?} — refusing to freeze a mismatched credential/family pair",
                            method.as_token(),
                            implied,
                            resolved.family_id
                        );
                        if let Some(ack) = job.on_admitted {
                            let _ = ack.send(Err(RunError::Config(msg.clone())));
                        }
                        return Err(RunError::Config(msg));
                    }
                }
            }

            // Warm the tool route CONCURRENTLY with the loop (gives the tool provider time
            // for its one-time setup on the first route open for a session). This must NOT
            // block admission: the admission ack — and with it the client's session.send
            // reply — fires at the RunStarted barrier inside the loop, so an await here
            // holds every fresh-session send hostage to the tool module's configure time
            // (observed live: AFT configure crossing the client's 10s send deadline under
            // load made ~15% of harness sends report "outcome unknown" while their runs
            // actually ran). The loop's first tool dispatch cannot happen before the first
            // model response returns, which gives the warm seconds of headroom; a dispatch
            // racing a still-cold route just opens it itself (the route cache handles the
            // race). A bind failure stays a tool-plane availability issue surfacing as a
            // normal NotSent at dispatch. Skipped when pausing for re-auth, and skipped
            // entirely for a TOOL-LESS run: a send with no tools can never dispatch, so
            // attaching the tool module would only add latency — and if the tool module
            // rejects this session's bind identity, it would stall a run that never needed
            // the module at all.
            if resolution.pause.is_none() && !job.req.tools.is_empty() {
                let plane = Arc::clone(&self.plane);
                let warm_bind_identity = job.req.session.clone();
                // Warm under the scope this run was admitted under (fresh or resumed), so the
                // route the run's first dispatch reuses carries that scope; a scoped session's
                // tool route is never opened unscoped.
                let warm_scope = job.req.scope.clone();
                // Bind detached warming to this actor generation before spawning. If teardown
                // wins before the task's first poll, this exact token is already cancelled.
                let lifecycle = plane.capture_session_lifecycle_token(&warm_bind_identity);
                tokio::spawn(async move {
                    let _ = plane
                        .warm_bind_for_lifecycle(
                            &warm_bind_identity,
                            warm_scope.as_ref(),
                            BIND_TIMEOUT,
                            lifecycle,
                        )
                        .await;
                });
            }

            let materialized_store =
                job.materialized_store(
                    self.session_store.clone() as Arc<dyn broca_core::SessionStore>
                );
            let frozen_context_limit = context_limit_for_episode(
                fresh_episode,
                job.req.context_limit,
                resolved.context_limit,
                prior_run
                    .as_ref()
                    .and_then(|prior| prior.config.context_limit),
            );

            // Thread the pause-for-reauth instruction + the frozen auth-selection into the
            // loop's controls. The pause admits the run then writes RunPaused{AuthRequired}
            // (resumable). The selection is frozen into a fresh RunStarted's RunConfig so a
            // later resume re-gets the SAME credential (the loop ignores it on a resume —
            // the value is already durable). Mock stays None; a fresh OpenCode run carries the
            // method-qualified id discovered from its auth-file entry.
            let mut control = job.control;
            control.hook_plane = self.hook_plane();
            // Module subscribers source ordinary AssistantMessage/ToolCall/ToolResult units from
            // the durable WAL projector. The actor's live-output task discards those same units,
            // so suppress their payload clones while retaining display, lifecycle, pause, and
            // error controls needed by the actor and direct loop consumers.
            control.emit_control_payloads = false;
            control.pause_for_auth_at_start = resolution.pause.clone();
            control.auth_selection = resolution.selection.clone();
            // Report this run's account evidence to routing when it has an admission id there:
            // every decision path except `legacy_static` (the operator's switch that bypasses
            // routing), fresh or resumed alike.
            let account_reporter = match (
                self.routing.as_ref(),
                resolution
                    .selection
                    .as_ref()
                    .and_then(|selection| selection.account_decision.as_ref())
                    .and_then(|decision| decision.admission_id.clone()),
            ) {
                (Some(routing), Some(admission_id)) => {
                    let view_digest = match &account_choice {
                        AccountChoice::Routed { view_digest, .. } => Some(view_digest.clone()),
                        _ => None,
                    };
                    Some(broca_module_serve::account_routing::RunReporter::start(
                        Arc::clone(routing),
                        admission_id,
                        job.req.run_id.0.clone(),
                        view_digest,
                        job.writer.durable_head(),
                    ))
                }
                _ => None,
            };
            control.account_reporter = account_reporter
                .clone()
                .map(|reporter| reporter as Arc<dyn broca_core::AccountReporter>);
            // Freeze the context limit onto a fresh run's RunStarted. A fresh run prefers the
            // caller override over the catalog; a resume uses only the WAL value.
            control.admitted_context_limit = frozen_context_limit;
            // What a new episode freezes onto RunStarted; core ignores it on a resume.
            job.req.context_management = Some(episode_context_management);

            // For the post-loop wire-auth-failure report: the credential id this run actually
            // USED (only when we proceeded with a vault token — `pause` is None means we got a
            // token and called the provider). A pre-loop `needs_reauth` pause is excluded (the
            // vault already knows; no provider call was made, so there's no wire rejection to
            // report). `None` on the mock/OpenCode paths (neither has a vault record version) or
            // a pre-loop pause.
            let used_credential = used_credential_for_report(
                &resolution.pause,
                resolution.selection,
                resolution.record_version,
            );
            // Read after the run: an in-step refresh may have moved the run to a newer token,
            // and the report must name the version the provider rejected last.
            let served_version = resolution
                .in_run_refresh
                .as_ref()
                .map(|refresh| Arc::clone(&refresh.served_version));
            let reported_version = resolution
                .in_run_refresh
                .as_ref()
                .map(|refresh| Arc::clone(&refresh.reported_version));
            let session = job.req.session.clone();

            resolved
                .provider
                .set_chatgpt_https_policy(broca_module_serve::transport_config::default_policy());
            let summary = run_loop_on_writer_admitted_with_prior_snapshot(
                job.req,
                &job.writer,
                mode,
                &resolved.provider,
                self.plane.as_ref(),
                &AllowAll,
                self.wal_store.as_ref(),
                materialized_store,
                control,
                &out,
                job.on_admitted,
                Some(prior_snapshot),
            )
            .await;

            // A finished run (not a paused one) has made its terminal durable before the loop
            // returned, and nothing has been appended since, so the durable head is the
            // terminal's own sequence number.
            if let (Some(reporter), Ok(finished)) = (&account_reporter, &summary) {
                if finished.paused.is_none() {
                    reporter.finish(
                        job.writer.lineage_id(),
                        job.writer.durable_head().load(Ordering::Acquire),
                        finished.reason,
                    );
                }
            }

            // Report a WIRE auth rejection to the vault so it invalidates a stale token
            // (marks it needs_reauth) instead of serving it again next run. This fires only
            // when the provider REJECTED a token we got from the vault — i.e. the vault
            // thought the credential was fresh but the provider returned 401 mid-run (the run
            // then paused AuthRequired, recording the status). Fire-and-forget; a 5xx/429 is a
            // provider hiccup rather than a dead credential — see `terminal_wire_auth_status`.
            //
            // IN PRACTICE ONLY 401 REACHES HERE. This comment used to say 401/403, and
            // `wire_auth_status` still matches both, but HTTP 403 now classifies Permanent
            // rather than Auth (transport.rs, "forbidden" is an entitlement refusal, not a stale
            // token) and the class check rejects it before the status match runs. No shipped
            // classifier currently pairs Auth with a 403.
            //
            // KEEP THE 403 ARM. It is not dead code, it is code waiting on a decision that is
            // still open: whether a given provider's 403 means "entitlement refused" or "token
            // is stale" is a judgment, and if one is ever reclassified back to Auth this arm
            // starts firing correctly with no other change. Deleting it would make that future
            // flip silently incomplete.
            //
            // Worth stating because nothing failed when that reclassification landed: the guard
            // kept compiling and kept passing its tests while the condition it named moved
            // elsewhere. An arm whose description outlives its trigger reads as coverage.
            if let (Some(vault), Some((cred_id, admitted_version))) =
                (self.vault.as_ref(), used_credential)
            {
                let record_version = served_version
                    .map(|served| served.load(Ordering::SeqCst))
                    .unwrap_or(admitted_version);
                if let Some(status) =
                    post_run_wire_auth_status(&summary, self.wal_store.clone(), session.clone())
                        .await
                {
                    // Provider rejection is authoritative even if the best-effort vault report
                    // cannot be delivered; never let this process serve the rejected token again.
                    evict_for_provider_rejection(&vault.cache, &cred_id, status, record_version);
                    // The in-step refresh already reported this version when it tried to
                    // recover; that report was the refresh trigger, and a second one for the
                    // same rejection would count one incident twice.
                    if !run_end_report_is_duplicate(reported_version.as_deref(), record_version) {
                        let _ =
                            report_wire_auth_failure(vault, &cred_id, status, record_version).await;
                    }
                }
            }

            summary
        }

        fn on_run_sealed(&self, sealed: broca_module::SealedRun) {
            if let Some(routing) = self.routing.as_ref() {
                broca_module_serve::account_routing::report_sealed(Arc::clone(routing), &sealed);
            }
        }

        async fn on_session_close(&self, session: &BindIdentity) {
            // Retire this session's tool and role routes on their shared consumer connections.
            self.plane.close_session_routes(session).await;
            if let Some(hooks) = &self.hooks {
                hooks.close_session_routes(session).await;
            }
            if let Some(plane) = &self.compaction_plane {
                plane.close_session_routes(session).await;
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        include!("provider_override_tests.rs");
        include!("vault_request_tests.rs");
        include!("role_plane_tests.rs");

        fn evict_for_provider_rejection(cache: &VaultLkgCache, credential_id: &str, status: u16) {
            let Some(record_version) = cache.get(credential_id).map(|token| token.record_version)
            else {
                return;
            };
            super::evict_for_provider_rejection(cache, credential_id, status, record_version);
        }

        /// A new episode whose request has `inherit_frozen_cache` set (a steer or interrupt
        /// that omitted `cache`) takes the cache policy and caller cache key frozen in the
        /// session's latest render config, never a policy resolved from its own absent cache
        /// request. A resume, a send that named its cache, and a session with no episode take
        /// none.
        #[test]
        fn an_inheriting_episode_takes_the_frozen_cache_policy_and_key() {
            let policy = marker_policy(broca_provider::wire::CacheTtl::OneHour);
            let mut frozen = broca_provider::families::anthropic_frozen_config("claude-test");
            frozen.cache = Some(policy.clone());
            frozen.prompt_cache_key = Some("frozen-key".into());
            let mut req = drift_request(std::path::Path::new("/inherit-cache"));
            let config = broca_wal::RunConfig {
                model: req.model.clone(),
                tools: req.tools.clone().into(),
                tool_choice: req.tool_choice.clone(),
                generation: req.generation.clone(),
                response_format: Default::default(),
                provider_options: serde_json::Value::Null,
                cache_tiers: serde_json::Value::Null,
                context_epoch: 0,
                renderer_version: frozen.renderer_version,
                frozen_render_config: serde_json::to_value(&frozen).unwrap(),
                auth_selection: None,
                tools_were_omitted: false,
                budget: None,
                context_limit: None,
                work_class: None,
                variant: None,
                service_tier: None,
                stop_when: vec![],
                context_management: None,
            };
            req.inherit_frozen_cache = true;
            assert_eq!(
                inherited_cache(&req, true, Some(&config)),
                Some((Some(policy), Some("frozen-key".into())))
            );
            assert_eq!(
                inherited_cache(&req, false, Some(&config)),
                None,
                "a resume"
            );
            assert_eq!(inherited_cache(&req, true, None), None, "no episode");
            req.inherit_frozen_cache = false;
            assert_eq!(
                inherited_cache(&req, true, Some(&config)),
                None,
                "a named cache"
            );
        }

        fn marker_policy(ttl: broca_provider::wire::CacheTtl) -> broca_provider::wire::CachePolicy {
            broca_provider::wire::CachePolicy {
                version: 1,
                class: broca_provider::wire::CacheClass::Markers {
                    syntax: broca_provider::wire::MarkerSyntax::AnthropicCacheControl,
                    ttl,
                    max_breakpoints: 4,
                    lookback_blocks: 20,
                    tail: None,
                    omit_ttl: false,
                },
            }
        }

        /// What `resolve_marker_policy` produces for a caller that declared a volatile tail on a
        /// marker family: the declaration is frozen into the policy and reaches `plan_markers`.
        fn marker_policy_honouring_tail(
            ttl: broca_provider::wire::CacheTtl,
        ) -> broca_provider::wire::CachePolicy {
            broca_provider::wire::CachePolicy {
                version: 1,
                class: broca_provider::wire::CacheClass::Markers {
                    syntax: broca_provider::wire::MarkerSyntax::AnthropicCacheControl,
                    ttl,
                    max_breakpoints: 4,
                    lookback_blocks: 20,
                    tail: Some(broca_protocol::request::CacheTail::Volatile),
                    omit_ttl: false,
                },
            }
        }

        fn hints_policy() -> broca_provider::wire::CachePolicy {
            broca_provider::wire::CachePolicy {
                version: 1,
                class: broca_provider::wire::CacheClass::Hints {
                    cache_key: "k".into(),
                    retention: None,
                },
            }
        }

        fn cache_request(
            role: Option<broca_protocol::request::CacheRole>,
            ttl: Option<&str>,
            key: Option<&str>,
            tail: Option<broca_protocol::request::CacheTail>,
        ) -> broca_protocol::request::CacheRequest {
            broca_protocol::request::CacheRequest {
                role,
                ttl: ttl.map(str::to_string),
                key: key.map(str::to_string),
                tail,
                mode: None,
            }
        }

        fn a_tool() -> broca_protocol::request::Tool {
            broca_protocol::request::Tool {
                name: "read".into(),
                description: String::new(),
                input_schema: serde_json::json!({"type": "object"}),
                module: None,
                execution_mode: broca_protocol::request::ExecutionMode::Parallel,
            }
        }

        #[test]
        fn context_limit_precedence_is_caller_then_catalog_then_none() {
            assert_eq!(
                context_limit_for_episode(true, Some(64_000), Some(128_000), None),
                Some(64_000),
                "a caller override wins over the catalog"
            );
            assert_eq!(
                context_limit_for_episode(true, None, Some(128_000), None),
                Some(128_000),
                "an omitted override falls back to the catalog"
            );
            assert_eq!(
                context_limit_for_episode(true, None, None, None),
                None,
                "two absent sources remain unknown rather than becoming zero"
            );
        }

        #[test]
        fn cache_markers_warn_only_when_the_run_cannot_read_them_back() {
            let ttl = broca_provider::wire::CacheTtl::FiveMinutes;

            // The reportable shape: paid breakpoints on a run that makes one provider call.
            let warning = single_call_cache_marker_warning(&Some(marker_policy(ttl)), &[])
                .expect("markers with no tools are paid for without an in-run read");
            assert!(warning.contains("no tools"), "warning names the cause");

            // THE CLAIM THE OLD TEXT GOT WRONG, pinned so it cannot come back. The scope is this
            // run, not this prefix: a later request re-sending the same leading bytes reads the
            // cache within its ttl, because the provider keys on content and not on a session.
            // Asserting on absence rather than presence -- the failure being guarded is a
            // confident sentence about the future, and any spelling of it is wrong.
            for forbidden in ["cannot be read back", "unreadable", "wasted write"] {
                assert!(
                    !warning.contains(forbidden),
                    "the warning must not claim the write is unrecoverable, found {forbidden:?} \
                     in {warning:?}"
                );
            }
            assert!(
                warning.contains("later request") && warning.contains("ttl"),
                "the warning must name the condition that repays the premium: {warning:?}"
            );

            // A run that can dispatch may reach a second step, so the write may be read back.
            assert!(
                single_call_cache_marker_warning(&Some(marker_policy(ttl)), &[a_tool()]).is_none(),
                "a tool-bearing run can read its own cache write"
            );

            // Hints carry a cache key rather than paid breakpoints, so a single-step run is free.
            let hints = broca_provider::wire::CachePolicy {
                version: 1,
                class: broca_provider::wire::CacheClass::Hints {
                    cache_key: "k".into(),
                    retention: None,
                },
            };
            assert!(
                single_call_cache_marker_warning(&Some(hints), &[]).is_none(),
                "hint policies cost nothing extra on a single-step run"
            );

            // No policy at all is the common case and must stay silent.
            assert!(single_call_cache_marker_warning(&None, &[]).is_none());
        }

        /// A caller states a fact about their own prompt with `cache.tail`. Two resolutions
        /// throw it away, and a thrown-away declaration is indistinguishable from an honoured
        /// one from where the caller stands -- so both must be said out loud.
        #[test]
        fn a_dropped_cache_tail_declaration_is_reported_to_the_caller() {
            use broca_protocol::request::{CacheRole, CacheTail};

            // Key-only: no role and no ttl, so resolution returns no policy at all and there is
            // no breakpoint plan for the declaration to change.
            let key_only = cache_request(
                None,
                None,
                Some("alfonso-classify-v1"),
                Some(CacheTail::Volatile),
            );
            let warning = dropped_cache_tail_warning(Some(&key_only), &None, "anthropic")
                .expect("a tail declaration with no resolved marker policy is dropped");
            assert!(
                warning.contains("cache.role") && warning.contains("cache.ttl"),
                "the warning names what the caller must send to turn markers on: {warning:?}"
            );

            // Hint families carry a cache key rather than placed breakpoints, so there is no
            // message anchor for the declaration to withhold.
            let declared = cache_request(
                Some(CacheRole::Primary),
                None,
                None,
                Some(CacheTail::Volatile),
            );
            let warning =
                dropped_cache_tail_warning(Some(&declared), &Some(hints_policy()), "openai")
                    .expect("a tail declaration on a hint family is dropped");
            assert!(
                warning.contains("openai") && warning.contains("hints"),
                "the warning names the provider and why it cannot act on the declaration: \
                 {warning:?}"
            );
        }

        /// The controls. A warning that also fires when the declaration was honoured, or when
        /// none was made, is noise the caller learns to skip -- which costs exactly the
        /// visibility the warning was added for.
        #[test]
        fn an_honoured_or_absent_cache_tail_declaration_stays_silent() {
            use broca_protocol::request::{CacheRole, CacheTail};
            let ttl = broca_provider::wire::CacheTtl::FiveMinutes;

            let declared = cache_request(
                Some(CacheRole::Primary),
                Some("5m"),
                None,
                Some(CacheTail::Volatile),
            );
            assert!(
                dropped_cache_tail_warning(
                    Some(&declared),
                    &Some(marker_policy_honouring_tail(ttl)),
                    "anthropic"
                )
                .is_none(),
                "a declaration the marker plan acts on has not been dropped"
            );

            // The same two drop paths, with no declaration to drop.
            let key_only = cache_request(None, None, Some("alfonso-classify-v1"), None);
            assert!(
                dropped_cache_tail_warning(Some(&key_only), &None, "anthropic").is_none(),
                "a key-only request that declared nothing is the common case and stays silent"
            );
            let role_only = cache_request(Some(CacheRole::Primary), None, None, None);
            assert!(
                dropped_cache_tail_warning(Some(&role_only), &Some(hints_policy()), "openai")
                    .is_none(),
                "a hint-family request that declared nothing stays silent"
            );
            assert!(
                dropped_cache_tail_warning(None, &None, "anthropic").is_none(),
                "a request with no cache preferences at all stays silent"
            );
        }

        /// A mock launch must not read the developer's auth-methods config.
        ///
        /// CKTUI's shape: the rule lived in a comment inside `async fn main`, where nothing
        /// could call it, so a refactor could drop the branch with every test still green. The
        /// loader here PANICS if reached, which is what makes the mock arm mean something -- a
        /// test that merely checked the returned config was empty would pass even if the file
        /// had been read and happened to be empty.
        #[test]
        fn a_mock_launch_never_reads_the_real_auth_methods_config() {
            let mocked = crate::method_config_for_launch(true, || {
                panic!("a mock launch must not touch the auth-methods config on disk")
            })
            .expect("the mock arm returns an empty config");
            // An empty config overrides nothing, so the caller's fallback survives. Uses the
            // real resolver rather than an accessor so the assertion is about the behaviour
            // that matters at admission.
            let resolved = broca_module_serve::auth_routing::resolve_credential_id(
                "anthropic",
                None,
                None,
                &mocked,
                broca_module_serve::AuthMethod::ApiKey,
            );
            assert_eq!(
                resolved.method,
                broca_module_serve::AuthMethod::ApiKey,
                "the mock arm must carry no provider defaults to override the caller's"
            );

            // The real arm must actually CALL the loader, or the mock assertion is vacuous:
            // returning empty unconditionally would satisfy it too.
            let mut loaded = false;
            let _ = crate::method_config_for_launch(false, || {
                loaded = true;
                Ok(broca_module_serve::MethodConfig::empty())
            });
            assert!(
                loaded,
                "a real launch must load the config rather than skipping it"
            );
        }

        #[test]
        fn auth_source_parses_default_and_explicit() {
            // Default (unset/empty) is now the vault.
            assert_eq!(AuthSource::from_env(None).unwrap(), AuthSource::Vault);
            assert_eq!(AuthSource::from_env(Some("")).unwrap(), AuthSource::Vault);
            assert_eq!(
                AuthSource::from_env(Some(" vault ")).unwrap(),
                AuthSource::Vault
            );
            // opencode is the explicit opt-back-in to the pre-vault auth file.
            assert_eq!(
                AuthSource::from_env(Some("opencode")).unwrap(),
                AuthSource::Opencode
            );
            // A non-empty unrecognized value is a loud misconfiguration, not a silent default.
            assert!(AuthSource::from_env(Some("voult")).is_err());
        }

        #[test]
        fn family_for_method_drives_wire_changing_families() {
            // Header-only methods keep the catalog family (None); wire-changing methods name
            // their family — the single authority that drives resolve_provider AND the guard.
            assert_eq!(family_for_method(AuthMethod::ApiKey), None);
            assert_eq!(family_for_method(AuthMethod::Oauth), None);
            assert_eq!(
                family_for_method(AuthMethod::Antigravity),
                Some(WireFamilyId::GoogleCodeAssist)
            );
            assert_eq!(
                family_for_method(AuthMethod::Chatgpt),
                Some(WireFamilyId::OpenAiResponsesChatgpt)
            );
        }

        fn load_method_config(label: &str, defaults: &str) -> broca_module_serve::MethodConfig {
            let path = broca_test_support::scratch_root().join(format!(
                "broca-opencode-methods-{label}-{}.json",
                std::process::id()
            ));
            std::fs::write(&path, format!(r#"{{"defaults":{defaults}}}"#)).unwrap();
            let methods = broca_module_serve::MethodConfig::load(&path).unwrap();
            std::fs::remove_file(path).unwrap();
            methods
        }

        #[test]
        fn caller_auth_pin_wins_over_provider_config_at_admission() {
            let methods = load_method_config("caller-pin", r#"{"openai":"oauth"}"#);
            let auth = AuthRequest {
                method: "apikey".into(),
                account: None,
            };
            let (pinned, account) =
                caller_auth_for_episode(Some(&auth), episode_auth_context(true, false)).unwrap();

            let selected = broca_module_serve::auth_routing::select_credential_id(
                None,
                "openai",
                pinned,
                account,
                &methods,
                FALLBACK_METHOD,
            );
            assert_eq!(selected.to_id_string(), "apikey:openai");
        }

        #[test]
        fn caller_auth_account_rides_through_to_credential_id() {
            let methods = load_method_config("caller-account", r#"{"openai":"oauth"}"#);
            let auth = AuthRequest {
                method: "apikey".into(),
                account: Some("astro".into()),
            };
            let (pinned, account) =
                caller_auth_for_episode(Some(&auth), episode_auth_context(true, false)).unwrap();

            let selected = broca_module_serve::auth_routing::select_credential_id(
                None,
                "openai",
                pinned,
                account,
                &methods,
                FALLBACK_METHOD,
            );
            assert_eq!(selected.to_id_string(), "apikey:openai:astro");
        }

        #[test]
        fn caller_auth_pin_is_refused_on_a_genuine_resume() {
            let auth = AuthRequest {
                method: "apikey".into(),
                account: None,
            };
            let error = caller_auth_for_episode(Some(&auth), episode_auth_context(false, true))
                .unwrap_err();

            assert!(matches!(
                error,
                RunError::Config(message)
                    if message.contains("genuine resume of an unfinished episode")
                        && message.contains("new episode after the prior episode finishes")
            ));
        }

        #[test]
        fn caller_auth_pin_is_honoured_for_a_new_episode_with_session_history() {
            let methods = load_method_config("new-episode-pin", r#"{"openai":"oauth"}"#);
            let auth = AuthRequest {
                method: "apikey".into(),
                account: Some("astro".into()),
            };
            let context = episode_auth_context(true, true);
            assert_eq!(context, EpisodeAuthContext::NewEpisode);
            let (pinned, account) = caller_auth_for_episode(Some(&auth), context).unwrap();

            let selected = broca_module_serve::auth_routing::select_credential_id(
                None,
                "openai",
                pinned,
                account,
                &methods,
                FALLBACK_METHOD,
            );
            assert_eq!(selected.to_id_string(), "apikey:openai:astro");
        }

        #[test]
        fn unknown_caller_auth_method_is_a_typed_config_refusal() {
            let auth = AuthRequest {
                method: "api-key".into(),
                account: None,
            };
            let error = caller_auth_for_episode(Some(&auth), episode_auth_context(true, false))
                .unwrap_err();

            assert!(matches!(
                error,
                RunError::Config(message)
                    if message.contains("unknown auth method 'api-key'")
                        && message.contains("'apikey', 'oauth', 'antigravity', and 'chatgpt'")
            ));
        }

        fn install_test_catalog() {
            let catalog = broca_catalog::Catalog::from_json_str(
                include_str!("../../broca-catalog/data/models.dev.json"),
                include_str!("../../broca-catalog/data/overlay.json"),
            )
            .expect("legacy test snapshot is well formed");
            let cache = Arc::new(broca_catalog::CatalogCache::empty());
            cache.install_parsed_for_test(&catalog, 0, broca_catalog::CatalogSource::Fusiform);
            broca_catalog::install_global_catalog_cache(cache);
        }

        fn resolve_for_test(
            model: &str,
            method: Option<AuthMethod>,
            family_override: Option<WireFamilyId>,
        ) -> broca_catalog::live::ProviderResolution {
            install_test_catalog();
            let result = broca_catalog::live::resolve_provider(
                model,
                None,
                2048,
                &AuthMode::Bearer(
                    "eyJhbGciOiJub25lIn0.eyJjaGF0Z3B0X2FjY291bnRfaWQiOiAiYWNjdC10ZXN0In0.sig"
                        .into(),
                ),
                &broca_catalog::live::ResolveOptions {
                    claude_code_persona: None,
                    auth_method: method,
                    family_override,
                    project_id: None,
                    session_id: Some("opencode-test"),
                    account_id: None,
                    cache_policy: None,
                    prompt_cache_key: None,
                    variant: None,
                    service_tier: None,
                    secret_refresher: None,
                    structured_output: None,
                    frozen_account_ref: None,
                    clock: None,
                },
            );
            match result {
                Ok(resolved) => resolved,
                Err(error) => panic!("provider resolution failed: {error}"),
            }
        }

        #[test]
        fn opencode_freeze_uses_discovered_method_and_preserves_resume_selection() {
            let methods = load_method_config(
                "stamp",
                r#"{"deepseek":"apikey","anthropic":"oauth","google":"antigravity"}"#,
            );

            let fresh = opencode_auth_for_episode(
                true,
                None,
                Some(AuthMethod::ApiKey),
                None,
                &methods,
                "deepseek",
            );
            assert_eq!(fresh.method, Some(AuthMethod::ApiKey));
            assert_eq!(
                fresh.selection.as_ref().unwrap().resolved_credential_id,
                None
            );
            // OpenCode auth makes no vault request, so none is recorded.
            assert_eq!(fresh.selection.as_ref().unwrap().vault_request, None);
            assert_eq!(
                fresh.selection.map(|selection| selection.credential_id),
                Some("apikey:deepseek".to_string())
            );

            // A configured special-backend method wins over the generic OAuth entry type, and
            // both the family driver and durable stamp consume that same method.
            let antigravity = opencode_auth_for_episode(
                true,
                None,
                Some(AuthMethod::Oauth),
                None,
                &methods,
                "google",
            );
            assert_eq!(antigravity.method, Some(AuthMethod::Antigravity));
            assert_eq!(
                antigravity
                    .selection
                    .as_ref()
                    .map(|selection| selection.credential_id.as_str()),
                Some("antigravity:google")
            );
            assert_eq!(
                antigravity.method.and_then(family_for_method),
                Some(WireFamilyId::GoogleCodeAssist)
            );

            let empty_methods = broca_module_serve::MethodConfig::empty();
            let unknown =
                opencode_auth_for_episode(true, None, None, None, &empty_methods, "unknown");
            assert!(unknown.selection.is_none());
            assert_eq!(unknown.method, None);

            let generic_oauth = opencode_auth_for_episode(
                true,
                None,
                Some(AuthMethod::Oauth),
                None,
                &empty_methods,
                "provider-with-oauth",
            );
            assert_eq!(generic_oauth.method, Some(AuthMethod::Oauth));
            assert_eq!(
                generic_oauth
                    .selection
                    .map(|selection| selection.credential_id),
                Some("oauth:provider-with-oauth".to_string())
            );

            // A legacy OpenCode WAL with no selection remains unselected on resume.
            let legacy = opencode_auth_for_episode(
                false,
                None,
                Some(AuthMethod::Oauth),
                None,
                &methods,
                "deepseek",
            );
            assert!(legacy.selection.is_none());
            assert_eq!(legacy.method, None);

            // A newer resume carries the exact frozen id and ignores both current config and the
            // auth-file entry type.
            let resumed = opencode_auth_for_episode(
                false,
                Some("oauth:deepseek:work"),
                Some(AuthMethod::ApiKey),
                None,
                &methods,
                "deepseek",
            );
            assert_eq!(resumed.method, Some(AuthMethod::Oauth));
            assert_eq!(
                resumed.selection.as_ref().unwrap().resolved_credential_id,
                None
            );
            assert_eq!(resumed.selection.as_ref().unwrap().vault_request, None);
            assert_eq!(
                resumed.selection.map(|selection| selection.credential_id),
                Some("oauth:deepseek:work".to_string())
            );
        }

        #[test]
        fn configured_opencode_chatgpt_selects_the_codex_family() {
            let methods = load_method_config("chatgpt", r#"{"openai":"chatgpt"}"#);
            let auth = opencode_auth_for_episode(
                true,
                None,
                Some(AuthMethod::Oauth),
                None,
                &methods,
                "openai",
            );
            validate_opencode_method_metadata("openai", auth.method).unwrap();
            assert_eq!(
                auth.selection
                    .as_ref()
                    .map(|selection| selection.credential_id.as_str()),
                Some("chatgpt:openai")
            );

            let family = auth.method.and_then(family_for_method);
            let resolved = resolve_for_test("openai/gpt-5.3-codex", auth.method, family);
            assert_eq!(resolved.family_id, WireFamilyId::OpenAiResponsesChatgpt);
        }

        #[test]
        fn chatgpt_pause_only_construction_is_gated_and_repaired_resume_is_strict() {
            install_test_catalog();
            let options = broca_catalog::live::ResolveOptions {
                claude_code_persona: None,
                auth_method: Some(AuthMethod::Chatgpt),
                family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
                project_id: None,
                session_id: Some("chatgpt-pause-test"),
                account_id: None,
                cache_policy: None,
                prompt_cache_key: None,
                variant: None,
                service_tier: None,
                secret_refresher: None,
                structured_output: None,
                frozen_account_ref: None,
                clock: None,
            };
            let paused = AuthResolution {
                mode: AuthMode::None,
                pause: Some(ProviderError {
                    provider_subcode: None,
                    refusal: None,
                    class: ErrorClass::Auth,
                    message: "needs reauth".into(),
                    status: None,
                    retry_after_secs: None,
                    provider_code: Some("needs_reauth".into()),
                    resets_at_ms: None,
                    resets_at_source: None,
                    limit_window: None,
                    account_ref: None,
                    credential_id: None,
                }),
                selection: Some(broca_core::AuthSelection {
                    credential_id: "chatgpt:openai".into(),
                    account_ref: None,
                    resolved_credential_id: None,
                    vault_request: None,
                    account_decision: None,
                }),
                method: Some(AuthMethod::Chatgpt),
                project_id: None,
                account_id: None,
                record_version: None,
                in_run_refresh: None,
            };
            let pause_provider = resolve_provider_for_episode(
                "openai/gpt-5.3-codex",
                None,
                Some(2048),
                &paused,
                &options,
                broca_provider::build_provider_http_client(),
                broca_catalog::live::EpisodeRenderConfig::default(),
            )
            .unwrap_or_else(|error| panic!("pause-only provider must resolve: {error}"));
            assert_eq!(pause_provider.auth_desc, "chatgpt-pause-only(openai)");

            let repaired_options = broca_catalog::live::ResolveOptions {
                account_id: Some("acct-repaired"),
                ..options.clone()
            };
            let repaired = AuthResolution {
                mode: AuthMode::Secret("repaired-chatgpt-token".into()),
                pause: None,
                selection: paused.selection.clone(),
                method: Some(AuthMethod::Chatgpt),
                project_id: None,
                account_id: Some("acct-repaired".into()),
                record_version: Some(9),
                in_run_refresh: None,
            };
            let resumed_provider = resolve_provider_for_episode(
                "openai/gpt-5.3-codex",
                None,
                Some(2048),
                &repaired,
                &repaired_options,
                broca_provider::build_provider_http_client(),
                broca_catalog::live::EpisodeRenderConfig::default(),
            )
            .unwrap_or_else(|error| panic!("repaired resume must resolve strictly: {error}"));
            assert_eq!(resumed_provider.auth_desc, "chatgpt(openai)");
            assert_eq!(
                resumed_provider.provider.render_config_blob(),
                pause_provider.provider.render_config_blob(),
                "auth headers stay outside RunConfig while repaired resume rebuilds them strictly"
            );

            let unpaused_missing_auth = AuthResolution {
                pause: None,
                ..paused
            };
            let error = match resolve_provider_for_episode(
                "openai/gpt-5.3-codex",
                None,
                Some(2048),
                &unpaused_missing_auth,
                &options,
                broca_provider::build_provider_http_client(),
                broca_catalog::live::EpisodeRenderConfig::default(),
            ) {
                Ok(_) => panic!("AuthMode::None without a pause must fail"),
                Err(error) => error,
            };
            assert!(
                error.to_string().contains("no ChatGPT token"),
                "unexpected error: {error}"
            );
        }

        #[test]
        fn opencode_antigravity_fails_loud_without_project_metadata_and_resume_keeps_method() {
            let configured = load_method_config("antigravity", r#"{"google":"antigravity"}"#);
            let fresh = opencode_auth_for_episode(
                true,
                None,
                Some(AuthMethod::Oauth),
                None,
                &configured,
                "google",
            );
            assert_eq!(fresh.method, Some(AuthMethod::Antigravity));
            assert!(matches!(
                validate_opencode_method_metadata("google", fresh.method),
                Err(RunError::Config(message))
                    if message.contains("requires a project_id")
                        && message.contains("BROCA_AUTH_SOURCE=opencode")
            ));

            // Current config and auth-file discovery must not replace the frozen method.
            let removed_config = broca_module_serve::MethodConfig::empty();
            let resumed = opencode_auth_for_episode(
                false,
                Some("antigravity:google"),
                Some(AuthMethod::Oauth),
                None,
                &removed_config,
                "google",
            );
            assert_eq!(resumed.method, Some(AuthMethod::Antigravity));
            assert_eq!(
                resumed.method.and_then(family_for_method),
                Some(WireFamilyId::GoogleCodeAssist)
            );
            assert!(matches!(
                validate_opencode_method_metadata("google", resumed.method),
                Err(RunError::Config(message)) if message.contains("requires a project_id")
            ));
        }

        #[test]
        fn generic_opencode_methods_keep_catalog_family_render_bytes() {
            let opts = broca_protocol::request::CallOptions::new(Vec::new());
            let baseline = resolve_for_test("google/gemini-2.5-flash", None, None);
            let baseline_bytes = baseline.provider.render_request(&opts);
            let baseline_version = baseline.provider.renderer_version();
            assert_eq!(baseline.family_id, WireFamilyId::Gemini);

            for method in [AuthMethod::Oauth, AuthMethod::ApiKey] {
                let resolved = resolve_for_test("google/gemini-2.5-flash", Some(method), None);
                assert_eq!(resolved.family_id, WireFamilyId::Gemini);
                assert_eq!(resolved.provider.renderer_version(), baseline_version);
                assert_eq!(resolved.provider.render_request(&opts), baseline_bytes);
            }
        }

        /// The version served at get is the version echoed at report — and a report is
        /// only possible when the run actually held a served token. Pre-loop pauses and
        /// version-less resolutions produce no report identity at all.
        #[test]
        fn report_identity_pairs_served_version_and_requires_a_served_token() {
            let selection = || {
                Some(broca_core::AuthSelection {
                    credential_id: "oauth:anthropic".into(),
                    account_ref: None,
                    resolved_credential_id: None,
                    vault_request: None,
                    account_decision: None,
                })
            };
            // Served token at version 7 → report identity carries exactly that version.
            assert_eq!(
                used_credential_for_report(&None, selection(), Some(7)),
                Some(("oauth:anthropic".to_string(), 7))
            );
            // A pre-loop pause means no provider call — no report identity.
            let pause = Some(ProviderError {
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::Auth,
                message: "needs reauth".into(),
                status: None,
                retry_after_secs: None,
                provider_code: None,
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
            });
            assert_eq!(
                used_credential_for_report(&pause, selection(), Some(7)),
                None
            );
            // An OpenCode selection is metadata only; with no vault-served version it cannot
            // produce a report call to the credentials module.
            assert_eq!(used_credential_for_report(&None, selection(), None), None);
            assert_eq!(used_credential_for_report(&None, None, Some(7)), None);
        }

        #[test]
        fn vault_success_proceeds_with_secret() {
            let token = VaultToken {
                token: "tok-123".into(),
                expires_at_ms: Some(1),
                record_version: 1,
                project_id: None,
                account_id: None,
                resolved_credential_id: None,
            };
            match vault_outcome_to_decision(Ok(token)) {
                AuthDecision::Proceed(AuthMode::Secret(s)) => assert_eq!(s, "tok-123"),
                _ => panic!("a successful token must Proceed with AuthMode::Secret"),
            }
        }

        #[test]
        fn needs_reauth_maps_to_pause_for_reauth() {
            match vault_outcome_to_decision(Err(VaultError::NeedsReauth)) {
                AuthDecision::PauseForReauth(e) => {
                    assert_eq!(e.class, ErrorClass::Auth);
                    assert_eq!(e.provider_code.as_deref(), Some("needs_reauth"));
                }
                _ => panic!("needs_reauth must map to PauseForReauth"),
            }
        }

        #[test]
        fn connectivity_vault_errors_fail_admission_transiently() {
            for err in [
                VaultError::Transport("connection reset".into()),
                VaultError::VaultLocked,
                VaultError::RefreshFailed,
            ] {
                match vault_outcome_to_decision(Err(err.clone())) {
                    AuthDecision::FailAdmissionTransient(message) => {
                        assert!(message.contains("transient — retry"));
                        assert!(!message.contains("credential not configured"));
                    }
                    _ => panic!("{err:?} must fail admission transiently"),
                }
            }
        }

        #[test]
        fn configuration_vault_errors_fail_admission_permanently() {
            for err in [
                VaultError::NotFound,
                VaultError::Corrupt,
                VaultError::RefreshUnsupported,
                VaultError::Protocol("bad reply".into()),
            ] {
                match vault_outcome_to_decision(Err(err.clone())) {
                    AuthDecision::FailAdmission(message) => {
                        assert!(message.contains("credential not configured or unusable"));
                    }
                    _ => panic!("{err:?} must fail admission permanently"),
                }
            }
        }

        /// AN UNSATISFIABLE TTL DEMAND MUST NOT BE RETRYABLE, and this is the one arm where the
        /// wrong classification is self-sustaining rather than merely wrong.
        ///
        /// The vault evaluates `min_ttl_ms` against a FRESHLY MINTED token, so a retry asks the
        /// same impossible question and spends another upstream mint doing it. Classified
        /// transient, broca would loop against the provider's mint budget forever; classified
        /// permanent, the run fails once with a message naming the repair.
        ///
        /// This is a SEPARATE TEST from `configuration_vault_errors_fail_admission_permanently`
        /// rather than a fifth entry in its list, because that list is about config faults and
        /// this is about a demand THIS CODEBASE chose. Someone pruning that list would have no
        /// reason to keep a TTL case in it.
        ///
        /// Before the variant existed, `ttl_unsatisfiable` failed to DECODE and reached this same
        /// permanent classification via `VaultError::Protocol` -- the safe answer, by accident,
        /// one `#[serde(other)] Unknown => RefreshFailed` edit away from inverting.
        #[test]
        fn an_unsatisfiable_ttl_demand_fails_admission_permanently() {
            match vault_outcome_to_decision(Err(VaultError::TtlUnsatisfiable)) {
                AuthDecision::FailAdmission(message) => {
                    assert!(
                        message.contains("reduce the min_ttl_ms demand"),
                        "the refusal must name the repair, not just fail: {message}"
                    );
                    assert!(
                        !message.contains("transient"),
                        "a retry cannot succeed, so the message must not invite one: {message}"
                    );
                }
                // AuthDecision has no Debug (it carries credential-adjacent text), so name the
                // arms that would be wrong rather than printing the value.
                AuthDecision::FailAdmissionTransient(message) => panic!(
                    "an unsatisfiable ttl demand classified TRANSIENT -- this is the retry loop \
                     against the provider's mint budget: {message}"
                ),
                _ => panic!(
                    "an unsatisfiable ttl demand must fail admission permanently, got another arm"
                ),
            }
        }

        fn cached_token(expires_at_ms: Option<i64>) -> VaultToken {
            VaultToken {
                token: "secret-token".into(),
                expires_at_ms,
                record_version: 7,
                project_id: None,
                account_id: None,
                resolved_credential_id: None,
            }
        }

        /// AN UNREACHABLE VAULT SERVED FROM CACHE RECORDS NO REQUEST -- and still serves the token.
        ///
        /// This is the production case the record was wrong for: a warm cache, a vault that does
        /// not answer, and the LKG fallback serving the cached token. The run authenticates, so
        /// it writes a RunStarted and an export fact -- which used to carry `vault_request`,
        /// claiming a credential.get the vault never received.
        ///
        /// BOTH HALVES ARE ASSERTED, because each alone matches a wrong fix: recording nothing
        /// ever would pass the first half, and a fix that stopped the cache fallback from serving
        /// would pass the second. Only the right fix keeps the token AND drops the claim.
        #[test]
        fn an_unreachable_vault_served_from_cache_records_no_request() {
            let request = broca_core::VaultRequest {
                min_ttl_ms: Some(VAULT_MIN_TTL_MS),
                force_refresh: None,
                op: Some(broca_core::VaultReadOp::GetScoped),
            };
            let cache = VaultLkgCache::default();
            cache.insert("apikey:openai".to_string(), cached_token(None));
            let gauge = broca_module_serve::serve::VaultGauge::not_in_use();
            let raw = VaultGet::no_reply(VaultError::Transport("route never opened".into()));

            let recorded = recorded_vault_request(&raw, request);
            let served = resolve_vault_outcome(
                &cache,
                &gauge,
                "apikey:openai",
                raw.result,
                1_000,
                VAULT_MIN_TTL_MS,
            );

            assert!(
                served.is_ok(),
                "the cache fallback must still serve the token when the vault is unreachable"
            );
            assert_eq!(
                recorded, None,
                "no reply came back, so the run must not claim a request the vault may never \
                 have received"
            );
        }

        /// EVERY REPLY IS RECORDED, INCLUDING A REFUSAL -- the vault has a row for each.
        ///
        /// The control for the test above: without it, recording nothing at all would pass. A
        /// served token, an authoritative refusal, a locked vault and an undecodable reply all
        /// mean the request reached the vault, so the join against the vault's record must find
        /// broca's half.
        #[test]
        fn every_vault_reply_records_the_request_including_a_refusal() {
            let request = broca_core::VaultRequest {
                min_ttl_ms: Some(VAULT_MIN_TTL_MS),
                force_refresh: None,
                op: Some(broca_core::VaultReadOp::GetScoped),
            };
            let replies: Vec<(&str, VaultGet)> = vec![
                ("served", VaultGet::replied(Ok(cached_token(None)))),
                ("not found", VaultGet::replied(Err(VaultError::NotFound))),
                (
                    "needs reauth",
                    VaultGet::replied(Err(VaultError::NeedsReauth)),
                ),
                ("locked", VaultGet::replied(Err(VaultError::VaultLocked))),
                (
                    "undecodable",
                    VaultGet::replied(Err(VaultError::Protocol("bad reply".into()))),
                ),
            ];
            for (label, reply) in replies {
                assert_eq!(
                    recorded_vault_request(&reply, request.clone()),
                    Some(request.clone()),
                    "the vault replied ({label}), so the request must be recorded"
                );
            }
        }

        /// A REVOKED HANDLE MUST NOT COME BACK WHEN THE VAULT GOES DOWN.
        ///
        /// The sequence is three real steps, and only the third is surprising: a good `get`
        /// caches a token; the operator revokes the handle, so the next `get` answers `NotFound`
        /// and admission refuses; the vault then becomes unreachable, and the LKG path serves the
        /// cached token for the credential broca ALREADY KNOWS IS GONE.
        ///
        /// `NotFound` used to fall through to `Other`, which does not evict. So the same
        /// credential was REFUSED while the vault was reachable and ACCEPTED once it was not --
        /// an incoherence, not merely a liberal cache. API keys make it worst: they carry no
        /// expiry, so there is no TTL that eventually ends the resurrection.
        ///
        /// The `NeedsReauth` arm is the CONTROL. It already evicted, so if this test ever passes
        /// for both arms without the NotFound mapping, it is asserting the control and nothing
        /// else.
        #[test]
        fn an_authoritative_negative_evicts_so_an_outage_cannot_resurrect_it() {
            for authoritative in [VaultError::NotFound, VaultError::NeedsReauth] {
                let cache = VaultLkgCache::default();
                cache.insert(
                    "apikey:openai".to_string(),
                    VaultToken {
                        token: "cached-key".into(),
                        // No expiry: an API key. Nothing times this out on its own.
                        expires_at_ms: None,
                        record_version: 1,
                        project_id: None,
                        account_id: None,
                        resolved_credential_id: None,
                    },
                );

                let class = lkg_error_class(&authoritative);
                evict_if_vault_authoritative(&cache, "apikey:openai", class);

                assert_eq!(
                    class,
                    LkgErrorClass::CredentialUnusable,
                    "{authoritative} is an authoritative negative and must classify as such"
                );
                assert!(
                    cache.get("apikey:openai").is_none(),
                    "{authoritative} left the token cached, so a later outage would serve it"
                );

                // The outage that used to resurrect it now finds nothing to serve.
                let outage = lkg_error_class(&VaultError::Transport("vault down".into()));
                assert_eq!(outage, LkgErrorClass::Connectivity);
                assert_eq!(
                    lkg_decision(None, 0, VAULT_MIN_TTL_MS, outage),
                    LkgDecision::Refuse,
                    "with the entry evicted, an outage must refuse rather than fall back"
                );
            }
        }

        /// The other half of the rule: a refusal ABOUT THE REQUEST OR THE VAULT is not a verdict
        /// on the credential, so it must NOT evict. Widening the eviction to every non-connectivity
        /// error would discard tokens that are still good -- `RefreshFailed` is transient by its
        /// own definition, and `TtlUnsatisfiable` is about a number this code chose.
        #[test]
        fn a_request_shaped_refusal_does_not_evict_a_good_token() {
            for benign in [
                VaultError::RefreshFailed,
                VaultError::TtlUnsatisfiable,
                VaultError::TooManyItems,
            ] {
                let cache = VaultLkgCache::default();
                cache.insert(
                    "apikey:openai".to_string(),
                    VaultToken {
                        token: "cached-key".into(),
                        expires_at_ms: None,
                        record_version: 1,
                        project_id: None,
                        account_id: None,
                        resolved_credential_id: None,
                    },
                );
                evict_if_vault_authoritative(&cache, "apikey:openai", lkg_error_class(&benign));
                assert!(
                    cache.get("apikey:openai").is_some(),
                    "{benign} is not a verdict on the credential and must not evict it"
                );
            }
        }

        #[test]
        fn lkg_decision_enforces_connectivity_and_ttl_margin() {
            let now_ms = 1_000_000;
            let expiring = CachedCredentialMeta {
                expires_at_ms: Some(now_ms + VAULT_MIN_TTL_MS),
            };
            assert_eq!(
                lkg_decision(
                    Some(&expiring),
                    now_ms,
                    VAULT_MIN_TTL_MS,
                    LkgErrorClass::Connectivity,
                ),
                LkgDecision::Serve
            );

            let too_close = CachedCredentialMeta {
                expires_at_ms: Some(now_ms + VAULT_MIN_TTL_MS - 1),
            };
            assert_eq!(
                lkg_decision(
                    Some(&too_close),
                    now_ms,
                    VAULT_MIN_TTL_MS,
                    LkgErrorClass::Connectivity,
                ),
                LkgDecision::Refuse
            );

            let no_expiry = CachedCredentialMeta {
                expires_at_ms: None,
            };
            assert_eq!(
                lkg_decision(
                    Some(&no_expiry),
                    now_ms,
                    VAULT_MIN_TTL_MS,
                    LkgErrorClass::Connectivity,
                ),
                LkgDecision::Serve
            );
            assert_eq!(
                lkg_decision(
                    Some(&no_expiry),
                    now_ms,
                    VAULT_MIN_TTL_MS,
                    LkgErrorClass::CredentialUnusable,
                ),
                LkgDecision::Refuse
            );
        }

        #[test]
        fn successful_resolve_populates_lkg_for_connectivity_fallback() {
            let cache = VaultLkgCache::default();
            let gauge = broca_module_serve::serve::VaultGauge::disconnected();
            let now_ms = 1_000_000;
            let token = cached_token(Some(now_ms + VAULT_MIN_TTL_MS));

            let resolved = resolve_vault_outcome(
                &cache,
                gauge.as_ref(),
                "apikey:openai",
                Ok(token.clone()),
                now_ms,
                VAULT_MIN_TTL_MS,
            )
            .unwrap();
            assert_eq!(resolved, token);

            let fallback = resolve_vault_outcome(
                &cache,
                gauge.as_ref(),
                "apikey:openai",
                Err(VaultError::Transport("credentials offline".into())),
                now_ms,
                VAULT_MIN_TTL_MS,
            )
            .unwrap();
            assert_eq!(fallback, token);
        }

        #[test]
        fn late_older_vault_reply_cannot_regress_lkg() {
            let credential_id = "apikey:openai";
            let cache = VaultLkgCache::default();
            let gauge = broca_module_serve::serve::VaultGauge::disconnected();
            let now_ms = 1_000_000;
            let mut older = cached_token(None);
            older.record_version = 1;
            let mut newer = cached_token(Some(now_ms + VAULT_MIN_TTL_MS));
            newer.record_version = 2;

            // The newer read finishes first; the older reply still belongs to its run,
            // but must not replace the shared fallback for subsequent admissions.
            for reply in [newer.clone(), older] {
                let version = reply.record_version;
                let served = resolve_vault_outcome(
                    &cache,
                    gauge.as_ref(),
                    credential_id,
                    Ok(reply),
                    now_ms,
                    VAULT_MIN_TTL_MS,
                )
                .unwrap();
                assert_eq!(served.record_version, version);
            }
            let fallback = resolve_vault_outcome(
                &cache,
                gauge.as_ref(),
                credential_id,
                Err(VaultError::Transport("credentials offline".into())),
                now_ms,
                VAULT_MIN_TTL_MS,
            )
            .unwrap();
            assert_eq!(fallback.record_version, 2);

            // An equal-version expiry update is useful even without a token rotation.
            newer.expires_at_ms = Some(now_ms + 2 * VAULT_MIN_TTL_MS);
            cache.insert(credential_id.into(), newer);
            assert_eq!(
                cache.get(credential_id).unwrap().expires_at_ms,
                Some(now_ms + 2 * VAULT_MIN_TTL_MS)
            );
            super::evict_for_provider_rejection(&cache, credential_id, 401, 1);
            assert_eq!(cache.get(credential_id).unwrap().record_version, 2);
            super::evict_for_provider_rejection(&cache, credential_id, 401, 2);
            assert!(cache.get(credential_id).is_none());
            assert!(resolve_vault_outcome(
                &cache,
                gauge.as_ref(),
                credential_id,
                Err(VaultError::Transport("credentials offline".into())),
                now_ms,
                VAULT_MIN_TTL_MS,
            )
            .is_err());
        }

        #[test]
        fn authoritative_auth_failures_evict_lkg_entries() {
            let cache = VaultLkgCache::default();
            cache.insert("apikey:openai".into(), cached_token(None));
            evict_if_vault_authoritative(
                &cache,
                "apikey:openai",
                LkgErrorClass::CredentialUnusable,
            );
            assert!(cache.get("apikey:openai").is_none());

            cache.insert("apikey:openai".into(), cached_token(None));
            evict_for_provider_rejection(&cache, "apikey:openai", 401);
            assert!(cache.get("apikey:openai").is_none());

            cache.insert("apikey:openai".into(), cached_token(None));
            evict_for_provider_rejection(&cache, "apikey:openai", 500);
            assert!(cache.get("apikey:openai").is_some());
        }

        #[test]
        fn provider_rejection_evicts_only_the_failed_record_version() {
            let credential_id = "apikey:openai";
            let cache = VaultLkgCache::default();
            let mut current = cached_token(None);
            current.record_version = 8;
            cache.insert(credential_id.into(), current);

            super::evict_for_provider_rejection(&cache, credential_id, 401, 7);
            assert_eq!(
                cache.get(credential_id).map(|token| token.record_version),
                Some(8)
            );

            super::evict_for_provider_rejection(&cache, credential_id, 401, 8);
            assert!(cache.get(credential_id).is_none());
        }

        fn err_with_status(status: Option<u16>) -> ProviderError {
            ProviderError {
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::Auth,
                message: "x".into(),
                status,
                retry_after_secs: None,
                provider_code: None,
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
            }
        }

        fn framed(seq: u64, record: broca_wal::WalRecord) -> broca_wal::FramedRecord {
            broca_wal::FramedRecord {
                seq,
                fence: 1,
                record,
            }
        }

        fn auth_pause(seq: u64, status: u16) -> broca_wal::FramedRecord {
            framed(
                seq,
                broca_wal::WalRecord::RunPaused {
                    ts_ms: None,
                    run_id: broca_protocol::RunId("run-1".into()),
                    reason: broca_protocol::PauseReason::AuthRequired,
                    error: Some(err_with_status(Some(status))),
                },
            )
        }

        fn run_finished(seq: u64, error: Option<ProviderError>) -> broca_wal::FramedRecord {
            framed(
                seq,
                broca_wal::WalRecord::RunFinished {
                    ts_ms: None,
                    reason: if error.is_some() {
                        broca_protocol::caller::RunFinishReason::Error
                    } else {
                        broca_protocol::caller::RunFinishReason::Completed
                    },
                    usage: broca_protocol::Usage::default(),
                    error,
                    retries_used: 0,
                },
            )
        }

        fn run_started(seq: u64, run_id: &str) -> broca_wal::FramedRecord {
            framed(
                seq,
                broca_wal::WalRecord::RunStarted {
                    session_class: None,
                    retention: None,
                    hooks: None,
                    title: None,
                    on_restart: None,
                    keep_warm: None,
                    marker_order_version: None,
                    manifest: None,
                    scope: None,
                    send_id: None,
                    request_identity_digest: None,
                    project_id: None,
                    principal: None,
                    run_id: broca_protocol::RunId(run_id.into()),
                    session: BindIdentity::new("/tmp/project", "harness", "session"),
                    ts_ms: None,
                    config: Box::new(broca_wal::RunConfig {
                        model: broca_protocol::request::ModelHandle::new("openai", "gpt-test"),
                        tools: Vec::new().into(),
                        tool_choice: Default::default(),
                        generation: Default::default(),
                        response_format: Default::default(),
                        provider_options: serde_json::Value::Null,
                        cache_tiers: serde_json::Value::Null,
                        context_epoch: 0,
                        renderer_version: 1,
                        frozen_render_config: serde_json::Value::Null,
                        auth_selection: None,
                        tools_were_omitted: false,
                        budget: None,
                        context_limit: None,
                        work_class: None,
                        variant: None,
                        service_tier: None,
                        stop_when: Vec::new(),
                        context_management: None,
                    }),
                    input: Vec::new().into(),
                    origin: broca_wal::RunOrigin::Fresh,
                    submission_id: None,
                },
            )
        }

        fn latest_wire_auth_status(records: &[broca_wal::FramedRecord]) -> Option<u16> {
            latest_terminal_error(records).and_then(wire_auth_status)
        }

        /// A provider's refusal of a requested service tier on a 403, classified exactly as the
        /// transport classifies it, is permanent with the stable tier code and is NOT reported to
        /// the vault as a dead credential. A genuine auth 403 on the same tier-carrying request,
        /// whose error names the key, is still `auth_required` and still reported.
        #[test]
        fn a_service_tier_refusal_403_is_never_reported_to_the_vault() {
            let tier = broca_provider::service_tier::ServiceTierRequest {
                tier: broca_catalog::live::ServiceTier::Priority,
                provider: "anthropic".into(),
            };
            let family = WireFamilyId::AnthropicMessages;
            let refusal = broca_provider::transport::classify_http_error(
                403,
                r#"{"type":"error","error":{"type":"permission_error","message":"Fast mode is not available for your organization."}}"#,
                None,
                Some((family, &tier)),
            );
            assert_eq!(refusal.class, ErrorClass::Permanent);
            assert_eq!(
                refusal.provider_code.as_deref(),
                Some(broca_provider::service_tier::SERVICE_TIER_UNAVAILABLE)
            );
            assert_eq!(wire_auth_status(&refusal), None);

            let dead_key = broca_provider::transport::classify_http_error(
                403,
                r#"{"type":"error","error":{"type":"authentication_error","message":"invalid x-api-key"}}"#,
                None,
                Some((family, &tier)),
            );
            assert_eq!(dead_key.class, ErrorClass::Auth);
            assert_eq!(wire_auth_status(&dead_key), Some(403));

            // Stored as the run's terminal error, the refusal still yields no status to report.
            let mut records = vec![run_started(1, "tier-run")];
            records.push(run_finished(2, Some(refusal)));
            assert_eq!(latest_wire_auth_status(&records), None);
        }

        /// An exhausted account is a rate limit on a healthy credential. Reporting it would force
        /// a refresh of a token that works, and on a subscription account that rotates the
        /// refresh token for nothing.
        #[test]
        fn an_account_limit_refusal_is_never_reported_to_the_vault() {
            let exhausted = ProviderError {
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::Transient,
                message: "429: account limit".into(),
                status: Some(429),
                retry_after_secs: Some(352_835.0),
                provider_code: Some(broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED.into()),
                resets_at_ms: Some(1_790_740_800_000),
                resets_at_source: None,
                limit_window: Some("seven_day".into()),
                account_ref: Some("d4bc9dbc".into()),
                credential_id: None,
            };
            assert_eq!(wire_auth_status(&exhausted), None);
            let mut records = vec![run_started(1, "limit-run")];
            records.push(run_finished(2, Some(exhausted)));
            assert_eq!(latest_wire_auth_status(&records), None);
        }

        /// An Anthropic API key that has run out of prepaid credit is a billing problem, not an
        /// invalid key, so it must never be reported to the vault as an auth failure. The refusal
        /// goes through the transport's real `classify_http_error`, and no auth status is derived
        /// from it, neither from the error itself nor from it stored as the run's terminal. The
        /// body is Anthropic's public shape, assumed rather than captured (see
        /// `anthropic_credit_balance_too_low` in the transport).
        #[test]
        fn an_anthropic_credit_refusal_is_never_reported_to_the_vault() {
            let refusal = broca_provider::transport::classify_http_error(
                400,
                r#"{"type":"error","error":{"type":"invalid_request_error","message":"Your credit balance is too low to access the Anthropic API. Please go to Plans & Billing to upgrade or purchase credits."}}"#,
                None,
                None,
            );
            assert!(refusal.is_billing_exhausted());
            assert_eq!(wire_auth_status(&refusal), None);
            let mut records = vec![run_started(1, "credit-run")];
            records.push(run_finished(2, Some(refusal)));
            assert_eq!(latest_wire_auth_status(&records), None);
        }

        #[test]
        fn opencode_wal_selection_resolves_the_metered_api_charge_basis() {
            let mut started = run_started(1, "opencode-run");
            if let broca_wal::WalRecord::RunStarted { config, .. } = &mut started.record {
                config.model = broca_protocol::request::ModelHandle::new("deepseek", "chat");
                config.auth_selection = Some(broca_core::AuthSelection {
                    credential_id: "apikey:deepseek".into(),
                    account_ref: None,
                    resolved_credential_id: None,
                    vault_request: None,
                    account_decision: None,
                });
            }
            let records = vec![started, run_finished(2, None)];
            let segments = broca_core::compute_segments(
                &records,
                "serve-test",
                &broca_module_serve::ChargeBasisTable::shipped_defaults(),
            );
            assert_eq!(segments.len(), 1);
            assert_eq!(
                segments[0].auth_selection.as_deref(),
                Some("apikey:deepseek")
            );
            assert_eq!(
                segments[0].charge_basis,
                broca_core::ChargeBasis::MeteredApi
            );
            assert_eq!(
                segments[0].charge_basis_mapping_version,
                broca_module_serve::CHARGE_BASIS_MAPPING_VERSION
            );
        }

        fn run_outcome(
            reason: RunFinishReason,
            paused: Option<PauseReason>,
        ) -> Result<RunSummary, RunError> {
            Ok(RunSummary {
                reason,
                paused,
                steps: 0,
                usage: broca_protocol::Usage::default(),
                retries_used: 0,
                final_step_finish_reason: None,
            })
        }

        #[test]
        fn wire_auth_wal_check_is_limited_to_auth_capable_outcomes() {
            let cases = [
                (
                    "completed",
                    run_outcome(RunFinishReason::Completed, None),
                    false,
                ),
                (
                    "auth pause",
                    run_outcome(
                        RunFinishReason::Interrupted,
                        Some(PauseReason::AuthRequired),
                    ),
                    true,
                ),
                (
                    "terminal error",
                    run_outcome(RunFinishReason::Error, None),
                    true,
                ),
                (
                    "transform unavailable",
                    run_outcome(RunFinishReason::TransformUnavailable, None),
                    false,
                ),
                (
                    "max steps",
                    run_outcome(RunFinishReason::MaxSteps, None),
                    false,
                ),
                (
                    "cancelled",
                    run_outcome(RunFinishReason::Cancelled, None),
                    false,
                ),
                (
                    "interrupted",
                    run_outcome(RunFinishReason::Interrupted, None),
                    false,
                ),
                (
                    "infrastructure error",
                    Err(RunError::Wal("disk unavailable".into())),
                    false,
                ),
            ];

            // PauseReason currently has no non-auth variant. TransformUnavailable is the
            // other resumable outcome and is represented by a finish reason with paused=None.
            for (name, outcome, expected) in cases {
                assert_eq!(should_check_wire_auth(&outcome), expected, "{name}");
            }
        }

        struct CountingWalStore {
            inner: FileWalStore,
            records: Vec<broca_wal::FramedRecord>,
            full_reads: std::sync::atomic::AtomicUsize,
        }

        impl CountingWalStore {
            fn new(records: Vec<broca_wal::FramedRecord>) -> Self {
                Self {
                    inner: FileWalStore::new(
                        broca_test_support::scratch_root()
                            .join(format!("broca-auth-gate-test-{}", std::process::id())),
                    ),
                    records,
                    full_reads: std::sync::atomic::AtomicUsize::new(0),
                }
            }

            fn full_reads(&self) -> usize {
                self.full_reads.load(std::sync::atomic::Ordering::SeqCst)
            }
        }

        impl WalStore for CountingWalStore {
            fn open(
                &self,
                session: &BindIdentity,
            ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
                self.inner.open(session)
            }

            fn read_records(
                &self,
                _session: &BindIdentity,
            ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
                self.full_reads
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(self.records.clone())
            }
        }

        #[tokio::test]
        async fn successful_vault_outcome_skips_post_run_wal_read() {
            let session = BindIdentity::new("/tmp/project", "harness", "session");
            let store = Arc::new(CountingWalStore::new(vec![run_finished(1, None)]));
            let outcome = run_outcome(RunFinishReason::Completed, None);

            assert_eq!(
                post_run_wire_auth_status(&outcome, store.clone(), session.clone()).await,
                None
            );
            assert_eq!(store.full_reads(), 0);

            // Control case: bypassing the outcome gate performs the full WAL read even though
            // the newest terminal is a successful finish and therefore yields no auth status.
            assert_eq!(
                terminal_wire_auth_status(store.clone(), session).await,
                None
            );
            assert_eq!(store.full_reads(), 1);
        }

        #[tokio::test]
        async fn auth_pause_still_reads_and_reports_wire_status() {
            let session = BindIdentity::new("/tmp/project", "harness", "session");
            let store = Arc::new(CountingWalStore::new(vec![auth_pause(1, 401)]));
            let outcome = run_outcome(
                RunFinishReason::Interrupted,
                Some(PauseReason::AuthRequired),
            );

            assert_eq!(
                post_run_wire_auth_status(&outcome, store.clone(), session).await,
                Some(401)
            );
            assert_eq!(store.full_reads(), 1);
        }

        #[test]
        fn successful_reauth_does_not_reuse_the_prior_pause_error() {
            let records = vec![auth_pause(1, 401), run_finished(2, None)];

            assert_eq!(latest_wire_auth_status(&records), None);
        }

        #[test]
        fn newest_auth_pause_remains_reportable() {
            let records = vec![auth_pause(1, 401)];

            assert_eq!(latest_wire_auth_status(&records), Some(401));
        }

        #[test]
        fn newest_non_auth_failure_hides_an_older_auth_pause() {
            let records = vec![
                auth_pause(1, 401),
                run_finished(2, Some(err_with_status(Some(500)))),
            ];

            assert_eq!(latest_wire_auth_status(&records), None);
        }

        #[test]
        fn clean_later_episode_hides_a_historical_auth_pause() {
            let records = vec![
                auth_pause(1, 401),
                run_finished(2, None),
                run_started(3, "run-2"),
                run_finished(4, None),
            ];

            assert_eq!(latest_wire_auth_status(&records), None);
        }

        #[test]
        fn only_auth_class_401_403_are_reportable_wire_auth_failures() {
            // An Auth-class 401/403 means the provider rejected the credential → report it to the
            // vault. A Permanent 403 means the credential is valid but forbidden → do not report.
            assert_eq!(wire_auth_status(&err_with_status(Some(401))), Some(401));
            assert_eq!(wire_auth_status(&err_with_status(Some(403))), Some(403));
            assert_eq!(
                wire_auth_status(&ProviderError {
                    provider_subcode: None,
                    refusal: None,
                    class: ErrorClass::Permanent,
                    message: "forbidden".into(),
                    status: Some(403),
                    retry_after_secs: None,
                    provider_code: None,
                    resets_at_ms: None,
                    resets_at_source: None,
                    limit_window: None,
                    account_ref: None,
                    credential_id: None,
                }),
                None
            );
            // Everything else is NOT a dead-credential signal — a transient hiccup or no
            // status must never be reported (it would wrongly invalidate a live credential).
            for s in [None, Some(429), Some(500), Some(503), Some(400), Some(200)] {
                assert_eq!(wire_auth_status(&err_with_status(s)), None, "status {s:?}");
            }
        }
    }
}

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

    for name in ["wal-durable", "wal-archive-durable"] {
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
        assert_private(&root);
        assert_private(&root.join("leases"));
        assert_private(&root.join("wal-durable"));
        assert_private(&root.join("wal-archive-durable"));
    }
}

#[cfg(test)]
mod state_directory_tests {
    use super::create_state_directories;
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
            assert_eq!(
                std::fs::read_dir(&path)
                    .expect("read newly created portable directory")
                    .count(),
                0,
                "boot must create {entry_id} before any session writes a WAL"
            );
        }
        assert!(
            portable_entries > 0,
            "placed descriptor has no portable entries"
        );
        let _ = std::fs::remove_dir_all(&state_root);
    }

    #[cfg(unix)]
    #[test]
    fn startup_does_not_change_modes_of_existing_durable_directories() {
        use std::os::unix::fs::PermissionsExt;

        let state_root = fresh_root("existing");
        std::fs::create_dir_all(&state_root).expect("create existing state root");
        let expected = [("wal-durable", 0o750), ("wal-archive-durable", 0o755)];
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
        let blocked = state_root.join("wal-durable");
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
