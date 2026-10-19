// Included in executor::tests so these drive the executor's real `decide_auth` against a real
// `VaultClient`; only the daemon on the other end of the socket is a stand-in.

/// A subc daemon stand-in hosting only the credentials module. It answers the catalog and
/// route-open control requests a `VaultClient` makes, answers every `credential.get_scoped`
/// with a token for the credential's current record_version (or a scoped `not_found`, when
/// told to), and keeps the method and params of every vault request exactly as they arrived.
/// A `credential.get` is answered too, so a regression that sends one is recorded and seen by
/// the assertions rather than failing somewhere less legible.
struct CapturingVaultDaemon {
    connection_file: std::path::PathBuf,
    /// Every vault request, in arrival order: (method, params).
    requests: Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>>,
    /// The consumer identity's module id presented on each route open, in order.
    route_open_identities: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    route_opens: Arc<std::sync::atomic::AtomicUsize>,
    errors: Arc<std::sync::Mutex<Vec<String>>>,
    server: tokio::task::JoinHandle<()>,
}

/// How the stand-in answers the credentials module's `route.open`: the refusals a restarting
/// module produces come from the daemon, before the vault sees anything.
#[derive(Clone, Copy)]
enum RouteOpenScript {
    Accept,
    /// Refuse the first `refusals` opens with `code`, then accept.
    RefuseThenAccept {
        code: &'static str,
        refusals: usize,
    },
    /// Refuse every open with `code`.
    RefuseForever(&'static str),
}

impl CapturingVaultDaemon {
    async fn start(root: &std::path::Path) -> Self {
        Self::start_with(root, RouteOpenScript::Accept).await
    }

    async fn start_with(root: &std::path::Path, script: RouteOpenScript) -> Self {
        Self::start_scripted(
            root,
            script,
            false,
            ReportScript::MarksStale,
            ExpiryScript::default(),
        )
        .await
    }

    /// As [`Self::start`], handling failure reports as `reports` says.
    async fn start_handling_reports(root: &std::path::Path, reports: ReportScript) -> Self {
        Self::start_scripted(
            root,
            RouteOpenScript::Accept,
            false,
            reports,
            ExpiryScript::default(),
        )
        .await
    }

    /// As [`Self::start`], serving tokens that expire as `expiry` says.
    async fn start_expiring(root: &std::path::Path, expiry: ExpiryScript) -> Self {
        Self::start_scripted(
            root,
            RouteOpenScript::Accept,
            false,
            ReportScript::MarksStale,
            expiry,
        )
        .await
    }

    /// As [`Self::start`], answering every `credential.get_scoped` with the vault's
    /// `not_found` refusal, the answer for both an unknown id and a missing grant.
    async fn start_refusing_scoped_reads(root: &std::path::Path) -> Self {
        Self::start_scripted(
            root,
            RouteOpenScript::Accept,
            true,
            ReportScript::MarksStale,
            ExpiryScript::default(),
        )
        .await
    }

    async fn start_scripted(
        root: &std::path::Path,
        script: RouteOpenScript,
        scoped_not_found: bool,
        reports: ReportScript,
        expiry: ExpiryScript,
    ) -> Self {
        use subc_transport::connection_file::{ConnectionInfo, Endpoint, SCHEMA_VERSION};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let key = vec![0x7e; 32];
        let daemon_id = [0xe7; 16];
        let connection_file = root.join("vault-connection.json");
        subc_transport::connection_file::write_atomic(
            &connection_file,
            &ConnectionInfo {
                schema: SCHEMA_VERSION,
                wire_version: None,
                endpoints: vec![Endpoint {
                    host: "127.0.0.1".into(),
                    port,
                }],
                key: key.clone(),
                daemon_id,
                pid: std::process::id(),
                daemon_ver: "capturing-vault".into(),
            },
        )
        .unwrap();
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let route_open_identities = Arc::new(std::sync::Mutex::new(Vec::new()));
        let route_opens = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let errors = Arc::new(std::sync::Mutex::new(Vec::new()));
        let record = Arc::new(std::sync::Mutex::new(StandInRecord::default()));
        let server = {
            let requests = requests.clone();
            let route_open_identities = route_open_identities.clone();
            let route_opens = route_opens.clone();
            let errors = errors.clone();
            tokio::spawn(async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        return;
                    };
                    let (key, requests, errors) = (key.clone(), requests.clone(), errors.clone());
                    let route_open_identities = route_open_identities.clone();
                    let route_opens = route_opens.clone();
                    let record = record.clone();
                    tokio::spawn(async move {
                        if let Err(error) = serve_capturing_vault(
                            stream,
                            key,
                            daemon_id,
                            CapturedTraffic {
                                requests,
                                route_open_identities,
                                route_opens,
                            },
                            script,
                            scoped_not_found,
                            reports,
                            expiry,
                            record,
                        )
                        .await
                        {
                            errors.lock().unwrap().push(error);
                        }
                    });
                }
            })
        };
        Self {
            connection_file,
            requests,
            route_open_identities,
            route_opens,
            errors,
            server,
        }
    }

    /// The params of every `credential.get_scoped`, in arrival order.
    fn scoped_reads(&self) -> Vec<serde_json::Value> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(method, _)| method == "credential.get_scoped")
            .map(|(_, params)| params.clone())
            .collect()
    }

    fn requests(&self) -> Vec<(String, serde_json::Value)> {
        self.requests.lock().unwrap().clone()
    }

    fn route_open_count(&self) -> usize {
        self.route_opens.load(std::sync::atomic::Ordering::SeqCst)
    }

    fn assert_healthy(&self) {
        assert!(
            self.errors.lock().unwrap().is_empty(),
            "vault stand-in failed: {:?}",
            self.errors.lock().unwrap()
        );
    }
}

impl Drop for CapturingVaultDaemon {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// How the stand-in handles `credential.report_auth_failure`, modelled on claustrum's read
/// surface: a report naming the CURRENT record_version marks the record stale, and the next
/// scoped read of a stale record refreshes it to a higher version.
#[derive(Clone, Copy)]
enum ReportScript {
    /// The vault's real behaviour: a current-version report marks the record stale.
    MarksStale,
    /// The report changes nothing, as for a version the vault no longer holds: the next read
    /// serves the same version again.
    NotApplied,
    /// Another reader refreshed the record between broca's read and its report, so the report
    /// names a superseded version and is not applied, and the re-read serves the newer token.
    AnotherReaderRefreshed,
    /// After the report the credential needs re-authentication: the refresh token is dead.
    NeedsReauth,
}

/// When the stand-in's tokens expire, and which read it refuses. The default serves tokens
/// that never expire (as for an API key) and refuses nothing.
#[derive(Clone, Copy, Default)]
struct ExpiryScript {
    /// Remaining life of the admission token (version 1) when it is served. A token served by a
    /// refresh has an hour left. `None` serves every token without an expiry.
    first_ttl_ms: Option<i64>,
    /// The 1-based `credential.get_scoped` read the stand-in refuses with `refresh_failed`.
    refuse_read: Option<usize>,
    /// The provider account the token belongs to. `None` models older vault responses that
    /// returned a token without identifying its account.
    account_id: Option<&'static str>,
    /// Drop the connection on this 1-based scoped read without a vault reply.
    drop_read: Option<usize>,
    /// Serve the current version unchanged even when the requested TTL is not met.
    unchanged_read: Option<usize>,
    /// Script a JWT and, optionally, a different vault account after rotation.
    chatgpt_jwt: bool,
    refreshed_account_id: Option<&'static str>,
    refreshed_jwt_other_account: bool,
}

/// The life a token served by a refresh has left.
const REFRESHED_TTL_MS: i64 = 60 * 60 * 1000;

/// The one credential record the stand-in serves, shared across its connections.
#[derive(Default)]
struct StandInRecord {
    /// Bumped by each refresh; 0 means "not read yet" and is shown as version 1.
    version: u64,
    stale: bool,
    reported: bool,
    /// When the current version expires, in Unix milliseconds; `None` for no expiry.
    expires_at_ms: Option<i64>,
    /// How many `credential.get_scoped` reads have arrived.
    scoped_reads: usize,
}

/// The token the stand-in serves at `version`: version 1 is the admission token.
fn stand_in_token(version: u64) -> String {
    if version <= 1 {
        "stand-in-token".into()
    } else {
        format!("stand-in-token-v{version}")
    }
}

/// What the stand-in records, shared across its connections.
struct CapturedTraffic {
    requests: Arc<std::sync::Mutex<Vec<(String, serde_json::Value)>>>,
    route_open_identities: Arc<std::sync::Mutex<Vec<Option<String>>>>,
    route_opens: Arc<std::sync::atomic::AtomicUsize>,
}

#[allow(clippy::too_many_arguments)]
async fn serve_capturing_vault(
    mut stream: tokio::net::TcpStream,
    key: Vec<u8>,
    daemon_id: [u8; 16],
    captured: CapturedTraffic,
    script: RouteOpenScript,
    scoped_not_found: bool,
    reports: ReportScript,
    expiry: ExpiryScript,
    record: Arc<std::sync::Mutex<StandInRecord>>,
) -> Result<(), String> {
    let CapturedTraffic {
        requests,
        route_open_identities,
        route_opens,
    } = captured;
    use subc_control::{CatalogEntry, ClientControlRequest, ClientControlResponse};
    use subc_protocol::{Flags, Frame, FrameType, Priority};
    use subc_transport::{read_frame, write_frame};

    async fn respond(
        stream: &mut tokio::net::TcpStream,
        request: &Frame,
        priority: Priority,
        body: Vec<u8>,
    ) -> Result<(), String> {
        let response = Frame::build(
            FrameType::Response,
            Flags::new(false, priority, false),
            request.header.channel,
            request.header.epoch,
            request.header.corr,
            body,
        )
        .map_err(|error| format!("build response: {error}"))?;
        write_frame(stream, &response)
            .await
            .map_err(|error| format!("write response: {error}"))
    }

    subc_transport::authenticate_server(
        &mut stream,
        &key,
        &daemon_id,
        "capturing-vault",
        broca_test_support::STAND_IN_AUTH_DEADLINE,
    )
    .await
    .map_err(|error| format!("authenticate: {error}"))?;
    let mut next_route: u16 = 10;
    while let Some(frame) = read_frame(&mut stream)
        .await
        .map_err(|error| format!("read frame: {error}"))?
    {
        match (frame.header.channel, frame.header.ty) {
            (0, FrameType::Ping) => {
                let pong = Frame::build(
                    FrameType::Pong,
                    frame.header.flags,
                    0,
                    0,
                    frame.header.corr,
                    Vec::new(),
                )
                .map_err(|error| format!("build pong: {error}"))?;
                write_frame(&mut stream, &pong)
                    .await
                    .map_err(|error| format!("write pong: {error}"))?;
            }
            (0, FrameType::Request) => {
                let request = serde_json::from_slice::<ClientControlRequest>(&frame.body)
                    .map_err(|error| format!("decode control request: {error}"))?;
                if let ClientControlRequest::RouteOpen {
                    consumer_identity, ..
                } = &request
                {
                    route_open_identities.lock().unwrap().push(
                        consumer_identity
                            .as_ref()
                            .map(|identity| identity.module_id.clone()),
                    );
                }
                if matches!(request, ClientControlRequest::RouteOpen { .. }) {
                    let seen = route_opens.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let refusal = match script {
                        RouteOpenScript::Accept => None,
                        RouteOpenScript::RefuseThenAccept { code, refusals } => {
                            (seen < refusals).then_some(code)
                        }
                        RouteOpenScript::RefuseForever(code) => Some(code),
                    };
                    if let Some(code) = refusal {
                        let body = serde_json::to_vec(&serde_json::json!({
                            "code": code,
                            "message": "stand-in refusal"
                        }))
                        .map_err(|error| error.to_string())?;
                        let refused = Frame::build(
                            FrameType::Error,
                            Flags::new(false, Priority::Passive, false),
                            frame.header.channel,
                            frame.header.epoch,
                            frame.header.corr,
                            body,
                        )
                        .map_err(|error| format!("build refusal: {error}"))?;
                        write_frame(&mut stream, &refused)
                            .await
                            .map_err(|error| format!("write refusal: {error}"))?;
                        continue;
                    }
                }
                let response = match request {
                    ClientControlRequest::CatalogList { .. } => ClientControlResponse::CatalogList {
                        generation: 1,
                        modules: vec![CatalogEntry::new(
                            broca_module_serve::consumer_routes::CREDENTIALS_MODULE_ID,
                            Vec::new(),
                            vec![
                                "credential.get".into(),
                                "credential.get_scoped".into(),
                                "credential.status".into(),
                                "credential.report_auth_failure".into(),
                            ],
                        )
                        .with_module_version(Some("stand-in".into()))
                        .with_ready(true)],
                        subc_ops: Vec::new(),
                    },
                    ClientControlRequest::RouteOpen { .. } => {
                        next_route += 1;
                        ClientControlResponse::RouteOpen {
                            route_channel: next_route,
                            route_epoch: 1,
                        }
                    }
                    other => return Err(format!("unexpected control request: {other:?}")),
                };
                let body = serde_json::to_vec(&response).map_err(|error| error.to_string())?;
                respond(&mut stream, &frame, Priority::Passive, body).await?;
            }
            (channel, FrameType::Request) if channel != 0 => {
                let request: serde_json::Value = serde_json::from_slice(&frame.body)
                    .map_err(|error| format!("decode route request: {error}"))?;
                let method = request["method"].as_str().unwrap_or_default().to_string();
                let token = |version: u64, expires_at_ms: Option<i64>| {
                    let secret = if expiry.chatgpt_jwt {
                        chatgpt_test_token(version, expiry.refreshed_jwt_other_account)
                    } else {
                        stand_in_token(version)
                    };
                    let account_id = if version > 1 {
                        expiry.refreshed_account_id.or(expiry.account_id)
                    } else {
                        expiry.account_id
                    };
                    serde_json::json!({"result": {
                        "payload": secret.into_bytes(),
                        "expires_at_ms": expires_at_ms,
                        "record_version": version.max(1),
                        "account_id": account_id
                    }})
                };
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64;
                // The vault's refusal shape, as claustrum's read surface sends it.
                let not_found = serde_json::json!({"result": {"error": {
                    "class": "permanent",
                    "code": "not_found"
                }}});
                if method == "credential.get_scoped" {
                    let mut state = record.lock().unwrap();
                    if expiry.drop_read.is_some_and(|wanted| state.scoped_reads + 1 >= wanted) {
                        state.scoped_reads += 1;
                        return Ok(());
                    }
                }
                let reply = match method.as_str() {
                    "credential.get" => token(1, None),
                    "credential.get_scoped" if scoped_not_found => not_found,
                    "credential.get_scoped" => {
                        let mut record = record.lock().unwrap();
                        record.scoped_reads += 1;
                        if record.reported && matches!(reports, ReportScript::NeedsReauth) {
                            serde_json::json!({"result": {"error": {
                                "class": "permanent",
                                "code": "needs_reauth"
                            }}})
                        } else if expiry.refuse_read == Some(record.scoped_reads) {
                            serde_json::json!({"result": {"error": {
                                "class": "transient",
                                "code": "refresh_failed"
                            }}})
                        } else {
                            if record.version == 0 {
                                // The admission read. Its token is served with whatever life
                                // the script gives it, standing in for a token read long ago.
                                record.version = 1;
                                record.expires_at_ms =
                                    expiry.first_ttl_ms.map(|ttl| now_ms + ttl);
                            } else {
                                // A stale record, or one whose token cannot meet the read's
                                // `min_ttl_ms`, refreshes on this read, as claustrum's engine
                                // does.
                                let min_ttl_ms =
                                    request["params"]["min_ttl_ms"].as_i64().unwrap_or(0);
                                let too_short = record
                                    .expires_at_ms
                                    .is_some_and(|expires| expires < now_ms + min_ttl_ms);
                                if (record.stale || too_short)
                                    && expiry.unchanged_read != Some(record.scoped_reads)
                                {
                                    record.version += 1;
                                    record.stale = false;
                                    record.expires_at_ms =
                                        expiry.first_ttl_ms.map(|_| now_ms + REFRESHED_TTL_MS);
                                }
                            }
                            token(record.version, record.expires_at_ms)
                        }
                    }
                    "credential.report_auth_failure" => {
                        let mut record = record.lock().unwrap();
                        record.reported = true;
                        if matches!(reports, ReportScript::AnotherReaderRefreshed) {
                            record.version = record.version.max(1) + 1;
                        }
                        let reported = request["params"]["record_version"].as_u64();
                        if matches!(reports, ReportScript::MarksStale)
                            && reported == Some(record.version.max(1))
                        {
                            record.stale = true;
                        }
                        // claustrum answers every accepted report this way, applied or not.
                        serde_json::json!({"result": {"accepted": true}})
                    }
                    other => return Err(format!("unexpected route request: {other}")),
                };
                requests
                    .lock()
                    .unwrap()
                    .push((method, request["params"].clone()));
                let body = serde_json::to_vec(&reply).map_err(|error| error.to_string())?;
                respond(&mut stream, &frame, Priority::Interactive, body).await?;
            }
            (_, FrameType::Goodbye) => {}
            (_, other) => return Err(format!("unexpected frame type: {other:?}")),
        }
    }
    Ok(())
}

/// The executor wired exactly as production wires it for the vault auth source, pointed at
/// the stand-in, on a connection presenting the broca module's consumer identity. There is no
/// handle file anywhere: the daemon reads every credential by grant.
async fn executor_with_capturing_vault(
    root: &std::path::Path,
    daemon: &CapturingVaultDaemon,
) -> LiveLoopExecutor {
    executor_with_capturing_vault_within(root, daemon, Duration::from_secs(2)).await
}

/// As [`executor_with_capturing_vault`], with the vault client's readiness budget chosen.
async fn executor_with_capturing_vault_within(
    root: &std::path::Path,
    daemon: &CapturingVaultDaemon,
    ready_timeout: Duration,
) -> LiveLoopExecutor {
    executor_with_vault_addressing(
        root,
        daemon,
        ready_timeout,
        broca_module_serve::VaultCredentialIds::default(),
        supervised_broca_identity(),
    )
    .await
}

/// The consumer identity the daemon gives a module it spawned, here the broca module's.
fn supervised_broca_identity() -> Option<subc_control::ConsumerIdentity> {
    Some(subc_control::ConsumerIdentity {
        module_id: "broca".into(),
        launch_nonce: "stand-in-nonce".into(),
    })
}

/// As [`executor_with_capturing_vault_within`], with `vault.credential_ids` aliases and the
/// consumer identity the vault connection presents chosen explicitly. The identity is passed
/// in rather than read from the environment, which is shared by every test in this binary.
async fn executor_with_vault_addressing(
    root: &std::path::Path,
    daemon: &CapturingVaultDaemon,
    ready_timeout: Duration,
    credential_ids: broca_module_serve::VaultCredentialIds,
    consumer_identity: Option<subc_control::ConsumerIdentity>,
) -> LiveLoopExecutor {
    let vault_conn = broca_subc::SubcConnection::connect_with_consumer_identity(
        &daemon.connection_file,
        consumer_identity,
    )
    .await
    .unwrap();
    let tool_conn = broca_subc::SubcConnection::connect(&daemon.connection_file)
        .await
        .unwrap();
    LiveLoopExecutor::new(
        Arc::new(FileWalStore::new(root.join("wal"))),
        Arc::new(broca_store::SqliteStore::open(root.join("store.db")).unwrap()),
        Arc::new(SubcToolPlane::new(tool_conn, "unused")),
        None,
    )
    .with_vault(VaultAuth {
        client: broca_subc::VaultClient::new(
            Arc::new(vault_conn),
            broca_module_serve::consumer_routes::CREDENTIALS_MODULE_ID,
            &broca_protocol::BindIdentity::new(root, "broca", "vault"),
            ready_timeout,
        ),
        addressing: broca_module_serve::VaultAddressing::new(credential_ids),
        methods: broca_module_serve::MethodConfig::empty(),
        gauge: broca_module_serve::serve::VaultGauge::disconnected(),
        cache: VaultLkgCache::default(),
        listing: Default::default(),
    })
}

/// ADMISSION READS EVERY CREDENTIAL KIND BY GRANT, AND NEVER SENDS `credential.get`.
///
/// An OAuth credential and an API key are admitted with no handle file anywhere, and the vault
/// sees exactly one `credential.get_scoped` per admission, by credential id. Each run's frozen
/// `vault_request` is compared with the request as the stand-in captured it off the socket,
/// not with broca's own constants, so a call whose arguments drift from the record fails here.
#[tokio::test]
async fn admission_reads_every_credential_kind_by_grant_and_never_sends_credential_get() {
    let root = fresh_vault_root("scoped-every-kind");
    assert!(
        !root.join("vault-handles.json").exists(),
        "no handle file exists for this admission to read"
    );
    let daemon = CapturingVaultDaemon::start(&root).await;
    let executor = executor_with_capturing_vault(&root, &daemon).await;

    let mut selections = Vec::new();
    for (provider, method, key) in [
        ("anthropic", AuthMethod::Oauth, "oauth:anthropic"),
        ("deepseek", AuthMethod::ApiKey, "apikey:deepseek"),
    ] {
        let auth = executor
            .decide_auth(provider, None, true, Some(method), None)
            .await
            .unwrap_or_else(|error| {
                daemon.assert_healthy();
                panic!("vault-resolved admission of {key}: {error:?}")
            });
        let selection = auth.selection.expect("a fresh vault run freezes a selection");
        assert_eq!(selection.credential_id, key);
        selections.push(selection);
    }
    daemon.assert_healthy();

    let requests = daemon.requests();
    assert_eq!(
        methods(&requests),
        vec!["credential.get_scoped", "credential.get_scoped"],
        "one scoped read per credential, and no credential.get at all: {requests:?}"
    );
    for ((_, sent), (selection, key)) in requests
        .iter()
        .zip(selections.iter().zip(["oauth:anthropic", "apikey:deepseek"]))
    {
        assert_eq!(sent["credential_id"], key);
        assert_eq!(sent.get("handle"), None);
        assert_eq!(sent.get("force_refresh"), None);
        let received = broca_core::VaultRequest {
            min_ttl_ms: sent.get("min_ttl_ms").map(|value| value.as_i64().unwrap()),
            force_refresh: None,
            op: Some(broca_core::VaultReadOp::GetScoped),
        };
        assert!(
            received.min_ttl_ms.is_some(),
            "broca demands a validity floor today; a vacuous None-to-None match proves nothing"
        );
        assert_eq!(selection.vault_request, Some(received));
        let frozen = serde_json::to_value(selection).unwrap();
        assert_eq!(frozen["vault_request"]["op"], "get_scoped");
        assert_eq!(frozen["vault_request"]["min_ttl_ms"], sent["min_ttl_ms"]);
    }

    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
}

fn fresh_vault_root(label: &str) -> std::path::PathBuf {
    let root = broca_test_support::scratch_root()
        .join(format!("broca-vault-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

#[tokio::test]
async fn a_delayed_client_auth_is_accepted_by_the_vault_stand_in() {
    use subc_protocol::{Flags, Frame, FrameType, Priority};
    use subc_transport::{ClientAuth, ClientHello, ServerProof};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn send_auth_message(
        stream: &mut tokio::net::TcpStream,
        message: &impl serde::Serialize,
    ) {
        let body = serde_json::to_vec(message).unwrap();
        let len = u32::try_from(body.len()).unwrap();
        assert!(len <= subc_transport::MAX_AUTH_MESSAGE_LEN);
        stream.write_u32_le(len).await.unwrap();
        stream.write_all(&body).await.unwrap();
    }

    let root = fresh_vault_root("delayed-client-auth");
    let daemon = CapturingVaultDaemon::start(&root).await;
    let info = subc_transport::connection_file::read(&daemon.connection_file).unwrap();
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", info.endpoints[0].port))
        .await
        .unwrap();
    let hello = ClientHello {
        client_nonce: [0x12; subc_transport::NONCE_LEN],
        role: subc_transport::DEFAULT_CLIENT_ROLE.into(),
    };
    send_auth_message(&mut stream, &hello).await;
    let len = stream.read_u32_le().await.unwrap();
    assert!(len <= subc_transport::MAX_AUTH_MESSAGE_LEN);
    let mut body = vec![0; len as usize];
    stream.read_exact(&mut body).await.unwrap();
    let proof: ServerProof = serde_json::from_slice(&body).unwrap();
    assert_eq!(proof.daemon_id, info.daemon_id);
    assert_eq!(
        proof.server_proof,
        subc_transport::compute_proof(
            &info.key,
            subc_transport::SERVER_PROOF_DOMAIN,
            &hello.client_nonce,
            &proof.server_nonce,
            &proof.daemon_id,
        )
    );

    // Send the prelude by hand so only the stand-in's deadline is exercised, not the
    // production client's 2-second budget. Delay ClientAuth without creating CPU load.
    tokio::time::sleep(Duration::from_millis(2300)).await;
    daemon.assert_healthy();
    let auth = ClientAuth {
        client_auth: subc_transport::compute_proof(
            &info.key,
            subc_transport::CLIENT_AUTH_DOMAIN,
            &hello.client_nonce,
            &proof.server_nonce,
            &proof.daemon_id,
        ),
    };
    send_auth_message(&mut stream, &auth).await;

    // A catalog reply proves the server completed authentication and entered its frame loop.
    let request = Frame::build(
        FrameType::Request,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        1,
        serde_json::to_vec(&subc_control::ClientControlRequest::CatalogList { module_id: None })
            .unwrap(),
    )
    .unwrap();
    subc_transport::write_frame(&mut stream, &request)
        .await
        .unwrap();
    let reply = tokio::time::timeout(
        Duration::from_secs(5),
        subc_transport::read_frame(&mut stream),
    )
    .await
    .unwrap()
    .unwrap()
    .unwrap();
    assert_eq!(reply.header.ty, FrameType::Response);
    assert_eq!(reply.header.corr, request.header.corr);
    let response: subc_control::ClientControlResponse =
        serde_json::from_slice(&reply.body).unwrap();
    let subc_control::ClientControlResponse::CatalogList { modules, .. } = response else {
        panic!("expected the stand-in's catalog reply");
    };
    assert_eq!(modules.len(), 1);
    assert_eq!(
        modules[0].module_id,
        broca_module_serve::consumer_routes::CREDENTIALS_MODULE_ID
    );
    daemon.assert_healthy();
    drop(stream);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
}

/// A credentials module that is restarting refuses `route.open` with `target_unavailable`
/// until it is back. Admission must wait that out and get a fresh token from the vault.
///
/// The cache is asserted EMPTY first, as it is after every broca restart: with a warm cache the
/// run would be admitted on a cached token even if the route open were never retried, and this
/// test would prove nothing.
#[tokio::test]
async fn a_temporarily_refused_vault_route_is_retried_and_served_by_the_vault() {
    let root = fresh_vault_root("route-refused-then-accepted");
    let daemon = CapturingVaultDaemon::start_with(
        &root,
        RouteOpenScript::RefuseThenAccept {
            code: "target_unavailable",
            refusals: 3,
        },
    )
    .await;
    let executor =
        executor_with_capturing_vault_within(&root, &daemon, crate::VAULT_READY_TIMEOUT).await;
    let vault = executor.vault.as_ref().expect("the vault auth source is wired");
    assert!(
        vault.cache.get("apikey:deepseek").is_none(),
        "the cache must start empty, or a cached token could admit the run"
    );

    let auth = executor
        .decide_auth("deepseek", None, true, Some(AuthMethod::ApiKey), None)
        .await
        .unwrap_or_else(|error| {
            daemon.assert_healthy();
            panic!("a refusal that clears within the budget must not fail admission: {error:?}")
        });
    daemon.assert_healthy();

    assert!(
        matches!(&auth.mode, AuthMode::Secret(token) if token == "stand-in-token"),
        "the token must be the one the vault served"
    );
    assert_eq!(
        daemon.route_open_count(),
        4,
        "three refusals, then the open that succeeded"
    );
    assert_eq!(daemon.scoped_reads().len(), 1, "the vault got the request");

    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
}

/// A terminal refusal is not retried: `unknown_module` will not change by waiting, so admission
/// fails at once after exactly one route open.
#[tokio::test]
async fn a_terminal_vault_route_refusal_fails_at_once_without_retry() {
    let root = fresh_vault_root("route-refused-terminal");
    let daemon =
        CapturingVaultDaemon::start_with(&root, RouteOpenScript::RefuseForever("unknown_module"))
            .await;
    let executor =
        executor_with_capturing_vault_within(&root, &daemon, crate::VAULT_READY_TIMEOUT).await;

    let started = std::time::Instant::now();
    let result = executor
        .decide_auth("deepseek", None, true, Some(AuthMethod::ApiKey), None)
        .await;
    let elapsed = started.elapsed();
    daemon.assert_healthy();

    assert!(
        result.is_err(),
        "an empty cache and a refused route leave nothing to admit on"
    );
    assert_eq!(daemon.route_open_count(), 1, "a terminal refusal is never retried");
    assert!(
        elapsed < Duration::from_secs(1),
        "a terminal refusal must fail at once, took {elapsed:?}"
    );
    assert!(daemon.requests().is_empty());

    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
}

/// A refusal that never clears gives up within the one readiness budget, not a budget per
/// phase and not never.
#[tokio::test]
async fn a_vault_route_refusal_that_never_clears_gives_up_within_the_ready_budget() {
    let root = fresh_vault_root("route-refused-forever");
    let daemon =
        CapturingVaultDaemon::start_with(&root, RouteOpenScript::RefuseForever("target_unavailable"))
            .await;
    let executor =
        executor_with_capturing_vault_within(&root, &daemon, crate::VAULT_READY_TIMEOUT).await;

    let started = std::time::Instant::now();
    let result = executor
        .decide_auth("deepseek", None, true, Some(AuthMethod::ApiKey), None)
        .await;
    let elapsed = started.elapsed();
    daemon.assert_healthy();

    assert!(result.is_err(), "the refusal outlasted the budget");
    assert!(
        elapsed <= crate::VAULT_READY_TIMEOUT + Duration::from_secs(1),
        "the route open must stop within the readiness budget, took {elapsed:?}"
    );
    assert!(
        elapsed >= crate::VAULT_READY_TIMEOUT - Duration::from_millis(500),
        "a temporary refusal must be retried for the budget, gave up after {elapsed:?}"
    );
    assert!(
        daemon.route_open_count() > 1,
        "a temporary refusal must be retried"
    );
    assert!(daemon.requests().is_empty());

    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
}

/// A ROUTE OPEN THE DAEMON REFUSED RECORDS NO REQUEST -- and the cached token is still served.
///
/// The refusal comes from the daemon, so the vault never saw a credential.get. It surfaces as
/// `VaultError::Protocol`, the same variant as an undecodable vault reply, which is why the
/// record cannot be decided from the error variant: deciding that way recorded this request.
/// The refusal outlasts the retry budget, so the warm cache has to serve the run.
#[tokio::test]
async fn a_refused_vault_route_served_from_cache_records_no_request() {
    let root = fresh_vault_root("route-refused-cache-served");
    let daemon =
        CapturingVaultDaemon::start_with(&root, RouteOpenScript::RefuseForever("target_unavailable"))
            .await;
    let executor =
        executor_with_capturing_vault_within(&root, &daemon, Duration::from_secs(1)).await;
    let vault = executor.vault.as_ref().expect("the vault auth source is wired");
    vault.cache.insert(
        "apikey:deepseek".to_string(),
        VaultToken {
            token: "cached-token".into(),
            expires_at_ms: None,
            record_version: 3,
            project_id: None,
            account_id: None,
            resolved_credential_id: None,
        },
    );

    let auth = executor
        .decide_auth("deepseek", None, true, Some(AuthMethod::ApiKey), None)
        .await
        .unwrap_or_else(|error| {
            daemon.assert_healthy();
            panic!("a warm cache must admit the run while the route is refused: {error:?}")
        });
    daemon.assert_healthy();

    assert!(
        matches!(&auth.mode, AuthMode::Secret(token) if token == "cached-token"),
        "the cached token must be served"
    );
    assert!(
        daemon.route_open_count() > 1,
        "the refusal must have been retried before the cache was used"
    );
    assert!(
        daemon.requests().is_empty(),
        "the vault never received a read"
    );
    let selection = auth.selection.expect("a fresh vault run freezes a selection");
    assert_eq!(
        selection.vault_request, None,
        "the vault never saw the request, so the run must not claim one"
    );

    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
}

/// The aliases the scoped-read tests configure: `apikey:synthetic` is read as a different vault
/// id, every other key without a handle as itself.
fn synthetic_alias() -> broca_module_serve::VaultCredentialIds {
    broca_module_serve::VaultCredentialIds::from_pairs([("apikey:synthetic", "apikey:synthetic:main")])
}

/// A KEY IS READ BY GRANT AS ITS MAPPED ID, OR AS ITSELF.
///
/// It sends `credential.get_scoped` with the credential id and the ttl demand, and neither a
/// handle nor `force_refresh`, which the operation does not have. The run records what was
/// sent: `op: get_scoped` and no `force_refresh` at all. The route it went out on presented
/// the broca module's consumer identity, which is what lets the vault's grant apply.
#[tokio::test]
async fn a_key_is_read_by_grant_as_its_alias_or_itself_and_records_get_scoped() {
    let root = fresh_vault_root("scoped-read");
    let daemon = CapturingVaultDaemon::start(&root).await;
    let executor = executor_with_vault_addressing(
        &root,
        &daemon,
        Duration::from_secs(2),
        synthetic_alias(),
        supervised_broca_identity(),
    )
    .await;

    let mapped = executor
        .decide_auth("synthetic", None, true, Some(AuthMethod::ApiKey), None)
        .await
        .unwrap_or_else(|error| {
            daemon.assert_healthy();
            panic!("a granted scoped read admits the run: {error:?}")
        });
    let unmapped = executor
        .decide_auth("other", None, true, Some(AuthMethod::ApiKey), None)
        .await
        .unwrap_or_else(|error| {
            daemon.assert_healthy();
            panic!("a granted scoped read admits the run: {error:?}")
        });
    daemon.assert_healthy();

    assert!(matches!(&mapped.mode, AuthMode::Secret(token) if token == "stand-in-token"));
    let requests = daemon.requests();
    assert_eq!(
        requests,
        vec![
            (
                "credential.get_scoped".to_string(),
                serde_json::json!({"credential_id": "apikey:synthetic:main", "min_ttl_ms": VAULT_MIN_TTL_MS})
            ),
            (
                "credential.get_scoped".to_string(),
                serde_json::json!({"credential_id": "apikey:other", "min_ttl_ms": VAULT_MIN_TTL_MS})
            ),
        ],
        "the mapped id, then the key itself; never a handle, never force_refresh"
    );
    assert!(daemon
        .route_open_identities
        .lock()
        .unwrap()
        .iter()
        .all(|identity| identity.as_deref() == Some("broca")));

    for (auth, key) in [(mapped, "apikey:synthetic"), (unmapped, "apikey:other")] {
        let selection = auth.selection.expect("a fresh vault run freezes a selection");
        // The routing key is what the run froze; the alias only changes what the vault is asked.
        assert_eq!(selection.credential_id, key);
        assert_eq!(
            selection.vault_request,
            Some(broca_core::VaultRequest {
                min_ttl_ms: Some(VAULT_MIN_TTL_MS),
                force_refresh: None,
                op: Some(broca_core::VaultReadOp::GetScoped),
            })
        );
        let frozen = serde_json::to_value(&selection).unwrap();
        assert_eq!(frozen["vault_request"]["op"], "get_scoped");
        assert_eq!(frozen["vault_request"]["min_ttl_ms"], VAULT_MIN_TTL_MS);
        assert!(
            frozen["vault_request"].get("force_refresh").is_none(),
            "get_scoped sends no force_refresh, so none is recorded: {frozen}"
        );
    }

    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
}

/// A SCOPED NOT_FOUND IS FINAL: it is never retried any other way. The refusal keeps
/// not_found's class (a configuration failure of admission) and says what it can mean: it
/// names the id asked for and the principal, both causes, and `ck auth events`.
#[tokio::test]
async fn a_scoped_not_found_is_final_and_names_both_causes() {
    let root = fresh_vault_root("scoped-not-found");
    let daemon = CapturingVaultDaemon::start_refusing_scoped_reads(&root).await;
    let executor = executor_with_vault_addressing(
        &root,
        &daemon,
        Duration::from_secs(2),
        synthetic_alias(),
        supervised_broca_identity(),
    )
    .await;

    let refusal = match executor
        .decide_auth("synthetic", None, true, Some(AuthMethod::ApiKey), None)
        .await
    {
        Ok(_) => panic!("a scoped not_found must not admit the run"),
        Err(error) => error,
    };
    daemon.assert_healthy();

    assert_eq!(
        daemon
            .requests()
            .iter()
            .map(|(method, _)| method.as_str())
            .collect::<Vec<_>>(),
        vec!["credential.get_scoped"],
        "exactly one scoped read and no credential.get: a fallback would hide the misconfiguration"
    );
    let RunError::Config(message) = refusal else {
        panic!("a scoped not_found stays a configuration refusal, got {refusal:?}");
    };
    for needle in [
        "ck auth events",
        "apikey:synthetic:main",
        "reserved:broca",
        "no read grant",
        "no credential has id",
    ] {
        assert!(message.contains(needle), "missing {needle:?} in: {message}");
    }

    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
}

/// WITHOUT A CONSUMER IDENTITY A SCOPED READ IS REFUSED HERE, and nothing reaches the vault:
/// no route open, no request. Sent anyway, it would arrive as an anonymous caller and come
/// back not_found with nothing to say why.
#[tokio::test]
async fn a_scoped_read_without_a_consumer_identity_is_refused_before_sending() {
    let root = fresh_vault_root("scoped-no-identity");
    let daemon = CapturingVaultDaemon::start(&root).await;
    let executor = executor_with_vault_addressing(
        &root,
        &daemon,
        Duration::from_secs(2),
        synthetic_alias(),
        None,
    )
    .await;

    let refusal = match executor
        .decide_auth("synthetic", None, true, Some(AuthMethod::ApiKey), None)
        .await
    {
        Ok(_) => panic!("no identity, no scoped read"),
        Err(error) => error,
    };
    daemon.assert_healthy();

    assert!(daemon.requests().is_empty(), "the stand-in saw a request");
    assert_eq!(daemon.route_open_count(), 0, "the stand-in saw a route open");
    let RunError::Config(message) = refusal else {
        panic!("a missing identity is a configuration refusal, got {refusal:?}");
    };
    assert!(message.contains("no supervised consumer identity"), "{message}");
    assert!(message.contains("supervised broca module"), "{message}");
    assert!(message.contains("apikey:synthetic:main"), "{message}");

    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
}

/// A wire-auth failure is reported by the credential id the read used (the alias target when
/// the key has one), with the served record_version the vault fences on and `reporter_source`
/// `direct`, and nothing else: exactly the shape claustrum's wire contract pins for the id form.
#[tokio::test]
async fn a_wire_auth_failure_is_reported_by_credential_id_on_the_exact_wire_shape() {
    let root = fresh_vault_root("report-address");
    let daemon = CapturingVaultDaemon::start(&root).await;
    let executor = executor_with_vault_addressing(
        &root,
        &daemon,
        Duration::from_secs(2),
        synthetic_alias(),
        supervised_broca_identity(),
    )
    .await;
    let vault = executor.vault.as_ref().expect("the vault auth source is wired");

    report_wire_auth_failure(vault, "apikey:synthetic", 401, 5)
        .await
        .expect("the aliased report is accepted");
    report_wire_auth_failure(vault, "oauth:anthropic", 401, 6)
        .await
        .expect("the unaliased report is accepted");
    daemon.assert_healthy();

    assert_eq!(
        daemon.requests(),
        vec![
            (
                "credential.report_auth_failure".to_string(),
                serde_json::json!({
                    "credential_id": "apikey:synthetic:main",
                    "provider_status": 401,
                    "reporter_source": "direct",
                    "record_version": 5
                })
            ),
            (
                "credential.report_auth_failure".to_string(),
                serde_json::json!({
                    "credential_id": "oauth:anthropic",
                    "provider_status": 401,
                    "reporter_source": "direct",
                    "record_version": 6
                })
            ),
        ]
    );

    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
}

/// What one in-step 401 recovery produced.
struct Recovery {
    refreshed: Option<broca_provider::credential_refresh::RefreshedSecret>,
    logs: Vec<String>,
    /// The run's served version afterwards.
    served: u64,
    /// The record_version the refresher already sent a failure report for.
    reported: Option<u64>,
    /// The version this process's cache holds afterwards.
    cached: Option<u64>,
    requests: Vec<(String, serde_json::Value)>,
}

/// Admit a deepseek run against a stand-in that handles failure reports as `reports` says,
/// then ask the run's in-step refresher for a fresh token as the transport does after a 401.
async fn refresh_after_a_401(label: &str, reports: ReportScript) -> Recovery {
    refresh_after_a_rejection(label, reports, 401).await
}

/// [`refresh_after_a_401`] for a rejection the provider answered with `status`.
async fn refresh_after_a_rejection(label: &str, reports: ReportScript, status: u16) -> Recovery {
    let root = fresh_vault_root(label);
    let daemon = CapturingVaultDaemon::start_handling_reports(&root, reports).await;
    let executor = executor_with_capturing_vault(&root, &daemon).await;
    let auth = executor
        .decide_auth("deepseek", None, true, Some(AuthMethod::ApiKey), None)
        .await
        .unwrap_or_else(|error| panic!("vault-resolved admission: {error:?}"));
    let refresh = auth
        .in_run_refresh
        .expect("a token served by the vault can be recovered after a 401");
    assert_eq!(refresh.served_version.load(Ordering::SeqCst), 1);
    // Admission's own request stays on the frozen selection, unchanged by any later refresh.
    assert_eq!(
        auth.selection.unwrap().vault_request.unwrap().op,
        Some(broca_core::VaultReadOp::GetScoped)
    );

    let (refreshed, logs) = capture_credential_logs(async {
        refresh.refresher.0.refresh_rejected_secret(status).await
    }).await;
    daemon.assert_healthy();
    let cached = executor
        .vault
        .as_ref()
        .unwrap()
        .cache
        .get("apikey:deepseek")
        .map(|token| token.record_version);
    let recovery = Recovery {
        refreshed,
        logs,
        served: refresh.served_version.load(Ordering::SeqCst),
        reported: *refresh.reported_version.lock().unwrap(),
        cached,
        requests: daemon.requests(),
    };
    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
    recovery
}

fn methods(requests: &[(String, serde_json::Value)]) -> Vec<&str> {
    requests.iter().map(|(method, _)| method.as_str()).collect()
}

/// Admission, the report that is the refresh trigger, and the re-read, in that order.
const REPORT_THEN_REREAD: [&str; 3] = [
    "credential.get_scoped",
    "credential.report_auth_failure",
    "credential.get_scoped",
];

/// An applied report followed by a newer token re-sends once: the refresher returns the
/// newer token with evidence naming `get_scoped` and the new record_version, and moves the
/// run and the cache to it.
#[tokio::test]
async fn an_applied_report_then_a_newer_token_re_sends_once() {
    let recovery = refresh_after_a_401("in-run-applied", ReportScript::MarksStale).await;
    let refreshed = recovery.refreshed.expect("the re-read served a newer token");
    assert!(
        refreshed.secret == "stand-in-token-v2",
        "the refreshed secret is the newer token: false"
    );
    assert_eq!(
        refreshed.evidence,
        broca_protocol::CredentialRefresh {
            vault_op: "get_scoped".into(),
            min_ttl_ms: Some(VAULT_MIN_TTL_MS),
            force_refresh: false,
            record_version: Some(2),
            reason: Some("rejected".into()),
        }
    );
    assert_eq!(recovery.served, 2, "the run now uses the refreshed version");
    assert_eq!(recovery.cached, Some(2), "the cache holds the refreshed token");
    assert_eq!(recovery.reported, Some(1), "the rejected version counts as reported");
    assert_eq!(recovery.logs.len(), 1);
    assert!(recovery.logs[0].contains("outcome=refreshed") && recovery.logs[0].contains("old_version=1") && recovery.logs[0].contains("new_version=2"), "{}", recovery.logs[0]);
    assert!(!recovery.logs[0].contains("stand-in-token"));
    assert_eq!(methods(&recovery.requests), REPORT_THEN_REREAD);
    let (admission, report, reread) = (
        &recovery.requests[0].1,
        &recovery.requests[1].1,
        &recovery.requests[2].1,
    );
    assert_eq!(reread, admission, "the re-read is the same credential and demand");
    assert_eq!(
        report,
        &serde_json::json!({
            "credential_id": "apikey:deepseek",
            "provider_status": 401,
            "reporter_source": "direct",
            "record_version": 1
        })
    );
}

/// The in-step report carries the status the provider actually answered, not a fixed 401. The
/// transport sends only 401s here today; 403 stands in for any other rejection status so a
/// hard-coded 401 cannot pass.
#[tokio::test]
async fn the_in_step_report_carries_the_rejections_own_status() {
    let recovery =
        refresh_after_a_rejection("in-run-status", ReportScript::MarksStale, 403).await;
    assert_eq!(methods(&recovery.requests), REPORT_THEN_REREAD);
    assert_eq!(
        recovery.requests[1].1,
        serde_json::json!({
            "credential_id": "apikey:deepseek",
            "provider_status": 403,
            "reporter_source": "direct",
            "record_version": 1
        })
    );
    assert_eq!(recovery.reported, Some(1));
}

/// A report that is not applied, with the version unchanged on the re-read, means the token
/// broca holds is the current one: the 401 is not staleness. No token comes back, so the
/// transport keeps the 401 and the run pauses with no re-send and no loop.
#[tokio::test]
async fn a_not_applied_report_with_an_unchanged_version_pauses_without_re_sending() {
    let recovery = refresh_after_a_401("in-run-not-applied", ReportScript::NotApplied).await;
    assert!(
        recovery.refreshed.is_none(),
        "an unchanged version must not be re-sent"
    );
    assert_eq!(recovery.served, 1);
    assert_eq!(recovery.cached, None, "the rejected version is not cached again");
    assert_eq!(recovery.reported, Some(1));
    assert_eq!(methods(&recovery.requests), REPORT_THEN_REREAD);
    assert_eq!(recovery.logs.len(), 1);
    assert!(recovery.logs[0].contains("outcome=same_version") && recovery.logs[0].contains("rejected_version=1") && recovery.logs[0].contains("served_version=1") && recovery.logs[0].contains("expires_at_ms=none"), "{}", recovery.logs[0]);
    assert!(!recovery.logs[0].contains("stand-in-token"));
}

/// Another reader refreshed the credential first, so the report names a superseded version
/// and is not applied; the re-read still returns the newer token, and the run re-sends once.
#[tokio::test]
async fn a_stale_report_then_a_newer_token_from_another_reader_re_sends_once() {
    let recovery =
        refresh_after_a_401("in-run-other-reader", ReportScript::AnotherReaderRefreshed).await;
    let refreshed = recovery.refreshed.expect("the newer token is used");
    assert_eq!(refreshed.evidence.record_version, Some(2));
    assert_eq!(recovery.served, 2);
    assert_eq!(recovery.cached, Some(2));
    assert_eq!(methods(&recovery.requests), REPORT_THEN_REREAD);
}

/// `needs_reauth` on the re-read: no fresh token, so the transport keeps the 401 and the run
/// pauses without a re-send. The rejected version is out of the cache.
#[tokio::test]
async fn needs_reauth_on_the_re_read_yields_no_token() {
    let recovery = refresh_after_a_401("in-run-reauth", ReportScript::NeedsReauth).await;
    assert!(recovery.refreshed.is_none(), "needs_reauth must not produce a token");
    assert_eq!(recovery.served, 1);
    assert_eq!(recovery.cached, None, "neither the rejected token nor anything else is cached");
    assert_eq!(methods(&recovery.requests), REPORT_THEN_REREAD);
    assert_eq!(recovery.logs.len(), 1);
    assert!(recovery.logs[0].contains("outcome=reread_failed") && recovery.logs[0].contains("error_class=CredentialUnusable") && recovery.logs[0].contains("vault_replied=true"), "{}", recovery.logs[0]);
    assert!(!recovery.logs[0].contains("stand-in-token"));
}

/// What the pre-send check produced, and what a 401 recovery after it produced.
struct PreSend {
    refreshed: Option<broca_provider::credential_refresh::RefreshedSecret>,
    logs: Vec<String>,
    /// The run's served version and its expiry right after the pre-send check.
    served: u64,
    served_expires_at_ms: Option<i64>,
    reported: Option<u64>,
    cached: Option<u64>,
    /// Every vault request up to and including the pre-send check.
    requests: Vec<(String, serde_json::Value)>,
    /// The 401 recovery run after the check, when asked for.
    after_401: Option<broca_provider::credential_refresh::RefreshedSecret>,
}

/// Admit a deepseek run against a stand-in serving tokens that expire as `expiry` says, then
/// run the check the transport makes before each attempt. With `then_401`, also ask for a
/// fresh token as the transport does after a 401.
async fn pre_send_check(label: &str, expiry: ExpiryScript, then_401: bool) -> PreSend {
    let root = fresh_vault_root(label);
    let daemon = CapturingVaultDaemon::start_expiring(&root, expiry).await;
    let executor = executor_with_capturing_vault(&root, &daemon).await;
    let auth = executor
        .decide_auth("deepseek", None, true, Some(AuthMethod::ApiKey), None)
        .await
        .unwrap_or_else(|error| panic!("vault-resolved admission: {error:?}"));
    let refresh = auth
        .in_run_refresh
        .expect("a token served by the vault has an in-run refresher");
    assert_eq!(refresh.served_version.load(Ordering::SeqCst), 1);

    let (refreshed, logs) = capture_credential_logs(async {
        let first = refresh.refresher.0.refresh_expiring_secret().await;
        if refresh.served_expires_at_ms.lock().unwrap().is_none() {
            let second = refresh.refresher.0.refresh_expiring_secret().await;
            assert!(second.is_none());
        }
        first
    }).await;
    daemon.assert_healthy();
    let served = refresh.served_version.load(Ordering::SeqCst);
    let served_expires_at_ms = *refresh.served_expires_at_ms.lock().unwrap();
    let reported = *refresh.reported_version.lock().unwrap();
    let cached = executor
        .vault
        .as_ref()
        .unwrap()
        .cache
        .get("apikey:deepseek")
        .map(|token| token.record_version);
    let requests = daemon.requests();
    let after_401 = if then_401 {
        refresh.refresher.0.refresh_rejected_secret(401).await
    } else {
        None
    };
    daemon.assert_healthy();
    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
    PreSend {
        refreshed,
        logs,
        served,
        served_expires_at_ms,
        reported,
        cached,
        requests,
        after_401,
    }
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// A held token with two minutes left is replaced before the send by a plain re-read of the
/// same credential with the same `min_ttl_ms`. No failure report reaches the vault, because
/// nothing rejected the token. The `CredentialRefresh` the attempt record will carry has
/// `reason: "expiring"`, and the run, its tracked expiry, and the cache all move to the new
/// token.
#[tokio::test]
async fn an_expiring_token_is_re_read_before_the_send_without_a_failure_report() {
    let check = pre_send_check(
        "pre-send-expiring",
        ExpiryScript {
            first_ttl_ms: Some(2 * 60 * 1000),
            refuse_read: None,
            ..ExpiryScript::default()
        },
        false,
    )
    .await;
    let refreshed = check.refreshed.expect("the expiring token is replaced");
    assert!(
        refreshed.secret == "stand-in-token-v2",
        "the pre-send secret is the newer token: false"
    );
    assert_eq!(
        refreshed.evidence,
        broca_protocol::CredentialRefresh {
            vault_op: "get_scoped".into(),
            min_ttl_ms: Some(VAULT_MIN_TTL_MS),
            force_refresh: false,
            record_version: Some(2),
            reason: Some("expiring".into()),
        }
    );
    assert_eq!(
        methods(&check.requests),
        ["credential.get_scoped", "credential.get_scoped"],
        "admission and one re-read, and no failure report"
    );
    assert_eq!(check.requests[1].1, check.requests[0].1, "the same credential and demand");
    assert_eq!(check.served, 2, "the run now uses the refreshed version");
    assert!(
        check
            .served_expires_at_ms
            .is_some_and(|expires| expires > now_ms() + 50 * 60 * 1000),
        "the tracked expiry is the refreshed token's: {:?}",
        check.served_expires_at_ms
    );
    assert_eq!(check.cached, Some(2), "the cache holds the refreshed token");
    assert_eq!(check.reported, None, "no version counts as reported");
}

/// A held token with thirty minutes left has room for any request: the check asks the vault
/// nothing.
#[tokio::test]
async fn a_token_with_thirty_minutes_left_is_not_re_read() {
    let check = pre_send_check(
        "pre-send-fresh",
        ExpiryScript {
            first_ttl_ms: Some(30 * 60 * 1000),
            refuse_read: None,
            ..ExpiryScript::default()
        },
        false,
    )
    .await;
    assert!(check.refreshed.is_none());
    assert_eq!(
        methods(&check.requests),
        ["credential.get_scoped"],
        "admission only"
    );
    assert_eq!(check.served, 1);
}

/// An API key has no expiry, so the check never asks the vault anything.
#[tokio::test]
async fn an_api_key_without_an_expiry_is_never_re_read() {
    let check = pre_send_check("pre-send-api-key", ExpiryScript::default(), false).await;
    assert!(check.refreshed.is_none());
    assert_eq!(
        methods(&check.requests),
        ["credential.get_scoped"],
        "admission only"
    );
    assert_eq!(check.served, 1);
    assert_eq!(check.served_expires_at_ms, None);
    assert_eq!(check.logs.len(), 1);
    assert!(check.logs[0].contains("has no expiry"));
    assert!(!check.logs[0].contains("stand-in-token"));
}

/// The vault refuses the pre-send re-read: no token comes back, so the attempt goes out with
/// the held one and the run keeps its version. When the provider then answers 401, the
/// rejection path still reports and re-reads, and recovers with a newer token.
#[tokio::test]
async fn a_failed_pre_send_re_read_keeps_the_held_token_and_a_401_still_recovers() {
    let check = pre_send_check(
        "pre-send-refused",
        ExpiryScript {
            first_ttl_ms: Some(2 * 60 * 1000),
            refuse_read: Some(2),
            ..ExpiryScript::default()
        },
        true,
    )
    .await;
    assert!(check.refreshed.is_none(), "a refused re-read yields no token");
    assert_eq!(check.served, 1, "the run keeps the held version");
    let recovered = check.after_401.expect("the 401 path still recovers");
    assert_eq!(recovered.evidence.record_version, Some(2));
    assert_eq!(recovered.evidence.reason.as_deref(), Some("rejected"));
}

/// ONE REPORT PER REJECTION, for the version actually rejected. The in-step report is the
/// refresh trigger, so the run-end report never repeats it, even after a refresh moved the run
/// to a newer version that was never rejected. A rejection of the refreshed token that the
/// step could not recover from is reported once, with its own status and version. A run that
/// never tried to recover reports the version it held.
#[test]
fn the_run_end_report_names_only_a_rejection_nothing_has_reported() {
    struct NeverRefreshes;
    impl broca_provider::credential_refresh::SecretRefresher for NeverRefreshes {
        fn refresh_rejected_secret(
            &self,
            _status: u16,
        ) -> broca_provider::credential_refresh::RefreshFuture<
            '_,
            broca_provider::credential_refresh::RefreshedSecret,
        > {
            Box::pin(async { None })
        }
    }
    let in_run = |served: u64, reported: Option<u64>, unreported: Option<UnreportedRejection>| {
        InRunRefresh {
            refresher: broca_provider::credential_refresh::SharedSecretRefresher(Arc::new(
                NeverRefreshes,
            )),
            served_version: Arc::new(AtomicU64::new(served)),
            served_expires_at_ms: Arc::new(std::sync::Mutex::new(None)),
            reported_version: Arc::new(std::sync::Mutex::new(reported)),
            unreported_rejection: Arc::new(std::sync::Mutex::new(unreported)),
        }
    };
    let rejection = |status, record_version| UnreportedRejection {
        status,
        record_version,
    };
    // In-step refresh reported v1 and moved the run to v2, which was never rejected.
    assert_eq!(run_end_report_target(Some(&in_run(2, Some(1), None)), 1, 401), None);
    // In-step refresh reported v1 and the vault had nothing newer.
    assert_eq!(run_end_report_target(Some(&in_run(1, Some(1), None)), 1, 401), None);
    // The refreshed v2 was rejected too: report v2 alone, with its own status.
    assert_eq!(
        run_end_report_target(Some(&in_run(2, Some(1), Some(rejection(401, 2)))), 1, 401),
        Some(rejection(401, 2))
    );
    // A refresher that never ran: report the version the run holds now (3), with the status the
    // run's terminal error carries.
    assert_eq!(
        run_end_report_target(Some(&in_run(3, None, None)), 1, 401),
        Some(rejection(401, 3))
    );
    // A run with no in-step refresher reports the version admission served it (7).
    assert_eq!(run_end_report_target(None, 7, 401), Some(rejection(401, 7)));
}

struct ChatgptRouting {
    requests: std::sync::Mutex<Vec<serde_json::Value>>,
}

#[async_trait::async_trait]
impl broca_module_serve::account_routing::RoutingTransport for ChatgptRouting {
    async fn request(&self, body: Vec<u8>) -> broca_module_serve::account_routing::RoutingReply {
        self.requests.lock().unwrap().push(serde_json::from_slice(&body).unwrap());
        Ok(Ok(serde_json::to_vec(&serde_json::json!({"result": {
            "account": {"provider": "openai", "method": "chatgpt", "account_id": "chatgpt-b", "credential_id": "chatgpt:openai:second"},
            "reason": {"decision_id": "chatgpt-decision"}
        }})).unwrap()))
    }
}

async fn routed_chatgpt_choice() -> AccountChoice {
    use broca_module_serve::account_routing::{choose_account, ChoiceInputs, RoutingClient, ADMISSION_BUDGET};
    let routing = Arc::new(ChatgptRouting { requests: Default::default() });
    let client = RoutingClient::new(routing.clone(), broca_module_serve::account_routing::AccountSelectionConfig::default());
    let candidates = [ ("chatgpt:openai", "chatgpt-a"), ("chatgpt:openai:second", "chatgpt-b") ]
        .into_iter().map(|(id, account)| broca_subc::vault::ScopedCredential {
            id: id.into(), categories: vec!["openai".into()], credential_type: "chatgpt".into(),
            serves: vec!["openai".into()], refresh_adapter: Some("openai".into()), state: "active".into(),
            record_version: 1, operations: vec!["read".into()], account_id: Some(account.into()),
            email: None, org_name: None, provider_ids: Vec::new(), auth_method: None,
        }).collect();
    let listing = Arc::new(broca_subc::vault::ScopedList {
        credentials: candidates, grants: 1, grant_tuples: Vec::new(), view: "chatgpt-view".into(),
    });
    let credential = CredentialId::parse("chatgpt:openai").unwrap();
    let choice = choose_account(&client, ChoiceInputs {
        conversation_key: None, inherited_conversation_key: None,
        static_credential: &credential, caller_named_account: false,
        model: "gpt-6.1", session: &broca_protocol::BindIdentity::new("/tmp", "broca", "chatgpt"),
        send_id: None,
        run_id: "chatgpt-run", initial_input_bytes: 42,
    }, async { Some(listing) }, tokio::time::Instant::now() + ADMISSION_BUDGET).await.unwrap();
    let requests = routing.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["params"]["provider"], "openai");
    assert_eq!(requests[0]["params"]["candidates"]["accounts"], serde_json::json!([
        {"account_id": "chatgpt-a", "credential_id": "chatgpt:openai"},
        {"account_id": "chatgpt-b", "credential_id": "chatgpt:openai:second"}
    ]));
    choice
}

#[tokio::test]
async fn routed_chatgpt_reads_the_second_credential_and_freezes_the_decision() {
    let root = fresh_vault_root("chatgpt-routed");
    let daemon = CapturingVaultDaemon::start_expiring(&root, ExpiryScript {
        account_id: Some("chatgpt-b"), ..ExpiryScript::default()
    }).await;
    let executor = executor_with_capturing_vault(&root, &daemon).await;
    let choice = routed_chatgpt_choice().await;
    let resolved = executor.decide_auth_with_account("openai", None, true, Some(AuthMethod::Chatgpt), None, &choice, None).await.unwrap();
    let selection = resolved.selection.unwrap();
    assert_eq!(selection.credential_id, "chatgpt:openai:second");
    assert_eq!(selection.account_ref.as_deref(), Some("chatgpt-b"));
    let frozen = serde_json::to_value(selection).unwrap();
    assert_eq!(frozen["account_decision"]["path"], "routed");
    assert_eq!(frozen["account_decision"]["decision_id"], "chatgpt-decision");
    let requests = daemon.requests();
    assert_eq!(methods(&requests), ["credential.get_scoped"]);
    assert_eq!(requests[0].1["credential_id"], "chatgpt:openai:second");
    assert_eq!(resolved.method, Some(AuthMethod::Chatgpt));
    daemon.assert_healthy();
}

#[tokio::test]
async fn routed_chatgpt_refuses_a_different_vault_account_identity() {
    let root = fresh_vault_root("chatgpt-mismatch");
    let daemon = CapturingVaultDaemon::start_expiring(&root, ExpiryScript {
        account_id: Some("chatgpt-wrong"), ..ExpiryScript::default()
    }).await;
    let executor = executor_with_capturing_vault(&root, &daemon).await;
    let choice = routed_chatgpt_choice().await;
    match executor.decide_auth_with_account("openai", None, true, Some(AuthMethod::Chatgpt), None, &choice, None).await {
        Err(RunError::Config(message)) => assert!(message.starts_with("account_identity_mismatch:"), "{message}"),
        _ => panic!("a token for another account must refuse admission"),
    }
    assert_eq!(daemon.requests()[0].1["credential_id"], "chatgpt:openai:second");
    daemon.assert_healthy();
}

#[tokio::test]
async fn account_mismatch_does_not_populate_lkg() {
    let root = fresh_vault_root("chatgpt-mismatch-cache");
    let daemon = CapturingVaultDaemon::start_expiring(&root, ExpiryScript {
        account_id: Some("chatgpt-wrong"), ..ExpiryScript::default()
    }).await;
    let mut executor = executor_with_capturing_vault(&root, &daemon).await;
    let choice = routed_chatgpt_choice().await;
    let credential_id = "chatgpt:openai:second";
    match executor.decide_auth_with_account("openai", None, true, Some(AuthMethod::Chatgpt), None, &choice, None).await {
        Err(RunError::Config(message)) => assert!(message.starts_with("account_identity_mismatch:"), "{message}"),
        _ => panic!("a token for another account must refuse admission"),
    }
    assert!(executor.vault.as_ref().unwrap().cache.get(credential_id).is_none(),
        "the refused account must not enter the shared fallback cache");
    daemon.assert_healthy();

    // Keep the same cache, but point its client at a restarting vault that cannot open
    // a route. Both routed and static admissions must refuse rather than resurrect it.
    let outage_root = fresh_vault_root("chatgpt-mismatch-outage");
    let outage_daemon = CapturingVaultDaemon::start_with(&outage_root,
        RouteOpenScript::RefuseForever("target_unavailable")).await;
    let mut outage_executor = executor_with_capturing_vault_within(
        &outage_root, &outage_daemon, Duration::from_millis(100)).await;
    let outage_vault = Arc::try_unwrap(outage_executor.vault.take().unwrap())
        .unwrap_or_else(|_| panic!("outage fixture must own its vault"));
    Arc::get_mut(executor.vault.as_mut().unwrap()).unwrap().client = outage_vault.client;

    assert!(matches!(executor.decide_auth_with_account("openai", None, true,
        Some(AuthMethod::Chatgpt), None, &choice, None).await,
        Err(RunError::Config(message)) if message.starts_with("credential not configured or unusable:")),
        "a routed admission must not serve the mismatched token during an outage");
    assert!(matches!(executor.decide_auth("openai", None, true,
        Some(AuthMethod::Chatgpt), Some("second")).await,
        Err(RunError::Config(message)) if message.starts_with("credential not configured or unusable:")),
        "a static admission sharing the credential key must not serve the mismatched token");
    assert!(outage_daemon.requests().is_empty());
    assert!(outage_daemon.route_open_count() >= 2);
    outage_daemon.assert_healthy();
    drop(executor);
    drop(outage_executor);
    drop(daemon);
    drop(outage_daemon);
    let _ = std::fs::remove_dir_all(root);
    let _ = std::fs::remove_dir_all(outage_root);
}


/// Collect formatted credentials events while exercising a real refresher.
async fn capture_credential_logs<F: std::future::Future>(future: F) -> (F::Output, Vec<String>) {
    let (output, events) = broca_test_support::capture_logs(future).await;
    let logs = events
        .into_iter()
        .filter(|event| event.target == "credentials")
        .map(|event| event.message().to_owned())
        .collect();
    (output, logs)
}


#[tokio::test]
async fn rejected_reread_with_no_reply_is_logged_once_without_secret() {
    let root = fresh_vault_root("in-run-no-reply-log");
    let daemon = CapturingVaultDaemon::start_scripted(
        &root, RouteOpenScript::Accept, false, ReportScript::NotApplied,
        ExpiryScript { drop_read: Some(2), ..ExpiryScript::default() },
    ).await;
    let executor = executor_with_capturing_vault(&root, &daemon).await;
    let auth = executor.decide_auth("deepseek", None, true, Some(AuthMethod::ApiKey), None).await.unwrap();
    let refresh = auth.in_run_refresh.unwrap();
    let (_, logs) = capture_credential_logs(async { refresh.refresher.0.refresh_rejected_secret(401).await }).await;
    assert_eq!(logs.len(), 1);
    assert!(logs[0].contains("outcome=reread_failed") && logs[0].contains("rejected_version=1") && logs[0].contains("vault_replied=false") && logs[0].contains("error_class=Connectivity"), "{}", logs[0]);
    assert!(!logs[0].contains("stand-in-token"));
    drop(executor); drop(daemon); let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn pre_send_same_version_is_logged_once_without_secret() {
    let check = pre_send_check("pre-send-same-version-log", ExpiryScript {
        first_ttl_ms: Some(2 * 60 * 1000), unchanged_read: Some(2), ..ExpiryScript::default()
    }, false).await;
    assert_eq!(check.logs.len(), 1);
    assert!(check.logs[0].contains("outcome=same_version") && check.logs[0].contains("served_version=1"), "{}", check.logs[0]);
    assert!(!check.logs[0].contains("stand-in-token"));
}

fn chatgpt_test_token(version: u64, other_account: bool) -> String {
    let claims = if version > 1 && other_account {
        "eyJjaGF0Z3B0X2FjY291bnRfaWQiOiJhY2N0LW90aGVyIn0"
    } else {
        "eyJjaGF0Z3B0X2FjY291bnRfaWQiOiJhY2N0LWNoYXRncHQifQ"
    };
    format!("e30.{claims}.test-signature-{version}")
}

struct ChatgptRefreshRun {
    summary: broca_core::RunSummary,
    requests: Vec<(String, serde_json::Value)>,
    sent: Vec<(String, Vec<u8>)>,
    attempts: Vec<Option<broca_protocol::CredentialRefresh>>,
    pause_code: Option<String>,
}

/// Drive real vault admission, catalog auth shaping, transport and run-loop pause mapping.
/// Only the vault daemon and the ChatGPT HTTP backend are scripted locally.
async fn chatgpt_refresh_run(
    label: &str,
    reports: ReportScript,
    expiry: ExpiryScript,
    reject_first: bool,
) -> ChatgptRefreshRun {
    chatgpt_refresh_run_rejecting(label, reports, expiry, usize::from(reject_first)).await
}

/// [`chatgpt_refresh_run`] whose upstream answers the first `rejections` requests with a 401.
/// After the run it does what the executor does at run end: when the terminal is a wire auth
/// rejection, it evicts and reports whatever rejection the in-step refresh did not report.
async fn chatgpt_refresh_run_rejecting(
    label: &str,
    reports: ReportScript,
    mut expiry: ExpiryScript,
    rejections: usize,
) -> ChatgptRefreshRun {
    use broca_core::{run_loop, AllowAll, FileLeaseStore, StubToolPlane};
    use broca_protocol::content::{ContentBlock, Message, Role};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let root = fresh_vault_root(label);
    expiry.chatgpt_jwt = true;
    expiry.account_id = Some("acct-chatgpt");
    let daemon = CapturingVaultDaemon::start_scripted(
        &root,
        RouteOpenScript::Accept,
        false,
        reports,
        expiry,
    )
    .await;
    let executor = executor_with_capturing_vault(&root, &daemon).await;
    let auth = executor
        .decide_auth("openai", None, true, Some(AuthMethod::Chatgpt), None)
        .await
        .unwrap_or_else(|error| panic!("ChatGPT admission: {error}"));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = sent.clone();
    let server = tokio::spawn(async move {
        // Continue accepting so an erroneous extra resend fails a count assertion, not a timeout.
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = Vec::new();
            let mut buf = [0; 4096];
            let (headers, body_start, body_len) = loop {
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0, "complete HTTP request headers");
                bytes.extend_from_slice(&buf[..n]);
                if let Some(end) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
                    let len = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap();
                    break (headers, end + 4, len);
                }
            };
            while bytes.len() < body_start + body_len {
                let n = socket.read(&mut buf).await.unwrap();
                assert!(n > 0, "complete HTTP request body");
                bytes.extend_from_slice(&buf[..n]);
            }
            let rejected = {
                let mut sent = captured.lock().unwrap();
                let rejected = sent.len() < rejections;
                sent.push((headers, bytes[body_start..body_start + body_len].to_vec()));
                rejected
            };
            let (status, content_type, body) = if rejected {
                (
                    "401 Unauthorized",
                    "application/json",
                    "{\"error\":{\"code\":\"token_expired\",\"message\":\"token expired\"}}",
                )
            } else {
                ("200 OK", "text/event-stream",
                 "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_test\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n")
            };
            let response = format!("HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            socket.write_all(response.as_bytes()).await.unwrap();
        }
    });
    install_test_catalog();
    let options = broca_catalog::live::ResolveOptions {
        family_override: Some(WireFamilyId::OpenAiResponsesChatgpt),
        auth_method: Some(AuthMethod::Chatgpt),
        account_id: auth.account_id.as_deref(),
        frozen_account_ref: Some("acct-chatgpt"),
        session_id: Some("chatgpt-refresh-session"),
        prompt_cache_key: Some("chatgpt-refresh-cache"),
        secret_refresher: auth
            .in_run_refresh
            .as_ref()
            .map(|refresh| refresh.refresher.clone()),
        ..Default::default()
    };
    let resolved = resolve_provider_for_episode(
        "openai/gpt-5",
        Some(&url),
        Some(128),
        &auth,
        &options,
        build_provider_http_client(),
        Default::default(),
    )
    .unwrap_or_else(|error| panic!("ChatGPT resolution: {error}"));
    // This refresh fixture parses HTTP POST bodies; pin HTTPS so it tests token recovery,
    // not socket negotiation, while retaining its original send-count assertions.
    broca_core::Provider::set_chatgpt_https_policy(&resolved.provider, Arc::new(|| true));
    let session = BindIdentity::new(&root, "chatgpt-refresh", label);
    let req = broca_core::RunRequest { session_class: None, retention: None,
    title: None,
    on_restart: None,
    keep_warm: None,
    send_id: None,
    request_identity_digest: None,
    scope: None,
    project_id: None,
    principal: None,
    run_id: broca_protocol::RunId(label.into()),
    session: session.clone(),
    model: broca_protocol::request::ModelHandle {
        provider_module_id: "openai".into(),
        model_id: "gpt-5".into(),
    },
    input: vec![Message {
        cache_prefix_blocks: None,
        origin: None,
        ordinal: None,
        role: Role::User,
        content: vec![ContentBlock::Text { text: "hi".into() }],
    }],
    tools: vec![],
    policy_allowed: Default::default(),
    tool_choice: Default::default(),
    generation: Default::default(),
    cache: None,
    budget: None,
    context_limit: None,
    work_class: None,
    variant: None,
    service_tier: None,
    stop_when: vec![broca_protocol::request::StopCondition::StepCount { n: 1 }],
    submission_id: None,
    context_management: None,
    inherit_frozen_cache: false, structured_output: None, };
    let wal = FileWalStore::new(root.join("refresh-wal"));
    let lease = FileLeaseStore::new(root.join("refresh-lease"));
    let (tx, mut rx) = mpsc::channel(256);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    let summary = tokio::time::timeout(
        Duration::from_secs(30),
        run_loop(
            req,
            &resolved.provider,
            &StubToolPlane::new(),
            &lease,
            &AllowAll,
            &wal,
            &tx,
        ),
    )
    .await
    .expect("local ChatGPT run terminates")
    .expect("run succeeds or pauses");
    drop(tx);
    drain.await.unwrap();
    server.abort();
    let records = wal.read_records(&session).unwrap();
    if let (Some(status), Some((credential_key, admitted_version))) = (
        latest_terminal_error(&records).and_then(wire_auth_status),
        used_credential_for_report(&auth.pause, auth.selection.clone(), auth.record_version),
    ) {
        report_rejection_after_run(
            executor.vault.as_ref().unwrap(),
            &credential_key,
            admitted_version,
            auth.in_run_refresh.as_ref(),
            status,
        )
        .await;
    }
    let attempts = wal
        .read_records(&session)
        .unwrap()
        .into_iter()
        .filter_map(|frame| {
            if let broca_wal::WalRecord::ModelAttemptFinished {
                credential_refresh, ..
            } = frame.record
            {
                Some(credential_refresh)
            } else {
                None
            }
        })
        .collect();
    let pause_code = wal
        .read_records(&session)
        .unwrap()
        .into_iter()
        .find_map(|frame| match frame.record {
            broca_wal::WalRecord::RunPaused {
                error: Some(error), ..
            } => error.provider_code,
            _ => None,
        });
    daemon.assert_healthy();
    let result = ChatgptRefreshRun {
        summary,
        requests: daemon.requests(),
        sent: sent.lock().unwrap().clone(),
        attempts,
        pause_code,
    };
    drop(executor);
    drop(daemon);
    let _ = std::fs::remove_dir_all(root);
    result
}

/// Compare only booleans so even a failing assertion never formats an access token.
fn chatgpt_header_is(headers: &str, name: &str, expected: &str) -> bool {
    headers.lines().any(|line| {
        line.split_once(':')
            .is_some_and(|(key, value)| key.eq_ignore_ascii_case(name) && value.trim() == expected)
    })
}

fn assert_chatgpt_refreshed_headers(headers: &str) {
    assert!(chatgpt_header_is(
        headers,
        "authorization",
        &format!("Bearer {}", chatgpt_test_token(2, false))
    ));
    assert!(chatgpt_header_is(
        headers,
        "chatgpt-account-id",
        "acct-chatgpt"
    ));
    assert!(chatgpt_header_is(
        headers,
        "session-id",
        "chatgpt-refresh-cache"
    ));
    assert!(chatgpt_header_is(headers, "originator", "codex_cli_rs"));
    assert!(headers.lines().any(|line| line
        .to_ascii_lowercase()
        .starts_with("user-agent: codex_cli_rs/")));
}

#[tokio::test]
async fn chatgpt_expiring_token_refreshes_before_send() {
    let run = chatgpt_refresh_run(
        "chatgpt-expiring",
        ReportScript::MarksStale,
        ExpiryScript {
            first_ttl_ms: Some(120_000),
            ..Default::default()
        },
        false,
    )
    .await;
    assert_eq!(run.summary.paused, None);
    assert_eq!(run.sent.len(), 1);
    assert_chatgpt_refreshed_headers(&run.sent[0].0);
    assert_eq!(
        methods(&run.requests),
        ["credential.get_scoped", "credential.get_scoped"]
    );
    assert_eq!(run.attempts.len(), 1);
    assert_eq!(
        run.attempts[0].as_ref().unwrap().reason.as_deref(),
        Some("expiring")
    );
}

#[tokio::test]
async fn chatgpt_rejected_token_refreshes_and_resends_once() {
    let run = chatgpt_refresh_run(
        "chatgpt-rejected",
        ReportScript::AnotherReaderRefreshed,
        ExpiryScript::default(),
        true,
    )
    .await;
    assert_eq!(run.summary.paused, None);
    assert_eq!(run.sent.len(), 2);
    assert!(chatgpt_header_is(
        &run.sent[0].0,
        "authorization",
        &format!("Bearer {}", chatgpt_test_token(1, false))
    ));
    assert_chatgpt_refreshed_headers(&run.sent[1].0);
    assert!(
        run.sent[0].1 == run.sent[1].1,
        "refresh preserves rendered request bytes"
    );
    assert_eq!(
        methods(&run.requests),
        [
            "credential.get_scoped",
            "credential.report_auth_failure",
            "credential.get_scoped"
        ]
    );
    assert_eq!(run.requests[1].1["record_version"], 1);
    assert_eq!(run.attempts.len(), 2);
    assert!(run.attempts[0].is_none());
    assert_eq!(
        run.attempts[1].as_ref().unwrap().reason.as_deref(),
        Some("rejected")
    );
    assert_eq!(run.attempts[1].as_ref().unwrap().record_version, Some(2));
}

#[tokio::test]
async fn chatgpt_rejected_same_version_pauses_auth_required() {
    let run = chatgpt_refresh_run(
        "chatgpt-same-version",
        ReportScript::NotApplied,
        ExpiryScript::default(),
        true,
    )
    .await;
    assert_eq!(run.sent.len(), 1);
    assert_eq!(run.summary.paused, Some(PauseReason::AuthRequired));
    assert_eq!(run.attempts.len(), 1);
}

/// Every vault failure report the run sent, as (provider_status, record_version), in order.
fn failure_reports(requests: &[(String, serde_json::Value)]) -> Vec<(u64, u64)> {
    requests
        .iter()
        .filter(|(method, _)| method == "credential.report_auth_failure")
        .map(|(_, params)| {
            (
                params["provider_status"].as_u64().unwrap(),
                params["record_version"].as_u64().unwrap(),
            )
        })
        .collect()
}

/// ONE REJECTION, ONE REPORT. The vault had no newer token, so the in-step report of v1 is the
/// only rejection; the run-end report adds nothing.
#[tokio::test]
async fn chatgpt_rejected_same_version_is_reported_once() {
    let run = chatgpt_refresh_run_rejecting(
        "chatgpt-same-version-once",
        ReportScript::NotApplied,
        ExpiryScript::default(),
        1,
    )
    .await;
    assert_eq!(run.summary.paused, Some(PauseReason::AuthRequired));
    assert_eq!(run.sent.len(), 1);
    assert_eq!(failure_reports(&run.requests), [(401, 1)]);
}

/// The refreshed token is rejected too: v1 is reported in-step (the refresh trigger) and v2,
/// which nothing reported, once at run end. Never v1 twice and never v2 twice.
#[tokio::test]
async fn chatgpt_refreshed_token_rejected_again_is_reported_once_per_version() {
    let run = chatgpt_refresh_run_rejecting(
        "chatgpt-rejected-twice",
        ReportScript::MarksStale,
        ExpiryScript::default(),
        2,
    )
    .await;
    assert_eq!(run.summary.paused, Some(PauseReason::AuthRequired));
    assert_eq!(run.sent.len(), 2, "one in-step recovery, then the step ends");
    assert_chatgpt_refreshed_headers(&run.sent[1].0);
    assert_eq!(failure_reports(&run.requests), [(401, 1), (401, 2)]);
}

/// A refresh that recovers leaves nothing to report at run end: only the in-step report of v1.
#[tokio::test]
async fn chatgpt_recovered_rejection_sends_no_run_end_report() {
    let run = chatgpt_refresh_run_rejecting(
        "chatgpt-recovered-once",
        ReportScript::MarksStale,
        ExpiryScript::default(),
        1,
    )
    .await;
    assert_eq!(run.summary.paused, None);
    assert_eq!(run.sent.len(), 2);
    assert_eq!(failure_reports(&run.requests), [(401, 1)]);
}

#[tokio::test]
async fn chatgpt_refreshed_vault_account_mismatch_is_never_sent() {
    let run = chatgpt_refresh_run(
        "chatgpt-vault-account",
        ReportScript::AnotherReaderRefreshed,
        ExpiryScript {
            refreshed_account_id: Some("acct-other"),
            ..Default::default()
        },
        true,
    )
    .await;
    assert_eq!(run.sent.len(), 1);
    assert_eq!(run.summary.paused, Some(PauseReason::AuthRequired));
    assert_eq!(run.pause_code.as_deref(), Some("account_changed"));
}

#[tokio::test]
async fn chatgpt_refreshed_jwt_account_mismatch_is_never_sent() {
    let run = chatgpt_refresh_run(
        "chatgpt-jwt-account",
        ReportScript::AnotherReaderRefreshed,
        ExpiryScript {
            refreshed_jwt_other_account: true,
            ..Default::default()
        },
        true,
    )
    .await;
    assert_eq!(run.sent.len(), 1);
    assert_eq!(run.summary.paused, Some(PauseReason::AuthRequired));
    assert_eq!(run.pause_code.as_deref(), Some("account_changed"));
}

#[tokio::test]
async fn chatgpt_expiring_vault_account_mismatch_is_never_sent() {
    let run = chatgpt_refresh_run(
        "chatgpt-expiring-vault-account",
        ReportScript::MarksStale,
        ExpiryScript {
            first_ttl_ms: Some(120_000),
            refreshed_account_id: Some("acct-other"),
            ..Default::default()
        },
        false,
    ).await;
    assert!(run.sent.is_empty());
    assert_eq!(run.summary.paused, Some(PauseReason::AuthRequired));
    assert_eq!(run.pause_code.as_deref(), Some("account_changed"));
}

#[tokio::test]
async fn chatgpt_expiring_jwt_account_mismatch_is_never_sent() {
    let run = chatgpt_refresh_run(
        "chatgpt-expiring-jwt-account",
        ReportScript::MarksStale,
        ExpiryScript {
            first_ttl_ms: Some(120_000),
            refreshed_jwt_other_account: true,
            ..Default::default()
        },
        false,
    ).await;
    assert!(run.sent.is_empty());
    assert_eq!(run.summary.paused, Some(PauseReason::AuthRequired));
    assert_eq!(run.pause_code.as_deref(), Some("account_changed"));
}
