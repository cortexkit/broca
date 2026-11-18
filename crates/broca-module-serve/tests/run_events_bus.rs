//! Private JetStream tests. No production daemon, credentials, or installed ck-* binaries.
//!
//! Most tests here record run events in the outbox and drive the publisher directly.
//! `real_daemon_turn_publishes_*` instead runs the real `ck-broca` under the real `ck-subc` daemon,
//! with run events switched on in its config file, and checks that one ordinary turn reaches the
//! stream with nothing but the process's own wiring in between. The ck-bus credential service is
//! a stand-in registered from this process; it mints user credentials from the private broker's
//! own trust chain.
#[path = "common/routing_fixture.rs"]
mod routing_fixture;
#[cfg(unix)]
#[path = "common/stored_log.rs"]
mod stored_log;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use broca_module_serve::{bus_events::now_ms, bus_events_nats::NatsPublisher};
use broca_protocol::run_events::{RunEvent, RunEventDetail};
use broca_protocol::{BindIdentity, PauseReason, RunFinishReason};
use broca_store::{
    bus_events::{BusPublisher, DrainResult, RunEventStore as _},
    SqliteStore,
};
use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_nats::{ConnectConfig, NatsConnection};
use cortexkit_bus_testkit::{
    trust::{box_root_id, signer_root_id, unix_now},
    BusOptions, BusServer,
};
use cortexkit_bus_trait::{ContentDigest, DurableOwner, Stream, StreamCursor};
use nkeys::KeyPair;
use serde_json::json;
use sha2::{Digest, Sha256};

struct NoActor;
#[async_trait::async_trait]
impl broca_module::registry::LineageOpener for NoActor {
    async fn open(
        &self,
        _key: &broca_module::LineageKey,
        _gen: u64,
        _registry: broca_module::registry::RegistryHandle,
    ) -> Result<broca_module::registry::ActorHandle, broca_module::registry::OpenError> {
        panic!("run.event.get must not open an actor");
    }
}
use std::{sync::Arc, time::Duration};

struct Scratch(std::path::PathBuf);
impl Scratch {
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
fn scratch() -> Scratch {
    // A clock reading alone can repeat for tests starting together, so a counter keeps names unique.
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = broca_test_support::scratch_root().join(format!(
        "run-events-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).unwrap();
    Scratch(path)
}

fn jwt(signer: &KeyPair, subject: &str, nats: serde_json::Value) -> String {
    let mut claims =
        json!({"jti":"","iat":unix_now()-120,"iss":signer.public_key(),"sub":subject,"nats":nats});
    claims["jti"] = format!("{:X}", Sha256::digest(claims.to_string())).into();
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(br#"{"alg":"ed25519-nkey","typ":"JWT"}"#),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    format!(
        "{input}.{}",
        URL_SAFE_NO_PAD.encode(signer.sign(input.as_bytes()).unwrap())
    )
}

async fn fixture() -> Option<(BusServer, NatsConnection, AccountNames, String)> {
    let bus = match BusServer::start(BusOptions {
        directory: Some(scratch().path().join("broker")),
        ..Default::default()
    })
    .await
    {
        Ok(bus) => bus,
        Err(error) => {
            cortexkit_bus_testkit::skip_or_fail(error);
            return None;
        }
    };
    println!("private NATS {}", bus.version);
    let account = KeyPair::new_account();
    // Testkit's default account has no JetStream quota. Provision only this disposable account.
    let claims = json!({"type":"account","version":2,"signing_keys":[bus.trust.box_root_public()],"limits":{"subs":-1,"data":-1,"payload":-1,"imports":-1,"exports":-1,"wildcards":true,"conn":-1,"leaf":-1,"mem_storage":-1,"disk_storage":-1,"streams":-1,"consumer":-1},"default_permissions":{"pub":{},"sub":{}}});
    std::fs::write(
        bus.dir
            .join("jwt")
            .join(format!("{}.jwt", account.public_key())),
        jwt(
            &bus.trust.signer.root(&signer_root_id()).pair,
            &account.public_key(),
            claims,
        ),
    )
    .unwrap();
    let conn = fixture_connection(&bus, &account.public_key()).await;
    let names = AccountNames::derive("box_broca_test").unwrap();
    Some((bus, conn, names, account.public_key()))
}

async fn fixture_connection(bus: &BusServer, account_public: &str) -> NatsConnection {
    let user = Arc::new(KeyPair::new_user());
    let user_jwt = bus
        .trust
        .user_jwt(&box_root_id(), account_public, &user, unix_now() - 60);
    let config = ConnectConfig::new(user.public_key()).unwrap();
    let options = async_nats::ConnectOptions::with_jwt(user_jwt, move |nonce| {
        let user = user.clone();
        async move { user.sign(&nonce).map_err(async_nats::AuthError::new) }
    });
    NatsConnection::connect(&bus.url, options, config)
        .await
        .unwrap()
}

async fn provision(conn: &NatsConnection, names: &AccountNames) -> NatsPublisher {
    let js = async_nats::jetstream::new(conn.client());
    let stream = js
        .create_stream(async_nats::jetstream::stream::Config {
            name: names.streams().event.clone(),
            subjects: vec![names.event_binding()],
            duplicate_window: Duration::from_secs(120),
            ..Default::default()
        })
        .await
        .unwrap();
    stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            durable_name: Some(AccountNames::module_consumer_name("prefrontal-core").unwrap()),
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await
        .unwrap();
    NatsPublisher {
        stream: conn.stream(names.streams().event.clone()),
        account: names.clone(),
        connection: conn.clone(),
    }
}

/// The events a store session's writer hands the outbox for one run: it started, a steer it
/// took was consumed at its first step, it paused for a restart, and it ended. The seqs are its
/// records' seqs, as the writer's projection gives them (the steer's own record is seq 2).
fn run_events(session: &BindIdentity, run_id: &str) -> Vec<RunEvent> {
    let event = |seq, detail| RunEvent {
        v: 1,
        session_ref: broca_core::wal_writer::agent_session_ref(session),
        run_id: run_id.into(),
        seq,
        at_ms: now_ms(),
        detail,
    };
    vec![
        event(1, RunEventDetail::Started {}),
        event(
            3,
            RunEventDetail::SteerConsumed {
                step: 1,
                consumed: 1,
                send_ids: vec!["steer-a".into()],
                truncated: false,
            },
        ),
        event(
            4,
            RunEventDetail::Paused {
                pause_reason: PauseReason::Restart,
            },
        ),
        event(
            5,
            RunEventDetail::Ended {
                terminal: RunFinishReason::Completed,
                error_class: None,
                structured_output: None,
            },
        ),
    ]
}

/// Records `events` in `store`'s outbox, as the store writer's sink does once their records are
/// durable.
fn record(store: &SqliteStore, session: &BindIdentity, mut events: Vec<RunEvent>) {
    assert!(
        store
            .record_run_events(session, "0123456789abcdef", &mut events, now_ms())
            .unwrap(),
        "the outbox records events once the bus account is known"
    );
}

#[tokio::test]
async fn real_bus_headers_digest_sequence_and_ack_retention() {
    use broca_module::serve::ServeHandler;
    let Some((_bus, conn, names, _)) = fixture().await else {
        return;
    };
    let bus = provision(&conn, &names).await;
    let temp = scratch();
    let session = BindIdentity::new("/test", "test", "events");
    let store = Arc::new(SqliteStore::open(temp.path().join("run-index.db")).unwrap());
    store.enable_bus_events(names.account()).unwrap();
    record(&store, &session, run_events(&session, "bus-run"));
    let registry =
        broca_module::LineageRegistry::new_with_run_index(Arc::new(NoActor), Some(store.clone()));
    let mut cursor = bus
        .stream
        .consumer(DurableOwner::Module("prefrontal-core"))
        .await
        .unwrap();
    for (seq, kind) in [
        (1, "run.started"),
        (3, "run.steer_consumed"),
        (4, "run.paused"),
        (5, "run.ended"),
    ] {
        let event = store.pending_bus_event().unwrap().unwrap();
        assert_eq!(
            store.drain_bus_event(&bus, now_ms()).await.unwrap(),
            DrainResult::Published
        );
        let delivery = tokio::time::timeout(Duration::from_secs(5), cursor.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(delivery.message.id, event.event_key);
        assert_eq!(delivery.message.headers.len(), 6);
        assert_eq!(delivery.message.headers["session"], session.session);
        assert_eq!(delivery.message.headers["seq"], seq.to_string());
        assert_eq!(delivery.message.headers["kind"], kind);
        assert_eq!(delivery.message.headers["run_id"], "bus-run");
        assert_eq!(
            delivery.message.headers["session_ref"],
            broca_core::wal_writer::agent_session_ref(&session)
        );
        let broca_wire::RunEventGetResponse::Found { event_key, jcs } = registry
            .run_event_get(&session, &delivery.message.id)
            .await
            .unwrap()
        else {
            panic!("published claim-check missing")
        };
        assert_eq!(event_key, delivery.message.id);
        let body = jcs.into_bytes();
        assert_eq!(delivery.message.digest, ContentDigest::of_bytes(&body));
        assert_eq!(body, event.body);
        let decoded: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(serde_jcs::to_vec(&decoded).unwrap(), body);
        assert_eq!(decoded["seq"], seq);
        cursor.ack().await.unwrap();
        // Replaying the identical claim-check is a JetStream duplicate, with the original sequence.
        let retry = bus.publish(&event).await.unwrap();
        assert!(retry.duplicate());
        assert_eq!(retry.stream_sequence(), delivery.stream_seq);
    }
    assert!(store.pending_bus_event().unwrap().is_none());
}

/// A send state notice reaches its sender's subscriber with the notice itself as the payload,
/// and its message id names the change, so publishing it again is a JetStream duplicate. An
/// empty payload carrying only a digest, as run events are sent, would put no notice in the
/// message, since a send state notice has no `run.event.get`. Read with a raw async-nats
/// consumer, not broca's `Stream` cursor (which decodes claim checks), so the check sees the
/// bytes on the wire.
#[tokio::test]
async fn real_bus_send_state_notice_is_its_own_payload() {
    use broca_protocol::send_states::SendStateNotice;
    use futures::StreamExt as _;
    let Some((_bus, conn, names, _)) = fixture().await else {
        return;
    };
    let bus = provision(&conn, &names).await;
    let temp = scratch();
    let store = Arc::new(SqliteStore::open(temp.path().join("run-index.db")).unwrap());
    store.enable_bus_events(names.account()).unwrap();
    let notice = SendStateNotice {
        v: 1,
        session_ref: "0123456789abcdef0123456789abcdef".into(),
        send_id: "aft:bash:bash-1".into(),
        seq: 7,
        state: "pending",
    };
    let body = notice.to_bytes().unwrap();
    assert!(store
        .record_send_states(&[("aft".into(), notice.clone())], now_ms())
        .unwrap());
    let js = async_nats::jetstream::new(conn.client());
    let consumer = js
        .get_stream(names.streams().event.clone())
        .await
        .unwrap()
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            durable_name: Some(AccountNames::module_consumer_name("aft").unwrap()),
            filter_subject: notice.subject(&names, "aft").unwrap(),
            ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
            ..Default::default()
        })
        .await
        .unwrap();
    let event = store.pending_bus_event().unwrap().unwrap();
    assert_eq!(
        store.drain_bus_event(&bus, now_ms()).await.unwrap(),
        DrainResult::Published
    );
    let mut messages = consumer.messages().await.unwrap();
    let message = tokio::time::timeout(Duration::from_secs(5), messages.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(message.payload.as_ref(), body.as_slice());
    let decoded: serde_json::Value = serde_json::from_slice(&message.payload).unwrap();
    assert_eq!(decoded["state"], "pending");
    let headers = message.headers.as_ref().unwrap();
    assert_eq!(
        headers
            .get(async_nats::header::NATS_MESSAGE_ID)
            .unwrap()
            .as_str(),
        notice.message_id()
    );
    message.ack().await.unwrap();
    let retry = bus.publish(&event).await.unwrap();
    assert!(
        retry.duplicate(),
        "the same notice published again is a duplicate"
    );
    assert!(store.pending_bus_event().unwrap().is_none());
}

#[tokio::test]
async fn real_bus_outage_queues_then_reconnect_resumes_identical_event() {
    let Some((broker, conn, names, account_public)) = fixture().await else {
        return;
    };
    let publisher = provision(&conn, &names).await;
    let session = BindIdentity::new("/test", "test", "outage");
    let store = SqliteStore::open_in_memory().unwrap();
    store.enable_bus_events(names.account()).unwrap();
    let directory = broker.dir.clone();
    let trust = broker.trust.clone();
    broker.stop().await;
    record(
        &store,
        &session,
        run_events(&session, "bus-run")[..1].to_vec(),
    );
    let event = store.pending_bus_event().unwrap().unwrap();
    assert_eq!(
        tokio::time::timeout(
            Duration::from_secs(10),
            store.drain_bus_event(&publisher, now_ms())
        )
        .await
        .unwrap()
        .unwrap(),
        DrainResult::Retry
    );
    assert_eq!(
        store.pending_bus_event().unwrap().unwrap().event_key,
        event.event_key
    );
    drop(publisher);
    drop(conn);
    let broker = BusServer::start(BusOptions {
        directory: Some(directory),
        trust: Some(trust),
        ..Default::default()
    })
    .await
    .unwrap();
    let conn = fixture_connection(&broker, &account_public).await;
    let publisher = NatsPublisher {
        stream: conn.stream(names.streams().event.clone()),
        account: names,
        connection: conn.clone(),
    };
    assert_eq!(
        store.drain_bus_event(&publisher, now_ms()).await.unwrap(),
        DrainResult::Published
    );
    let mut cursor = publisher
        .stream
        .consumer(DurableOwner::Module("prefrontal-core"))
        .await
        .unwrap();
    let delivery = tokio::time::timeout(Duration::from_secs(5), cursor.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(delivery.message.id, event.event_key);
    assert_eq!(delivery.message.digest, event.digest());
    cursor.ack().await.unwrap();
}

// ---- one turn through the real daemon -----------------------------------------------------------

/// The ck-bus credential service as broca reaches it: a management surface named `ckbus` that
/// answers `ckbus.credential`, `ckbus.credential_renew` and `ckbus.nonce_sign` with a
/// `{"result": ...}` body. It holds one user key, whose JWT the private broker's trust chain signs
/// for the provisioned account, and signs the broker's connect nonce with it.
#[cfg(unix)]
struct FakeCkBus {
    user: Arc<KeyPair>,
    jwt: String,
    acct: String,
    server_url: String,
    /// Every method broca asked for, in order.
    methods: Arc<std::sync::Mutex<Vec<String>>>,
}

#[cfg(unix)]
#[async_trait::async_trait]
impl subc_client_rs::ModuleHandler for FakeCkBus {
    async fn handle(
        &self,
        _ctx: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        use base64::engine::general_purpose::STANDARD;
        let request: serde_json::Value = serde_json::from_slice(&body).unwrap_or_default();
        let method = request["method"].as_str().unwrap_or_default().to_string();
        self.methods.lock().unwrap().push(method.clone());
        let public = self.user.public_key();
        let exp = unix_now() + 3600;
        let result = match method.as_str() {
            "ckbus.credential" => json!({
                "jwt": self.jwt,
                "exp": exp,
                "acct": self.acct,
                "inbox_prefix": ConnectConfig::new(public.clone()).unwrap().inbox_prefix(),
                "server_url": self.server_url,
                "credential_public": public,
            }),
            "ckbus.credential_renew" => {
                json!({"jwt": self.jwt, "exp": exp, "credential_public": public})
            }
            "ckbus.nonce_sign" => {
                let nonce = STANDARD
                    .decode(request["params"]["nonce_b64"].as_str().unwrap_or_default())
                    .unwrap_or_default();
                json!({"signature_b64": STANDARD.encode(self.user.sign(&nonce).unwrap())})
            }
            other => {
                return subc_client_rs::HandlerOutcome::Error {
                    code: "unknown_method".into(),
                    message: format!("the ckbus stand-in does not serve {other}"),
                }
            }
        };
        subc_client_rs::HandlerOutcome::Response(
            serde_json::to_vec(&json!({ "result": result })).unwrap(),
        )
    }

    async fn on_bind(
        &self,
        _req: &subc_client_rs::RouteBindRequest,
    ) -> subc_client_rs::BindDecision {
        subc_client_rs::BindDecision::accept()
    }
}

#[cfg(unix)]
const GEMINI_MODEL: &str = "gemini-2.5-flash";

/// One Gemini SSE chunk carrying a short answer and its usage, for every model request.
#[cfg(unix)]
const GEMINI_TEXT_SSE: &str = concat!(
    "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"done\"}],\"role\":\"model\"},",
    "\"finishReason\":\"STOP\",\"index\":0}],\"usageMetadata\":{\"promptTokenCount\":5,",
    "\"candidatesTokenCount\":1}}\n\n",
);

#[cfg(unix)]
async fn serve_gemini() -> String {
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
        .await
        .unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut reader = tokio::io::BufReader::new(stream);
                let mut length = 0usize;
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    if line == "\r\n" || line == "\n" {
                        break;
                    }
                    if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                        length = value.trim().parse().unwrap_or(0);
                    }
                }
                let mut body = vec![0u8; length];
                if reader.read_exact(&mut body).await.is_err() {
                    return;
                }
                let mut stream = reader.into_inner();
                let head = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
                    GEMINI_TEXT_SSE.len()
                );
                let _ = stream.write_all(head.as_bytes()).await;
                let _ = stream.write_all(GEMINI_TEXT_SSE.as_bytes()).await;
                let _ = stream.shutdown().await;
            });
        }
    });
    url
}

#[cfg(unix)]
struct DaemonRig {
    daemon: tokio::process::Child,
    dir: std::path::PathBuf,
    ckbus: Option<tokio::task::JoinHandle<()>>,
}

#[cfg(unix)]
impl Drop for DaemonRig {
    fn drop(&mut self) {
        if let Some(task) = self.ckbus.take() {
            task.abort();
        }
        broca_test_support::kill_and_reap_tree(&mut self.daemon);
        broca_test_support::dump_rig_process_logs_on_failure(&self.dir, "daemon");
        broca_test_support::remove_rig_dir(&self.dir);
    }
}

#[cfg(unix)]
fn outbox_rows(db: &std::path::Path, run_id: &str) -> Result<Vec<(String, Option<u64>)>, String> {
    let conn =
        rusqlite::Connection::open_with_flags(db, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .map_err(|e| e.to_string())?;
    let mut statement = conn
        .prepare("SELECT subject, published_ms FROM bus_event_outbox WHERE run_id=?1 ORDER BY seq")
        .map_err(|e| e.to_string())?;
    let rows = statement
        .query_map([run_id], |row| Ok((row.get(0)?, row.get(1)?)))
        .map_err(|e| e.to_string())?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string());
    rows
}

/// One ordinary turn on a module whose config switches run events on must put its `run.started`
/// and `run.ended` on the account's event stream, acknowledged, with no test code between the
/// session store and the broker. The publisher's health block must also count the event batches
/// the run recorded in the store's outbox, which is what tells an operator an armed publisher is
/// actually working.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_daemon_turn_publishes_run_started_and_run_ended() {
    use std::time::Instant;
    let subc_source = broca_test_support::subc_bin("ck-subc", "SUBC_CORE_BIN");
    if !subc_source.exists() {
        assert!(
            std::env::var_os("BROCA_REQUIRE_SUBC").is_none_or(|value| value.is_empty()),
            "run_events_bus: {subc_source:?} is missing, but BROCA_REQUIRE_SUBC requires it"
        );
        println!("SKIP real_daemon_turn_publishes_run_started_and_run_ended: {subc_source:?} is missing; set BROCA_REQUIRE_SUBC=1 to require it");
        return;
    }
    let Some((bus, conn, names, account_public)) = fixture().await else {
        return;
    };
    let publisher = provision(&conn, &names).await;
    let model_url = serve_gemini().await;
    let subc = broca_test_support::ckdev_binary(&subc_source, broca_test_support::scratch_root());
    std::env::remove_var("SUBC_MODULE_ID");
    std::env::remove_var("SUBC_LAUNCH_NONCE");

    let dir = scratch().path().join("rig");
    let config_home = dir.join("config");
    let runtime_dir = dir.join("runtime");
    let state_root = dir.join("broca-state");
    let project = dir.join("project");
    for path in [&runtime_dir, &project, &state_root.join("catalog")] {
        std::fs::create_dir_all(path).unwrap();
    }
    let project = project.canonicalize().unwrap();
    std::fs::write(
        state_root.join("catalog/fusiform-lkg.json"),
        serde_json::to_vec_pretty(&routing_fixture::with_served_routing(json!({
            "source": "run-events-bus",
            "resolved_at_ms": 1,
            "catalog_version": 1,
            "models": {
                format!("google/{GEMINI_MODEL}"): {
                    "existence": "present",
                    "capability.attachment": false,
                    "capability.reasoning": false,
                    "capability.tool_call": true,
                    "limit.context": 1000000,
                    "limit.output": 8192
                }
            }
        })))
        .unwrap(),
    )
    .unwrap();
    // The switch is read from the module's own config file, exactly as in production.
    let broca_config = dir.join("broca.jsonc");
    std::fs::write(
        &broca_config,
        "{\n  // run events on, as in production\n  \"bus_run_events_enabled\": true,\n  // no deployment-key vault here: sessions are stored plain\n  \"session_storage\": {\"encryption\": \"off\"},\n}\n",
    )
    .unwrap();
    let subc_config = config_home.join("cortexkit/subc.jsonc");
    std::fs::create_dir_all(subc_config.parent().unwrap()).unwrap();
    std::fs::write(
        &subc_config,
        serde_json::to_vec_pretty(&json!({
            "version": 1,
            "modules": {
                "broca": {
                    "program": broca_test_support::ckdev_binary(
                        env!("CARGO_BIN_EXE_ck-broca"),
                        broca_test_support::scratch_root(),
                    ),
                    "args": [],
                    "env": {
                        "BROCA_STATE_ROOT": state_root,
                        "BROCA_CONFIG_PATH": broca_config,
                        "BROCA_PROVIDER_BASE_URL": model_url,
                        "BROCA_TOOL_MODULE": "unused-tools",
                        "XDG_DATA_HOME": dir.join("data-home"),
                    },
                    "enabled": true
                }
            }
        }))
        .unwrap(),
    )
    .unwrap();
    let daemon = broca_test_support::retry_executable_file_busy(|| {
        broca_test_support::tokio_rig_command(&subc, &dir, "daemon")?
            .env("XDG_CONFIG_HOME", &config_home)
            .env("XDG_RUNTIME_DIR", &runtime_dir)
            .env("XDG_DATA_HOME", dir.join("data-home"))
            .env_remove("BROCA_STATE_ROOT")
            .env("SUBC_PORT", "0")
            .env_remove("SUBC_LAUNCH_NONCE")
            .env_remove("SUBC_LAUNCH_NONCE_FD")
            .env_remove("SUBC_MODULE_ID")
            .kill_on_drop(true)
            .spawn()
    })
    .expect("spawn ck-subc");
    let mut rig = DaemonRig {
        daemon,
        dir: dir.clone(),
        ckbus: None,
    };
    let connection_file = runtime_dir.join("subc-connection.json");
    broca_test_support::wait_until("the daemon's connection file", || async {
        if connection_file.exists() {
            Ok(())
        } else {
            Err("not published yet".to_string())
        }
    })
    .await;
    let user = Arc::new(KeyPair::new_user());
    let methods = Arc::new(std::sync::Mutex::new(Vec::new()));
    let ckbus = FakeCkBus {
        jwt: bus
            .trust
            .user_jwt(&box_root_id(), &account_public, &user, unix_now() - 60),
        user,
        acct: names.account().to_string(),
        server_url: bus.url.clone(),
        methods: methods.clone(),
    };
    let manifest = subc_protocol::manifest::ModuleManifest::builder("ckbus", "0.0.0")
        .provides(vec![
            subc_protocol::manifest::ProviderRole::ManagementSurface {
                operations: [
                    "ckbus.credential",
                    "ckbus.credential_renew",
                    "ckbus.nonce_sign",
                ]
                .iter()
                .map(|name| subc_protocol::manifest::ManagementOperation {
                    name: name.to_string(),
                    kind: subc_protocol::manifest::ManagementOperationKind::Mutate,
                    description: None,
                })
                .collect(),
                config_schema: json!({}),
                observability: Vec::new(),
                identity_scope: Vec::new(),
                concurrency: subc_protocol::manifest::Concurrency::ModuleManaged,
            },
        ])
        .build();
    let (_, serving) = subc_client_rs::serve_with_handle(&connection_file, manifest, ckbus)
        .await
        .expect("register the ckbus stand-in");
    rig.ckbus = Some(tokio::spawn(async move {
        let _ = serving.await;
    }));

    // Events a run records before the publisher learns its account wait in memory until it does.
    // Send the turn once ck-bus has issued the credential, so this test watches events published
    // as the run records them.
    broca_test_support::wait_until("broca's ckbus.credential request", || async {
        let seen = methods.lock().unwrap().clone();
        if seen.iter().any(|method| method == "ckbus.credential") {
            Ok(())
        } else {
            Err(format!("{seen:?}"))
        }
    })
    .await;

    // One ordinary turn.
    let caller = broca_subc::SubcConnection::connect(&connection_file)
        .await
        .unwrap();
    let session = BindIdentity::new(&project, "runner", "bus-turn");
    let route = caller
        .route_open_management_within(
            "broca",
            &session,
            broca_subc::RouteOpenRetry::within(Duration::from_secs(30), Duration::from_millis(100)),
        )
        .await
        .expect("open broca");
    caller
        .route_request(
            &route,
            serde_json::to_vec(&json!({"method": "session.send", "params": {
                "prompt": "hello",
                "send_id": "bus-turn-send",
                "tools": [],
                "model": {"provider": "google", "model": GEMINI_MODEL},
            }}))
            .unwrap(),
        )
        .await
        .expect("session.send");
    let sent = Instant::now();
    // The route carries no scope, so the daemon keys the session under the unscoped root,
    // whatever folder the bind named.
    let stored_as = BindIdentity::new(
        broca_protocol::session::UNSCOPED_SESSION_ROOT,
        "runner",
        "bus-turn",
    );
    let run_id = broca_test_support::wait_until("the turn's run end", || async {
        use broca_session_store::record::Record;
        let records = stored_log::stored_records(&state_root, &stored_as).await;
        let run_id = records.iter().find_map(|record| match record {
            Record::RunStart(start) => Some(start.run_id.clone()),
            _ => None,
        });
        match run_id {
            Some(run_id)
                if records
                    .iter()
                    .any(|record| matches!(record, Record::RunEnd(_))) =>
            {
                Ok(run_id)
            }
            _ => Err(format!("{records:?}")),
        }
    })
    .await;

    println!("turn finished {:?} after the send", sent.elapsed());
    // Both lifecycle events reach the broker, read back from the stream itself.
    let mut cursor = publisher
        .stream
        .consumer(DurableOwner::Module("prefrontal-core"))
        .await
        .unwrap();
    let mut kinds = Vec::new();
    // A live run's events are due within seconds of its terminal. A publisher that walked every
    // historical session at startup took about 18 s here on an Apple laptop before the turn's
    // events went out, so the deadline also holds startup work to what this period needs.
    let stream_deadline = Instant::now() + Duration::from_secs(STREAM_DEADLINE_S);
    while !(kinds.contains(&"run.started".to_string()) && kinds.contains(&"run.ended".to_string()))
    {
        let remaining = stream_deadline.saturating_duration_since(Instant::now());
        let delivery = match tokio::time::timeout(remaining, cursor.next()).await {
            Ok(Ok(Some(delivery))) => delivery,
            // A pull that expires with nothing to deliver; ask again until the deadline.
            Ok(Ok(None)) if !remaining.is_zero() => continue,
            other => {
                let last = match other {
                    Err(_) | Ok(Ok(_)) => "timed out",
                    Ok(Err(_)) => "consumer error",
                };
                let db = state_root.join("run-index.db");
                panic!(
                    "the turn's run events never reached the stream (got {kinds:?}, then {last}); \
                     outbox rows: {:?}; broca health: {}\n{}",
                    outbox_rows(&db, &run_id),
                    broca_metrics(&connection_file).await,
                    bus_log_lines(&dir.join("data-home/cortexkit/broca/logs")),
                );
            }
        };
        assert_eq!(delivery.message.headers["run_id"], run_id);
        kinds.push(delivery.message.headers["kind"].clone());
        cursor.ack().await.unwrap();
    }
    assert_eq!(kinds.first().map(String::as_str), Some("run.started"));
    println!(
        "both events on the stream {:?} after the send",
        sent.elapsed()
    );

    // The outbox records each lifecycle event as acknowledged by the broker.
    let db = state_root.join("run-index.db");
    let rows = broca_test_support::wait_until("both outbox rows acknowledged", || async {
        match outbox_rows(&db, &run_id) {
            Ok(rows)
                if rows
                    .iter()
                    .any(|(s, p)| s.ends_with(".run.started.v1") && p.is_some())
                    && rows
                        .iter()
                        .any(|(s, p)| s.ends_with(".run.ended.v1") && p.is_some()) =>
            {
                Ok(rows)
            }
            other => Err(format!("{other:?}")),
        }
    })
    .await;
    assert!(
        rows.iter().all(|(_, published)| published.is_some()),
        "{rows:?}"
    );

    // Health shows the pipeline armed and moving, not just enabled.
    let metrics = broca_metrics(&connection_file).await;
    let events = &metrics["bus_events"];
    assert_eq!(events["enabled"], true, "{metrics}");
    assert_eq!(
        events["bus_account_known"], true,
        "health must say the publisher knows its account: {events}"
    );
    assert!(
        events["bus_events_published"].as_u64() >= Some(2),
        "{events}"
    );
    assert_eq!(events["bus_outbox_depth"], 0, "{events}");
    assert!(
        events["bus_store_batches_recorded"].as_u64() >= Some(1),
        "health must count the turn's recorded event batches: {events}"
    );
    assert_eq!(events["bus_store_batches_waiting"], 0, "{events}");
    assert!(
        events["bus_loop_pass_age_ms"]
            .as_u64()
            .is_some_and(|age| age < 5_000),
        "the publisher loop must still be making passes: {events}"
    );

    assert_eq!(events["bus_outbox_dropped"], 0, "{events}");
    assert_eq!(events["bus_store_batches_dropped"], 0, "{events}");
    drop(rig);
}

/// Seconds from a turn's terminal to both of its lifecycle events on the stream.
#[cfg(unix)]
const STREAM_DEADLINE_S: u64 = 5;

/// The module's own run event log lines, without the per-session notice that recovered history
/// was too old to publish, which the seeded history repeats thousands of times.
#[cfg(unix)]
fn bus_log_lines(logs: &std::path::Path) -> String {
    let mut lines = Vec::new();
    for entry in std::fs::read_dir(logs).into_iter().flatten().flatten() {
        let text = std::fs::read_to_string(entry.path()).unwrap_or_default();
        lines.extend(
            text.lines()
                .filter(|line| line.contains("bus") && !line.contains("older than 24 hours"))
                .map(str::to_string),
        );
    }
    lines.join("\n")
}

#[cfg(unix)]
async fn broca_metrics(connection_file: &std::path::Path) -> serde_json::Value {
    let conn = broca_subc::SubcConnection::connect(connection_file)
        .await
        .unwrap();
    conn.supervisor_health_probe("broca")
        .await
        .expect("broca health")
        .metrics
        .unwrap_or_default()
}
