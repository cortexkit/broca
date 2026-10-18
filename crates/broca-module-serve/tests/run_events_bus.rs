//! Private JetStream tests. No production daemon, credentials, or installed ck-* binaries.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use broca_core::{FileWalStore, WalStore};
use broca_module_serve::{bus_events::now_ms, bus_events_nats::NatsPublisher};
use broca_protocol::{request::ModelHandle, BindIdentity, RunFinishReason, RunId, Usage};
use broca_store::{
    bus_events::{BusPublisher, DrainResult},
    SqliteStore,
};
use broca_wal::{RunConfig, WalRecord};
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

fn started(session: &BindIdentity) -> WalRecord {
    WalRecord::RunStarted {
        run_id: RunId("bus-run".into()),
        session: session.clone(),
        ts_ms: Some(now_ms()),
        config: Box::new(RunConfig {
            model: ModelHandle::new("fixture", "model"),
            tools: vec![].into(),
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
            stop_when: vec![],
            context_management: None,
        }),
        input: Arc::new(vec![]),
        origin: Default::default(),
        on_restart: None,
        keep_warm: None,
        project_id: None,
        principal: None,
        retention: None,
        session_class: None,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        title: None,
        scope: None,
        marker_order_version: None,
        manifest: None,
        hooks: None,
    }
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

/// One private broker at a time. `cortexkit-bus-testkit` 0.1.1 picks a free port, releases it and
/// hands the number to `nats-server`, so two brokers starting together can pick the same port: on
/// 2026-10-18 two servers in this binary both tried monitor port 58739 inside the full suite, and the
/// loser exited with `bind: address already in use`. Each test holds this guard for its whole run,
/// including the outage test's broker restart. Drop it once the testkit binds its ports itself.
static BROKER: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
    }
}

fn project(store: &SqliteStore, wal: &FileWalStore, session: &BindIdentity) {
    // `flush_for_read` returns the WAL file's identity (only Unix has one), not a sequence. Take
    // the head from the records decoded before the flush, which that flush then proves durable.
    let candidate = wal.read_records_range(session, 0, None, None).unwrap();
    let broca_wal::WalRangeRecords::Complete(candidate) = candidate.records() else {
        panic!("WAL missing");
    };
    let head = candidate.last().map_or(0, |record| record.seq);
    wal.flush_for_read(session).unwrap();
    let range = wal
        .read_records_range(session, 0, Some(head), None)
        .unwrap();
    let epoch = range.lineage_id.unwrap().to_string();
    let broca_wal::WalRangeRecords::Complete(records) = range.into_records() else {
        panic!("WAL missing");
    };
    store
        .project_bus_events(session, &epoch, &records, now_ms())
        .unwrap();
}

#[tokio::test]
async fn real_bus_headers_digest_sequence_and_ack_retention() {
    let _broker = BROKER.lock().await;
    use broca_module::serve::ServeHandler;
    let Some((_bus, conn, names, _)) = fixture().await else {
        return;
    };
    let bus = provision(&conn, &names).await;
    let temp = scratch();
    let wal = Arc::new(FileWalStore::new(temp.path().join("wal")));
    let session = BindIdentity::new("/test", "test", "events");
    let store = Arc::new(SqliteStore::open(temp.path().join("run-index.db")).unwrap());
    store.enable_bus_events(names.account()).unwrap();
    let mut writer = wal.open(&session).unwrap();
    writer.append(1, &started(&session), true).unwrap();
    writer
        .append(
            1,
            &WalRecord::Steered {
                retention: None,
                ts_ms: Some(now_ms()),
                send_id: Some("steer-a".into()),
                request_identity_digest: None,
                delivery: Some(broca_protocol::request::Delivery::Steer),
                title: None,
                input: Arc::new(vec![]),
                hooks: None,
            },
            true,
        )
        .unwrap();
    writer
        .append(
            1,
            &WalRecord::StepStarted {
                step_id: broca_protocol::StepId(1),
            },
            true,
        )
        .unwrap();
    writer
        .append(
            1,
            &WalRecord::RunPaused {
                run_id: RunId("bus-run".into()),
                ts_ms: Some(now_ms()),
                reason: broca_protocol::PauseReason::Restart,
                error: None,
            },
            true,
        )
        .unwrap();
    writer
        .append(
            1,
            &WalRecord::RunFinished {
                reason: RunFinishReason::Completed,
                ts_ms: Some(now_ms()),
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
            true,
        )
        .unwrap();
    project(&store, &wal, &session);
    let registry = broca_module::LineageRegistry::new_with_reader(
        Arc::new(NoActor),
        wal.clone(),
        Some(store.clone()),
    );
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
        assert_eq!(delivery.message.headers.len(), 5);
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

#[tokio::test]
async fn real_bus_outage_queues_then_reconnect_resumes_identical_event() {
    let _broker = BROKER.lock().await;
    let Some((broker, conn, names, account_public)) = fixture().await else {
        return;
    };
    let publisher = provision(&conn, &names).await;
    let temp = scratch();
    let wal = FileWalStore::new(temp.path().join("wal"));
    let session = BindIdentity::new("/test", "test", "outage");
    let store = SqliteStore::open_in_memory().unwrap();
    store.enable_bus_events(names.account()).unwrap();
    let directory = broker.dir.clone();
    let trust = broker.trust.clone();
    broker.stop().await;
    wal.open(&session)
        .unwrap()
        .append(1, &started(&session), true)
        .unwrap();
    project(&store, &wal, &session);
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

#[test]
fn bus_crash_child() {
    let Some(root) = std::env::var_os("BROCA_BUS_CRASH_FIXTURE") else {
        return;
    };
    let root = std::path::PathBuf::from(root);
    let session = BindIdentity::new("/test", "test", "crash");
    let wal = FileWalStore::new(root.join("wal"));
    let index = Arc::new(SqliteStore::open(root.join("run-index.db")).unwrap());
    wal.set_session_ref_index(index);
    wal.open(&session)
        .unwrap()
        .append(1, &started(&session), true)
        .unwrap();
    // Exit without destructors after the WAL barrier but before any outbox projection or publish.
    std::process::exit(86);
}

#[tokio::test]
async fn real_bus_crash_after_wal_commit_replays_after_restart() {
    let _broker = BROKER.lock().await;
    let Some((_bus, conn, names, _)) = fixture().await else {
        return;
    };
    let publisher = provision(&conn, &names).await;
    let temp = scratch();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "bus_crash_child"])
        .env("BROCA_BUS_CRASH_FIXTURE", temp.path())
        .status()
        .unwrap();
    assert_eq!(child.code(), Some(86));
    let store = SqliteStore::open(temp.path().join("run-index.db")).unwrap();
    store.enable_bus_events(names.account()).unwrap();
    let wal = FileWalStore::new(temp.path().join("wal"));
    let session = BindIdentity::new("/test", "test", "crash");
    assert!(
        store.bus_event_sessions().unwrap().contains(&session),
        "startup must discover the crash-committed WAL without a run-index admission row"
    );
    project(&store, &wal, &session);
    let pending = store.pending_bus_event().unwrap().unwrap();
    drop(store);
    // A second crash, this time after the outbox transaction, preserves the exact body and id.
    let store = SqliteStore::open(temp.path().join("run-index.db")).unwrap();
    store.enable_bus_events(names.account()).unwrap();
    assert_eq!(
        store.pending_bus_event().unwrap().unwrap().body,
        pending.body
    );
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
    assert_eq!(delivery.message.id, pending.event_key);
    assert_eq!(delivery.message.digest, pending.digest());
    cursor.ack().await.unwrap();
}
