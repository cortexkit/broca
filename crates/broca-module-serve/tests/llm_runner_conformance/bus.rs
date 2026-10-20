//! A private broker and credential service that outlive a killed runner.
use super::*;
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_nats::{ConnectConfig, NatsConnection};
use cortexkit_bus_testkit::{
    trust::{box_root_id, signer_root_id, unix_now},
    BusOptions, BusServer,
};
use cortexkit_bus_trait::Stream;
use nkeys::KeyPair;
use sha2::{Digest, Sha256};

pub(super) struct BusRig {
    server: BusServer,
    pub connection: NatsConnection,
    pub names: AccountNames,
    user: Arc<KeyPair>,
    jwt: String,
}
fn jwt(signer: &KeyPair, subject: &str, nats: Value) -> String {
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
impl BusRig {
    pub async fn new(root: &Path) -> Self {
        let server = BusServer::start(BusOptions {
            directory: Some(root.join("broker")),
            ..Default::default()
        })
        .await
        .expect("private NATS prerequisite");
        println!("private {}", server.version);
        let account = KeyPair::new_account();
        std::fs::write(server.dir.join("jwt").join(format!("{}.jwt", account.public_key())), jwt(
            &server.trust.signer.root(&signer_root_id()).pair, &account.public_key(),
            json!({"type":"account","version":2,"signing_keys":[server.trust.box_root_public()],"limits":{"subs":-1,"data":-1,"payload":-1,"imports":-1,"exports":-1,"wildcards":true,"conn":-1,"leaf":-1,"mem_storage":-1,"disk_storage":-1,"streams":-1,"consumer":-1},"default_permissions":{"pub":{},"sub":{}}}),
        )).unwrap();
        let user = Arc::new(KeyPair::new_user());
        let jwt = server.trust.user_jwt(
            &box_root_id(),
            &account.public_key(),
            &user,
            unix_now() - 60,
        );
        let key = user.clone();
        let options = async_nats::ConnectOptions::with_jwt(jwt.clone(), move |nonce| {
            let key = key.clone();
            async move { key.sign(&nonce).map_err(async_nats::AuthError::new) }
        });
        let connection = NatsConnection::connect(
            &server.url,
            options,
            ConnectConfig::new(user.public_key()).unwrap(),
        )
        .await
        .unwrap();
        let names = AccountNames::derive("box_compaction_test").unwrap();
        let stream = async_nats::jetstream::new(connection.client())
            .create_stream(async_nats::jetstream::stream::Config {
                name: names.streams().event.clone(),
                subjects: vec![names.event_binding()],
                ..Default::default()
            })
            .await
            .unwrap();
        let spec = cortexkit_bus_naming::event_consumer_specs(&names)
            .unwrap()
            .into_iter()
            .find(|s| s.consumer.durable == "m_broca")
            .unwrap();
        stream
            .create_consumer(async_nats::jetstream::consumer::pull::Config {
                durable_name: Some(spec.consumer.durable),
                filter_subjects: spec.consumer.filter_subjects,
                ack_policy: async_nats::jetstream::consumer::AckPolicy::Explicit,
                ack_wait: spec.ack_wait,
                max_ack_pending: spec.max_ack_pending,
                ..Default::default()
            })
            .await
            .unwrap();
        Self {
            server,
            connection,
            names,
            user,
            jwt,
        }
    }
    pub async fn register(self: &Arc<Self>, connection_file: &Path) -> tokio::task::JoinHandle<()> {
        let manifest = subc_protocol::manifest::ModuleManifest::builder("ckbus", "test")
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
                    concurrency: subc_protocol::manifest::Concurrency::ModuleManaged,
                    observability: vec![],
                    identity_scope: vec![],
                },
            ])
            .build();
        let (_, serve) =
            subc_client_rs::serve_with_handle(connection_file, manifest, Credentials(self.clone()))
                .await
                .unwrap();
        tokio::spawn(async move {
            let _ = serve.await;
        })
    }
    pub async fn publish(&self, view: &broca_core::compaction_provider::view::CompactionView) {
        use broca_core::compaction_provider::notice::{event_subject, ViewReady, VIEW_READY_EVENT};
        let ready = ViewReady::for_view(view);
        let publication = self
            .connection
            .stream(self.names.streams().event.clone())
            .publish(
                &event_subject(self.names.account(), "magic-context", VIEW_READY_EVENT),
                &ready.message_id(),
                ready.digest.parse().unwrap(),
                ready.to_headers(),
            )
            .await
            .unwrap();
        assert!(!publication.duplicate());
    }
}
struct Credentials(Arc<BusRig>);
#[async_trait]
impl subc_client_rs::ModuleHandler for Credentials {
    async fn handle(
        &self,
        _: subc_client_rs::RequestCtx,
        body: Vec<u8>,
    ) -> subc_client_rs::HandlerOutcome {
        let request: Value = serde_json::from_slice(&body).unwrap();
        let public = self.0.user.public_key();
        let result = match request["method"].as_str().unwrap_or_default() {
            "ckbus.credential" => {
                json!({"jwt":self.0.jwt,"exp":unix_now()+3600,"acct":self.0.names.account(),"inbox_prefix":ConnectConfig::new(&public).unwrap().inbox_prefix(),"server_url":self.0.server.url,"credential_public":public})
            }
            "ckbus.credential_renew" => {
                json!({"jwt":self.0.jwt,"exp":unix_now()+3600,"credential_public":public})
            }
            "ckbus.nonce_sign" => {
                json!({"signature_b64":STANDARD.encode(self.0.user.sign(&STANDARD.decode(request["params"]["nonce_b64"].as_str().unwrap()).unwrap()).unwrap())})
            }
            method => {
                return subc_client_rs::HandlerOutcome::Error {
                    code: "unknown_method".into(),
                    message: method.into(),
                }
            }
        };
        subc_client_rs::HandlerOutcome::Response(
            serde_json::to_vec(&json!({"result":result})).unwrap(),
        )
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_bus_mid_run_receipt_crash_cuts_resume_identical_request_bytes() {
    use broca_core::{
        compaction_provider::{
            lineage::MessageRef,
            view::{CompactionView, ViewRange},
        },
        WalStore,
    };
    assert!(
        sibling_bin("ck-subc", "SUBC_CORE_BIN").exists(),
        "native real-kill needs ck-subc"
    );
    assert!(
        sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN").exists(),
        "native real-kill needs fake-aft-stub"
    );
    let root = broca_test_support::scratch_root().join("bus-mid-run-real-kill");
    std::fs::create_dir_all(&root).unwrap();
    let project = root.join("project");
    std::fs::create_dir_all(&project).unwrap();
    let mut captured = Vec::new();
    for cut in ["ViewReceiptRecorded", "ViewReadyAcked"] {
        let runtime = root.join(cut);
        std::fs::create_dir_all(&runtime).unwrap();
        let mut subject = BrocaSubject::new(&runtime).await;
        subject.project_root = project.canonicalize().unwrap();
        subject.bus = Some(Arc::new(BusRig::new(&runtime).await));
        let session = "bus-crash";
        subject
            .install_script(
                session,
                Script {
                    turns: vec![cortexkit_role_llm_runner_conformance::ScriptedTurn::new(
                        vec![ScriptedPart::Text("answer".into())],
                    )],
                },
            )
            .await
            .unwrap();
        subject.install_setup(
            session,
            cortexkit_role_llm_runner_conformance::CompactionSetup::Ready(
                cortexkit_role_llm_runner_conformance::CompactionMessage {
                    compaction_id: "head".into(),
                    version: 1,
                    from: 0,
                    to: 0,
                    replacement: vec![],
                },
            ),
        );
        let mut live = subject.start(&runtime).await.unwrap();
        let route = subject
            .session_route(&live, session, &subject.owner_stamp())
            .await
            .unwrap();
        let mut fields = subject.send_fields(session, true);
        fields.insert("send_id".into(), json!("first"));
        fields.insert(
            "input".into(),
            json!([{"role":"user","content":[{"type":"text","text":"original"}]}]),
        );
        let reply = route
            .request("session.send", Value::Object(fields))
            .await
            .unwrap();
        assert!(matches!(reply, Reply::Response(_)), "{reply:?}");
        let wal = broca_core::FileWalStore::new(live.state_root.join("wal"));
        let lineage = wal
            .read_records_range(&subject.bind_identity(session), 0, None, None)
            .unwrap()
            .lineage_id
            .unwrap()
            .to_string();
        let replacement = json!({"role":"user","content":[{"type":"text","text":"bus summary"}]});
        let view = CompactionView::new(
            broca_core::agent_session_ref(&subject.bind_identity(session)),
            2,
            lineage,
            MessageRef {
                ordinal: 0,
                mid: "m0".into(),
            },
            vec![ViewRange::replace(0, 1, json!([replacement]).to_string())],
        );
        subject.offer_bus_view(session, view.clone(), replacement);
        let gate = live.state_root.join("test-compaction-bus-gate");
        std::fs::write(&gate, cut).unwrap();
        subject.bus.as_ref().unwrap().publish(&view).await;
        broca_test_support::wait_until("bus receipt crash checkpoint", || async {
            std::fs::read_to_string(live.state_root.join("test-compaction-bus-reached"))
                .map_err(|e| e.to_string())
                .and_then(|value| if value == cut { Ok(()) } else { Err(value) })
        })
        .await;
        assert!(
            !subject
                .records(&live, session)
                .iter()
                .any(|r| matches!(r, broca_wal::WalRecord::CompactionViewApplied { .. })),
            "view applied mid-step"
        );
        let receipts = broca_store::SqliteStore::open(live.state_root.join("run-index.db"))
            .unwrap()
            .compaction_receipts("", 256)
            .unwrap();
        assert_eq!(receipts.len(), 1, "receipt was not durable before {cut}");
        assert!(live.kill_all(), "no runner process was SIGKILLed at {cut}");
        drop(route);
        subject
            .shared
            .compaction
            .lock()
            .unwrap()
            .get_mut(session)
            .unwrap()
            .gates
            .clear();
        std::fs::remove_file(gate).unwrap();
        drop(live);
        let live = subject.start(&runtime).await.unwrap();
        let route = subject
            .session_route(&live, session, &subject.owner_stamp())
            .await
            .unwrap();
        let mut fields = subject.send_fields(session, false);
        fields.insert("send_id".into(), json!("resume"));
        fields.insert(
            "input".into(),
            json!([{"role":"user","content":[{"type":"text","text":"next"}]}]),
        );
        let reply = route
            .request("session.send", Value::Object(fields))
            .await
            .unwrap();
        assert!(matches!(reply, Reply::Response(_)), "{reply:?}");
        let records = subject.records(&live, session);
        assert!(
            records
                .iter()
                .any(|r| matches!(r, broca_wal::WalRecord::CompactionViewApplied { .. })),
            "startup inbox did not apply bus view after {cut}"
        );
        let bytes = subject.shared.raw_bodies.lock().unwrap()[session]
            .last()
            .unwrap()
            .clone();
        assert!(String::from_utf8_lossy(&bytes).contains("bus summary"));
        assert!(!String::from_utf8_lossy(&bytes).contains("original"));
        captured.push(bytes);
    }
    assert_eq!(
        captured[0], captured[1],
        "receipt-before-ACK and ACK-before-boundary cuts changed resumed request bytes"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_bus_fitting_idle_view_records_estimate_then_releases_held_queue_head() {
    use broca_core::{
        compaction_provider::{
            lineage::MessageRef,
            view::{CompactionView, ViewRange},
        },
        WalStore,
    };
    let root = broca_test_support::scratch_root().join("bus-idle-ceiling-head");
    std::fs::create_dir_all(&root).unwrap();
    let mut subject = BrocaSubject::new(&root).await;
    subject.bus = Some(Arc::new(BusRig::new(&root).await));
    let session = "bus-idle";
    subject
        .install_script(
            session,
            Script {
                turns: vec![
                    cortexkit_role_llm_runner_conformance::ScriptedTurn::new(vec![
                        ScriptedPart::Text("large answer ".repeat(4000)),
                    ]),
                    cortexkit_role_llm_runner_conformance::ScriptedTurn::new(vec![
                        ScriptedPart::Text("queued answer".into()),
                    ]),
                ],
            },
        )
        .await
        .unwrap();
    subject.install_setup(
        session,
        cortexkit_role_llm_runner_conformance::CompactionSetup::Ready(
            cortexkit_role_llm_runner_conformance::CompactionMessage {
                compaction_id: "head".into(),
                version: 1,
                from: 0,
                to: 0,
                replacement: vec![],
            },
        ),
    );
    let live = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let mut fields = subject.send_fields(session, true);
    fields.insert("send_id".into(), json!("first"));
    fields.insert("context_limit".into(), json!(10_000));
    fields.insert(
        "input".into(),
        json!([{"role":"user","content":[{"type":"text","text":"original"}]}]),
    );
    assert!(matches!(
        route
            .request("session.send", Value::Object(fields))
            .await
            .unwrap(),
        Reply::Response(_)
    ));
    let mut fields = subject.send_fields(session, false);
    fields.insert("send_id".into(), json!("queued"));
    fields.insert("delivery".into(), json!("queue"));
    fields.insert(
        "input".into(),
        json!([{"role":"user","content":[{"type":"text","text":"next"}]}]),
    );
    // A queued send does not reach a model yet, so use the raw route instead of the
    // compaction adapter's send helper, which waits for a model receipt.
    let reply = route
        .connection
        .route_request(
            &route.route,
            serde_json::to_vec(&json!({"method":"session.send","params":fields})).unwrap(),
        )
        .await
        .unwrap();
    assert!(reply.to_string().contains("pending"));
    subject.release_model_compaction(session).await.unwrap();
    broca_test_support::wait_until("finished run with held queue head", || async {
        let records = subject.records(&live, session);
        if records
            .iter()
            .any(|r| matches!(r, broca_wal::WalRecord::RunFinished { .. }))
            && subject.shared.raw_bodies.lock().unwrap()[session].len() == 1
        {
            Ok(())
        } else {
            Err("run not finished or head was sent".to_string())
        }
    })
    .await;
    let before = subject.records(&live, session);
    let wal = broca_core::FileWalStore::new(live.state_root.join("wal"));
    let lineage = wal
        .read_records_range(&subject.bind_identity(session), 0, None, None)
        .unwrap()
        .lineage_id
        .unwrap()
        .to_string();
    let replacement = json!({"role":"user","content":[{"type":"text","text":"idle summary"}]});
    let view = CompactionView::new(
        broca_core::agent_session_ref(&subject.bind_identity(session)),
        2,
        lineage,
        MessageRef {
            ordinal: 1,
            mid: "m1".into(),
        },
        vec![ViewRange::replace(0, 2, json!([replacement]).to_string())],
    );
    subject.offer_bus_view(session, view.clone(), replacement);
    subject.bus.as_ref().unwrap().publish(&view).await;
    broca_test_support::wait_until("compaction wakes the pending head", || async {
        if subject.shared.raw_bodies.lock().unwrap()[session].len() == 2 {
            Ok(())
        } else {
            Err("pending head remains held".to_string())
        }
    })
    .await;
    let records = subject.records(&live, session);
    let applied = records
        .iter()
        .position(|r| matches!(r, broca_wal::WalRecord::CompactionViewApplied { .. }))
        .unwrap();
    let notice = records.iter().position(|r| matches!(r, broca_wal::WalRecord::CompactionNotice { headers, .. } if headers.get("applied_view_version").map(String::as_str) == Some("2"))).unwrap();
    let queued = records
        .iter()
        .enumerate()
        .skip(before.len())
        .find_map(|(at, r)| matches!(r, broca_wal::WalRecord::RunStarted { .. }).then_some(at))
        .unwrap();
    assert!(
        applied < notice && notice < queued,
        "queue admitted before its durable fitting estimate"
    );
    let broca_wal::WalRecord::CompactionNotice { headers, .. } = &records[notice] else {
        unreachable!()
    };
    assert!(headers["est_tokens"].parse::<u64>().unwrap() < 1000);
    let bytes = subject.shared.raw_bodies.lock().unwrap()[session]
        .last()
        .unwrap()
        .clone();
    assert!(String::from_utf8_lossy(&bytes).contains("idle summary"));
    assert!(!String::from_utf8_lossy(&bytes).contains("large answer"));
}
