use broca_module::session_cmd::{LineageCmd, LineageStatus, SendKey};
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_loop::writer::Writer;
use broca_session_store::backend::MemoryBackend;
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use serde_json::{json, Value};
use std::collections::VecDeque;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const A: &str = "chatgpt-a";
const B: &str = "chatgpt-b";
const CRED_A: &str = "chatgpt:openai";
const CRED_B: &str = "chatgpt:openai:second";

struct SignedOutRouting {
    requests: std::sync::Mutex<Vec<(u64, Value)>>,
    request_bytes: std::sync::Mutex<Vec<Vec<u8>>>,
    replies: std::sync::Mutex<VecDeque<broca_module_serve::account_routing::RoutingReply>>,
    missing: std::sync::Mutex<Option<&'static str>>,
    pending: std::sync::atomic::AtomicBool,
    before_continue: std::sync::Mutex<Option<Box<dyn FnOnce() + Send>>>,
    replay_b: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl broca_module_serve::account_routing::RoutingTransport for SignedOutRouting {
    async fn capability(
        &self,
        name: &str,
    ) -> broca_module_serve::routing_capabilities::CapabilitySupport {
        use broca_module_serve::routing_capabilities::CapabilitySupport;
        let missing = *self.missing.lock().unwrap();
        if missing == Some("unknown") {
            CapabilitySupport::Unknown
        } else if missing == Some(name) {
            CapabilitySupport::NotDeclared
        } else {
            CapabilitySupport::Declared
        }
    }

    async fn request(&self, bytes: Vec<u8>) -> broca_module_serve::account_routing::RoutingReply {
        let request: Value = serde_json::from_slice(&bytes).unwrap();
        self.requests
            .lock()
            .unwrap()
            .push((now_ms() as u64, request.clone()));
        if request["method"] == "route.account_for" {
            if request["params"]["mode"] == "continue" {
                self.request_bytes.lock().unwrap().push(bytes);
                if let Some(hook) = self.before_continue.lock().unwrap().take() {
                    hook();
                }
                if self.pending.load(Ordering::SeqCst) {
                    std::future::pending::<()>().await;
                }
                return self
                    .replies
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("one scripted continuation per ask");
            }
            let (account, credential, decision) = if self.replay_b.load(Ordering::SeqCst) { (B, CRED_B, "decision-b") } else { (A, CRED_A, "decision-a") };
            return Ok(Ok(serde_json::to_vec(&json!({"result": {
                "account": {"provider":"openai","method":"chatgpt","account_id":account,"credential_id":credential},
                "reason": {"decision_id":decision}
            }})).unwrap()));
        }
        Ok(Ok(br#"{"result":{}}"#.to_vec()))
    }
}

impl SignedOutRouting {
    fn questions(&self) -> Vec<(u64, Value)> {
        self.requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, request)| request["params"]["mode"] == "continue")
            .cloned()
            .collect()
    }
}

fn answer(value: Value) -> broca_module_serve::account_routing::RoutingReply {
    Ok(Ok(serde_json::to_vec(&json!({"result": value})).unwrap()))
}

fn offer() -> broca_module_serve::account_routing::RoutingReply {
    answer(json!({"kind":"offer", "credential_id":CRED_B, "account":B,
        "decision_id":"decision-b", "valid_until_ms":now_ms() + 60_000}))
}

fn needs_reauth() -> Value {
    json!({"result":{"error":{"class":"permanent","code":"needs_reauth"}}})
}

fn token(account: &str, expiring: bool) -> Value {
    json!({"result":{"payload":stand_in_token(1).into_bytes(),"record_version":1,
        "account_id":account,"expires_at_ms":expiring.then(|| now_ms() + 120_000)}})
}

type StoreSession = Session<&'static MemoryBackend>;
type Commands = mpsc::UnboundedSender<LineageCmd>;

struct SignedOutRig {
    root: std::path::PathBuf,
    daemon: CapturingVaultDaemon,
    executor: Arc<LiveLoopExecutor>,
    routing: Arc<SignedOutRouting>,
    sent: Arc<std::sync::Mutex<Vec<String>>>,
    server: tokio::task::JoinHandle<()>,
    parts: SharedParts,
    status_a: Arc<std::sync::atomic::AtomicU16>,
    status_b: Arc<std::sync::atomic::AtomicU16>,
    reset_after_s: Arc<std::sync::atomic::AtomicU64>,
    scripted_b: Arc<std::sync::Mutex<VecDeque<String>>>,
}

impl SignedOutRig {
    async fn new(
        label: &str,
        fetch_failure: bool,
        missing: Option<&'static str>,
        replies: Vec<broca_module_serve::account_routing::RoutingReply>,
    ) -> Self {
        install_test_catalog();
        let root = fresh_vault_root(label);
        let daemon = CapturingVaultDaemon::start(&root).await;
        daemon.record.lock().unwrap().credential_replies.extend([
            (
                CRED_A.into(),
                VecDeque::from(if fetch_failure {
                    vec![
                        {
                            let mut expired = token(A, true);
                            expired["result"]["expires_at_ms"] = json!(now_ms() - 1);
                            expired
                        },
                        needs_reauth(),
                    ]
                } else {
                    vec![token(A, false)]
                }),
            ),
            (CRED_B.into(), VecDeque::from([token(B, false)])),
        ]);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let captured = sent.clone();
        let status_a = Arc::new(std::sync::atomic::AtomicU16::new(if fetch_failure {
            200
        } else {
            401
        }));
        let status = status_a.clone();
        let status_b = Arc::new(std::sync::atomic::AtomicU16::new(200));
        let other_status = status_b.clone();
        let reset_after_s = Arc::new(std::sync::atomic::AtomicU64::new(259_200));
        let reset = reset_after_s.clone();
        let scripted_b: Arc<std::sync::Mutex<VecDeque<String>>> = Arc::default();
        let body_script = scripted_b.clone();
        let server = tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let mut buf = [0u8; 4096];
                let (headers, body_start, body_len) = loop {
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0, "complete HTTP headers");
                    bytes.extend_from_slice(&buf[..n]);
                    if let Some(end) = bytes.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                        let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
                        let len = headers
                            .lines()
                            .find_map(|line| {
                                let (name, value) = line.split_once(':')?;
                                name.eq_ignore_ascii_case("content-length")
                                    .then(|| value.trim().parse::<usize>().unwrap())
                            })
                            .unwrap_or(0);
                        break (headers, end + 4, len);
                    }
                };
                while bytes.len() < body_start + body_len {
                    let n = socket.read(&mut buf).await.unwrap();
                    assert!(n > 0, "complete HTTP body");
                    bytes.extend_from_slice(&buf[..n]);
                }
                if headers.starts_with("GET ") {
                    socket.write_all(b"HTTP/1.1 426 Upgrade Required\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
                    continue;
                }
                let account = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("chatgpt-account-id")
                            .then(|| value.trim().to_owned())
                    })
                    .expect("each send names its account");
                captured.lock().unwrap().push(account.clone());
                let usage_wall = account == A && status.load(Ordering::SeqCst) == 429;
                let limit_body = json!({"error":{"type":"usage_limit_reached","message":"usage exhausted","resets_in_seconds":reset.load(Ordering::SeqCst)}}).to_string();
                let scripted = if account == B {
                    body_script.lock().unwrap().pop_front()
                } else {
                    None
                };
                let (status, content_type, body) = if let Some(body) = &scripted {
                    ("200 OK", "text/event-stream", body.as_str())
                } else if usage_wall {
                    (
                        "429 Too Many Requests",
                        "application/json",
                        limit_body.as_str(),
                    )
                } else if (account == A && status.load(Ordering::SeqCst) == 401)
                    || (account == B && other_status.load(Ordering::SeqCst) == 401)
                {
                    (
                        "401 Unauthorized",
                        "application/json",
                        r#"{"error":{"code":"token_expired","message":"token expired"}}"#,
                    )
                } else {
                    ("200 OK", "text/event-stream", "event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_test\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":0}}}\n\n")
                };
                let headers = if usage_wall {
                    "x-codex-primary-used-percent: 100\r\nx-codex-primary-window-minutes: 10080\r\nx-codex-secondary-used-percent: 0\r\nx-codex-secondary-window-minutes: 0\r\n"
                } else {
                    ""
                };
                let response = format!("HTTP/1.1 {status}\r\n{headers}Content-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        let routing = Arc::new(SignedOutRouting {
            requests: Default::default(),
            request_bytes: Default::default(),
            replies: std::sync::Mutex::new(replies.into()),
            missing: std::sync::Mutex::new(missing),
            pending: Default::default(),
            before_continue: Default::default(),
            replay_b: Default::default(),
        });
        let listing = Arc::new(broca_module_serve::account_routing::ListingCache::default());
        let credentials: Vec<_> = [(CRED_A, A), (CRED_B, B)].into_iter().map(|(id, account)| json!({
            "id":id,"type":"chatgpt","categories":["llm"],"serves":[],"state":"active", "record_version":1,
            "operations":["read"],"auth_method":"chatgpt","provider_ids":["openai"],"account_id":account
        })).collect();
        listing.store(
            serde_json::from_value(
                json!({"credentials":credentials,"grants":1,"grant_tuples":[],"view":"fixture"}),
            )
            .unwrap(),
        );
        let mut executor = executor_with_vault_addressing(
            &root,
            &daemon,
            Duration::from_secs(2),
            listing,
            supervised_broca_identity(),
        )
        .await;
        executor.provider_overrides =
            broca_module_serve::provider_overrides::ProviderOverrides::parse(
                &json!({"providers":{"openai":{"base_url":url}}}).to_string(),
            )
            .unwrap();
        executor.routing = Some(Arc::new(
            broca_module_serve::account_routing::RoutingClient::new(
                routing.clone(),
                Default::default(),
            ),
        ));
        Self {
            root,
            daemon,
            executor: Arc::new(executor),
            routing,
            sent,
            server,
            parts: SharedParts::new(Box::new(MemoryParts::default())),
            status_a,
            status_b,
            reset_after_s,
            scripted_b,
        }
    }

    fn script_a(&self, replies: Vec<Value>) {
        self.daemon
            .record
            .lock()
            .unwrap()
            .credential_replies
            .insert(CRED_A.into(), replies.into());
    }

    fn actor(&self, session: StoreSession) -> (Commands, tokio::task::JoinHandle<StoreSession>) {
        let actor = broca_module::store_actor::StoreActor::new(
            BindIdentity::new(&self.root, "test", "session"),
            session,
            self.executor.clone(),
            Box::new(self.parts.clone()),
        );
        let (commands, receiver) = mpsc::unbounded_channel();
        (commands, tokio::spawn(actor.serve(receiver)))
    }

    fn triple(&self) -> broca_session_store::head::SessionTriple {
        broca_session_store::head::SessionTriple {
            project_root: self.root.to_str().unwrap().into(),
            harness: "test".into(),
            session: "session".into(),
        }
    }

    async fn session(&self) -> (&'static MemoryBackend, StoreSession) {
        let backend: &'static MemoryBackend = Box::leak(Box::default());
        let session = Session::create(
            backend,
            broca_session_store::backend::SessionAddr("0123456789abcdef".into()),
            self.triple(),
            &broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
            false,
        )
        .await
        .unwrap();
        (backend, session)
    }

    async fn reopen(&self, backend: &'static MemoryBackend) -> StoreSession {
        Session::load(
            backend,
            broca_session_store::backend::SessionAddr("0123456789abcdef".into()),
            &self.triple(),
            &broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        )
        .await
        .unwrap()
    }
}

impl Drop for SignedOutRig {
    fn drop(&mut self) {
        self.server.abort();
    }
}

async fn send_signed_out(commands: &Commands) -> broca_module::actor::SendOutcome {
    send_signed_out_with_auth(commands, false).await
}

async fn send_signed_out_with_auth(
    commands: &Commands,
    explicit_auth: bool,
) -> broca_module::actor::SendOutcome {
    send_signed_out_named(commands, explicit_auth, "signed-out-run").await
}

async fn send_signed_out_named(
    commands: &Commands,
    explicit_auth: bool,
    run_id: &str,
) -> broca_module::actor::SendOutcome {
    let (reply, answer) = tokio::sync::oneshot::channel();
    let mut request = json!({
        "model":{"provider_module_id":"openai","model_id":"gpt-5"},
        "input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],
        "tools":[], "stop_when":[{"type":"step_count","n":1}]
    });
    if explicit_auth {
        request["auth"] = json!({"method":"chatgpt"});
    }
    commands
        .send(LineageCmd::Send {
            session_class: None,
            retention: None,
            stop_sequences_named: false,
            conversation_key: Some("broca-session:signed-out".into()),
            title: None,
            run_id: broca_protocol::RunId(run_id.into()),
            request: Box::new(serde_json::from_value(request).unwrap()),
            kind: broca_module::executor::RunKind::FreshOrResume,
            key: SendKey::RunId,
            skip_materialization: false,
            delivery: broca_protocol::Delivery::Queue,
            plan: None,
            order: None,
            reply,
        })
        .unwrap();
    answer.await.unwrap()
}

async fn actor_idle(commands: &Commands) {
    broca_test_support::wait_until("signed-out actor settles", || async {
        let (reply, answer) = tokio::sync::oneshot::channel();
        commands.send(LineageCmd::Status { reply }).unwrap();
        if matches!(answer.await.unwrap(), LineageStatus::Idle) {
            Ok(())
        } else {
            Err("still active".into())
        }
    })
    .await;
}

async fn records(session: &StoreSession) -> Vec<(u64, Record)> {
    session.records_from(0).await.unwrap()
}

fn assert_signed_out_question(routing: &SignedOutRouting, count: usize) {
    let questions = routing.questions();
    assert_eq!(questions.len(), count, "{questions:#?}");
    for (_, request) in &questions {
        let params = &request["params"];
        assert_eq!(params["run_id"], "signed-out-run");
        assert_eq!(params["admission_id"], "signed-out-run");
        assert_eq!(params["previous_decision_id"], "decision-a");
        assert_eq!(params["exhausted"]["credential_id"], CRED_A);
        assert_eq!(params["exhausted"]["account"], A);
        assert_eq!(params["exhausted"]["limit_window"], "signed_out");
        assert!(params["exhausted"]["observed_at_ms"].as_u64().unwrap() > 0);
        for forbidden in ["pool", "retry_after_s", "resets_at_ms"] {
            assert!(!params["exhausted"]
                .as_object()
                .unwrap()
                .contains_key(forbidden));
        }
    }
    if count > 1 {
        assert_ne!(
            questions[0].1["params"]["selection_id"],
            questions[1].1["params"]["selection_id"]
        );
    }
}

#[tokio::test]
async fn signed_out_vault_fetch_moves_without_pause() {
    let rig = SignedOutRig::new("signed-out-fetch", true, None, vec![offer()]).await;
    let (_, session) = rig.session().await;
    let (commands, task) = rig.actor(session);
    assert!(matches!(
        send_signed_out(&commands).await,
        broca_module::actor::SendOutcome::Active { .. }
    ));
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    assert_eq!(
        session.operations().run.as_ref().unwrap().finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
    assert_eq!(*rig.sent.lock().unwrap(), [B]);
    assert_signed_out_question(&rig.routing, 1);
    assert!(!records(&session)
        .await
        .iter()
        .any(|(_, record)| matches!(record, Record::Pause(_))));
    if let Some(path) = std::env::var_os("BROCA_SIGNED_OUT_SAMPLE") {
        std::fs::write(path, &rig.routing.request_bytes.lock().unwrap()[0]).unwrap();
    }
    rig.daemon.assert_healthy();
}

#[tokio::test]
async fn signed_out_fresh_401_moves_without_pause() {
    let rig = SignedOutRig::new("signed-out-401", false, None, vec![offer()]).await;
    rig.script_a(vec![token(A, false), versioned_token(A, 2)]);
    let (_, session) = rig.session().await;
    let (commands, task) = rig.actor(session);
    send_signed_out(&commands).await;
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    assert_eq!(
        session.operations().run.as_ref().unwrap().finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
    assert_eq!(*rig.sent.lock().unwrap(), [A, A, B]);
    assert_signed_out_question(&rig.routing, 1);
    let records = records(&session).await;
    assert!(!records
        .iter()
        .any(|(_, record)| matches!(record, Record::Pause(_))));
    let moved = records
        .iter()
        .position(|(_, record)| matches!(record, Record::AuthRefrozen(_)))
        .unwrap();
    let attempted_b = records.iter().position(|(_, record)| matches!(record, Record::Attempt(attempt) if attempt.served_account_ref.as_deref() == Some(B))).unwrap();
    assert!(moved < attempted_b);
    let observations: Vec<_> = rig
        .routing
        .requests
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, request)| request["method"] == "route.account_observe")
        .map(|(_, request)| request.clone())
        .collect();
    assert!(observations
        .iter()
        .any(|request| request["params"]["kind"] == "admission_started"
            && request["params"]["served_account_id"] == B));
    rig.daemon.assert_healthy();
}

#[tokio::test]
async fn usage_wall_explicit_auth_moves_without_reapplying_admission_hints() {
    let rig = SignedOutRig::new("usage-wall-explicit-auth", false, None, vec![offer()]).await;
    rig.status_a.store(429, Ordering::SeqCst);
    let (_, session) = rig.session().await;
    let (commands, task) = rig.actor(session);
    assert!(matches!(
        send_signed_out_with_auth(&commands, true).await,
        broca_module::actor::SendOutcome::Active { .. }
    ));
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    let run = session.operations().run.as_ref().unwrap();
    assert_eq!(
        run.finished,
        Some(broca_protocol::RunFinishReason::Completed),
        "stored error: {:?}",
        run.end_error
    );
    assert_eq!(*rig.sent.lock().unwrap(), [A, B]);
    let questions = rig.routing.questions();
    assert_eq!(questions.len(), 1);
    assert_eq!(
        questions[0].1["params"]["exhausted"]["limit_window"],
        "seven_day"
    );
    assert!(!records(&session)
        .await
        .iter()
        .any(|(_, record)| matches!(record, Record::Pause(_))));
}

#[tokio::test]
async fn signed_out_hold_wakes_at_until_and_relogin_cancels_it() {
    for restored in [false, true] {
        let until = now_ms() + 3_000;
        let rig = SignedOutRig::new(if restored { "signed-out-relogin" } else { "signed-out-timer" }, true, None,
            vec![answer(json!({"kind":"hold","routing_code":"no_compatible_account","detail":{},"until_ms":until})), offer()]).await;
        let (_, session) = rig.session().await;
        let (commands, task) = rig.actor(session);
        send_signed_out(&commands).await;
        actor_idle(&commands).await;
        assert_signed_out_question(&rig.routing, 1);
        assert!(rig.sent.lock().unwrap().is_empty());
        if !restored {
            send_signed_out(&commands).await;
            actor_idle(&commands).await;
            assert_signed_out_question(&rig.routing, 1);
        }
        if restored {
            rig.script_a(vec![token(A, false)]);
            send_signed_out(&commands).await;
            actor_idle(&commands).await;
            broca_test_support::wait_until("the cancelled hold's deadline passes", || async {
                if now_ms() >= until {
                    Ok(())
                } else {
                    Err("deadline still future".into())
                }
            })
            .await;
            actor_idle(&commands).await;
        } else {
            broca_test_support::wait_until("held sign-in pause asks at its deadline", || async {
                if rig.routing.questions().len() == 2 {
                    Ok(())
                } else {
                    Err("waiting for timer".into())
                }
            })
            .await;
            assert!(rig.routing.questions()[1].0 >= until as u64);
            actor_idle(&commands).await;
        }
        drop(commands);
        let session = task.await.unwrap();
        assert_eq!(
            session.operations().run.as_ref().unwrap().finished,
            Some(broca_protocol::RunFinishReason::Completed)
        );
        assert_eq!(*rig.sent.lock().unwrap(), [if restored { A } else { B }]);
        assert_signed_out_question(&rig.routing, if restored { 1 } else { 2 });
        assert_eq!(records(&session).await.iter().filter(|(_, record)| matches!(record, Record::Pause(pause) if pause.reason == broca_protocol::PauseReason::AuthRequired)).count(), 1);
        rig.daemon.assert_healthy();
    }
}

#[tokio::test]
async fn signed_out_untimed_hold_reasks_only_on_keyed_retry() {
    let rig = SignedOutRig::new(
        "signed-out-untimed",
        true,
        None,
        vec![
            answer(json!({"kind":"hold","routing_code":"policy_wait","detail":{}})),
            offer(),
        ],
    )
    .await;
    let (_, session) = rig.session().await;
    let (commands, task) = rig.actor(session);
    send_signed_out(&commands).await;
    actor_idle(&commands).await;
    assert_signed_out_question(&rig.routing, 1);
    let held_at = std::time::Instant::now();
    send_signed_out(&commands).await;
    actor_idle(&commands).await;
    assert_signed_out_question(&rig.routing, 1);
    broca_test_support::wait_until("untimed hold answer-time floor", || async {
        (held_at.elapsed() >= Duration::from_secs(30)).then_some(()).ok_or_else(|| "hold floor has not elapsed".into())
    }).await;
    assert_signed_out_question(&rig.routing, 1);
    send_signed_out(&commands).await;
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    assert_eq!(
        session.operations().run.as_ref().unwrap().finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
    assert_signed_out_question(&rig.routing, 2);
    assert_eq!(*rig.sent.lock().unwrap(), [B]);
}

#[tokio::test]
async fn signed_out_refuse_unreachable_and_missing_capabilities_stay_paused() {
    for missing in [
        None,
        Some("unreachable"),
        Some("account_continue_signed_out"),
        Some("account_continue"),
        Some("account_continue_resume"),
        Some("unknown"),
    ] {
        let replies = if missing == Some("unreachable") {
            vec![Err("routing transport unavailable".into())]
        } else {
            vec![answer(
                json!({"kind":"refuse","routing_code":"pin_unavailable","detail":{}}),
            )]
        };
        let rig = SignedOutRig::new(
            &format!("signed-out-refused-{}", missing.unwrap_or("refuse")),
            true,
            missing,
            replies,
        )
        .await;
        let (_, session) = rig.session().await;
        let (commands, task) = rig.actor(session);
        send_signed_out(&commands).await;
        actor_idle(&commands).await;
        let expected = usize::from(missing.is_none() || missing == Some("unreachable"));
        assert_signed_out_question(&rig.routing, expected);
        send_signed_out(&commands).await;
        actor_idle(&commands).await;
        assert_signed_out_question(&rig.routing, expected);
        drop(commands);
        let session = task.await.unwrap();
        assert_eq!(
            session.operations().run.as_ref().unwrap().paused,
            Some(broca_protocol::PauseReason::AuthRequired)
        );
        assert_eq!(session.operations().run.as_ref().unwrap().finished, None);
        assert!(rig.sent.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn signed_out_actor_open_moves_a_preexisting_pause() {
    let rig = SignedOutRig::new(
        "signed-out-open",
        false,
        Some("account_continue_signed_out"),
        vec![],
    )
    .await;
    let (backend, session) = rig.session().await;
    let (commands, task) = rig.actor(session);
    send_signed_out(&commands).await;
    actor_idle(&commands).await;
    drop(commands);
    drop(task.await.unwrap());
    assert_signed_out_question(&rig.routing, 0);
    rig.script_a(vec![needs_reauth()]);
    // Open the stored pause against a now-capable router, without sending another command.
    let routing = Arc::new(SignedOutRouting {
        requests: Default::default(),
        request_bytes: Default::default(),
        replies: std::sync::Mutex::new(VecDeque::from([offer()])),
        missing: std::sync::Mutex::new(None),
        pending: Default::default(),
        before_continue: Default::default(),
        replay_b: Default::default(),
    });
    let mut rig = rig;
    Arc::get_mut(&mut rig.executor).unwrap().routing = Some(Arc::new(
        broca_module_serve::account_routing::RoutingClient::new(
            routing.clone(),
            Default::default(),
        ),
    ));
    rig.routing = routing;
    let (commands, task) = rig.actor(rig.reopen(backend).await);
    broca_test_support::wait_until(
        "actor open continues a preexisting sign-in pause",
        || async {
            if rig.routing.questions().len() == 1 {
                Ok(())
            } else {
                Err("waiting for open recovery".into())
            }
        },
    )
    .await;
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    let records = records(&session).await;
    assert_eq!(
        session.operations().run.as_ref().unwrap().finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
    assert_signed_out_question(&rig.routing, 1);
    assert_eq!(*rig.sent.lock().unwrap(), [A, B]);
    let moved = records
        .iter()
        .position(|(_, record)| matches!(record, Record::AuthRefrozen(_)))
        .unwrap();
    assert!(matches!(&records[moved + 1].1, Record::Resumed(_)));
    assert_eq!(
        records
            .iter()
            .filter(|(_, record)| matches!(record, Record::Pause(_)))
            .count(),
        1
    );
}

#[tokio::test]
async fn signed_out_transient_vault_and_timeout_never_ask() {
    for pre_send in [false, true] {
        for code in ["refresh_failed", "timeout"] {
            let rig = SignedOutRig::new(
                &format!("signed-out-vault-{code}-{pre_send}"),
                true,
                None,
                vec![],
            )
            .await;
            let failed = json!({"result":{"error":{"class":"transient","code":code}}});
            let mut replies = Vec::new();
            if pre_send {
                let mut expired = token(A, true);
                expired["result"]["expires_at_ms"] = json!(now_ms() - 1);
                replies.push(expired);
            }
            replies.push(failed);
            rig.script_a(replies);
            let (_, session) = rig.session().await;
            let (commands, task) = rig.actor(session);
            send_signed_out(&commands).await;
            actor_idle(&commands).await;
            drop(commands);
            let session = task.await.unwrap();
            assert_signed_out_question(&rig.routing, 0);
            assert_eq!(
                *rig.sent.lock().unwrap(),
                if pre_send { vec![A] } else { vec![] }
            );
            assert!(!records(&session)
                .await
                .iter()
                .any(|(_, record)| matches!(record, Record::Pause(_))));
        }
    }
}

#[tokio::test]
async fn signed_out_unexpired_held_token_still_completes_on_its_account() {
    let rig = SignedOutRig::new("signed-out-still-valid", true, None, vec![]).await;
    rig.script_a(vec![token(A, true), needs_reauth()]);
    let (_, session) = rig.session().await;
    let (commands, task) = rig.actor(session);
    send_signed_out(&commands).await;
    actor_idle(&commands).await;
    drop(commands);
    let session = task.await.unwrap();
    assert_eq!(
        session.operations().run.as_ref().unwrap().finished,
        Some(broca_protocol::RunFinishReason::Completed)
    );
    assert_eq!(*rig.sent.lock().unwrap(), [A]);
    assert_signed_out_question(&rig.routing, 0);
    assert_eq!(
        rig.daemon.scoped_reads().len(),
        2,
        "the vault actually returned needs_reauth before the send"
    );
}

#[tokio::test]
async fn signed_out_actor_open_survives_every_store_write_cut() {
    let mut cuts = 0;
    let mut uncut_completed = false;
    let mut saw_durable_move = false;
    let mut saw_send_before_cut = false;
    for writes in 0..64 {
        let mut rig = SignedOutRig::new(
            &format!("signed-out-cut-{writes}"),
            false,
            Some("account_continue_signed_out"),
            vec![],
        )
        .await;
        let (backend, session) = rig.session().await;
        let (commands, task) = rig.actor(session);
        send_signed_out(&commands).await;
        actor_idle(&commands).await;
        drop(commands);
        drop(task.await.unwrap());
        assert_signed_out_question(&rig.routing, 0);
        assert_eq!(*rig.sent.lock().unwrap(), [A]);
        rig.script_a(vec![needs_reauth()]);
        let routing = Arc::new(SignedOutRouting {
            requests: Default::default(),
            request_bytes: Default::default(),
            replies: std::sync::Mutex::new(VecDeque::from([offer(), offer()])),
            missing: std::sync::Mutex::new(None),
            pending: Default::default(),
            before_continue: Default::default(),
            replay_b: Default::default(),
        });
        Arc::get_mut(&mut rig.executor).unwrap().routing = Some(Arc::new(
            broca_module_serve::account_routing::RoutingClient::new(
                routing.clone(),
                Default::default(),
            ),
        ));
        rig.routing = routing;
        let session = rig.reopen(backend).await;
        backend.crash_after(Some(writes));
        let (commands, task) = rig.actor(session);
        actor_idle(&commands).await;
        drop(commands);
        let session = task.await.unwrap();
        backend.crash_after(None);
        let completed = session.operations().run.as_ref().unwrap().finished
            == Some(broca_protocol::RunFinishReason::Completed);
        saw_send_before_cut |= rig.sent.lock().unwrap().iter().any(|account| account == B);
        drop(session);
        // Recovery reads the storage left by the simulated crash. The test supplies neither
        // precomputed state nor a provider step: the actor decides whether to resume or finish.
        let session = rig.reopen(backend).await;
        let before = records(&session).await;
        let auths = before
            .iter()
            .filter(|(_, record)| matches!(record, Record::AuthRefrozen(_)))
            .count();
        let resumes = before
            .iter()
            .filter(|(_, record)| matches!(record, Record::Resumed(_)))
            .count();
        assert_eq!(
            auths, resumes,
            "cut {writes}: move and pause lift must share a batch"
        );
        saw_durable_move |= auths > 0;
        let (commands, task) = rig.actor(session);
        actor_idle(&commands).await;
        drop(commands);
        let recovered = task.await.unwrap();
        let run = recovered.operations().run.as_ref().unwrap();
        assert!(
            matches!(
                run.finished,
                Some(
                    broca_protocol::RunFinishReason::Completed
                        | broca_protocol::RunFinishReason::Interrupted
                )
            ) || run.paused == Some(broca_protocol::PauseReason::AuthRequired),
            "cut {writes}: {run:?}"
        );
        let sent = rig.sent.lock().unwrap().clone();
        assert_eq!(
            sent.iter().filter(|account| *account == A).count(),
            1,
            "cut {writes}: no repeat of the original request"
        );
        assert!(
            sent.iter().filter(|account| *account == B).count() <= 1 && sent.len() <= 2,
            "cut {writes}: no provider request is sent twice: {sent:?}"
        );
        let mut moved = false;
        for (_, record) in records(&recovered).await {
            match record {
                Record::AuthRefrozen(auth) => {
                    assert_eq!(auth.auth_selection["account_ref"], B);
                    moved = true;
                }
                Record::Attempt(attempt) if attempt.served_account_ref.as_deref() == Some(B) => {
                    assert!(moved, "cut {writes}: attempt before durable move")
                }
                _ => {}
            }
        }
        if completed {
            uncut_completed = true;
            break;
        }
        cuts += 1;
    }
    assert!(cuts > 3, "covered {cuts} write cuts");
    assert!(uncut_completed, "the uncut reference must complete");
    assert!(saw_durable_move && saw_send_before_cut);
    eprintln!("sign-in recovery covered {cuts} failed write cuts and one uncut completion");
}

#[tokio::test]
async fn signed_out_at_a_due_account_limit_wake_moves_or_pauses_on_refusal() {
    for outcome in ["offer", "refuse", "hold"] {
        let offered = outcome == "offer";
        let hold_until = now_ms() + 120_000;
        let reply = if offered {
            offer()
        } else if outcome == "hold" {
            answer(
                json!({"kind":"hold","routing_code":"no_compatible_account","detail":{},"until_ms":hold_until}),
            )
        } else {
            answer(json!({"kind":"refuse","routing_code":"pin_unavailable","detail":{}}))
        };
        let rig = SignedOutRig::new(
            &format!("signed-out-limit-wake-{outcome}"),
            false,
            None,
            vec![reply],
        )
        .await;
        rig.status_a.store(200, Ordering::SeqCst);
        let (backend, session) = rig.session().await;
        let (commands, task) = rig.actor(session);
        send_signed_out_named(&commands, false, "cache-seed").await;
        actor_idle(&commands).await;
        rig.status_a.store(429, Ordering::SeqCst);
        rig.reset_after_s.store(3, Ordering::SeqCst);
        send_signed_out(&commands).await;
        actor_idle(&commands).await;
        assert_signed_out_question(&rig.routing, 0);
        drop(commands);
        let session = task.await.unwrap();
        assert_eq!(
            session.operations().run.as_ref().unwrap().paused,
            Some(broca_protocol::PauseReason::AccountLimit)
        );
        let before = records(&session).await;
        let until = before
            .iter()
            .find_map(|(_, record)| match record {
                Record::Pause(pause)
                    if pause.reason == broca_protocol::PauseReason::AccountLimit =>
                {
                    pause
                        .account_limit
                        .as_ref()
                        .and_then(|pause| pause.parse()["resume_not_before_ms"].as_u64())
                }
                _ => None,
            })
            .unwrap();
        drop(session);
        rig.script_a(vec![needs_reauth()]);
        let (commands, task) = rig.actor(rig.reopen(backend).await);
        broca_test_support::wait_until("due quota wake observes sign-in loss", || async {
            if rig.routing.questions().len() == 1 {
                Ok(())
            } else {
                Err("waiting for account-limit timer".into())
            }
        })
        .await;
        assert!(rig.routing.questions()[0].0 >= until);
        actor_idle(&commands).await;
        if !offered {
            send_signed_out(&commands).await;
            actor_idle(&commands).await;
            assert_signed_out_question(&rig.routing, 1);
        }
        drop(commands);
        let session = task.await.unwrap();
        let run = session.operations().run.as_ref().unwrap();
        let after = records(&session).await;
        let auth_pauses = after.iter().filter(|(_, record)| matches!(record, Record::Pause(pause) if pause.reason == broca_protocol::PauseReason::AuthRequired)).count();
        if offered {
            assert_eq!(
                run.finished,
                Some(broca_protocol::RunFinishReason::Completed)
            );
            assert_eq!(auth_pauses, 0);
            assert_eq!(*rig.sent.lock().unwrap(), [A, A, B]);
        } else {
            assert_eq!(run.paused, Some(broca_protocol::PauseReason::AuthRequired));
            assert_eq!(run.finished, None);
            assert_eq!(auth_pauses, 1);
            assert_eq!(
                run.end_error.as_ref().unwrap()["account_continue"]["routing_code"],
                if outcome == "hold" {
                    "no_compatible_account"
                } else {
                    "pin_unavailable"
                }
            );
            if outcome == "hold" {
                assert_eq!(
                    run.end_error.as_ref().unwrap()["account_continue"]["until_ms"],
                    hold_until
                );
            }
            assert_eq!(*rig.sent.lock().unwrap(), [A, A]);
        }
        assert_signed_out_question(&rig.routing, 1);
    }
}
