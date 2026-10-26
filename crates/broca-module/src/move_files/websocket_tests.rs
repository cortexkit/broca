use super::*;
use crate::executor::{RunExecutor, RunJob};
use broca_core::provider::Provider;
use broca_provider::{
    families::openai_responses_chatgpt_frozen_config,
    live_provider_with_clock_and_refresher,
    transport::{build_provider_http_client, ChatgptConnections},
    wire::AuthHandle,
};
use futures::{SinkExt, StreamExt};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_tungstenite::tungstenite::Message as WsMessage;

struct WsExecutor {
    provider: Box<dyn Provider>,
    connections: ChatgptConnections,
    wal: Arc<FileWalStore>,
}

#[async_trait::async_trait]
impl RunExecutor for WsExecutor {
    async fn execute(
        &self,
        job: RunJob,
        out: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
    ) -> Result<broca_core::RunSummary, broca_core::RunError> {
        let mode = match job.kind {
            crate::executor::RunKind::FreshOrResume => broca_core::StartMode::FreshOrResume,
            crate::executor::RunKind::AppendEpisode => broca_core::StartMode::AppendEpisode,
            crate::executor::RunKind::ResumeAfterAuth => broca_core::StartMode::ResumeAfterAuth,
        };
        broca_core::run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            job.writer.as_ref(),
            mode,
            &self.provider,
            &broca_core::StubToolPlane::new(),
            &broca_core::AllowAll,
            self.wal.as_ref(),
            None,
            job.control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
        .await
    }

    fn retire_session_transport(&self, session: &BindIdentity) {
        self.connections.retire_session(session);
    }

    fn retire_idle_session_transport(&self, session: &BindIdentity) {
        self.connections.retire_idle_session(session);
    }

    async fn on_session_close(&self, session: &BindIdentity) {
        self.connections.retire_session(session);
    }

    fn retire_all_transports(&self) {
        self.connections.clear();
    }
}

#[tokio::test]
async fn refused_busy_move_leaves_real_websocket_stream_and_generation_untouched() {
    let mut rig = Rig::new("ws-busy", true);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let generations = Arc::new(AtomicUsize::new(0));
    let closes = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let (finish, finish_rx) = tokio::sync::watch::channel(false);
    let server = {
        let generations = generations.clone();
        let closes = closes.clone();
        let started = started.clone();
        tokio::spawn(async move {
            let mut peers = tokio::task::JoinSet::new();
            loop {
                let (tcp, _) = listener.accept().await.unwrap();
                let generations = generations.clone();
                let closes = closes.clone();
                let started = started.clone();
                let mut finish = finish_rx.clone();
                peers.spawn(async move {
                    let mut config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default();
                    config.extensions.permessage_deflate = Some(tokio_tungstenite::tungstenite::extensions::compression::deflate::DeflateConfig::default());
                    let mut socket = tokio_tungstenite::accept_async_with_config(tcp, Some(config)).await.unwrap();
                    while let Some(Ok(message)) = socket.next().await {
                        match message {
                            WsMessage::Text(_) => {
                                generations.fetch_add(1, Ordering::SeqCst);
                                socket.send(WsMessage::Text(json!({"type":"response.created","response":{"id":"r"}}).to_string().into())).await.unwrap();
                                started.notify_one();
                                while !*finish.borrow() {
                                    tokio::select! {
                                        _ = async { finish.wait_for(|done| *done).await.map(|_| ()).unwrap(); } => {},
                                        message = socket.next() => match message {
                                            Some(Ok(WsMessage::Ping(bytes))) => {
                                                if socket.send(WsMessage::Pong(bytes)).await.is_err() { return; }
                                            }
                                            Some(Ok(WsMessage::Text(_))) => { generations.fetch_add(1, Ordering::SeqCst); }
                                            Some(Ok(WsMessage::Close(_))) | Some(Err(_)) | None => { closes.fetch_add(1, Ordering::SeqCst); return; }
                                            _ => {}
                                        }
                                    }
                                }
                                for event in [
                                    json!({"type":"response.output_text.delta","delta":"done"}),
                                    json!({"type":"response.completed","response":{"id":"r","status":"completed","usage":{"input_tokens":12,"output_tokens":1}}}),
                                ] {
                                    if socket.send(WsMessage::Text(event.to_string().into())).await.is_err() { return; }
                                }
                            }
                            WsMessage::Close(_) => { closes.fetch_add(1, Ordering::SeqCst); return; }
                            _ => {}
                        }
                    }
                });
            }
        })
    };
    let client = build_provider_http_client();
    let connections = client.chatgpt_connections().clone();
    let mut frozen = openai_responses_chatgpt_frozen_config("m");
    frozen.base_url = Some(url);
    frozen.chatgpt_wire = Some(
        serde_json::from_str(include_str!(
            "../../../broca-provider/tests/fixtures/chatgpt_codex_0_163_identity.json"
        ))
        .unwrap(),
    );
    let provider = live_provider_with_clock_and_refresher(
        Some(client),
        frozen,
        AuthHandle::None,
        None,
        None,
        None,
        broca_core::clock::Clock::default(),
    )
    .unwrap();
    provider.set_chatgpt_https_policy(Arc::new(|| false));
    let executor = Arc::new(WsExecutor {
        provider,
        connections,
        wal: rig.wal.clone(),
    });
    let opener = Arc::new(crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(rig.root.join("leases"))),
        rig.wal.clone(),
        executor,
    ));
    rig.artifacts.registry = LineageRegistry::new_with_reader(opener, rig.wal.clone(), None);
    let session = rig.driver.plans[0].session.clone();
    let mut request: broca_protocol::request::SubmissionRequest = serde_json::from_value(json!({
        "model":broca_protocol::ModelHandle::new("openai","m"),
        "input":[{"role":"user","content":[{"type":"text","text":"hold the response"}]}],
        "tools":[]
    }))
    .unwrap();
    request.scope = broca_protocol::SendScope::Internal;
    let initial_records = rig.wal.read_records(&session).unwrap().len();
    let admitted = rig
        .artifacts
        .registry
        .route_submission(
            &session,
            broca_protocol::RunId("ws-held".into()),
            request,
            crate::executor::RunKind::FreshOrResume,
            false,
        )
        .await
        .unwrap();
    assert!(matches!(admitted, crate::actor::SendOutcome::Active { .. }));
    broca_test_support::event_timeout(
        "provider response.create received",
        started.notified(),
        || {
            format!(
                "generations={} closes={}",
                generations.load(Ordering::SeqCst),
                closes.load(Ordering::SeqCst)
            )
        },
    )
    .await
    .unwrap();
    let busy = rig
        .store
        .execute(
            "reserved:entorhinal",
            "agent_data.busy",
            &json!({"refs":[rig.params["refs"][0].clone()]}),
            &rig.artifacts,
        )
        .await
        .unwrap();
    assert_eq!(busy["refs"][0]["state"], "busy");
    assert_eq!(rig.op(Op::Prepare).await.unwrap_err().code.as_str(), "busy");
    assert_eq!(closes.load(Ordering::SeqCst), 0);
    assert_eq!(generations.load(Ordering::SeqCst), 1);
    finish.send(true).unwrap();
    broca_test_support::wait_until("run completes after refused busy move", || async {
        let active = rig.artifacts.registry.slot_counts().runs_in_flight;
        if active == 0 {
            Ok(())
        } else {
            Err(format!("{active} runs still in flight"))
        }
    })
    .await;
    assert_eq!(
        generations.load(Ordering::SeqCst),
        1,
        "a refused move must not cause a generation retry"
    );
    assert_eq!(
        closes.load(Ordering::SeqCst),
        0,
        "healthy completion retains the original socket"
    );
    let records = rig.wal.read_records(&session).unwrap();
    assert!(records[initial_records..].iter().any(|frame| matches!(
        &frame.record,
        broca_wal::WalRecord::RunFinished {
            reason: broca_protocol::RunFinishReason::Completed,
            ..
        }
    )));
    assert!(rig.artifacts.registry.stop_and_seal().await);
    server.abort();
}
