use super::*;
use broca_module::serve::{
    CancelResponse, RetractResponse, RunStatusResponse, SendRequest, SendResponse,
    SessionReadResponse, StatusResponse,
};

struct EmptyFlowHistory;
impl broca_core::WalStore for EmptyFlowHistory {
    fn open(
        &self,
        _: &BrocaBindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        panic!("the frame admission fixture must not open an actor")
    }
    fn read_records(
        &self,
        _: &BrocaBindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        Ok(vec![])
    }
}

struct NoFlowActor;
#[async_trait::async_trait]
impl broca_module::registry::LineageOpener for NoFlowActor {
    async fn open(
        &self,
        _: &BrocaBindIdentity,
        _: u64,
        _: broca_module::registry::RegistryHandle,
    ) -> Result<broca_module::registry::ActorHandle, broca_module::registry::OpenError> {
        panic!("the frame admission fixture must not create a lineage")
    }
}

struct FlowSendHandler {
    sends: mpsc::UnboundedSender<SendRequest>,
}
#[async_trait::async_trait]
impl ServeHandler for FlowSendHandler {
    async fn authorize_session(
        &self,
        _: &BrocaBindIdentity,
        _: Option<&broca_protocol::BindPrincipal>,
    ) -> Result<(), ServeError> {
        Ok(())
    }
    async fn send(&self, request: SendRequest) -> Result<SendResponse, ServeError> {
        assert!(self.sends.send(request).is_ok());
        Ok(SendResponse::Active {
            run_id: "flow-compat-run".into(),
            delivered: None,
        })
    }
    async fn retract(
        &self,
        _: &BrocaBindIdentity,
        _: broca_protocol::SubmissionId,
    ) -> Result<RetractResponse, ServeError> {
        unreachable!()
    }
    async fn subscribe(
        &self,
        _: &BrocaBindIdentity,
        _: SubscribeFrom,
        _: mpsc::Sender<Delivery>,
    ) -> Result<(), ServeError> {
        unreachable!()
    }
    async fn read(&self, _: SessionReadRequest) -> Result<SessionReadResponse, ServeError> {
        unreachable!()
    }
    async fn cancel(
        &self,
        _: &BrocaBindIdentity,
        _: broca_protocol::RunId,
    ) -> Result<CancelResponse, ServeError> {
        unreachable!()
    }
    async fn status(&self, _: &BrocaBindIdentity) -> Result<StatusResponse, ServeError> {
        unreachable!()
    }
    async fn run_status(
        &self,
        _: &BrocaBindIdentity,
        _: broca_protocol::RunId,
    ) -> Result<RunStatusResponse, ServeError> {
        unreachable!()
    }
}

struct FlowFrames {
    routes: Routes,
    handler: Arc<FlowSendHandler>,
    registry: Arc<LineageRegistry>,
    writer: EgressSender<Frame>,
    written: EgressReceiver<Frame>,
    requests: JoinSet<Result<(), ServeIoError>>,
    cancel: CancellationToken,
}
impl FlowFrames {
    fn new(sender: mpsc::UnboundedSender<SendRequest>) -> Self {
        let (writer, written) = broca_diagnostics::channel(16, Arc::default());
        Self {
            routes: Routes::new(true),
            handler: Arc::new(FlowSendHandler { sends: sender }),
            registry: Arc::new(LineageRegistry::new_with_reader(
                Arc::new(NoFlowActor),
                Arc::new(EmptyFlowHistory),
                None,
            )),
            writer,
            written,
            requests: JoinSet::new(),
            cancel: CancellationToken::new(),
        }
    }
    async fn frame(&mut self, frame: Frame) {
        assert!(handle_frame(
            frame,
            &self.writer,
            &mut self.routes,
            &self.handler,
            &self.registry,
            &VaultGauge::not_in_use(),
            None,
            &self.cancel,
            &Arc::new(Semaphore::new(4)),
            &mut self.requests,
            None
        )
        .await
        .unwrap());
    }
    async fn reply(&mut self) -> Frame {
        tokio::time::timeout(std::time::Duration::from_secs(5), self.written.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn bind(&mut self, channel: u16, project: &std::path::Path, flow: bool) {
        let principal = subc_protocol::Principal::Reserved {
            module_id: "basal".into(),
        };
        let scope = flow.then(|| {
            let mut stamp = subc_protocol::scope::ScopeStamp::new(
                subc_protocol::Principal::Reserved {
                    module_id: "prefrontal-core".into(),
                },
                "flow",
                1,
                subc_protocol::scope::ScopeKind::Head,
            )
            .with_owner_authorized(true);
            stamp.attributes.flow_id = Some("flow-compat".into());
            stamp
        });
        let request = ModuleControlRequest::RouteBind {
            route_channel: channel,
            epoch: 1,
            target: subc_protocol::RouteTarget::ManagementSurface {
                module_id: "broca".into(),
            },
            identity: SubcBindIdentity::new(
                project.to_string_lossy().into_owned(),
                "runner",
                format!("flow-{channel}"),
            ),
            principal: Some(principal),
            scope,
            role_versions: None,
            consumer_capabilities: None,
            admission_facts: None,
            call_mode: None,
        };
        self.frame(
            Frame::build(
                FrameType::Request,
                control_flags(),
                0,
                0,
                u64::from(channel),
                serde_json::to_vec(&request).unwrap(),
            )
            .unwrap(),
        )
        .await;
        if !self.routes.bind_tasks.is_empty() {
            let completed = tokio::time::timeout(
                std::time::Duration::from_secs(5),
                self.routes.bind_tasks.join_next(),
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();
            finish_bind(completed, &self.writer, &mut self.routes)
                .await
                .unwrap();
        }
        let ack = self.reply().await;
        assert_eq!(
            ack.header.ty,
            FrameType::Response,
            "{}",
            String::from_utf8_lossy(&ack.body)
        );
    }
}

impl Drop for FlowFrames {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

// Bind and dispatch real protocol frames up to send admission. Observing the admitted
// request instead of starting a run isolates scope/refusal behavior from providers.
#[tokio::test]
async fn basal_flow_send_preserves_master_admission_and_refusal_bytes() {
    let project =
        broca_test_support::scratch_root().join(format!("flow-send-compat-{}", std::process::id()));
    std::fs::create_dir_all(&project).unwrap();
    let (sender, mut received) = mpsc::unbounded_channel();
    let mut rig = FlowFrames::new(sender);
    rig.bind(7, &project, true).await;
    let request = serde_json::json!({"method":"session.send","params":{
        "model":{"provider":"mock","model":"m"},"prompt":"hello","send_id":"flow-compat-send","tools":[]
    }});
    rig.frame(
        Frame::build(
            FrameType::Request,
            control_flags(),
            7,
            1,
            2,
            serde_json::to_vec(&request).unwrap(),
        )
        .unwrap(),
    )
    .await;
    let active = rig.reply().await;
    assert_eq!(active.header.ty, FrameType::Response);
    assert_eq!(
        active.body,
        br#"{"result":{"state":"active","run_id":"flow-compat-run"}}"#
    );
    let accepted = received.try_recv().unwrap();
    let basal = broca_protocol::BindPrincipal::Reserved {
        module_id: "basal".into(),
    };
    assert_eq!(accepted.request.principal.as_ref(), Some(&basal));
    let broca_protocol::SendScope::Stamped(stamp) = accepted.request.scope else {
        panic!("flow stamp was discarded")
    };
    assert_eq!(stamp.scope.flow_id.as_deref(), Some("flow-compat"));
    assert_eq!(stamp.scope.sender.as_ref(), Some(&basal));
    assert!(stamp.owner_authorized);

    rig.frame(
        Frame::build(
            FrameType::Request,
            control_flags(),
            7,
            1,
            20,
            serde_json::to_vec(&serde_json::json!({"method":"health.check","params":{}})).unwrap(),
        )
        .unwrap(),
    )
    .await;
    let routed_health = rig.reply().await;
    let error: serde_json::Value = serde_json::from_slice(&routed_health.body).unwrap();
    assert_eq!(routed_health.header.ty, FrameType::Error);
    assert_eq!(error["code"], "unknown_method");
    assert_eq!(
        routed_health.body,
        br#"{"code":"unknown_method","message":"unknown method 'health.check'"}"#
    );
    assert!(received.try_recv().is_err());

    rig.bind(8, &project, false).await;
    rig.frame(
        Frame::build(
            FrameType::Request,
            control_flags(),
            8,
            1,
            3,
            serde_json::to_vec(&request).unwrap(),
        )
        .unwrap(),
    )
    .await;
    let missing_scope = rig.reply().await;
    let error: serde_json::Value = serde_json::from_slice(&missing_scope.body).unwrap();
    assert_eq!(missing_scope.header.ty, FrameType::Error);
    assert_eq!(error["code"], "flow_scope_required");
    assert_eq!(missing_scope.body,br#"{"code":"flow_scope_required","message":"this principal may send only under a flow scope","class":"permanent","detail":{"kind":"flow_scope_required","principal":"reserved:basal"}}"#);
    assert!(
        received.try_recv().is_err(),
        "a missing flow stamp must not reach send admission"
    );

    if let Some(path) = std::env::var_os("CAPTURE_BROCA_FLOW_COMPAT") {
        let bytes = serde_json::to_vec_pretty(&serde_json::json!({
            "active":String::from_utf8(active.body).unwrap(),
            "missing_scope":String::from_utf8(missing_scope.body).unwrap(),
            "routed_health":String::from_utf8(routed_health.body).unwrap(),
        }))
        .unwrap();
        std::fs::write(path, bytes).unwrap();
        println!("captured Basal flow send and missing-scope refusal bodies");
    }
}
