struct CancelAdmissionProbe {
    unreadable: bool,
    pending_release: bool,
    migration_reads: std::sync::atomic::AtomicUsize,
}

#[async_trait::async_trait]
impl ServeHandler for CancelAdmissionProbe {
    async fn authorize_session_owner(
        &self,
        _: &BrocaBindIdentity,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
    ) -> Result<(), ServeError> {
        if self.unreadable {
            return Err(ServeError::Open(
                "ownership history is unreadable".into(),
                None,
            ));
        }
        let ownership = broca_core::SessionOwnershipV1 {
            episodes: 1,
            first_principal: Some(broca_protocol::BindPrincipal::Reserved {
                module_id: "owner".into(),
            }),
            ..Default::default()
        };
        broca_core::scope_admission::decide_owner_access(&ownership, principal, scope)
            .map_err(ServeError::ScopeRefused)
    }
    async fn check_migration(&self, _: &BrocaBindIdentity, _: bool) -> Result<(), ServeError> {
        self.migration_reads.fetch_add(1, Ordering::SeqCst);
        if self.pending_release {
            Err(ServeError::SessionMoving {
                cut_id: "cut-a".into(),
            })
        } else {
            Err(ServeError::SessionMoved {
                holder: Some("0123456789abcdef0123456789abcdef".into()),
            })
        }
    }
    async fn send(&self, _: SendRequest) -> Result<SendResponse, ServeError> {
        unreachable!()
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

async fn cancel_admission_probe(
    unreadable: bool,
    pending_release: bool,
    owner: bool,
) -> (serde_json::Value, usize) {
    let handler = Arc::new(CancelAdmissionProbe {
        unreadable,
        pending_release,
        migration_reads: std::sync::atomic::AtomicUsize::new(0),
    });
    let (mut daemon, mut written, serve) =
        bound_frame_loop(handler.clone(), "cancel-admission").await;
    let bind = ModuleControlRequest::RouteBind {
        route_channel: 7,
        epoch: 2,
        target: subc_protocol::RouteTarget::ManagementSurface {
            module_id: "broca".into(),
        },
        identity: SubcBindIdentity::new("runner", "cancel-admission"),
        principal: Some(subc_protocol::Principal::Reserved {
            module_id: if owner { "owner" } else { "outsider" }.into(),
        }),
        consumer_capabilities: None,
        admission_facts: None,
        scope: None,
        role_versions: None,
        call_mode: None,
    };
    write_frame(
        &mut daemon,
        &Frame::build(
            FrameType::Request,
            control_flags(),
            0,
            0,
            20,
            serde_json::to_vec(&bind).unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        next_written(&mut written).await.header.ty,
        FrameType::Response
    );
    write_frame(
        &mut daemon,
        &Frame::build(
            FrameType::Request,
            control_flags(),
            7,
            2,
            21,
            serde_json::to_vec(
                &serde_json::json!({"method":"run.cancel","params":{"unknown":true}}),
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let reply = next_written(&mut written).await;
    assert_eq!((reply.header.ty, reply.header.corr), (FrameType::Error, 21));
    let body = serde_json::from_slice(&reply.body).unwrap();
    let reads = handler.migration_reads.load(Ordering::SeqCst);
    drop(daemon);
    serve.await.unwrap().unwrap();
    (body, reads)
}

#[tokio::test]
async fn cancel_unauthorized_precedes_migration_and_invalid_params() {
    for pending_release in [false, true] {
        let (body, reads) = cancel_admission_probe(false, pending_release, false).await;
        assert_eq!(body["code"], "scope_owner_mismatch", "{body}");
        assert_eq!(
            reads, 0,
            "an unauthorized caller must not reach the migration gate"
        );
    }
}

#[tokio::test]
async fn cancel_unreadable_ownership_precedes_migration_for_owner_and_non_owner() {
    for owner in [false, true] {
        for pending_release in [false, true] {
            let (body, reads) = cancel_admission_probe(true, pending_release, owner).await;
            assert_eq!(body["code"], "open_failed", "{body}");
            assert_eq!(
                reads, 0,
                "unreadable ownership must fail before the migration gate"
            );
        }
    }
}

#[tokio::test]
async fn cancel_authorized_owner_retains_moved_and_moving_fences() {
    for pending_release in [false, true] {
        let (body, reads) = cancel_admission_probe(false, pending_release, true).await;
        assert_eq!(
            body["code"],
            if pending_release {
                "session_moving"
            } else {
                "session_held_elsewhere"
            },
            "{body}"
        );
        assert_eq!(reads, 1);
    }
}
