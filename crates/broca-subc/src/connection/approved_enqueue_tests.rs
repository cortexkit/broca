use super::*;

#[tokio::test]
async fn approved_push_rechecks_cancel_and_stop_after_capacity_is_reserved() {
    for cancel in [true, false] {
        let (tx, mut rx) = mpsc::channel(1);
        let token = CancellationToken::new();
        let signals = broca_core::tool_plane::DispatchSignals::new(
            if cancel { Some(&token) } else { None },
            if cancel { None } else { Some(&token) },
        );
        let permit = tx.reserve().await.unwrap();
        token.cancel();
        let frame =
            Frame::build(FrameType::Request, data_flags(), 5, 3, 1, br#"{}"#.to_vec()).unwrap();
        assert!(matches!(
            SubcConnection::push_approved(permit, frame, &signals),
            Err(ApprovedSetupError::Interrupted)
        ));
        assert!(rx.try_recv().is_err());
    }
}
