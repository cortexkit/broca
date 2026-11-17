use super::*;

#[tokio::test]
async fn open_failed_serializes_each_shared_class() {
    for (class, spelling) in [
        (broca_protocol::ErrorClass::Transient, "transient"),
        (broca_protocol::ErrorClass::Permanent, "permanent"),
        (broca_protocol::ErrorClass::Auth, "auth_required"),
        (
            broca_protocol::ErrorClass::ContextOverflow,
            "context_overflow",
        ),
    ] {
        let (tx, mut rx) = broca_diagnostics::channel(1, Arc::default());
        send_serve_error(
            &tx,
            1,
            7,
            1,
            2,
            &ServeError::Open("known cause".into(), Some(class)),
        )
        .await
        .unwrap();
        let frame = rx.recv().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&frame.body).unwrap();
        assert_eq!(body["detail"]["class"], spelling);
        assert_eq!(body["message"], "open failed: known cause");
    }
}
