use super::*;
use broca_core::{
    lease::lease_key_for, wal_writer::WalStore, FileLeaseStore, FileWalStore, LeaseStore, WalHandle,
};
use broca_wal::{DispatchOutcome, WalRecord};
use serde_json::json;

#[tokio::test]
async fn capture_off_image_result_records_notice_and_writes_no_media() {
    assert!(!broca_protocol::attachments_enabled());
    let data = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC";
    assert_eq!(data.len(), 92);
    for is_error in [false, true] {
        let root = broca_test_support::scratch_root().join(format!(
            "tool-capture-off-{}-{is_error}",
            std::process::id()
        ));
        let mut ctx = batch_ctx("release");
        let store = Arc::new(FileWalStore::new(root.join("wal")));
        let lease = FileLeaseStore::new(root.join("leases"))
            .acquire(&lease_key_for(&ctx.bind))
            .unwrap();
        let (writer, lease) =
            WalHandle::open_and_spawn_observed(store.clone(), ctx.bind.clone(), lease, None)
                .await
                .unwrap();
        ctx.media_capture = writer.media_capture();
        let raw = json!({"content":[
            {"type":"text","text":"before"},
            {"type":"image","mimeType":"image/png","data":data},
            {"type":"text","text":"after"}
        ],"isError":is_error});
        let conn = ScriptedToolPlaneConnection::new(
            vec![vec![catalog_entry("aft", &["read"])]; 2],
            vec![Ok(raw)],
        );
        let plane = SubcToolPlane::with_connection(conn, "aft");
        let mut call = call("read");
        call.tool_call_id = ToolCallId("release-call".into());
        let manifest = ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]);
        plane
            .batch_started(&ctx, std::slice::from_ref(&call), &manifest)
            .await
            .unwrap();
        let DispatchResult::Completed(result) = plane.dispatch(&ctx, &call).await else {
            panic!("tool must complete")
        };
        writer
            .record(WalRecord::ToolResult {
                finished_at_ms: None,
                tags: None,
                batch_id: ctx.batch_id.clone(),
                result: Arc::new(result.clone()),
                hooks: None,
                reason: None,
                outcome: DispatchOutcome::Completed,
                withdraw_elicitation: None,
                provider_held: None,
            })
            .into_result()
            .unwrap();
        writer.sync().await.unwrap();

        for directory in ["media", "media-durable", "media-ephemeral", "media-uploads"] {
            assert!(
                !root.join(directory).exists(),
                "capture-off tool created {directory}"
            );
        }
        let expected = if is_error {
            r#"{"tool_call_id":"release-call","output":{"kind":"error","message":"beforeafter\n[image/png not shown to the model: this runner does not pass tool images to models yet]"},"is_error":true}"#
        } else {
            r#"{"tool_call_id":"release-call","output":{"kind":"text","text":"beforeafter\n[image/png not shown to the model: this runner does not pass tool images to models yet]"},"is_error":false}"#
        };
        assert_eq!(serde_json::to_vec(&result).unwrap(), expected.as_bytes());
        let frames = store.read_records(&ctx.bind).unwrap();
        assert_eq!(frames.len(), 1);
        assert!(!frames[0]
            .record
            .required_features()
            .contains(&"media-content/v1"));
        let WalRecord::ToolResult { result: stored, .. } = &frames[0].record else {
            panic!("expected stored tool result")
        };
        assert_eq!(serde_json::to_vec(stored).unwrap(), expected.as_bytes());
        let wal_bytes = std::fs::read(store.wal_path(&ctx.bind)).unwrap();
        assert!(!wal_bytes
            .windows(b"media-content/v1".len())
            .any(|window| window == b"media-content/v1"));
        assert!(writer.media_capture().is_none());
        writer.shutdown().await.unwrap();
        drop(lease);
    }
}
