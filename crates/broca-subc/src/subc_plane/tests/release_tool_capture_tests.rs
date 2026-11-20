use super::*;
use serde_json::json;

#[tokio::test]
async fn unavailable_capture_replaces_the_image_with_a_notice() {
    let data = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC";
    assert_eq!(data.len(), 92);
    for is_error in [false, true] {
        let mut ctx = batch_ctx("release");
        ctx.media_capture = None;
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
        let expected = if is_error {
            r#"{"tool_call_id":"release-call","output":{"kind":"error","message":"beforeafter\n[image/png not shown to the model: this runner does not pass tool images to models yet]"},"is_error":true}"#
        } else {
            r#"{"tool_call_id":"release-call","output":{"kind":"text","text":"beforeafter\n[image/png not shown to the model: this runner does not pass tool images to models yet]"},"is_error":false}"#
        };
        assert_eq!(serde_json::to_vec(&result).unwrap(), expected.as_bytes());
        assert!(ctx.media_capture.is_none());
    }
}
