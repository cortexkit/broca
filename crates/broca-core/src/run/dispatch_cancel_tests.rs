//! The plain dispatcher refuses a whole batch when the caller's immediate cancellation is
//! already set as the batch begins: nothing reaches the tool plane and no dispatch intent is
//! written, so no call of the batch can be reported as possibly applied.

use super::*;

#[tokio::test]
async fn a_cancel_before_the_batch_starts_opens_no_route_and_writes_no_intent() {
    let (writer, records) = writer();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let control = RunControl {
        cancel: Some(cancel),
        ..Default::default()
    };
    let plane = crate::StubToolPlane::new().with_canned_text("edit", "ok");
    let bind = BindIdentity::new("/project", "caller", "cancelled-batch");
    let (out, _rx) = mpsc::channel(64);
    let outcome = dispatch_batch(
        &RunId("r".into()),
        StepId(1),
        &bind,
        None,
        vec![call("a"), call("b")],
        &ToolManifest::from_tools(vec![tool()]),
        &plane,
        &crate::AllowAll,
        &writer,
        None,
        &[],
        false,
        None,
        None,
        None,
        &out,
        Some(&control),
    )
    .await
    .unwrap();
    assert_eq!(outcome.interruption, Some(BatchInterruption::CallerCancel));
    assert!(
        plane.log().is_empty(),
        "the tool plane was told a cancelled batch started"
    );
    let records = records.lock().unwrap().clone();
    assert!(
        !records
            .iter()
            .any(|record| matches!(record, WalRecord::ToolDispatchIntent { .. })),
        "a cancelled batch wrote a dispatch intent"
    );
    for id in ["a", "b"] {
        assert!(records.iter().any(|record| matches!(record,
            WalRecord::ToolResult { result, outcome: DispatchOutcome::Interrupted, .. }
                if result.tool_call_id.0 == id)));
    }
    let _ = writer.shutdown().await;
}
