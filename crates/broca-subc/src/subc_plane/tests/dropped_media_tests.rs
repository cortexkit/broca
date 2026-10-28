use super::*;
use broca_core::{
    lease::lease_key_for, wal_writer::WalStore, FileLeaseStore, FileWalStore, LeaseStore, WalHandle,
};
use broca_protocol::{ToolOutput, ToolOutputPart, ToolResult};
use broca_wal::{DispatchOutcome, WalRecord};

const PNG: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC";
const GIF: &str = "R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAICRAEAOw==";
const PNG_NOTICE: &str =
    "[image/png not shown to the model: this runner does not pass tool images to models yet]";
const GIF_NOTICE: &str =
    "[image/gif not shown to the model: this runner does not pass tool images to models yet]";

fn image(mime: &str, data: &str) -> serde_json::Value {
    json!({"type":"image", "mimeType":mime, "data":data, "width":1, "height":1})
}

fn result_record(ctx: &BatchCtx, result: ToolResult) -> WalRecord {
    WalRecord::ToolResult {
        finished_at_ms: None,
        tags: None,
        batch_id: ctx.batch_id.clone(),
        result: Arc::new(result),
        hooks: None,
        reason: None,
        outcome: DispatchOutcome::Completed,
        withdraw_elicitation: None,
        provider_held: None,
    }
}

async fn record_reply(raw: serde_json::Value, negotiated: bool) -> ToolResult {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let root = broca_test_support::scratch_root().join(format!(
        "dropped-media-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let mut ctx = ctx();
    let store = Arc::new(FileWalStore::new(root.join("wal")));
    let lease = FileLeaseStore::new(root.join("leases"))
        .acquire(&lease_key_for(&ctx.bind))
        .unwrap();
    let (writer, lease) =
        WalHandle::open_and_spawn_observed(store.clone(), ctx.bind.clone(), lease, None)
            .await
            .unwrap();
    ctx.media_capture = writer.media_capture();
    assert_eq!(
        ctx.media_capture.is_some(),
        cfg!(feature = "test-attachments")
    );

    let rig = Rig::new(true, true, negotiated, false);
    *rig.reply.lock().unwrap() = raw;
    let plane = rig.plane(true);
    let call = call("media-notice-call");
    plane
        .batch_started(
            &ctx,
            std::slice::from_ref(&call),
            &manifest_declaring(negotiated, false),
        )
        .await
        .unwrap();
    let broca_core::tool_plane::DispatchResult::Completed(result) =
        plane.dispatch(&ctx, &call).await
    else {
        panic!("tool must complete")
    };
    writer
        .record(result_record(&ctx, result.clone()))
        .into_result()
        .unwrap();
    writer.sync().await.unwrap();
    writer.shutdown().await.unwrap();
    drop(lease);

    let frames = store.read_records(&ctx.bind).unwrap();
    assert_eq!(frames.len(), 1);
    let WalRecord::ToolResult { result: stored, .. } = &frames[0].record else {
        panic!("expected recorded tool result")
    };
    assert_eq!(
        serde_json::to_vec(stored).unwrap(),
        serde_json::to_vec(&result).unwrap()
    );
    assert_eq!(
        crate::content_v1::tests::render_model_request((**stored).clone()),
        crate::content_v1::tests::render_model_request(result.clone())
    );
    if !cfg!(feature = "test-attachments") {
        for directory in ["media", "media-durable", "media-ephemeral", "media-uploads"] {
            assert!(
                !root.join(directory).exists(),
                "capture-off tool created {directory}"
            );
        }
    }
    result
}

#[tokio::test]
async fn dropped_tool_media_is_recorded_or_captured_at_dispatch() {
    let cases = [
        (
            json!([{"type":"text","text":"Read image attachment (image/png)."}, image("image/png", PNG)]),
            format!("Read image attachment (image/png).\n{PNG_NOTICE}"),
            vec!["image/png"],
        ),
        (
            json!([image("image/png", PNG)]),
            PNG_NOTICE.into(),
            vec!["image/png"],
        ),
        (
            json!([image("image/png", PNG), image("image/gif", GIF)]),
            format!("{PNG_NOTICE}\n{GIF_NOTICE}"),
            vec!["image/png", "image/gif"],
        ),
    ];
    for negotiated in [false, true] {
        for (content, expected, mimes) in &cases {
            let result = record_reply(json!({"content":content}), negotiated).await;
            assert!(!result.is_error);
            if cfg!(feature = "test-attachments") {
                let ToolOutput::Parts { parts } = &result.output else {
                    panic!("images must be captured")
                };
                let actual: Vec<_> = parts
                    .iter()
                    .filter_map(|part| match part {
                        ToolOutputPart::Media { media } => Some(media.mime.as_str()),
                        _ => None,
                    })
                    .collect();
                assert_eq!(&actual, mimes);
                let encoded = serde_json::to_string(&result).unwrap();
                assert!(
                    !encoded.contains("not shown"),
                    "captured media must not have a drop notice"
                );
            } else {
                let text = match &result.output {
                    ToolOutput::Text { text } => text.clone(),
                    ToolOutput::Parts { parts } => parts
                        .iter()
                        .map(|part| match part {
                            ToolOutputPart::Text { text } => text.as_str(),
                            _ => panic!("capture-off result must be text only"),
                        })
                        .collect::<Vec<_>>()
                        .join("\n"),
                    other => panic!("unexpected result: {other:?}"),
                };
                assert_eq!(&text, expected, "negotiated={negotiated}");
            }
        }
    }
}

#[tokio::test]
async fn no_media_result_matches_pinned_pre_notice_bytes() {
    for (negotiated, fixture) in [
        (
            false,
            include_str!("../../../tests/fixtures/content_v1/stored_legacy_text.json"),
        ),
        (
            true,
            include_str!("../../../tests/fixtures/content_v1/stored_v1_text.json"),
        ),
    ] {
        let result = record_reply(
            json!({"content":[
                {"type":"text","text":"Read text: "}, {"type":"text","text":"12:00\n"}
            ]}),
            negotiated,
        )
        .await;
        assert_eq!(
            serde_json::to_vec(&result).unwrap(),
            fixture.trim_end().as_bytes()
        );
    }
}

#[test]
fn historical_image_result_is_not_rewritten_on_read_or_render() {
    let fixture =
        include_str!("../../../tests/fixtures/content_v1/stored_old_image.json").trim_end();
    let result: ToolResult = serde_json::from_str(fixture).unwrap();
    assert_eq!(serde_json::to_vec(&result).unwrap(), fixture.as_bytes());
    let root =
        broca_test_support::scratch_root().join(format!("historical-media-{}", std::process::id()));
    let store = FileWalStore::new(root.join("wal"));
    let ctx = ctx();
    {
        let mut wal = store.open(&ctx.bind).unwrap();
        wal.append(1, &result_record(&ctx, result), true).unwrap();
    }
    let before = std::fs::read(store.wal_path(&ctx.bind)).unwrap();
    let frames = store.read_records(&ctx.bind).unwrap();
    let WalRecord::ToolResult { result, .. } = &frames[0].record else {
        panic!("expected historical tool result")
    };
    assert_eq!(serde_json::to_vec(result).unwrap(), fixture.as_bytes());
    let rendered = crate::content_v1::tests::render_model_request((**result).clone());
    let rendered = String::from_utf8(rendered).unwrap();
    assert!(rendered
        .contains("[non-text tool content omitted: type=image mimeType=image/png data≈92 bytes]"));
    assert!(!rendered.contains("this runner does not pass tool images"));
    assert_eq!(std::fs::read(store.wal_path(&ctx.bind)).unwrap(), before);
}

#[test]
fn legacy_dropped_blobs_name_each_mime_without_an_extra_blank_line() {
    let raw = json!({"content":[
        {"type":"text","text":"done\n"},
        {"type":"audio","mimeType":"audio/wav","data":"AA=="},
        {"type":"resource","resource":{"uri":"file:///note.pdf","mimeType":"application/pdf","blob":"AA=="}},
        {"type":"audio","mimeType":"audio/wav","data":"AA=="}
    ]});
    let result = map_legacy_call_tool_result(&ToolCallId("call".into()), &raw);
    assert_eq!(result.output, ToolOutput::Text {
        text: concat!(
            "done\n",
            "[audio/wav not shown to the model: this runner does not pass tool images to models yet]\n",
            "[application/pdf not shown to the model: this runner does not pass tool images to models yet]\n",
            "[audio/wav not shown to the model: this runner does not pass tool images to models yet]"
        ).into()
    });
}
