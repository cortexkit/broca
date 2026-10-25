use super::*;
use base64::{engine::general_purpose::STANDARD, Engine};
use broca_core::{
    media_store::{MediaStore, MediaStoreClass},
    tool_media::ToolMediaCapture,
    wal_writer::WalStore,
};
use broca_protocol::{MediaKind, MediaRef, ToolOutputPart};
use serde_json::json;
use std::path::PathBuf;

fn png() -> Vec<u8> {
    let hex = "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de0000000c49444154789c63f8cfc0000003010100c9fe92ef0000000049454e44ae426082";
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

fn media_block(kind: &str, mime: &str, bytes: &[u8]) -> serde_json::Value {
    json!({"type": kind, "mimeType": mime, "data": STANDARD.encode(bytes)})
}

fn capture_ctx(batch: &str) -> (PathBuf, BatchCtx, MediaStore) {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let root = broca_test_support::scratch_root().join(format!(
        "tool-capture-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    let mut ctx = batch_ctx(batch);
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    drop(wal.open(&ctx.bind).unwrap());
    ctx.media_capture = Some(ToolMediaCapture::new(
        wal.uploads().unwrap(),
        ctx.bind.clone(),
    ));
    let store = MediaStore::open(&root, MediaStoreClass::NonDurable).unwrap();
    (root, ctx, store)
}

async fn dispatch_reply(
    ctx: &BatchCtx,
    raw: serde_json::Value,
) -> (
    SubcToolPlane,
    Arc<ScriptedToolPlaneConnection>,
    ToolCall,
    ToolResult,
) {
    let conn = ScriptedToolPlaneConnection::new(
        vec![vec![catalog_entry("aft", &["read"])]; 2],
        vec![Ok(raw)],
    );
    let plane = SubcToolPlane::with_connection(conn.clone(), "aft");
    let mut call = call("read");
    call.tool_call_id = ToolCallId("capture-call".into());
    let manifest = ToolManifest::from_tools(vec![request_tool("read", Some("aft"))]);
    plane
        .batch_started(ctx, std::slice::from_ref(&call), &manifest)
        .await
        .unwrap();
    let DispatchResult::Completed(result) = plane.dispatch(ctx, &call).await else {
        panic!("executed tool must complete")
    };
    (plane, conn, call, result)
}

fn parts(result: &ToolResult) -> &[ToolOutputPart] {
    let ToolOutput::Parts { parts } = &result.output else {
        panic!("capturable replies must use Parts: {result:?}")
    };
    parts
}

fn reference(part: &ToolOutputPart) -> &MediaRef {
    let ToolOutputPart::Media { media } = part else {
        panic!("expected captured reference, got {part:?}")
    };
    media
}

fn assert_round_trip(result: &ToolResult) {
    assert_eq!(result.tool_call_id.0, "capture-call");
    let encoded = serde_json::to_vec(result).unwrap();
    assert_eq!(
        serde_json::from_slice::<ToolResult>(&encoded).unwrap(),
        *result
    );
    assert!(!String::from_utf8(encoded).unwrap().contains("data\""));
}

#[tokio::test]
async fn text_image_text_captures_before_fencing_and_repeat_publishes_nothing() {
    let (root, ctx, store) = capture_ctx("mixed");
    let raw = json!({"content": [{"type":"text","text":"before"}, media_block("image", "image/jpeg", &png()), {"type":"text","text":"after"}]});
    let (plane, conn, call, result) = dispatch_reply(&ctx, raw).await;
    assert_round_trip(&result);
    assert!(!result.is_error);
    let parts = parts(&result);
    assert_eq!(parts.len(), 3);
    assert_eq!(
        parts[0],
        ToolOutputPart::Text {
            text: "before".into()
        }
    );
    assert_eq!(
        parts[2],
        ToolOutputPart::Text {
            text: "after".into()
        }
    );
    let media = reference(&parts[1]);
    assert_eq!(media.mime, "image/png");
    assert_eq!(
        media.sha256,
        "b1ff9c8ea3a780bad09b346c423d2d0e46815926879b18e841d928376a946640"
    );
    assert_eq!((media.width, media.height), (Some(1), Some(1)));
    assert_eq!(store.read_verified(media).unwrap(), png());
    let manifest = root
        .join("media/refs")
        .join(broca_core::wal_writer::session_addr(&ctx.bind));
    assert_eq!(
        std::fs::read_to_string(&manifest).unwrap(),
        format!("{}\n", media.sha256)
    );
    // Removing the publication directory makes an accidental second capture observable.
    let before = std::fs::read(&manifest).unwrap();
    std::fs::rename(root.join("media/objects"), root.join("published")).unwrap();
    let DispatchResult::Completed(repeated) = plane.dispatch(&ctx, &call).await else {
        panic!("fenced repeat must complete")
    };
    assert_eq!(repeated, result);
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(std::fs::read(manifest).unwrap(), before);
    assert!(!root.join("media/objects").exists());
}

#[tokio::test]
async fn multiple_media_and_error_preserve_identity_order_and_status() {
    let (_root, ctx, store) = capture_ctx("multiple");
    let audio = b"RIFF\x04\0\0\0WAVE";
    let pdf = b"%PDF-1.7\nfixture\n%%EOF";
    let raw = json!({"isError":true,"content":[
        media_block("image", "image/png", &png()),
        {"type":"resource","resource":{"uri":"file:///note","text":"between"}},
        media_block("audio", "audio/wav", audio),
        {"type":"resource","resource":{"uri":"file:///doc.pdf","mimeType":"application/pdf","blob":STANDARD.encode(pdf)}},
        {"type":"resource_link","uri":"https://example.com/image.png","mimeType":"image/png"},
        {"type":"future_block"}
    ]});
    let (_, _, _, result) = dispatch_reply(&ctx, raw).await;
    assert_round_trip(&result);
    assert!(result.is_error);
    let parts = parts(&result);
    assert_eq!(parts.len(), 6);
    assert_eq!(reference(&parts[0]).kind, MediaKind::Image);
    assert_eq!(
        parts[1],
        ToolOutputPart::Text {
            text: "between".into()
        }
    );
    assert_eq!(reference(&parts[2]).kind, MediaKind::Audio);
    assert_eq!(store.read_verified(reference(&parts[2])).unwrap(), audio);
    assert_eq!(reference(&parts[3]).kind, MediaKind::Document);
    assert_eq!(reference(&parts[3]).mime, "application/pdf");
    assert_eq!(store.read_verified(reference(&parts[3])).unwrap(), pdf);
    assert_eq!(
        parts[4],
        ToolOutputPart::Text {
            text: "[non-text tool content omitted: type=resource_link mimeType=image/png]".into()
        }
    );
    assert_eq!(
        parts[5],
        ToolOutputPart::Text {
            text: "[non-text tool content omitted: type=future_block]".into()
        }
    );
}

#[tokio::test]
async fn failed_publication_and_registration_are_completed_marker_parts() {
    for registration in [false, true] {
        let (root, ctx, _) = capture_ctx("failed");
        if registration {
            let manifest = root
                .join("media/refs")
                .join(broca_core::wal_writer::session_addr(&ctx.bind));
            std::fs::create_dir(manifest).unwrap();
        } else {
            std::fs::remove_dir(root.join("media/objects")).unwrap();
            std::fs::write(root.join("media/objects"), b"not a directory").unwrap();
        }
        let (_, _, _, result) = dispatch_reply(
            &ctx,
            json!({"content":[media_block("image", "image/png", &png())]}),
        )
        .await;
        assert_round_trip(&result);
        assert!(!result.is_error);
        assert!(
            matches!(&parts(&result)[0], ToolOutputPart::Text { text } if text.ends_with("reason=store_failed]"))
        );
    }
}

#[tokio::test]
async fn marker_only_parts_validate_before_publication() {
    let (root, ctx, _) = capture_ctx("invalid");
    let mut truncated = png();
    truncated.truncate(40);
    let raw = json!({"content":[
        media_block("image", "image/png", &truncated),
        {"type":"audio","mimeType":"audio/wav","data":"invalid-base64!"},
        media_block("image", "image/png", b"not media")
    ]});
    let (_, _, _, result) = dispatch_reply(&ctx, raw).await;
    let parts = parts(&result);
    assert_eq!(parts.len(), 3);
    for (part, reason) in parts.iter().zip(["undecodable", "undecodable", "mime"]) {
        assert!(
            matches!(part, ToolOutputPart::Text { text } if text.ends_with(&format!("reason={reason}]")))
        );
    }
    assert_eq!(
        std::fs::read_dir(root.join("media/objects"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(
        std::fs::read_dir(root.join("media/refs")).unwrap().count(),
        0
    );
}

#[tokio::test]
async fn over_profile_item_limit_but_under_store_maximum_is_captured() {
    let (_root, ctx, store) = capture_ctx("large");
    let mut pdf = b"%PDF-1.7\n".to_vec();
    pdf.resize(4_500_001, b' ');
    pdf.extend_from_slice(b"%%EOF");
    let raw = json!({"content":[{"type":"resource","resource":{"uri":"file:///large.pdf","mimeType":"application/pdf","blob":STANDARD.encode(&pdf)}}]});
    let (_, _, _, result) = dispatch_reply(&ctx, raw).await;
    let media = reference(&parts(&result)[0]);
    let profile = broca_provider::media_profile::select_media_profile(
        broca_provider::spec::WireFamilyId::BedrockConverse,
        None,
        "anthropic.claude",
        Default::default(),
        None,
    );
    assert!(!profile
        .kind(MediaKind::Document)
        .max_item_bytes
        .unwrap()
        .admits(media.byte_size));
    assert!(media.byte_size < broca_core::media_validation::store_max_bytes(MediaKind::Document));
    assert_eq!(store.read_verified(media).unwrap(), pdf);
}

#[tokio::test]
async fn late_context_captures_through_the_same_handle() {
    let (_root, mut ctx, store) = capture_ctx("late:approval-key");
    ctx.batch_id = BatchId("late:approval-key".into());
    let (_, _, _, result) = dispatch_reply(
        &ctx,
        json!({"content":[media_block("image", "image/png", &png())]}),
    )
    .await;
    assert_eq!(
        store.read_verified(reference(&parts(&result)[0])).unwrap(),
        png()
    );
}

#[tokio::test]
async fn reference_only_reply_is_a_provenance_marker_even_when_object_exists() {
    let (_root, ctx, store) = capture_ctx("forged");
    let reference = ctx
        .media_capture
        .as_ref()
        .unwrap()
        .capture(
            &ctx.batch_id,
            &ToolCallId("another-call".into()),
            png(),
            "image/png".into(),
        )
        .await
        .unwrap();
    assert_eq!(store.read_verified(&reference).unwrap(), png());
    let mut block = serde_json::to_value(&reference).unwrap();
    block["type"] = json!("media");
    let (_, _, _, result) = dispatch_reply(&ctx, json!({"content":[block]})).await;
    assert_eq!(
        result.output,
        ToolOutput::Text {
            text: broca_protocol::content::media_not_shown_marker(
                &reference,
                broca_protocol::MediaMarkerReason::Provenance
            )
        }
    );
}

#[tokio::test]
async fn negotiated_image_slots_capture_without_losing_error_code_or_text() {
    let (_root, ctx, store) = capture_ctx("negotiated");
    let raw = json!({"content":[{"type":"text","text":"before"}, media_block("image", "image/png", &png()), {"type":"text","text":"after"}], "isError":true, "errorCode":"provider_error"});
    let mut raw = raw;
    raw["content"][1]["width"] = json!(1);
    raw["content"][1]["height"] = json!(1);
    let decoded = serde_json::from_value(raw.clone()).unwrap();
    let mut result =
        crate::content_v1::map_content_v1_result(&ToolCallId("capture-call".into()), &decoded)
            .unwrap();
    let captures = tool_capture::content_v1_captures(&mut result, &raw);
    let result = tool_capture::capture(&ctx, result, captures).await;
    assert_round_trip(&result);
    assert!(result.is_error);
    assert_eq!(
        parts(&result)[0],
        ToolOutputPart::Text {
            text: "[tool error: provider_error]".into()
        }
    );
    assert_eq!(
        parts(&result)[1],
        ToolOutputPart::Text {
            text: "before".into()
        }
    );
    assert_eq!(
        store.read_verified(reference(&parts(&result)[2])).unwrap(),
        png()
    );
    assert_eq!(
        parts(&result)[3],
        ToolOutputPart::Text {
            text: "after".into()
        }
    );
}

#[tokio::test]
async fn missing_capture_handle_is_a_completed_store_failed_marker() {
    let ctx = batch_ctx("no-store");
    let (_, _, _, result) = dispatch_reply(
        &ctx,
        json!({"content":[media_block("image", "image/jpeg", &png())]}),
    )
    .await;
    assert_eq!(parts(&result)[0], ToolOutputPart::Text { text: "[media not shown: kind=image mime=image/png bytes=69 sha256=b1ff9c8ea3a7 reason=store_failed]".into() });
}

#[tokio::test]
async fn over_store_maximum_is_a_marker_and_publishes_nothing() {
    let (root, ctx, _) = capture_ctx("too-large");
    let mut bytes = png();
    bytes.resize(33_554_433, 0);
    let (_, _, _, result) = dispatch_reply(
        &ctx,
        json!({"content":[media_block("image", "image/png", &bytes)]}),
    )
    .await;
    assert!(
        matches!(&parts(&result)[0], ToolOutputPart::Text { text } if text.ends_with("reason=too_large]"))
    );
    assert_eq!(
        std::fs::read_dir(root.join("media/objects"))
            .unwrap()
            .count(),
        0
    );
    assert_eq!(
        std::fs::read_dir(root.join("media/refs")).unwrap().count(),
        0
    );
}
