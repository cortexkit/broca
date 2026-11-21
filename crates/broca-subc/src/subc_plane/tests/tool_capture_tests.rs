use super::*;
use base64::{engine::general_purpose::STANDARD, Engine};
use broca_core::tool_media::MediaCapture;
use broca_protocol::{MediaKind, MediaRef, ToolOutputPart};
use broca_session_loop::PendingMedia;
use serde_json::json;
use sha2::{Digest, Sha256};

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

/// A batch whose tool media the run loop's own capture handle holds, as a run does until the
/// call's reply is appended.
fn capture_ctx(batch: &str) -> (BatchCtx, Arc<PendingMedia>) {
    let mut ctx = batch_ctx(batch);
    let media = Arc::new(PendingMedia::default());
    ctx.media_capture = Some(media.clone() as Arc<dyn MediaCapture>);
    (ctx, media)
}

/// `reference` names exactly `bytes`: the capture handle computed its digest and size from the
/// bytes it validated and holds.
fn names(reference: &MediaRef, bytes: &[u8]) -> bool {
    reference.sha256 == format!("{:x}", Sha256::digest(bytes))
        && reference.byte_size == bytes.len() as u64
}

/// What the handle holds for the dispatched call.
fn held(ctx: &BatchCtx, media: &PendingMedia) -> Vec<MediaRef> {
    media.captured_for(&ctx.batch_id, &ToolCallId("capture-call".into()))
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
    let (ctx, held_media) = capture_ctx("mixed");
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
    assert!(names(media, &png()));
    assert_eq!(held(&ctx, &held_media), vec![media.clone()]);
    // A repeat of a call already answered gets the recorded answer: no second tool call, no
    // second capture.
    let DispatchResult::Completed(repeated) = plane.dispatch(&ctx, &call).await else {
        panic!("fenced repeat must complete")
    };
    assert_eq!(repeated, result);
    assert_eq!(conn.tool_calls.load(Ordering::SeqCst), 1);
    assert_eq!(held(&ctx, &held_media), vec![media.clone()]);
}

#[tokio::test]
async fn multiple_media_and_error_preserve_identity_order_and_status() {
    let (ctx, _) = capture_ctx("multiple");
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
    assert!(names(reference(&parts[2]), audio));
    assert_eq!(reference(&parts[3]).kind, MediaKind::Document);
    assert_eq!(reference(&parts[3]).mime, "application/pdf");
    assert!(names(reference(&parts[3]), pdf));
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
async fn marker_only_parts_validate_before_publication() {
    let (ctx, held_media) = capture_ctx("invalid");
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
    assert!(held(&ctx, &held_media).is_empty());
}

#[tokio::test]
async fn late_context_captures_through_the_same_handle() {
    let (mut ctx, _) = capture_ctx("late:approval-key");
    ctx.batch_id = BatchId("late:approval-key".into());
    let (_, _, _, result) = dispatch_reply(
        &ctx,
        json!({"content":[media_block("image", "image/png", &png())]}),
    )
    .await;
    assert!(names(reference(&parts(&result)[0]), &png()));
}

#[tokio::test]
async fn reference_only_reply_is_a_provenance_marker_even_when_object_exists() {
    let (ctx, _) = capture_ctx("forged");
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
    assert!(names(&reference, &png()));
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
            .into()
        }
    );
}

#[tokio::test]
async fn negotiated_image_slots_capture_with_legacy_error_text() {
    let (ctx, _) = capture_ctx("negotiated");
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
            text: "before".into()
        }
    );
    assert!(names(reference(&parts(&result)[1]), &png()));
    assert_eq!(
        parts(&result)[2],
        ToolOutputPart::Text {
            text: "after".into()
        }
    );
}

#[tokio::test]
async fn missing_capture_handle_marks_an_explicit_capture_request_store_failed() {
    let ctx = batch_ctx("no-store");
    let raw = json!({"content":[media_block("image", "image/jpeg", &png())]});
    let (result, captures) = map_call_tool_result(&ToolCallId("capture-call".into()), &raw);
    let result = tool_capture::capture(&ctx, result, captures).await;
    assert_eq!(parts(&result)[0], ToolOutputPart::Text { text: "[media not shown: kind=image mime=image/png bytes=69 sha256=b1ff9c8ea3a7 reason=store_failed]".into() });
}

/// Capture a reply's media straight from its mapping, past intake. Intake discards a reply this
/// large before capture (over 64 KiB counted on a legacy route, over 3 MiB serialized on a
/// `tool-result/v1` one), so capture's own limits are only reachable this way, and they still
/// guard every other path into it.
async fn capture_past_intake(ctx: &BatchCtx, raw: serde_json::Value) -> ToolResult {
    let (result, captures) = map_call_tool_result(&ToolCallId("capture-call".into()), &raw);
    tool_capture::capture(ctx, result, captures).await
}

#[tokio::test]
async fn over_profile_item_limit_but_under_store_maximum_is_captured() {
    let (ctx, _) = capture_ctx("large");
    let mut pdf = b"%PDF-1.7\n".to_vec();
    pdf.resize(4_500_001, b' ');
    pdf.extend_from_slice(b"%%EOF");
    let raw = json!({"content":[{"type":"resource","resource":{"uri":"file:///large.pdf","mimeType":"application/pdf","blob":STANDARD.encode(&pdf)}}]});
    let result = capture_past_intake(&ctx, raw).await;
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
    assert!(names(media, &pdf));
}

#[tokio::test]
async fn over_store_maximum_is_a_marker_and_publishes_nothing() {
    let (ctx, held_media) = capture_ctx("too-large");
    let mut bytes = png();
    bytes.resize(33_554_433, 0);
    let result = capture_past_intake(
        &ctx,
        json!({"content":[media_block("image", "image/png", &bytes)]}),
    )
    .await;
    assert!(
        matches!(&parts(&result)[0], ToolOutputPart::Text { text } if text.ends_with("reason=too_large]"))
    );
    assert!(held(&ctx, &held_media).is_empty());
}

/// A reply on a route that doesn't read `tool-result/v1` counts its whole JSON body, media data
/// included, toward the 64 KiB limit (the tool-provider contract's legacy rule). Over it, the
/// whole output is discarded at intake, before any media is captured.
#[tokio::test]
async fn an_oversized_legacy_reply_is_discarded_before_capture() {
    let (ctx, held_media) = capture_ctx("legacy-too-large");
    let mut bytes = png();
    bytes.resize(70_000, 0);
    let (_, _, _, result) = dispatch_reply(
        &ctx,
        json!({"content":[media_block("image", "image/png", &bytes)]}),
    )
    .await;
    assert!(result.is_error);
    let ToolOutput::Error { code, .. } = &result.output else {
        panic!("an oversized legacy reply must be a size error: {result:?}")
    };
    assert_eq!(code.as_deref(), Some("tool_result_too_large"));
    assert!(held(&ctx, &held_media).is_empty());
}
