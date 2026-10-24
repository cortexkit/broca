use super::{check_supplied_media, check_upload_references};
use serde_json::json;

fn image() -> serde_json::Value {
    json!({
        "kind": "image",
        "mime": "image/png",
        "byte_size": 67,
        "sha256": "ab".repeat(32),
        "width": 1,
        "height": 1
    })
}

fn media_block() -> serde_json::Value {
    let mut block = image();
    block["type"] = "media".into();
    block
}

fn messages(value: serde_json::Value) -> Vec<broca_protocol::Message> {
    serde_json::from_value(value).expect("wire messages decode")
}

#[test]
fn an_imported_media_block_is_refused_naming_its_member() {
    let refusal = check_supplied_media(&messages(json!([
        {"role": "user", "content": [{"type": "text", "text": "hi"}]},
        {"role": "user", "content": [media_block()]}
    ])))
    .expect_err("a media block cannot be supplied");
    assert_eq!(refusal.0, "messages[1].content[0]");
    assert!(refusal.1.contains("upload references"), "{}", refusal.1);
}

#[test]
fn an_imported_tool_result_media_part_is_refused_naming_its_part() {
    let refusal = check_supplied_media(&messages(json!([
        {"role": "tool", "content": [{
            "type": "tool_result", "tool_call_id": "c",
            "output": {"kind": "parts", "parts": [
                {"kind": "text", "text": "a"},
                {"kind": "text", "text": "b"},
                {"kind": "media", "media": image()}
            ]}
        }]}
    ])))
    .expect_err("a media part cannot be supplied");
    assert_eq!(refusal.0, "messages[0].content[0].output.parts[2]");
}

#[test]
fn an_import_without_media_passes() {
    assert_eq!(
        check_supplied_media(&messages(json!([
            {"role": "user", "content": [{"type": "text", "text": "hi"}]},
            {"role": "tool", "content": [{
                "type": "tool_result", "tool_call_id": "c",
                "output": {"kind": "parts", "parts": [{"kind": "text", "text": "a"}]}
            }]}
        ]))),
        Ok(())
    );
}

#[test]
fn a_send_naming_media_inline_is_refused_with_the_upload_rule() {
    let block = check_upload_references(&json!({"content": [media_block()]}))
        .expect_err("a media block is not an upload reference");
    assert!(
        block.starts_with("content[0] names media inline"),
        "{block}"
    );
    assert!(block.contains("upload references"), "{block}");

    let part = check_upload_references(&json!({"content": [
        {"type": "upload", "upload_id": "u-1"},
        {"type": "tool_result", "tool_call_id": "c",
         "output": {"kind": "parts", "parts": [{"kind": "media", "media": image()}]}}
    ]}))
    .expect_err("a media part is not an upload reference");
    assert!(part.starts_with("content[1] names media inline"), "{part}");
}

#[test]
fn a_send_with_only_upload_references_keeps_its_existing_refusal() {
    let refusal = check_upload_references(&json!({"content": [
        {"type": "upload", "upload_id": "u-1"}
    ]}))
    .expect_err("attachment admission is not wired yet");
    assert!(refusal.contains("upload u-1"), "{refusal}");
}
