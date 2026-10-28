//! Reader samples through the production serve/core/WAL path with pinned replies shaped like
//! the AFT tool provider's encoder.
//! The daemon test separately covers route negotiation and the external tool transport.

mod delivery_rig;

use broca_core::DispatchResult;
use broca_protocol::{ToolCallId, ToolOutput, ToolResult};
use delivery_rig::{result, Rig, Step};
use serde_json::json;
use sha2::{Digest, Sha256};

const PNG: &str =
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAIAAACQd1PeAAAADElEQVR4nGP4z8AAAAMBAQDJ/pLvAAAAAElFTkSuQmCC";
const IMAGE_TEXT: &str = "Read image attachment (image/png, 1×1, 69 B).";

async fn sample(v1: bool) -> Vec<u8> {
    let mut rig = Rig::start(if v1 {
        "tool-result-after"
    } else {
        "tool-result-before"
    })
    .await;
    let fixtures = [
        ("read-text", "Read text: 12:00\n", false),
        ("read-image", IMAGE_TEXT, false),
        ("read-error", "File not found: missing.txt", true),
    ];
    for (id, text, error) in fixtures {
        let tool_call_id = ToolCallId(id.into());
        let result = if v1 {
            let mut raw = json!({"content":[{"type":"text","text":text}],"isError":error,
                "structuredContent":{"id":id,"success":!error,"text":text,"complete":true,"truncated":false}});
            if id == "read-image" {
                raw["structuredContent"]["attachments"] = json!([{"kind":"image","mime":"image/png","data":PNG,"width":1,"height":1,"alt":"one pixel"}]);
                raw["content"].as_array_mut().unwrap().push(json!({"type":"image","mimeType":"image/png","data":PNG,"width":1,"height":1,"alt":"one pixel"}));
            }
            if error {
                raw["errorCode"] = json!("not_found");
            }
            broca_subc::content_v1::map_content_v1_result(
                &tool_call_id,
                &serde_json::from_value(raw).unwrap(),
            )
            .unwrap()
        } else {
            // The AFT tool provider's legacy format puts images only in structuredContent.attachments. Its decoder
            // stores the content text and ignores those attachments and native error codes.
            ToolResult {
                tool_call_id,
                output: if error {
                    ToolOutput::error(text)
                } else {
                    ToolOutput::Text { text: text.into() }
                },
                is_error: error,
            }
        };
        rig.script
            .tool_results
            .lock()
            .unwrap()
            .insert(id.into(), DispatchResult::Completed(result));
    }
    rig.script.push(&[
        Step::Calls(&["read-text", "read-image", "read-error"]),
        Step::Text("done"),
    ]);
    rig.send("read-fixtures", "Read the fixtures", None).await;
    let records = rig.wait_for_finished(1).await;
    assert_eq!(
        records
            .iter()
            .filter(|r| matches!(r, broca_wal::WalRecord::ToolResult { .. }))
            .count(),
        3
    );
    rig.wait_idle().await;
    let read = rig
        .request(7, json!({"method":"session.read","params":{}}))
        .await;
    let page = result(&read);
    let encoded = page.to_string();
    assert_eq!(
        encoded.matches("not shown to the model").count(),
        usize::from(v1)
    );
    assert!(encoded.contains("File not found: missing.txt"));
    rig.finish().await;
    read.body
}

#[tokio::test]
async fn capture_tool_result_v1_session_read_bytes() {
    let before = sample(false).await;
    let after = sample(true).await;
    if let Some(dir) = std::env::var_os("BROCA_TOOL_RESULT_SAMPLES") {
        let dir = std::path::PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut sums = String::new();
        for (name, bytes) in [
            ("before.session_read.reply.json", before),
            ("after.session_read.reply.json", after),
        ] {
            std::fs::write(dir.join(name), &bytes).unwrap();
            sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(&bytes)));
        }
        std::fs::write(dir.join("SHA256SUMS"), sums).unwrap();
    }
}
