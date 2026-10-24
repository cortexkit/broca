//! A populated `session.head` reply captured before model, usage, sender and incarnation were added.
mod delivery_rig;

use broca_core::{FileWalStore, WalStore};
use broca_wal::WalRecord;
use delivery_rig::*;
use serde_json::json;

#[tokio::test]
async fn populated_head_preserves_pre_addition_reply_bytes() {
    let mut rig = Rig::start("populated-legacy-head").await;
    let mut start: WalRecord = serde_json::from_str(
        include_str!("../../broca-wal/tests/fixtures/before_message_times.jsonl")
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    if let WalRecord::RunStarted {
        run_id,
        session,
        ts_ms,
        ..
    } = &mut start
    {
        *run_id = broca_protocol::RunId("legacy-run".into());
        *session = rig.session.clone();
        *ts_ms = Some(500);
    }
    let records = [
        start,
        serde_json::from_value(json!({"type":"model_step_finished","step_id":1,
            "assistant_message":{"message_id":"legacy-answer","content":[{"type":"text","text":"answer"}]},
            "usage":{"input_tokens":7,"output_tokens":3},"retries_used":0})).unwrap(),
        serde_json::from_value(json!({"type":"run_finished","reason":"completed","ts_ms":506,
            "usage":{"input_tokens":7,"output_tokens":3},"retries_used":0})).unwrap(),
    ];
    let store = FileWalStore::new(rig.dir.join("wal"));
    let mut wal = store.open(&rig.session).unwrap();
    for record in records {
        wal.append(1, &record, false).unwrap();
    }
    wal.sync().unwrap();
    drop(wal);

    let head = rig
        .request(7, json!({"method":"session.head","params":{}}))
        .await;
    assert_eq!(head.header.ty, subc_protocol::FrameType::Response);
    let answer = result(&head);
    assert!(answer["updated_at"].is_u64());
    assert!(answer["lineage_id"].is_string());
    assert_eq!(answer["last_ordinal"], 1);
    assert_eq!(answer["first_ordinal"], 0);
    assert_eq!(answer["history_version"], 0);
    assert_eq!(answer["head"], json!({"wal_seq":3,"sub_index":0}));
    assert_eq!(
        answer["last_run_state"],
        json!({"run_id":"legacy-run","state":"completed","reason":"completed"})
    );
    assert_eq!(answer["input"], json!({"attachments":"no"}));

    let mut bytes = String::from_utf8(head.body).unwrap();
    // The new optional members are appended after the old head fields. Preserve the original
    // wire bytes, rather than serializing a JSON map that could hide changes to field order.
    let additions = ["model", "usage", "sender", "incarnation"];
    let end = additions
        .iter()
        .filter_map(|key| bytes.find(&format!(",\"{key}\":")))
        .min();
    if let Some(end) = end {
        bytes.truncate(end);
        bytes.push_str("}}");
    }
    let mut legacy = answer.clone();
    for key in additions {
        legacy.as_object_mut().unwrap().remove(key);
    }
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&bytes).unwrap(),
        json!({"result":legacy})
    );
    bytes = bytes.replace(
        &format!(
            "\"lineage_id\":\"{}\"",
            answer["lineage_id"].as_str().unwrap()
        ),
        "\"lineage_id\":\"@LOG_ID@\"",
    );
    bytes = bytes.replace(
        &format!("\"updated_at\":{}", answer["updated_at"].as_u64().unwrap()),
        "\"updated_at\":\"@UPDATED_AT@\"",
    );
    bytes = bytes.replace("legacy-run", "@RUN_ID@");
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/golden/session_head.before_head_additions.full.json");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&path, format!("{bytes}\n")).unwrap();
        println!("session_head.before_head_additions.full.json: {bytes}");
    }
    assert_eq!(bytes, std::fs::read_to_string(path).unwrap().trim_end());
    rig.finish().await;
}
