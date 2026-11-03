use std::sync::Arc;

use broca_protocol::{
    ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult, Usage,
};
use broca_wal::{FramedRecord, WalRecord};
use cortexkit_role_compaction::part::{PartBody, ToolStatus};
use serde_json::json;

use super::metadata::{self, Fold};

fn text(text: &str) -> Message {
    Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
}

fn started(input: Vec<Message>) -> WalRecord {
    let mut record: WalRecord = serde_json::from_str(
        include_str!("../../../broca-wal/tests/fixtures/compaction_v2_records.jsonl")
            .lines()
            .last()
            .unwrap(),
    )
    .unwrap();
    let WalRecord::RunStarted {
        input: stored,
        compaction_plan_version,
        ..
    } = &mut record
    else {
        panic!("fixture is not a run start")
    };
    *stored = Arc::new(input);
    *compaction_plan_version = Some(cortexkit_compaction_view::PLAN_VERSION);
    record
}

fn assistant() -> Message {
    Message::new(
        Role::Assistant,
        vec![
            ContentBlock::Reasoning {
                text: "private".into(),
                signature: Some("signature".into()),
            },
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("call".into()),
                tool_name: "read".into(),
                input: json!({"path":"a"}),
                thought_signature: None,
            }),
        ],
    )
}

fn frame(seq: u64, record: WalRecord) -> FramedRecord {
    FramedRecord {
        seq,
        fence: 1,
        record,
    }
}

fn model_step(message: &Message) -> WalRecord {
    serde_json::from_value(json!({"type":"model_step_finished","step_id":0,"assistant_message":{"message_id":"a","content":message.content},"usage":{"input_tokens":5,"cached_input_tokens":2,"cache_write_tokens":3,"output_tokens":0},"written_at_ms":20})).unwrap()
}

fn terminal() -> WalRecord {
    serde_json::from_value(json!({"type":"tool_result","batch_id":"b","result":ToolResult { tool_call_id:ToolCallId("call".into()), output:ToolOutput::Json { value:json!({"a":1}) }, is_error:false },"outcome":"completed","finished_at_ms":30})).unwrap()
}

#[test]
fn compaction_v2_metadata_reserves_pending_assistant_across_restart_and_held_steer() {
    let assistant = assistant();
    let records = [
        frame(1, started(vec![text("question")])),
        frame(2, serde_json::from_value(json!({"type":"step_started","step_id":0})).unwrap()),
        frame(3, model_step(&assistant)),
        frame(4, serde_json::from_value(json!({"type":"steered","input":[text("held steer")]})).unwrap()),
        frame(5, serde_json::from_value(json!({"type":"tool_batch_started","batch_id":"b","step_id":0,"planned_calls":["call"]})).unwrap()),
        frame(6, terminal()),
        frame(7, serde_json::from_value(json!({"type":"tool_batch_finished","batch_id":"b"})).unwrap()),
        frame(8, serde_json::from_value(json!({"type":"step_started","step_id":1})).unwrap()),
    ];
    let mut fold = Fold::default();
    for record in &records[..4] {
        fold.observe(record).unwrap();
    }
    let (reserved, pending) = fold.pending_assistant().unwrap();
    assert_eq!(reserved, 1);
    assert_eq!(pending.tag_base, 1);
    let entry = metadata::project_entry(
        &assistant,
        pending,
        reserved,
        "m12345678.1",
        20,
        &records[2..4],
    )
    .unwrap();
    assert_eq!(entry.completed_at, None);
    assert!(
        matches!(&entry.parts[1].body, PartBody::Tool(tool) if tool.status == ToolStatus::Running)
    );
    let checkpoint = serde_json::to_vec(&fold).unwrap();
    let checkpoint_text = String::from_utf8_lossy(&checkpoint);
    assert!(!checkpoint_text.contains("private"));
    assert!(!checkpoint_text.contains("held steer"));
    let mut resumed: Fold = serde_json::from_slice(&checkpoint).unwrap();
    assert_eq!(resumed.pending_assistant().unwrap().0, reserved);
    let mut rows = std::collections::BTreeMap::new();
    for record in &records[4..] {
        rows.extend(resumed.observe(record).unwrap().rows);
    }
    assert_eq!(rows.keys().copied().collect::<Vec<_>>(), [1, 2, 3]);
    assert_eq!(rows[&1].tag_base, 1);
    assert_eq!(rows[&2].tag_base, 2);
    assert_eq!(rows[&3].tag_base, 2);
    let entry =
        metadata::project_entry(&assistant, &rows[&1], 1, "m12345678.1", 20, &records[2..7])
            .unwrap();
    assert_eq!(entry.ordinal, 2);
    assert_eq!(entry.completed_at, Some(20));
    assert!(
        matches!(&entry.parts[1].body, PartBody::Tool(tool) if tool.status == ToolStatus::Completed && tool.output.as_deref() == Some("{\"a\":1}"))
    );
    assert_eq!(entry.usage.unwrap().prompt_tokens, Some(10));
    let raw: Vec<_> = records.iter().map(|record| record.record.clone()).collect();
    let replay = crate::replay(&raw).unwrap();
    assert_eq!(replay.prompt.len(), 4);
    assert_eq!(replay.prompt[1].role, Role::Assistant);
    assert_eq!(replay.prompt[2].role, Role::Tool);
    assert_eq!(replay.prompt[3].content, text("held steer").content);
}

#[test]
fn compaction_v2_usage_keeps_absent_counts_distinct_from_reported_zero() {
    assert_eq!(metadata::step_usage(Usage::default()), None);
    let usage = metadata::step_usage(Usage {
        input_tokens: Some(0),
        output_tokens: Some(0),
        ..Usage::default()
    })
    .unwrap();
    assert_eq!(usage.prompt_tokens, None);
    assert_eq!(usage.output_tokens, Some(0));
    assert_eq!(usage.cache_read_tokens, None);
    assert_eq!(usage.cache_write_tokens, None);
    assert_eq!(usage.reasoning_tokens, None);
}
