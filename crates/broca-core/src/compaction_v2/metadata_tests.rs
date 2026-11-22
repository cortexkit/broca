use std::sync::Arc;

use broca_protocol::{
    ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult, Usage,
};
use broca_records::record::WalRecord;
use broca_wal::FramedRecord;
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
                input: json!({"path":"a"}).into(),
                thought_signature: None,
                provider_state: None,
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
    serde_json::from_value(json!({"type":"tool_result","batch_id":"b","result":ToolResult { tool_call_id:ToolCallId("call".into()), output:ToolOutput::Json { value:json!({"a":1}).into() }, is_error:false },"outcome":"completed","finished_at_ms":30})).unwrap()
}

#[test]
fn compaction_v2_recorded_steer_marks_decide_its_notice() {
    let owner = broca_protocol::BindPrincipal::Reserved {
        module_id: "core".into(),
    };
    for (marks, notice) in [
        (Some(json!({"notice":true})), Some("core")),
        (Some(json!({"speaks_for":{"agent_id":"Ada"}})), None),
        (None, Some("core")),
    ] {
        let assistant = assistant();
        let records = [
            frame(1, started(vec![text("question")])),
            frame(2, serde_json::from_value(json!({"type":"step_started","step_id":0})).unwrap()),
            frame(3, model_step(&assistant)),
            frame(4, serde_json::from_value(json!({"type":"steered","input":[text("owner steer")],"sent_by":owner,"message_marks":marks})).unwrap()),
            frame(5, serde_json::from_value(json!({"type":"tool_batch_started","batch_id":"b","step_id":0,"planned_calls":["call"]})).unwrap()),
            frame(6, terminal()),
            frame(7, serde_json::from_value(json!({"type":"tool_batch_finished","batch_id":"b"})).unwrap()),
            frame(8, serde_json::from_value(json!({"type":"step_started","step_id":1})).unwrap()),
        ];
        let mut fold = Fold::default();
        let mut rows = std::collections::BTreeMap::new();
        for record in &records {
            rows.extend(fold.observe(record).unwrap().rows);
        }
        let steer = serde_json::to_value(rows[&3].notice.as_ref()).unwrap();
        assert_eq!(steer["sender"].as_str(), notice, "{marks:?}");
        assert_eq!(
            serde_json::to_value(rows[&3].speaks_for.as_ref()).unwrap(),
            marks
                .as_ref()
                .and_then(|marks| marks.get("speaks_for").cloned())
                .unwrap_or(serde_json::Value::Null),
            "{marks:?}"
        );
    }
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
