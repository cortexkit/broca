use std::sync::Arc;

use broca_protocol::{
    ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult, Usage,
};
use broca_records::record::WalRecord;
use broca_wal::FramedRecord;
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

fn marked_input_entry(
    principal: broca_protocol::BindPrincipal,
    marks: Option<serde_json::Value>,
) -> cortexkit_role_compaction::read::Entry {
    let message = text("owner words");
    let mut record = started(vec![message.clone()]);
    let WalRecord::RunStarted {
        sent_by,
        principal: owner,
        message_marks,
        ..
    } = &mut record
    else {
        unreachable!()
    };
    *sent_by = Some(principal.clone());
    *owner = Some(principal);
    *message_marks = marks.map(|value| serde_json::from_value(value).unwrap());
    let frames = [frame(1, record)];
    let mut fold = Fold::default();
    let rows = fold.observe(&frames[0]).unwrap().rows;
    metadata::project_entry(&message, &rows[&0], 0, "m0", 10, &frames).unwrap()
}

#[test]
fn compaction_v2_recorded_notice_preserves_its_module_owner_sender() {
    let entry = marked_input_entry(
        broca_protocol::BindPrincipal::Reserved {
            module_id: "core".into(),
        },
        Some(json!({"notice":true})),
    );
    let value = serde_json::to_value(entry).unwrap();
    assert_eq!(value["notice"]["sender"], "core");
    assert!(value["notice"].get("template").is_none());
}

#[test]
fn compaction_v2_recorded_speaks_for_is_not_a_module_notice() {
    let entry = marked_input_entry(
        broca_protocol::BindPrincipal::Reserved {
            module_id: "core".into(),
        },
        Some(json!({"speaks_for":{"agent_id":"Ada"}})),
    );
    assert!(entry.notice.is_none());
}

#[test]
fn compaction_v2_recorded_notice_from_a_direct_caller_names_no_sender() {
    let marked = marked_input_entry(
        broca_protocol::BindPrincipal::Direct,
        Some(json!({"notice":true})),
    );
    let unmarked = marked_input_entry(broca_protocol::BindPrincipal::Direct, None);
    assert!(marked.notice.is_none());
    assert_eq!(
        serde_json::to_vec(&marked).unwrap(),
        serde_json::to_vec(&unmarked).unwrap()
    );
}

#[test]
fn compaction_v2_recorded_queued_marks_survive_projection_checkpoint() {
    for (marks, notice) in [
        (Some(json!({"notice":true})), Some("core")),
        (Some(json!({"speaks_for":{"agent_id":"Ada"}})), None),
        (None, Some("core")),
    ] {
        let owner = broca_protocol::BindPrincipal::Reserved {
            module_id: "core".into(),
        };
        let start = started(vec![text("question")]);
        let WalRecord::RunStarted { run_id, .. } = &start else {
            unreachable!()
        };
        let delivered = frame(
            4,
            serde_json::from_value(json!({
                "type":"send_delivered", "submission_id":"queued", "run_id":run_id,
                "mode":"steer", "input":[text("owner words")]
            }))
            .unwrap(),
        );
        let before = [
            frame(1, start),
            frame(2, serde_json::from_value(json!({"type":"delivery_ordered", "submission_id":"queued", "order":{"modes":["steer"]}})).unwrap()),
            frame(3, serde_json::from_value(json!({
                "type":"queued", "submission_id":"queued", "principal":owner, "sent_by":owner,
                "message_marks":marks,
                "request":{"model":broca_protocol::ModelHandle::new("fake","fake"),"input":[text("owner words")]}
            })).unwrap()),
        ];
        let mut fold = Fold::default();
        for record in &before {
            fold.observe(record).unwrap();
        }
        let checkpoint = serde_json::to_vec(&fold).unwrap();
        let checkpoint_text = String::from_utf8_lossy(&checkpoint);
        assert!(!checkpoint_text.contains("owner words"));
        // An unmarked send's checkpoint keeps the bytes it had before marks were recorded.
        assert_eq!(
            checkpoint_text.contains("\"marks\""),
            marks.is_some(),
            "{checkpoint_text}"
        );
        let mut resumed: Fold = serde_json::from_slice(&checkpoint).unwrap();
        let mut rows = resumed.observe(&delivered).unwrap().rows;
        let next = frame(
            5,
            serde_json::from_value(json!({"type":"step_started","step_id":1})).unwrap(),
        );
        rows.extend(resumed.observe(&next).unwrap().rows);
        let entry = metadata::project_entry(
            &text("owner words"),
            &rows[&1],
            1,
            "m1",
            10,
            &[delivered, next],
        )
        .unwrap();
        let value = serde_json::to_value(&entry).unwrap();
        assert_eq!(value["notice"]["sender"].as_str(), notice, "{marks:?}");
        // The queued send's speaker survives the checkpoint into its delivered entry.
        assert_eq!(
            value.get("speaks_for"),
            marks.as_ref().and_then(|marks| marks.get("speaks_for")),
            "{marks:?}"
        );
    }
}

#[test]
fn compaction_v2_unmarked_principals_keep_their_legacy_notice_and_digest() {
    use super::entries::{project, EntryFacts};
    for (principal, notice) in [
        (
            broca_protocol::BindPrincipal::Reserved {
                module_id: "core".into(),
            },
            Some(cortexkit_role_compaction::read::NoticeOrigin::new("core")),
        ),
        (broca_protocol::BindPrincipal::Direct, None),
    ] {
        let actual = marked_input_entry(principal, None);
        let expected = project(
            &[text("owner words")],
            &[EntryFacts {
                mid: "m0".into(),
                ordinal: 1,
                tag_base: Some(0),
                written_at: 10,
                completed_at: None,
                notice: notice.clone(),
                speaks_for: None,
                usage: None,
                generated_under: None,
            }],
            &Default::default(),
            true,
        )
        .remove(0);
        assert_eq!(actual.notice, notice);
        assert_eq!(actual.digest, expected.digest);
        assert_eq!(
            serde_json::to_vec(&actual).unwrap(),
            serde_json::to_vec(&expected).unwrap()
        );
    }
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

/// A speaker is projected onto the entries of a marked send, named or not, alone or with a
/// notice, and never moves the entry's digest: attribution is outside every checksum.
#[test]
fn compaction_v2_recorded_speaker_projects_without_moving_the_digest() {
    let owner = broca_protocol::BindPrincipal::Reserved {
        module_id: "core".into(),
    };
    assert!(marked_input_entry(owner.clone(), None).speaks_for.is_none());
    let unnamed = marked_input_entry(
        owner.clone(),
        Some(json!({"speaks_for":{"agent_id":"agent-a"}})),
    );
    for marks in [
        json!({"speaks_for":{"agent_id":"agent-a"}}),
        json!({"speaks_for":{"agent_id":"agent-a","name":"Ada"}}),
        json!({"speaks_for":{"agent_id":"agent-a","name":"Ada"},"notice":true}),
    ] {
        let entry = marked_input_entry(owner.clone(), Some(marks.clone()));
        assert_eq!(
            serde_json::to_value(entry.speaks_for.as_ref()).unwrap(),
            marks["speaks_for"],
        );
        let notice = marks["notice"] == json!(true);
        assert_eq!(entry.notice.is_some(), notice, "{marks}");
        assert_eq!(
            entry.digest,
            cortexkit_role_compaction::read::entry_digest(
                entry.role.clone(),
                entry.notice.as_ref(),
                &entry.parts,
            ),
            "{marks}"
        );
        if !notice {
            // A name changes nothing but the speaker the entry carries.
            let mut renamed = entry.clone();
            renamed.speaks_for = unnamed.speaks_for.clone();
            assert_eq!(renamed, unnamed, "{marks}");
        }
        let mut gated = entry.clone();
        super::entries::gate_speaks_for(std::slice::from_mut(&mut gated), true);
        assert_eq!(gated, entry);
        super::entries::gate_speaks_for(std::slice::from_mut(&mut gated), false);
        assert!(gated.speaks_for.is_none());
        assert_eq!(gated.digest, entry.digest);
    }
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
