use broca_protocol::{ContentBlock, Role, ToolOutput};
use rusqlite::Connection;

use super::{opencode1, tests::accept, HeadError};

const SQL: &str = include_str!("../../tests/fixtures/head/opencode1.sql");

fn fixture(tag: &str) -> (std::path::PathBuf, Connection) {
    let root =
        broca_test_support::scratch_root().join(format!("opencode1-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let connection = Connection::open(root.join("source.db")).unwrap();
    connection.execute_batch(SQL).unwrap();
    (root, connection)
}

#[test]
fn opencode1_extract_preserves_schema_and_only_selected_rows_then_projects_history() {
    let (root, source) = fixture("projection");
    source.execute_batch("PRAGMA journal_mode=WAL; INSERT INTO part VALUES ('prt_wal', 'msg_001', 'ses_synthetic', 1004, '{\"type\":\"text\",\"text\":\"Committed WAL\"}');").unwrap();
    let extracted = crate::opencode_extract::extract(
        &root.join("source.db"),
        "ses_synthetic",
        &root.join("out"),
    )
    .unwrap();
    assert_eq!(extracted.messages, 9);
    assert_eq!(extracted.newest_mid.as_deref(), Some("msg_009"));
    assert_eq!(
        extracted.sha256,
        crate::opencode_extract::sha256_file(&extracted.path).unwrap()
    );
    broca_owner_only::validate(&extracted.path).unwrap();
    let db = Connection::open(&extracted.path).unwrap();
    for table in ["session", "message", "part"] {
        let sql = |db: &Connection| {
            db.query_row(
                "SELECT sql FROM sqlite_schema WHERE name=?1",
                [table],
                |row| row.get::<_, String>(0),
            )
            .unwrap()
        };
        assert_eq!(sql(&source), sql(&db));
    }
    assert_eq!(
        db.query_row("SELECT COUNT(*) FROM session", [], |row| row
            .get::<_, u64>(0))
            .unwrap(),
        1
    );
    assert_eq!(
        db.query_row(
            "SELECT COUNT(*) FROM part WHERE session_id='ses_other'",
            [],
            |row| row.get::<_, u64>(0)
        )
        .unwrap(),
        0
    );
    assert_eq!(
        db.query_row("SELECT extra FROM message WHERE id='msg_001'", [], |row| {
            row.get::<_, Vec<u8>>(0)
        })
        .unwrap(),
        [1, 2]
    );
    let history = opencode1::read(&extracted.path, "ses_synthetic", &mut accept).unwrap();
    assert_eq!(history.messages.len(), 8);
    assert_eq!(history.dropped_system_messages, 1);
    assert_eq!(
        history
            .rows
            .iter()
            .map(|row| row.message)
            .collect::<Vec<_>>(),
        [
            Some(0),
            Some(1),
            Some(3),
            Some(4),
            None,
            Some(6),
            Some(7),
            None,
            None
        ]
    );
    assert!(history.rows[6].finished_summary);
    assert!(
        history.rows[7].finished_summary,
        "the plugin counts even an errored finished summary"
    );
    assert!(!history.rows[8].finished_summary);
    let prompt = &history.messages[0];
    assert_eq!(prompt.mid.as_deref(), Some("msg_001"));
    assert_eq!(prompt.written_at_ms, Some(1000));
    assert_eq!(prompt.created_at_ms, Some(1000));
    assert_eq!(prompt.media.len(), 1);
    assert!(prompt
        .message
        .content
        .iter()
        .any(|part| matches!(part, ContentBlock::Text{text} if text=="Committed WAL")));
    assert!(!format!("{history:?}").contains("Ignored text"));
    assert!(!format!("{history:?}").contains("never-import-this"));
    assert!(
        matches!(&history.messages[1].message.content[1], ContentBlock::Reasoning{text, signature:None} if text=="Synthetic reasoning")
    );
    assert!(history
        .messages
        .iter()
        .all(|message| message.calls.is_empty()));
    assert_eq!(history.messages[2].media.len(), 1);
    assert_eq!(history.messages[2].media[0].part, Some(1));
    let ids: Vec<_> = history
        .messages
        .iter()
        .flat_map(|message| &message.message.content)
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(call.tool_call_id.0.as_str()),
            ContentBlock::ToolResult(result) => Some(result.tool_call_id.0.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(ids, ["reused", "reused", "reused-2", "reused-2"]);
    assert_eq!(history.messages[5].message.role, Role::Tool);
    assert!(
        matches!(&history.messages[5].message.content[0], ContentBlock::ToolResult(result) if result.output == ToolOutput::Text{text:"[Old tool result content cleared]".into()})
    );
    drop(db);
    drop(source);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn opencode1_unanswered_calls_and_external_media_refuse_by_message() {
    let (root, db) = fixture("refusals");
    db.execute(
        "UPDATE part SET data=json_set(data,'$.state.status','running') WHERE id='prt_009'",
        [],
    )
    .unwrap();
    let error = opencode1::read(&root.join("source.db"), "ses_synthetic", &mut accept).unwrap_err();
    assert!(
        matches!(error, HeadError::OpenCall {mid, call_id} if mid=="msg_004" && call_id=="reused")
    );
    db.execute("UPDATE part SET data=json_set(data,'$.url','file:///synthetic/image.png') WHERE id='prt_004'", []).unwrap();
    let error = opencode1::read(&root.join("source.db"), "ses_synthetic", &mut accept).unwrap_err();
    assert!(matches!(error, HeadError::Media {mid,..} if mid=="msg_001"));
    assert!(crate::opencode_extract::extract(
        &root.join("source.db"),
        "ses_absent",
        &root.join("out")
    )
    .unwrap_err()
    .to_string()
    .contains("ses_absent"));
    drop(db);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn opencode1_aborted_turns_signed_separators_and_interrupted_results_follow_host_projection() {
    let (root, db) = fixture("errors");
    db.execute_batch("UPDATE message SET data=json_set(data,'$.error',json('{\"name\":\"MessageAbortedError\"}')) WHERE id='msg_002';
        UPDATE part SET data=json_set(data,'$.text','') WHERE id='prt_005';
        UPDATE part SET data=json_set(data,'$.state.status','error','$.state.error','failure','$.state.metadata',json('{\"interrupted\":true,\"output\":\"Partial output\"}')) WHERE id='prt_009';
        INSERT INTO part VALUES ('prt_subtask','msg_003','ses_synthetic',3001,'{\"type\":\"subtask\"}');
        INSERT INTO part VALUES ('prt_plain','msg_003','ses_synthetic',3002,'{\"type\":\"file\",\"mime\":\"text/plain\",\"url\":\"file:///must-not-open\"}');").unwrap();
    let history = opencode1::read(&root.join("source.db"), "ses_synthetic", &mut accept).unwrap();
    assert_eq!(
        history.messages.len(),
        8,
        "aborted partial text/tool turns remain visible"
    );
    assert!(
        matches!(&history.messages[1].message.content[0], ContentBlock::Text{text} if text==" ")
    );
    assert!(history.messages[3]
        .message
        .content
        .iter()
        .any(|block| matches!(block,
        ContentBlock::Text{text} if text=="The following tool was executed by the user")));
    assert!(
        matches!(&history.messages[5].message.content[0], ContentBlock::ToolResult(result)
        if !result.is_error && result.output==ToolOutput::Text{text:"Partial output".into()})
    );
    db.execute(
        "UPDATE part SET data=json_remove(data,'$.state.metadata') WHERE id='prt_009'",
        [],
    )
    .unwrap();
    let history = opencode1::read(&root.join("source.db"), "ses_synthetic", &mut accept).unwrap();
    assert!(
        matches!(&history.messages[5].message.content[0], ContentBlock::ToolResult(result)
        if result.is_error && result.output==ToolOutput::error("failure"))
    );
    drop(db);
    std::fs::remove_dir_all(root).unwrap();
}
