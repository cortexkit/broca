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

/// OpenCode's own schema gives `session` a foreign key to `project`, a table the extract
/// doesn't copy. The extract must still copy the session's rows rather than refuse them.
#[test]
fn opencode1_extract_copies_rows_whose_foreign_keys_name_tables_it_leaves_out() {
    let root = broca_test_support::scratch_root()
        .join(format!("opencode1-foreign-keys-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let source = Connection::open(root.join("source.db")).unwrap();
    source
        .execute_batch(
            "PRAGMA foreign_keys=ON;
             CREATE TABLE project (id text PRIMARY KEY);
             CREATE TABLE session (id text PRIMARY KEY, project_id text NOT NULL,
                 CONSTRAINT fk_session_project FOREIGN KEY (project_id) REFERENCES project(id)
                 ON DELETE CASCADE);
             CREATE TABLE message (id text PRIMARY KEY, session_id text NOT NULL,
                 time_created integer NOT NULL, data text NOT NULL,
                 FOREIGN KEY (session_id) REFERENCES session(id) ON DELETE CASCADE);
             CREATE TABLE part (id text PRIMARY KEY, message_id text NOT NULL,
                 session_id text NOT NULL, time_created integer NOT NULL, data text NOT NULL,
                 FOREIGN KEY (message_id) REFERENCES message(id) ON DELETE CASCADE);
             INSERT INTO project VALUES ('prj_1');
             INSERT INTO session VALUES ('ses_fk', 'prj_1');
             INSERT INTO message VALUES ('msg_1', 'ses_fk', 1, '{}');
             INSERT INTO part VALUES ('prt_1', 'msg_1', 'ses_fk', 1, '{}');",
        )
        .unwrap();
    let extracted =
        crate::opencode_extract::extract(&root.join("source.db"), "ses_fk", &root.join("out"))
            .unwrap();
    assert_eq!(
        (extracted.messages, extracted.newest_mid.as_deref()),
        (1, Some("msg_1"))
    );
    let copy = Connection::open(&extracted.path).unwrap();
    let count = |table: &str| {
        copy.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
            row.get::<_, u64>(0)
        })
        .unwrap()
    };
    assert_eq!(
        (count("session"), count("message"), count("part")),
        (1, 1, 1)
    );
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
fn opencode1_old_unfinished_calls_use_host_results_but_last_row_calls_refuse() {
    let (root, db) = fixture("refusals");
    db.execute(
        "UPDATE part SET data=json_set(data,'$.state.status','running') WHERE id='prt_009'",
        [],
    )
    .unwrap();
    let history = opencode1::read(&root.join("source.db"), "ses_synthetic", &mut accept).unwrap();
    assert!(
        matches!(&history.messages[4].message.content[0], ContentBlock::ToolCall(call)
        if call.tool_call_id.0=="reused-2" && call.input==serde_json::json!({}))
    );
    assert!(
        matches!(&history.messages[5].message.content[0], ContentBlock::ToolResult(result)
        if result.is_error && result.tool_call_id.0=="reused-2" && result.output==ToolOutput::error("[Tool execution was interrupted]"))
    );
    db.execute("UPDATE part SET data=json_remove(json_set(data,'$.state.status','pending'),'$.state.input') WHERE id='prt_009'", []).unwrap();
    let history = opencode1::read(&root.join("source.db"), "ses_synthetic", &mut accept).unwrap();
    assert!(
        matches!(&history.messages[4].message.content[0], ContentBlock::ToolCall(call) if call.input==serde_json::json!({}))
    );
    db.execute(
        "UPDATE part SET message_id='msg_009' WHERE id='prt_009'",
        [],
    )
    .unwrap();
    let error = opencode1::read(&root.join("source.db"), "ses_synthetic", &mut accept).unwrap_err();
    assert!(
        matches!(error, HeadError::OpenCall {mid, call_id} if mid=="msg_009" && call_id=="reused")
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
fn opencode1_check_counts_projection_kinds_and_reports_only_refusal_ordinals() {
    let (root, db) = fixture("counts");
    let path = root.join("source.db");
    let counts = opencode1::check(&path, "ses_synthetic", &mut accept).unwrap();
    assert_eq!(
        counts.source_rows_by_role,
        [("user", 3), ("assistant", 5), ("system", 1)].into()
    );
    assert_eq!(
        counts.imported_messages_by_role,
        [("user", 3), ("assistant", 3), ("tool", 2), ("system", 0)].into()
    );
    assert_eq!(counts.tool_calls, 2);
    assert_eq!(
        counts.tool_results,
        opencode1::ToolResults {
            completed: 2,
            ..Default::default()
        }
    );
    assert_eq!((counts.media_objects, counts.media_occurrences), (1, 2));
    assert_eq!(
        (
            counts.dropped_system_rows,
            counts.rows_projected_nothing,
            counts.finished_summaries
        ),
        (1, 3, 2)
    );
    db.execute_batch("UPDATE part SET data=json_set(data,'$.state.status','error','$.state.error','sensitive failure') WHERE id='prt_009';
        UPDATE part SET data=json_set(data,'$.state.status','error','$.state.metadata',json('{\"interrupted\":true,\"output\":\"sensitive output\"}')) WHERE id='prt_007';").unwrap();
    let counts = opencode1::check(&path, "ses_synthetic", &mut accept).unwrap();
    assert_eq!(
        counts.tool_results,
        opencode1::ToolResults {
            error: 1,
            interrupted_as_result: 1,
            ..Default::default()
        }
    );
    db.execute("UPDATE part SET data=json_set(data,'$.state.status','running','$.state.input',json('{\"path\":\"recorded-input\"}')) WHERE id='prt_009'", []).unwrap();
    let counts = opencode1::check(&path, "ses_synthetic", &mut accept).unwrap();
    assert_eq!(
        counts.tool_results,
        opencode1::ToolResults {
            interrupted_as_result: 1,
            open_call_placeholders: 1,
            ..Default::default()
        }
    );
    let history = opencode1::read(&path, "ses_synthetic", &mut accept).unwrap();
    assert!(
        matches!(&history.messages[4].message.content[0], ContentBlock::ToolCall(call) if call.input==serde_json::json!({"path":"recorded-input"}))
    );
    db.execute(
        "UPDATE part SET message_id='msg_009' WHERE id='prt_009'",
        [],
    )
    .unwrap();
    assert_eq!(
        opencode1::check(&path, "ses_synthetic", &mut accept).unwrap_err(),
        opencode1::Refusal {
            code: "import_open_call",
            message_ordinal: Some(9)
        }
    );
    assert_eq!(
        opencode1::check(&path, "missing-sensitive-id", &mut accept).unwrap_err(),
        opencode1::Refusal {
            code: "import_source_unreadable",
            message_ordinal: None
        }
    );
    db.execute("UPDATE message SET id='' WHERE id='msg_001'", [])
        .unwrap();
    assert_eq!(
        opencode1::check(&path, "ses_synthetic", &mut accept).unwrap_err(),
        opencode1::Refusal {
            code: "import_source_unmapped",
            message_ordinal: Some(1)
        }
    );
    drop(db);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn opencode1_summary_flags_match_the_sql_query_and_ties_keep_every_row_in_id_order() {
    let (root, db) = fixture("summary-order");
    let path = root.join("source.db");
    let baseline = opencode1::read(&path, "ses_synthetic", &mut accept).unwrap();
    assert!(
        !baseline.rows[0].finished_summary,
        "a summary object is not true"
    );
    assert!(
        !baseline.rows[2].finished_summary,
        "another finish is not stop"
    );
    assert!(
        baseline.rows[7].finished_summary && baseline.rows[7].message.is_none(),
        "an errored stop summary still counts"
    );
    assert_eq!(
        baseline
            .rows
            .iter()
            .map(|row| row.mid.as_str())
            .collect::<Vec<_>>(),
        [
            "msg_001", "msg_002", "msg_003", "msg_004", "msg_005", "msg_006", "msg_007", "msg_008",
            "msg_009"
        ]
    );
    let source = crate::opencode::load_messages(&db, "ses_synthetic").unwrap();
    assert_eq!((&source[7].0, source[7].2), (&"msg_008".to_owned(), 8000));
    assert_eq!((&source[8].0, source[8].2), (&"msg_009".to_owned(), 8000));
    let extracted =
        crate::opencode_extract::extract(&path, "ses_synthetic", &root.join("out")).unwrap();
    let copy = Connection::open(&extracted.path).unwrap();
    assert_eq!(
        crate::opencode::load_messages(&copy, "ses_synthetic").unwrap(),
        source,
        "extract preserves ids and time_created, including ties"
    );
    db.execute_batch("UPDATE message SET data=json_set(data,'$.finish','stop') WHERE id='msg_003';
        UPDATE message SET data=json_set(data,'$.summary',1,'$.finish','stop') WHERE id='msg_005';
        UPDATE message SET data=json_set(data,'$.summary','1','$.finish','stop') WHERE id='msg_009';").unwrap();
    let history = opencode1::read(&path, "ses_synthetic", &mut accept).unwrap();
    let mut query = db.prepare("SELECT id, COALESCE(json_extract(data,'$.summary'),0) = 1 AND COALESCE(json_extract(data,'$.finish'),'') = 'stop' FROM message WHERE session_id='ses_synthetic' ORDER BY time_created,id").unwrap();
    let sql = query
        .query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, bool>(1)?))
        })
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    assert_eq!(
        history
            .rows
            .iter()
            .map(|row| (row.mid.clone(), row.finished_summary))
            .collect::<Vec<_>>(),
        sql
    );
    assert!(
        history.rows[2].finished_summary,
        "there is no assistant role condition"
    );
    drop(query);
    drop(copy);
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
