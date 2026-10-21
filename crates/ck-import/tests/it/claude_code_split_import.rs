use std::path::PathBuf;

use broca_core::{
    replay, run_import, FileWalStore, ImportError, ImportOrigin, ImportOutcome, ImportRequest,
    RunId, WalHandle, WalStore,
};
use broca_protocol::{BindIdentity, ContentBlock, Message, Role, ToolOutput};
use ck_session_import::{claude_code::ClaudeCodeReader, HarnessReader, ImportedSession};

fn fixture(session: &str) -> ImportedSession {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../ck-session-import/tests/fixtures/claude-code-split");
    ClaudeCodeReader::new(root).convert(session).unwrap()
}

fn request(session: &BindIdentity, imported: &ImportedSession, run_id: &str) -> ImportRequest {
    ImportRequest {
        project_id: None,
        principal: None,
        run_id: RunId(run_id.into()),
        session: session.clone(),
        messages: imported.messages.clone(),
        origin: ImportOrigin {
            source_ref: imported.source_ref.clone(),
        },
    }
}

async fn import_and_replay(
    imported: &ImportedSession,
    label: &str,
) -> (ImportOutcome, Vec<Message>) {
    let root = broca_test_support::scratch_root()
        .join(format!("ck-import-claude-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let store = FileWalStore::new(root.join("wal"));
    let session = BindIdentity::new(&root, "claude-import-test", label);
    let writer = WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap();
    let outcome = run_import(request(&session, imported, label), &writer, &store)
        .await
        .unwrap();
    writer.shutdown().await.unwrap();
    let records = store
        .read_records(&session)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect::<Vec<_>>();
    let prompt = replay(&records).unwrap().prompt;
    std::fs::remove_dir_all(root).unwrap();
    (outcome, prompt)
}

fn assert_actual_results(prompt: &[Message]) {
    let outputs = prompt
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::ToolResult(result) => match &result.output {
                ToolOutput::Text { text } if !result.is_error => {
                    Some((result.tool_call_id.0.as_str(), text.as_str()))
                }
                _ => panic!("unexpected tool result {result:?}"),
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(outputs, [("toolu_A", "ACTUAL_A"), ("toolu_B", "ACTUAL_B")]);
}

#[tokio::test]
async fn split_claude_code_tools_import_without_synthesizing_or_dropping_results() {
    let (outcome, prompt) = import_and_replay(&fixture("split"), "split").await;
    assert_eq!(
        outcome.synthesized_tool_results, 0,
        "split response must not synthesize interruptions"
    );
    assert_eq!(outcome.dropped_orphan_tool_results, 0);
    assert_eq!(
        prompt
            .iter()
            .map(|message| message.role)
            .collect::<Vec<_>>(),
        [Role::User, Role::Assistant, Role::Tool]
    );
    assert_actual_results(&prompt);
}

#[tokio::test]
async fn interleaved_claude_code_results_keep_their_source_position() {
    let (outcome, prompt) = import_and_replay(&fixture("interleaved"), "interleaved").await;
    assert_eq!(outcome.synthesized_tool_results, 0);
    assert_eq!(outcome.dropped_orphan_tool_results, 0);
    assert_eq!(
        prompt
            .iter()
            .map(|message| message.role)
            .collect::<Vec<_>>(),
        [
            Role::User,
            Role::Assistant,
            Role::Tool,
            Role::Assistant,
            Role::Tool
        ]
    );
    assert_actual_results(&prompt);
}

#[tokio::test]
async fn existing_split_import_is_not_rewritten_by_reimport() {
    let fixed = fixture("split");
    let mut legacy = fixed.clone();
    // Reproduce the reader's old output so this test starts with an already-durable bad import.
    let mut second_fragment = legacy.messages[1].clone();
    second_fragment.content = legacy.messages[1].content.split_off(1);
    legacy.messages.insert(2, second_fragment);

    let root = broca_test_support::scratch_root()
        .join(format!("ck-import-claude-existing-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let store = FileWalStore::new(root.join("wal"));
    let session = BindIdentity::new(&root, "claude-import-test", "existing");
    let writer = WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap();
    let old = run_import(request(&session, &legacy, "existing"), &writer, &store)
        .await
        .unwrap();
    assert_eq!(old.synthesized_tool_results, 1);
    assert_eq!(old.dropped_orphan_tool_results, 1);
    let before = store.read_records(&session).unwrap();
    let retry = run_import(request(&session, &legacy, "existing"), &writer, &store)
        .await
        .unwrap();
    assert!(retry.already_imported);
    assert_eq!(
        run_import(request(&session, &fixed, "existing"), &writer, &store).await,
        Err(ImportError::ImportIdReuse)
    );
    assert_eq!(
        run_import(request(&session, &fixed, "new-run"), &writer, &store).await,
        Err(ImportError::LineageNotEmpty)
    );
    assert_eq!(store.read_records(&session).unwrap().len(), before.len());
    writer.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
