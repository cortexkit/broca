use std::path::PathBuf;

use broca_core::{
    replay, run_import, FileWalStore, ImportOrigin, ImportRequest, RunId, WalHandle, WalStore,
};
use broca_protocol::{BindIdentity, ContentBlock, ToolOutput};
use ck_session_import::{codex::CodexReader, HarnessReader};

#[tokio::test]
async fn parallel_codex_tools_import_without_synthesizing_or_dropping_results() {
    let fixture_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../ck-session-import/tests/fixtures/codex/parallel");
    let imported = CodexReader::new(fixture_root)
        .convert("codex-parallel-tools")
        .unwrap();
    let root = broca_test_support::scratch_root()
        .join(format!("ck-import-codex-parallel-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let store = FileWalStore::new(root.join("wal"));
    let session = BindIdentity::new(&root, "codex-import-test", "parallel-tools");
    let writer = WalHandle::spawn(store.open(&session).unwrap(), 1).expect("spawn writer");

    let outcome = run_import(
        ImportRequest {
            project_id: None,
            principal: None,
            run_id: RunId("run-codex-parallel-tools".into()),
            session: session.clone(),
            messages: imported.messages,
            origin: ImportOrigin {
                source_ref: imported.source_ref,
            },
        },
        &writer,
        &store,
    )
    .await
    .unwrap();
    assert_eq!(outcome.synthesized_tool_results, 0);
    assert_eq!(outcome.dropped_orphan_tool_results, 0);
    writer.shutdown().await;

    let records = store
        .read_records(&session)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect::<Vec<_>>();
    let state = replay(&records).unwrap();
    let outputs = state
        .prompt
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::ToolResult(result) => match &result.output {
                ToolOutput::Text { text } => Some((result.tool_call_id.0.as_str(), text.as_str())),
                _ => None,
            },
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(outputs, [("c1", "ok1"), ("c2", "ok2")]);

    let _ = std::fs::remove_dir_all(root);
}
