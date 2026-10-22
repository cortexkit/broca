//! Multi-module real-subc spine coverage for per-tool module routing.
//!
//! One `SubcToolPlane` session opens one route per hinted module, dispatches calls in one
//! batch through the real daemon, and keeps the existing `(session, tool_call_id)` fence
//! independent of the route split.

use crate::common;

use std::{fs, path::Path, time::Duration};

use broca_core::{BatchCtx, DispatchResult, ToolManifest, ToolPlane};
use broca_protocol::{
    content::{ToolCall, ToolOutput},
    request::{ExecutionMode, Tool},
    BatchId, BindIdentity, ToolCallId, ToolResult,
};
use broca_subc::SubcToolPlane;
use serde_json::{json, Value};
use tokio::time::{sleep, Instant};

use common::{checkout_bin, decide_subc_bin, subc_required, wait_for_module_tool, ModuleSpec, Rig};

const MODULE_A: &str = "aft-a";
const MODULE_B: &str = "aft-b";
const TOOL_A: &str = "tool_a";
const TOOL_B: &str = "tool_b";
const RESULT_A: &str = "RESULT-A";
const RESULT_B: &str = "RESULT-B";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_batch_routes_each_call_to_its_hinted_module_and_fences_redispatch() {
    let Some((rig, events_a, events_b)) = start_two_stub_rig("multi-module-spine").await else {
        return;
    };
    let plane = ready_plane(&rig).await;
    let bind = BindIdentity::new(&rig.dir, "multi-module-spine", "s1");
    let ctx = BatchCtx {
        batch_id: BatchId("batch-1".into()),
        bind: bind.clone(),
        run_id: broca_protocol::RunId("r1".into()),
        scope: None,
    };
    let manifest = ToolManifest::from_tools(hinted_tools());
    let calls = calls();

    plane.batch_started(&ctx, &calls, &manifest).await.unwrap();
    let first_a = completed(plane.dispatch(&ctx, &calls[0]).await);
    let first_b = completed(plane.dispatch(&ctx, &calls[1]).await);
    assert_text(&first_a, RESULT_A);
    assert_text(&first_b, RESULT_B);

    let second_a = completed(plane.dispatch(&ctx, &calls[0]).await);
    let second_b = completed(plane.dispatch(&ctx, &calls[1]).await);
    assert_eq!(
        second_a, first_a,
        "redispatch should return A's fenced result"
    );
    assert_eq!(
        second_b, first_b,
        "redispatch should return B's fenced result"
    );

    wait_for_tool_calls(&events_a, TOOL_A, 1, "module A").await;
    wait_for_tool_calls(&events_b, TOOL_B, 1, "module B").await;
    assert_eq!(count_tool_calls(&events_a, TOOL_A), 1);
    assert_eq!(count_tool_calls(&events_b, TOOL_B), 1);
    plane.batch_finished(&ctx).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_session_routes_sweeps_all_module_routes() {
    let Some((rig, events_a, events_b)) = start_two_stub_rig("multi-module-close").await else {
        return;
    };
    let plane = ready_plane(&rig).await;
    let bind = BindIdentity::new(&rig.dir, "multi-module-spine", "s-close");
    let ctx = BatchCtx {
        batch_id: BatchId("batch-close".into()),
        bind: bind.clone(),
        run_id: broca_protocol::RunId("r1".into()),
        scope: None,
    };
    let manifest = ToolManifest::from_tools(hinted_tools());
    let calls = calls();

    plane.batch_started(&ctx, &calls, &manifest).await.unwrap();
    assert_text(&completed(plane.dispatch(&ctx, &calls[0]).await), RESULT_A);
    assert_text(&completed(plane.dispatch(&ctx, &calls[1]).await), RESULT_B);
    plane.batch_finished(&ctx).await.unwrap();

    plane.close_session_routes(&bind).await;
    wait_for_kind(&events_a, "detach", 1, "module A detach").await;
    wait_for_kind(&events_b, "detach", 1, "module B detach").await;
}

async fn start_two_stub_rig(tag: &str) -> Option<(Rig, std::path::PathBuf, std::path::PathBuf)> {
    let fake_aft_path = checkout_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN");
    let fake_aft = decide_subc_bin(
        fake_aft_path.exists().then_some(fake_aft_path),
        subc_required(),
        "fake-aft-stub",
    )?;
    let events_a = broca_test_support::scratch_root().join(format!(
        "broca-multi-module-{tag}-a-{}.jsonl",
        std::process::id()
    ));
    let events_b = broca_test_support::scratch_root().join(format!(
        "broca-multi-module-{tag}-b-{}.jsonl",
        std::process::id()
    ));
    let _ = fs::remove_file(&events_a);
    let _ = fs::remove_file(&events_b);

    let rig = Rig::start_with_modules(
        tag,
        vec![
            stub_module(&fake_aft, MODULE_A, TOOL_A, RESULT_A, &events_a),
            stub_module(&fake_aft, MODULE_B, TOOL_B, RESULT_B, &events_b),
        ],
    )
    .await?;
    Some((rig, events_a, events_b))
}

fn stub_module(
    program: &Path,
    module_id: &str,
    tool_name: &str,
    result: &str,
    events_path: &Path,
) -> ModuleSpec {
    ModuleSpec {
        module_id: module_id.into(),
        program: program.to_path_buf(),
        args: Vec::new(),
        env: vec![
            ("FAKE_AFT_MODULE_ID".into(), module_id.into()),
            ("FAKE_AFT_TOOLS".into(), tool_name.into()),
            ("FAKE_AFT_TOOLCALL_RESULT".into(), result.into()),
            (
                "FAKE_AFT_EVENTS_PATH".into(),
                events_path.to_string_lossy().into_owned(),
            ),
        ],
    }
}

async fn ready_plane(rig: &Rig) -> SubcToolPlane {
    let plane = SubcToolPlane::new(rig.connect().await, MODULE_A);
    wait_for_module_tool(&plane, MODULE_A, TOOL_A).await;
    wait_for_module_tool(&plane, MODULE_B, TOOL_B).await;
    plane
}

fn hinted_tools() -> Vec<Tool> {
    vec![tool(TOOL_A, MODULE_A), tool(TOOL_B, MODULE_B)]
}

fn tool(name: &str, module: &str) -> Tool {
    Tool {
        name: name.into(),
        description: format!("{name} test tool"),
        input_schema: json!({"type": "object"}),
        module: Some(module.into()),
        execution_mode: ExecutionMode::Parallel,
        reply: None,
    }
}

fn calls() -> Vec<ToolCall> {
    vec![
        ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId("call-a".into()),
            tool_name: TOOL_A.into(),
            input: json!({"module": "a"}),
        },
        ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId("call-b".into()),
            tool_name: TOOL_B.into(),
            input: json!({"module": "b"}),
        },
    ]
}

fn completed(result: DispatchResult) -> ToolResult {
    match result {
        DispatchResult::Completed(result) => result,
        DispatchResult::NotSent { reason } => panic!("tool was not sent: {reason}"),
        DispatchResult::SentButOutcomeUnknown { reason } => {
            panic!("tool outcome was unknown: {reason}")
        }
        DispatchResult::ScopeDrained { reason, sent } => {
            panic!("tool route was drained ({reason}, sent: {sent})")
        }
    }
}

fn assert_text(result: &ToolResult, expected: &str) {
    assert!(
        !result.is_error,
        "tool result should be successful: {result:?}"
    );
    assert!(
        matches!(&result.output, ToolOutput::Text { text } if text == expected),
        "unexpected tool output: {:?}",
        result.output
    );
}

async fn wait_for_tool_calls(path: &Path, tool_name: &str, expected: usize, label: &str) {
    wait_until(label, || count_tool_calls(path, tool_name) >= expected).await;
}

async fn wait_for_kind(path: &Path, kind: &str, expected: usize, label: &str) {
    wait_until(label, || count_kind(path, kind) >= expected).await;
}

async fn wait_until(label: &str, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if done() {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {label}");
        sleep(Duration::from_millis(25)).await;
    }
}

fn count_tool_calls(path: &Path, tool_name: &str) -> usize {
    read_events(path)
        .into_iter()
        .filter(|event| event["kind"] == "tool_call" && event["name"] == tool_name)
        .count()
}

fn count_kind(path: &Path, kind: &str) -> usize {
    read_events(path)
        .into_iter()
        .filter(|event| event["kind"] == kind)
        .count()
}

fn read_events(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter_map(|line| serde_json::from_str(line).ok())
        .collect()
}
