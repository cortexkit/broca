//! Crash-cut resume test for the real subc tool spine.
//!
//! Existing durability tests exercise resume through the in-process tool plane. This
//! test drives the same WAL cuts through the real path: `run_loop` →
//! `SubcToolPlane` → `subc-core` → `fake-aft-stub` → `run_loop`.
//!
//! `fake-aft-stub` writes one `{"kind":"tool_call", ...}` JSON line to
//! `FAKE_AFT_EVENTS_PATH` for each tool execution. The scripted tool inputs carry a
//! stable `call_id`, so the test can count executions per logical tool call. For a
//! cut that keeps a `tool_dispatch_intent` without the matching `tool_result`, a real
//! crash could have happened after the AFT side performed the work but before the
//! runner made the result durable. To exercise that worst case, the test seeds the
//! event log with the reference execution for every intent already present in the
//! truncated WAL, then resumes. The invariant is that resume never appends a second
//! execution for those intent-bearing calls; the run interrupts for review instead
//! of risking a duplicate side effect.
//!
//! For cuts that are not indeterminate, the resumed run finishes with the same
//! terminal reason as the no-crash reference run. Calls that still need dispatch on
//! resume must go through the real wire once and return the deterministic
//! `FAKE_AFT_TOOLCALL_RESULT` text.

use crate::common;

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};

use async_trait::async_trait;
use broca_core::{
    provider::Provider, run_loop, AllowAll, FileLeaseStore, FileWalStore, OutputEvent, RunRequest,
    WalStore,
};
use broca_protocol::{
    caller::{ControlUnit, RunFinishReason},
    content::{AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput},
    provider::{NormalizedEvent, ProviderControl},
    request::{CallOptions, ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice},
    BindIdentity, FinishReason, GenerationConfig, MessageId, RunId, StepId, ToolCallId, Usage,
};
use broca_subc::SubcToolPlane;
use serde_json::{json, Value};
use tokio::{
    sync::mpsc,
    time::{sleep, Duration},
};

use common::{wait_for_aft, Rig};

const TOOL_NAME: &str = "write_note";
const RESULT_TEXT: &str = "REAL-SUBC-RESUME-RESULT";
const RUN_ID: &str = "resume-spine";
const HARNESS: &str = "resume-spine";
const SESSION: &str = "s-resume-spine";
const CALL_IDS: [&str; 2] = ["spine_call_1", "spine_call_2"];
const WAL_HEADER_LEN: usize = 4 + 1 + 8 + 8 + 32;
const WAL_RECORD_FRAMING_VERSION: u8 = 1;
const WAL_GATED_FRAMING_VERSION: u8 = 3;

#[derive(Clone)]
struct StepScriptProvider {
    scripts: Arc<Vec<Vec<NormalizedEvent>>>,
}

impl StepScriptProvider {
    fn new() -> Self {
        Self {
            scripts: Arc::new(scripts()),
        }
    }
}

#[async_trait]
impl Provider for StepScriptProvider {
    async fn step(&self, step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        let idx = step_id.0.saturating_sub(1) as usize;
        let script = self.scripts.get(idx).cloned().unwrap_or_default();
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for ev in script {
                if tx.send(ev).await.is_err() {
                    break;
                }
            }
        });
        rx
    }
}

fn scripts() -> Vec<Vec<NormalizedEvent>> {
    vec![
        tool_step(
            "m1",
            "spine_call_1",
            json!({"call_id": "spine_call_1", "payload": "alpha"}),
        ),
        tool_step(
            "m2",
            "spine_call_2",
            json!({"call_id": "spine_call_2", "payload": "beta"}),
        ),
        text_step("m3", "both real-subc tool calls finished"),
    ]
}

fn tool_step(message_id: &str, tool_call_id: &str, input: Value) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(message_id.into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId(tool_call_id.into()),
                    tool_name: TOOL_NAME.into(),
                    input,
                    thought_signature: None,
                })],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(10),
                output_tokens: Some(2),
                ..Default::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

fn text_step(message_id: &str, text: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(message_id.into()),
                content: vec![ContentBlock::Text { text: text.into() }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(8),
                output_tokens: Some(3),
                ..Default::default()
            },
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

fn request(project_root: &Path) -> RunRequest {
    let tools = vec![Tool {
        name: TOOL_NAME.into(),
        description: "durability probe tool".into(),
        input_schema: json!({"type": "object"}),
        module: None,
        execution_mode: ExecutionMode::Parallel,
    }];
    let policy_allowed = tools.iter().map(|t| t.name.clone()).collect();
    RunRequest {
        retention: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId(RUN_ID.into()),
        session: BindIdentity::new(project_root, HARNESS, SESSION),
        model: ModelHandle::new("aft-stub", "stub-model"),
        input: vec![Message {
            cache_prefix_blocks: None,
            origin: None,
            ordinal: None,
            role: Role::User,
            content: vec![ContentBlock::Text {
                text: "write two notes through the real subc spine".into(),
            }],
        }],
        tools,
        policy_allowed,
        tool_choice: ToolChoice::Auto,
        generation: GenerationConfig::default(),
        cache: None,
        stop_when: vec![StopCondition::StepCount { n: 8 }],
        submission_id: None,
        budget: None,
        context_limit: None,
        work_class: None,
        context_management: None,
        variant: None,
        service_tier: None,
        inherit_frozen_cache: false,
    }
}

async fn run_once(rig: &Rig, state_dir: &Path) -> (RunFinishReason, Vec<OutputEvent>) {
    let conn = rig.connect().await;
    let plane = SubcToolPlane::new(conn, "aft");
    wait_for_aft(&plane, TOOL_NAME).await;

    let lease = FileLeaseStore::new(state_dir.join("leases"));
    let wal = FileWalStore::new(state_dir.join("wal"));
    let provider = StepScriptProvider::new();
    let (tx, mut rx) = mpsc::channel(512);
    let collector = tokio::spawn(async move {
        let mut events = Vec::new();
        while let Some(event) = rx.recv().await {
            events.push(event);
        }
        events
    });

    let summary = run_loop(
        request(&rig.dir),
        &provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        &tx,
    )
    .await
    .expect("run_loop should not fail its lease or WAL");
    drop(tx);
    (summary.reason, collector.await.unwrap())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn crash_cut_resume_uses_real_subc_spine_exactly_once() {
    let events_path = broca_test_support::scratch_root().join(format!(
        "broca-resume-spine-events-{}.jsonl",
        std::process::id()
    ));
    let events_path_s = events_path.to_string_lossy().into_owned();
    let Some(rig) = Rig::start(
        "resume-spine",
        &[
            ("FAKE_AFT_MODULE_ID", "aft"),
            ("FAKE_AFT_TOOLS", TOOL_NAME),
            ("FAKE_AFT_TOOLCALL_RESULT", RESULT_TEXT),
            ("FAKE_AFT_EVENTS_PATH", events_path_s.as_str()),
        ],
    )
    .await
    else {
        return; // skipped (binaries absent; Rig::start printed the build command)
    };

    reset_events(&events_path, &[]);
    let reference_dir = rig.dir.join("reference");
    fs::create_dir_all(&reference_dir).unwrap();
    let (reference_reason, reference_output) = run_once(&rig, &reference_dir).await;
    assert_eq!(reference_reason, RunFinishReason::Completed);
    assert_all_output_results_are_real_stub_text(&reference_output);

    let expected_reference_counts = expected_counts([(CALL_IDS[0], 1), (CALL_IDS[1], 1)]);
    wait_for_expected_event_counts(&events_path, &expected_reference_counts, "reference").await;
    let reference_counts =
        settle_event_counts(&events_path, &expected_reference_counts, "reference").await;
    assert_eq!(
        reference_counts, expected_reference_counts,
        "the no-crash reference executes each scripted tool call once through fake-aft-stub"
    );
    let reference_events = read_tool_exec_events(&events_path);
    let reference_events_by_call = one_event_per_call(&reference_events);

    let reference_wal = wal_path(&reference_dir, &rig.dir);
    let reference_frames = framed_records_with_offsets(&reference_wal);
    assert!(
        reference_frames.len() > 1,
        "reference WAL should contain enough records to crash-cut"
    );

    for keep in 1..=reference_frames.len() {
        let label = format!("cut@{keep}/{}", reference_frames.len());
        let cut_dir = rig.dir.join(format!("cut-{keep}"));
        fs::create_dir_all(&cut_dir).unwrap();
        let cut_wal = wal_path(&cut_dir, &rig.dir);
        copy_wal_prefix(&reference_wal, &cut_wal, reference_frames[keep - 1].1);

        let cut_records = records_only(&reference_frames[..keep]);
        let predicted_reason = if indeterminate_ids(&cut_records).is_empty() {
            reference_reason
        } else {
            RunFinishReason::Interrupted
        };
        let intents_before = intent_ids(&cut_records);
        let completed_before = completed_ids(&cut_records);
        let seed_events = seed_events_for_intents(&reference_events_by_call, &intents_before);
        let counts_before = event_counts_from_values(&seed_events);
        reset_events(&events_path, &seed_events);

        let (reason, output) = run_once(&rig, &cut_dir).await;
        assert_eq!(
            reason, predicted_reason,
            "{label}: resume terminal reason follows the WAL replay decision"
        );

        let expected_after = expected_event_counts(predicted_reason, &intents_before);
        wait_for_expected_event_counts(&events_path, &expected_after, &label).await;
        let counts_after = settle_event_counts(&events_path, &expected_after, &label).await;
        assert_eq!(
            counts_after,
            expected_after,
            "{label}: every call that could have left the process executes once, and resume never duplicates it"
        );

        for id in &completed_before {
            assert_eq!(
                counts_before.get(id).copied().unwrap_or(0),
                1,
                "{label}: {id} had one pre-crash execution before resume"
            );
            assert_eq!(
                counts_after.get(id).copied().unwrap_or(0),
                1,
                "{label}: {id} already had a durable result and was not re-dispatched through subc"
            );
        }

        if predicted_reason == RunFinishReason::Interrupted {
            assert!(
                intents_before
                    .iter()
                    .any(|id| !completed_before.contains(id)),
                "{label}: only intent-without-result cuts should interrupt"
            );
        } else {
            assert_all_output_results_are_real_stub_text(&output);
        }

        let final_records = read_wal_records(&cut_dir, &rig.dir);
        assert_final_wal(&label, predicted_reason, &final_records, &intents_before);
    }

    let _ = fs::remove_file(&events_path);
}

#[derive(Clone)]
struct ToolExecEvent {
    call_id: String,
    value: Value,
}

fn read_tool_exec_events(path: &Path) -> Vec<ToolExecEvent> {
    let Ok(contents) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let lines = contents.lines().collect::<Vec<_>>();
    let mut events = Vec::new();
    for (idx, line) in lines.iter().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let value: Value = match serde_json::from_str(line) {
            Ok(value) => value,
            Err(err) if idx + 1 == lines.len() => {
                // The fake stub appends JSONL concurrently with this test. A partial
                // final line means we observed an in-progress append; polling will
                // re-read once the line is complete. Any earlier malformed line is
                // not an append race and remains a hard failure.
                eprintln!("ignoring torn trailing fake-aft event line: {err}");
                continue;
            }
            Err(err) => panic!("non-final fake-aft event JSON is malformed: {err}; line={line:?}"),
        };
        if value.get("kind").and_then(Value::as_str) == Some("tool_call") {
            let call_id = value
                .get("arguments")
                .and_then(|args| args.get("call_id"))
                .and_then(Value::as_str)
                .expect("tool_call event includes the scripted call_id argument")
                .to_string();
            events.push(ToolExecEvent { call_id, value });
        }
    }
    events
}

async fn wait_for_expected_event_counts(
    path: &Path,
    expected: &BTreeMap<String, usize>,
    label: &str,
) -> BTreeMap<String, usize> {
    broca_test_support::wait_until(label, || async {
        let counts = event_counts(&read_tool_exec_events(path));
        assert_no_extra_events(&counts, expected, label);
        if &counts == expected {
            return Ok(counts);
        }
        Err(format!(
            "execution_events={counts:?}, expected={expected:?}, path={}",
            path.display()
        ))
    })
    .await
}

async fn settle_event_counts(
    path: &Path,
    expected: &BTreeMap<String, usize>,
    label: &str,
) -> BTreeMap<String, usize> {
    let mut counts = event_counts(&read_tool_exec_events(path));
    for _ in 0..5 {
        sleep(Duration::from_millis(25)).await;
        counts = event_counts(&read_tool_exec_events(path));
        assert_no_extra_events(&counts, expected, label);
    }
    counts
}

fn assert_no_extra_events(
    observed: &BTreeMap<String, usize>,
    expected: &BTreeMap<String, usize>,
    label: &str,
) {
    for (id, count) in observed {
        let expected_count = expected.get(id).copied().unwrap_or(0);
        assert!(
            *count <= expected_count,
            "{label}: observed {count} fake-aft executions for {id}, but expected at most {expected_count}"
        );
    }
}

fn one_event_per_call(events: &[ToolExecEvent]) -> BTreeMap<String, Value> {
    let mut by_call: BTreeMap<String, Value> = BTreeMap::new();
    for event in events {
        assert!(
            by_call
                .insert(event.call_id.clone(), event.value.clone())
                .is_none(),
            "reference run emitted more than one execution event for {}",
            event.call_id
        );
    }
    for id in CALL_IDS {
        assert!(
            by_call.contains_key(id),
            "reference run did not emit an execution event for {id}"
        );
    }
    by_call
}

fn seed_events_for_intents(
    reference_events_by_call: &BTreeMap<String, Value>,
    intents_before: &BTreeSet<String>,
) -> Vec<Value> {
    intents_before
        .iter()
        .map(|id| {
            reference_events_by_call
                .get(id)
                .unwrap_or_else(|| panic!("reference event missing for intent-bearing call {id}"))
                .clone()
        })
        .collect()
}

fn reset_events(path: &Path, events: &[Value]) {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let mut contents = String::new();
    for event in events {
        contents.push_str(&event.to_string());
        contents.push('\n');
    }
    fs::write(path, contents).unwrap();
}

fn event_counts(events: &[ToolExecEvent]) -> BTreeMap<String, usize> {
    let mut counts = BTreeMap::new();
    for event in events {
        *counts.entry(event.call_id.clone()).or_insert(0) += 1;
    }
    counts
}

fn event_counts_from_values(events: &[Value]) -> BTreeMap<String, usize> {
    let execs = events
        .iter()
        .map(|value| ToolExecEvent {
            call_id: value
                .get("arguments")
                .and_then(|args| args.get("call_id"))
                .and_then(Value::as_str)
                .expect("seeded fake-aft event includes call_id")
                .to_string(),
            value: value.clone(),
        })
        .collect::<Vec<_>>();
    event_counts(&execs)
}

fn expected_counts<const N: usize>(pairs: [(&str, usize); N]) -> BTreeMap<String, usize> {
    pairs
        .into_iter()
        .map(|(id, count)| (id.to_string(), count))
        .collect()
}

fn expected_event_counts(
    predicted_reason: RunFinishReason,
    intents_before: &BTreeSet<String>,
) -> BTreeMap<String, usize> {
    CALL_IDS
        .into_iter()
        .filter_map(|id| {
            let should_have_execution =
                intents_before.contains(id) || predicted_reason == RunFinishReason::Completed;
            should_have_execution.then(|| (id.to_string(), 1))
        })
        .collect()
}

fn assert_all_output_results_are_real_stub_text(events: &[OutputEvent]) {
    for event in events {
        if let OutputEvent::Control(ControlUnit::ToolResult { result }) = event {
            assert!(
                !result.is_error,
                "real fake-aft-stub should return a successful tool result for {}",
                result.tool_call_id.0
            );
            assert_eq!(
                tool_output_text(&result.output),
                RESULT_TEXT,
                "tool result text came from FAKE_AFT_TOOLCALL_RESULT over the real subc wire"
            );
        }
    }
}

fn tool_output_text(output: &ToolOutput) -> &str {
    match output {
        ToolOutput::Text { text } => text,
        ToolOutput::Error { message } => message,
        ToolOutput::Denied { reason } => reason,
        ToolOutput::Json { .. } => "",
    }
}

fn wal_path(state_dir: &Path, project_root: &Path) -> PathBuf {
    FileWalStore::new(state_dir.join("wal")).wal_path(&request(project_root).session)
}

fn framed_records_with_offsets(path: &Path) -> Vec<(Value, usize)> {
    let bytes = fs::read(path).unwrap();
    let mut offset = 0usize;
    let mut framed = Vec::new();
    while offset < bytes.len() {
        assert!(
            bytes.len() - offset >= WAL_HEADER_LEN,
            "reference WAL ended inside a frame header"
        );
        let len = u32::from_le_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
        let frame_end = offset + WAL_HEADER_LEN + len;
        assert!(
            frame_end <= bytes.len(),
            "reference WAL ended inside a frame payload"
        );
        let payload = &bytes[offset + WAL_HEADER_LEN..frame_end];
        match bytes[offset + 4] {
            WAL_RECORD_FRAMING_VERSION => {
                let record = serde_json::from_slice(payload).expect("WAL record JSON payload");
                framed.push((record, frame_end));
            }
            // A record that requires a WAL feature (a dispatch intent naming its module, say)
            // is a gated frame: its payload wraps the record as `{"requires", "record"}`.
            WAL_GATED_FRAMING_VERSION => {
                let gated: Value = serde_json::from_slice(payload).expect("gated WAL payload");
                framed.push((gated["record"].clone(), frame_end));
            }
            _ => {}
        }
        offset = frame_end;
    }
    framed
}

fn copy_wal_prefix(source: &Path, dest: &Path, keep_bytes: usize) {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent).unwrap();
    }
    let bytes = fs::read(source).unwrap();
    fs::write(dest, &bytes[..keep_bytes]).unwrap();
}

fn records_only(framed: &[(Value, usize)]) -> Vec<Value> {
    framed.iter().map(|(record, _)| record.clone()).collect()
}

fn read_wal_records(state_dir: &Path, project_root: &Path) -> Vec<Value> {
    let store = FileWalStore::new(state_dir.join("wal"));
    let session = request(project_root).session;
    store
        .read_records(&session)
        .unwrap()
        .into_iter()
        .map(|frame| serde_json::to_value(&frame.record).unwrap())
        .collect()
}

fn intent_ids(records: &[Value]) -> BTreeSet<String> {
    records
        .iter()
        .filter_map(|record| match record_type(record) {
            Some("tool_dispatch_intent") => string_field(record, "tool_call_id"),
            _ => None,
        })
        .collect()
}

fn completed_ids(records: &[Value]) -> BTreeSet<String> {
    records
        .iter()
        .filter_map(|record| match record_type(record) {
            Some("tool_result") => record
                .get("result")
                .and_then(|result| string_field(result, "tool_call_id")),
            Some("tool_no_dispatch") => string_field(record, "tool_call_id"),
            _ => None,
        })
        .collect()
}

fn indeterminate_ids(records: &[Value]) -> BTreeSet<String> {
    let intents = intent_ids(records);
    let completed = completed_ids(records);
    let mut indeterminate = intents
        .difference(&completed)
        .cloned()
        .collect::<BTreeSet<_>>();
    for record in records {
        if record_type(record) == Some("tool_result")
            && string_field(record, "outcome").as_deref() == Some("unknown")
        {
            if let Some(id) = record
                .get("result")
                .and_then(|result| string_field(result, "tool_call_id"))
            {
                indeterminate.insert(id);
            }
        }
    }
    indeterminate
}

fn assert_final_wal(
    label: &str,
    predicted_reason: RunFinishReason,
    records: &[Value],
    intents_before: &BTreeSet<String>,
) {
    let final_intents = intent_ids(records);
    assert_eq!(
        final_intents,
        expected_intents(predicted_reason, intents_before),
        "{label}: final WAL contains exactly the intended dispatch intents"
    );

    let mut result_counts: BTreeMap<String, usize> = BTreeMap::new();
    for record in records {
        if record_type(record) != Some("tool_result") {
            continue;
        }
        let result = record.get("result").expect("tool_result has result body");
        let id = string_field(result, "tool_call_id").expect("tool_result has tool_call_id");
        *result_counts.entry(id.clone()).or_insert(0) += 1;
        if string_field(record, "outcome").as_deref() == Some("completed") {
            assert_eq!(
                result.get("is_error").and_then(Value::as_bool),
                Some(false),
                "{label}: completed tool result for {id} should not be an error"
            );
            assert_eq!(
                result_text(result),
                Some(RESULT_TEXT),
                "{label}: completed tool result text should be the real fake-aft-stub response"
            );
        }
    }

    for id in CALL_IDS {
        let expected_results = if predicted_reason == RunFinishReason::Completed
            || intents_before.contains(id) && result_counts.contains_key(id)
        {
            1
        } else {
            0
        };
        assert_eq!(
            result_counts.get(id).copied().unwrap_or(0),
            expected_results,
            "{label}: final WAL has the expected number of tool_result records for {id}"
        );
    }
}

fn record_type(record: &Value) -> Option<&str> {
    record.get("type").and_then(Value::as_str)
}

fn string_field(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(Value::as_str).map(str::to_string)
}

fn result_text(result: &Value) -> Option<&str> {
    result
        .get("output")
        .and_then(|output| output.get("text"))
        .and_then(Value::as_str)
}

fn expected_intents(
    predicted_reason: RunFinishReason,
    intents_before: &BTreeSet<String>,
) -> BTreeSet<String> {
    if predicted_reason == RunFinishReason::Completed {
        CALL_IDS.into_iter().map(str::to_string).collect()
    } else {
        intents_before.clone()
    }
}

// Keep a direct ref to subc_protocol so the manifest match in common resolves.
use subc_protocol as _;
