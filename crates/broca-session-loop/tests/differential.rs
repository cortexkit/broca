//! Today's loop and the new loop, driven by the same scripted provider and tool plane, must
//! make the same run.
//!
//! Each scenario runs once through today's loop (`broca_core::run_loop`) on a real file log,
//! and once through the new loop on a new-format session. Today's log is converted with the
//! proven converter, so both runs are compared as new-format records. Three things must match:
//! - **the requests:** every `CallOptions` the provider was asked to send, compared whole,
//!   including the render inputs that never serialize;
//! - **the records:** kind by kind and field by field, apart from fields that hold a time or
//!   an id minted per log, which the harness normalizes by name;
//! - **the tool calls:** what the tool plane was asked to run, in order.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::{
    provider::Provider, run_loop, AllowAll, FileLeaseStore, FileWalStore, RunRequest, StubReply,
    StubToolPlane,
};
use broca_protocol::content::{AssistantMessage, ContentBlock, Message, Role, ToolCall};
use broca_protocol::provider::{NormalizedEvent, ProviderControl};
use broca_protocol::request::{
    CallOptions, ExecutionMode, ModelHandle, StopCondition, Tool, ToolChoice,
};
use broca_protocol::{FinishReason, GenerationConfig, MessageId, RunId, StepId, ToolCallId, Usage};
use broca_crash_harness::wal::LogImage;
use broca_session_loop::{resume, run, Admission, MemoryParts, Seams, Writer};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::Record;
use broca_session_store::session::{Session, SessionError};
use serde_json::{json, Value};
use tokio::sync::mpsc;

/// Serves scripted steps in order and records every request it was asked to send.
#[derive(Clone)]
struct ScriptedProvider {
    scripts: Arc<Mutex<VecDeque<Vec<NormalizedEvent>>>>,
    sent: Arc<Mutex<Vec<CallOptions>>>,
}

impl ScriptedProvider {
    fn new(scripts: Vec<Vec<NormalizedEvent>>) -> Self {
        Self {
            scripts: Arc::new(Mutex::new(scripts.into())),
            sent: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

#[async_trait]
impl Provider for ScriptedProvider {
    async fn step(&self, _step_id: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.sent.lock().unwrap().push(opts.clone());
        let script = self.scripts.lock().unwrap().pop_front().unwrap_or_default();
        let (tx, rx) = mpsc::channel(64);
        tokio::spawn(async move {
            for event in script {
                let _ = tx.send(event).await;
            }
        });
        rx
    }
}

fn sent_requests(provider: &ScriptedProvider) -> Vec<CallOptions> {
    let sent = provider.sent.lock().unwrap();
    sent.clone()
}

fn finished(reason: FinishReason) -> NormalizedEvent {
    NormalizedEvent::Control(ProviderControl::Finished {
        usage: Usage {
            input_tokens: Some(100),
            output_tokens: Some(20),
            ..Usage::default()
        },
        finish_reason: reason,
        retries_used: 0,
    })
}

fn answer(id: &str, content: Vec<ContentBlock>, reason: FinishReason) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content,
            },
        }),
        finished(reason),
    ]
}

fn text_step(id: &str, text: &str) -> Vec<NormalizedEvent> {
    answer(
        id,
        vec![ContentBlock::Text { text: text.into() }],
        FinishReason::Stop,
    )
}

fn call(id: &str, name: &str, input: Value) -> ContentBlock {
    ContentBlock::ToolCall(ToolCall {
        thought_signature: None,
        tool_call_id: ToolCallId(id.into()),
        tool_name: name.into(),
        input,
    })
}

fn tool(name: &str, mode: ExecutionMode) -> Tool {
    Tool {
        name: name.into(),
        description: format!("{name} something"),
        input_schema: json!({"type": "object"}),
        module: None,
        execution_mode: mode,
        reply: None,
    }
}

/// One scenario: what the caller sends and what the provider and tools answer.
struct Scenario {
    input: Vec<Message>,
    tools: Vec<Tool>,
    stop_when: Vec<StopCondition>,
    steps: Vec<Vec<NormalizedEvent>>,
}

impl Scenario {
    fn plane(&self) -> StubToolPlane {
        StubToolPlane::new()
            .with_canned_text("read", "file contents")
            .with_tool("grep", |_| StubReply::ExecError("no match".into()))
            .with_tool("fetch", |_| {
                StubReply::PlaneUnavailable("route closed".into())
            })
            .with_tool("write", |_| {
                StubReply::OutcomeUnknown("connection reset".into())
            })
    }

    fn request(&self) -> RunRequest {
        RunRequest {
            route_decision_id: None,
            input_written_at_ms: None,
            directives: None,
            session_class: None,
            retention: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("r1".into()),
            session: bind(),
            model: ModelHandle::new("stub", "m"),
            input: self.input.clone(),
            tools: self.tools.clone(),
            policy_allowed: self.tools.iter().map(|t| t.name.clone()).collect(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            cache: None,
            stop_when: self.stop_when.clone(),
            submission_id: None,
            budget: None,
            context_limit: None,
            work_class: None,
            context_management: None,
            variant: None,
            service_tier: None,
            inherit_frozen_cache: false,
            structured_output: None,
        }
    }

    fn admission(&self, provider: &ScriptedProvider) -> Admission {
        Admission {
            run_id: "r1".into(),
            model: ModelHandle::new("stub", "m"),
            input: self.input.clone(),
            tools: self.tools.clone(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            response_format: Default::default(),
            provider_options: Value::Null,
            render_config: provider.render_config_blob(),
            renderer_version: provider.renderer_version(),
            stop_when: self.stop_when.clone(),
            ts_ms: Some(0),
            written_at_ms: Some(0),
        }
    }
}

/// What one loop did: its records as new-format records, the requests it sent, and the calls
/// its tool plane ran.
#[derive(Debug)]
struct Outcome {
    records: Vec<Value>,
    requests: Vec<CallOptions>,
    calls: Vec<String>,
}

async fn today(scenario: &Scenario) -> Outcome {
    today_from(scenario, None, scenario.steps.clone()).await.0
}

/// Runs today's loop, starting from the log bytes `start` when given (a crash's cut log, which
/// today's loop resumes), with the provider serving `steps`. Returns what it did, its whole
/// log, and the log's file name.
async fn today_from(
    scenario: &Scenario,
    start: Option<(&str, &[u8])>,
    steps: Vec<Vec<NormalizedEvent>>,
) -> (Outcome, LogImage, String) {
    let dir = broca_test_support::scratch_root().join(format!(
        "session-loop-differential-{}-{}",
        std::process::id(),
        rand_suffix()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(dir.join("wal")).unwrap();
    if let Some((name, bytes)) = start {
        std::fs::write(dir.join("wal").join(name), bytes).unwrap();
    }
    let provider = ScriptedProvider::new(steps);
    let plane = scenario.plane();
    let lease = FileLeaseStore::new(dir.join("lease"));
    let wal = FileWalStore::new(dir.join("wal"));
    let (tx, mut rx) = mpsc::channel(512);
    let drain = tokio::spawn(async move { while rx.recv().await.is_some() {} });
    run_loop(
        scenario.request(),
        &provider,
        &plane,
        &lease,
        &AllowAll,
        &wal,
        &tx,
    )
    .await
    .expect("today's loop runs");
    drop(tx);
    drain.await.unwrap();
    let file = std::fs::read_dir(dir.join("wal"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .find(|path| path.extension().is_some_and(|ext| ext == "wal"))
        .expect("today's loop wrote a log");
    let image = LogImage::read(&file);
    let name = file.file_name().unwrap().to_string_lossy().into_owned();
    let converted =
        broca_session_convert::convert(&image.framed(), None).expect("the log converts");
    let _ = std::fs::remove_dir_all(&dir);
    (
        Outcome {
            records: normalized(&converted.records),
            requests: sent_requests(&provider),
            calls: plane
                .log()
                .iter()
                .map(|event| format!("{event:?}"))
                .collect(),
        },
        image,
        name,
    )
}

/// A session writer that also keeps every record it appended, for comparison.
struct Recording<'s, 'b> {
    session: &'s mut Session<'b, MemoryBackend>,
    records: Vec<Record>,
}

#[async_trait(?Send)]
impl Writer for Recording<'_, '_> {
    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        self.records.extend_from_slice(records);
        self.session.append(records, barrier).await
    }
    fn conversation(&self) -> &Conversation {
        &self.session.conversation
    }
    fn operations(&self) -> &Operations {
        &self.session.operations
    }
    fn session_id(&self) -> String {
        Writer::session_id(&*self.session)
    }
}

/// Both loops route tool calls under one bind, so their requests carry the same session
/// address and their tool calls the same bind.
fn bind() -> broca_protocol::BindIdentity {
    broca_protocol::BindIdentity::new("/differential/project", "differential", "s1")
}

async fn new_loop(scenario: &Scenario) -> Outcome {
    new_from(scenario, None, scenario.steps.clone()).await
}

/// Runs the new loop on a new session. With `start` (a crash's cut log, converted), the session
/// first holds those records and the loop resumes its run; the outcome's records are only what
/// the loop appended.
async fn new_from(
    scenario: &Scenario,
    start: Option<broca_session_convert::Converted>,
    steps: Vec<Vec<NormalizedEvent>>,
) -> Outcome {
    let bind = bind();
    let backend = MemoryBackend::default();
    let triple = SessionTriple {
        project_root: "/project".into(),
        harness: "differential".into(),
        session: "s1".into(),
    };
    let mut session = Session::create(
        &backend,
        SessionAddr("0123456789abcdef".into()),
        triple,
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap();
    let mut parts = MemoryParts::default();
    let resuming = match start {
        Some(converted) if converted.records.iter().any(|r| matches!(r, Record::RunStart(_))) => {
            session.append(&converted.records, true).await.unwrap();
            parts.parts = converted.parts;
            true
        }
        _ => false,
    };
    let provider = ScriptedProvider::new(steps);
    let plane = scenario.plane();
    let mut writer = Recording {
        session: &mut session,
        records: Vec::new(),
    };
    let now = || 0u64;
    let seams = Seams {
        provider: &provider,
        tools: &plane,
        bind: bind.clone(),
        out: None,
        cancel: Default::default(),
        now_ms: &now,
        session_address: Some(broca_core::wal_writer::session_addr(&bind)),
    };
    if resuming {
        resume(&mut writer, &parts, &seams).await
    } else {
        run(
            &mut writer,
            &mut parts,
            scenario.admission(&provider),
            &seams,
        )
        .await
    }
    .expect("the new loop runs");
    let records = writer.records.clone();
    Outcome {
        records: normalized(&records),
        requests: sent_requests(&provider),
        calls: plane
            .log()
            .iter()
            .map(|event| format!("{event:?}"))
            .collect(),
    }
}

fn rand_suffix() -> u64 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// The fields that hold a time or an id minted per log, replaced so two logs compare.
const NORMALIZED: &[&str] = &[
    "ts_ms",
    "written_at_ms",
    "finished_at_ms",
    "dispatched_at_ms",
    "last_content_at_ms",
    "call_key",
];

fn normalized(records: &[Record]) -> Vec<Value> {
    records
        .iter()
        .map(|record| {
            let mut value = serde_json::to_value(record).expect("records serialize");
            scrub(&mut value);
            json!({ record.name(): value })
        })
        .collect()
}

fn scrub(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, field) in map.iter_mut() {
                if NORMALIZED.contains(&key.as_str()) && !field.is_null() {
                    *field = json!("<normalized>");
                } else {
                    scrub(field);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(scrub),
        _ => {}
    }
}

async fn compare(scenario: Scenario) {
    let today = today(&scenario).await;
    let dump = std::env::var_os("DIFFERENTIAL_DUMP").is_some();
    if dump {
        for (index, record) in today.records.iter().enumerate() {
            eprintln!("today {index}: {record}");
        }
    }
    let new = new_loop(&scenario).await;
    if dump {
        for (index, record) in new.records.iter().enumerate() {
            eprintln!("new   {index}: {record}");
        }
    }
    assert_eq!(new.requests.len(), today.requests.len(), "request count");
    for (index, (new, today)) in new.requests.iter().zip(&today.requests).enumerate() {
        assert_eq!(new, today, "request {index}");
    }
    for (index, (new, today)) in new.records.iter().zip(&today.records).enumerate() {
        assert_eq!(new, today, "record {index}");
    }
    assert_eq!(new.records.len(), today.records.len(), "record count");
    assert_eq!(new.calls, today.calls, "tool calls");
}

fn user(text: &str) -> Message {
    Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
}

fn system(text: &str) -> Message {
    Message::new(Role::System, vec![ContentBlock::Text { text: text.into() }])
}

#[tokio::test]
async fn a_text_answer() {
    compare(Scenario {
        input: vec![system("Be brief."), user("hello")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps: vec![text_step("a1", "hi")],
    })
    .await;
}

#[tokio::test]
async fn a_tool_call_then_an_answer() {
    compare(Scenario {
        input: vec![user("read a")],
        tools: vec![tool("read", ExecutionMode::Sequential)],
        stop_when: vec![StopCondition::StepCount { n: 8 }],
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "read", json!({"path": "a"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    })
    .await;
}

#[tokio::test]
async fn parallel_calls_one_failing() {
    compare(Scenario {
        input: vec![user("look around")],
        tools: vec![
            tool("read", ExecutionMode::Parallel),
            tool("grep", ExecutionMode::Parallel),
        ],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    ContentBlock::Text {
                        text: "checking".into(),
                    },
                    call("c1", "read", json!({"path": "a"})),
                    call("c2", "grep", json!({"pattern": "x"})),
                ],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "done"),
        ],
    })
    .await;
}

#[tokio::test]
async fn sequential_calls_keep_their_order() {
    compare(Scenario {
        input: vec![user("read two files")],
        tools: vec![
            tool("read", ExecutionMode::Sequential),
            tool("grep", ExecutionMode::Parallel),
        ],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "read", json!({"path": "a"})),
                    call("c2", "grep", json!({"pattern": "b"})),
                    call("c3", "read", json!({"path": "c"})),
                ],
                FinishReason::ToolCalls,
            ),
            answer(
                "a2",
                vec![call("c4", "read", json!({"path": "d"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a3", "done"),
        ],
    })
    .await;
}

#[tokio::test]
async fn a_step_limit_with_calls_pending() {
    compare(Scenario {
        input: vec![user("read a")],
        tools: vec![tool("read", ExecutionMode::Parallel)],
        stop_when: vec![StopCondition::StepCount { n: 1 }],
        steps: vec![answer(
            "a1",
            vec![call("c1", "read", json!({"path": "a"}))],
            FinishReason::ToolCalls,
        )],
    })
    .await;
}

#[tokio::test]
async fn a_call_that_was_never_sent() {
    compare(Scenario {
        input: vec![user("fetch a")],
        tools: vec![tool("fetch", ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "fetch", json!({"url": "a"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "it failed"),
        ],
    })
    .await;
}

#[tokio::test]
async fn a_call_whose_outcome_is_unknown() {
    compare(Scenario {
        input: vec![user("write a")],
        tools: vec![tool("write", ExecutionMode::Parallel)],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![call("c1", "write", json!({"path": "a"}))],
                FinishReason::ToolCalls,
            ),
            text_step("a2", "should not be reached"),
        ],
    })
    .await;
}

/// Cuts today's log of `scenario` after each of its records, as a crash could, and resumes the
/// cut twice: today's loop resumes the cut log, and the new loop resumes a session holding the
/// same records converted. Both must send the same requests, run the same tool calls, and
/// append the same records.
async fn compare_every_cut(scenario: Scenario) {
    let (_, full, name) = today_from(&scenario, None, scenario.steps.clone()).await;
    let records = full.framed().len();
    assert!(records > 2, "the scenario wrote a log to cut");
    for cut in 1..records {
        let bytes = broca_crash_harness::cut_log(&full, cut, false);
        let frames = LogImage::parse(bytes.clone()).framed();
        // The provider serves the steps the cut log has no answer for yet.
        let answered = frames
            .iter()
            .filter(|frame| {
                serde_json::to_value(&frame.record).unwrap()["type"] == "model_step_finished"
            })
            .count();
        let steps = scenario.steps[answered..].to_vec();
        let converted = broca_session_convert::convert(&frames, None).expect("the cut converts");
        let before = converted.records.len();
        let (today, _, _) = today_from(&scenario, Some((&name, &bytes)), steps.clone()).await;
        let new = new_from(&scenario, Some(converted), steps).await;
        let today_appended = &today.records[before.min(today.records.len())..];
        let context = format!("cut after record {cut} of {records}");
        if std::env::var_os("DIFFERENTIAL_DUMP").is_some() {
            for (index, record) in today_appended.iter().enumerate() {
                eprintln!("{context} today {index}: {record}");
            }
            for (index, record) in new.records.iter().enumerate() {
                eprintln!("{context} new   {index}: {record}");
            }
        }
        assert_eq!(new.requests.len(), today.requests.len(), "{context}: request count");
        for (index, (new, today)) in new.requests.iter().zip(&today.requests).enumerate() {
            assert_eq!(new, today, "{context}: request {index}");
        }
        for (index, (new, today)) in new.records.iter().zip(today_appended).enumerate() {
            assert_eq!(new, today, "{context}: record {index}");
        }
        assert_eq!(new.records.len(), today_appended.len(), "{context}: record count");
        assert_eq!(new.calls, today.calls, "{context}: tool calls");
    }
}

#[tokio::test]
async fn every_cut_of_a_tool_run_resumes_alike() {
    compare_every_cut(Scenario {
        input: vec![user("read two files")],
        tools: vec![
            tool("read", ExecutionMode::Sequential),
            tool("grep", ExecutionMode::Parallel),
        ],
        stop_when: Vec::new(),
        steps: vec![
            answer(
                "a1",
                vec![
                    call("c1", "read", json!({"path": "a"})),
                    call("c2", "grep", json!({"pattern": "b"})),
                ],
                FinishReason::ToolCalls,
            ),
            answer("a2", vec![call("c3", "read", json!({"path": "c"}))], FinishReason::ToolCalls),
            text_step("a3", "done"),
        ],
    })
    .await;
}
