use std::sync::Arc;

use async_trait::async_trait;
use broca_core::{FileWalStore, MaterializedStore, WalStore};
use broca_protocol::content::{
    AssistantMessage, ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult,
};
use broca_protocol::{
    BatchId, BindIdentity, MessageId, RunFinishReason, StepId, ToolCallId, Usage,
};
use broca_wal::record::DispatchOutcome;
use broca_wal::{Wal, WalRecord};
use cortexkit_role_llm_runner::tool_calls::{outcomes, ToolCallsPage, ToolCallsRequest};
use serde_json::{json, Value};

use super::{LineageKey, ServeError, ServeHandler, SessionReadRequest};

struct NeverOpen;

#[async_trait]
impl crate::registry::LineageOpener for NeverOpen {
    async fn open(
        &self,
        _key: &LineageKey,
        _gen: u64,
        _registry: crate::registry::RegistryHandle,
    ) -> Result<crate::registry::ActorHandle, crate::registry::OpenError> {
        panic!("run.tool_calls must not open a session actor")
    }
}

struct ClosedSession {
    _root: std::path::PathBuf,
    registry: crate::LineageRegistry,
    key: LineageKey,
    wal_store: Arc<FileWalStore>,
    store: Arc<broca_store::SqliteStore>,
    wal: Option<Box<dyn Wal + Send>>,
}

impl ClosedSession {
    fn new(name: &str) -> Self {
        Self::with_directives(name, None)
    }

    fn with_directives(name: &str, directives: Option<Value>) -> Self {
        let root = broca_test_support::scratch_root()
            .join(format!("run-tool-calls-{name}-{}", std::process::id()));
        let project = root.join("project");
        std::fs::create_dir_all(&project).unwrap();
        let key = BindIdentity::new(project, "runner", name);
        let wal_store = Arc::new(FileWalStore::new(root.join("wal")));
        let store = Arc::new(broca_store::SqliteStore::open(root.join("store.db")).unwrap());
        let mut wal = wal_store.open(&key).unwrap();
        let mut started = json!({
            "type": "run_started",
            "run_id": "run-1",
            "session": key.clone(),
            "config": {
                "model": {"provider_module_id": "provider", "model_id": "model"},
                "tools": [],
                "tool_choice": {"type": "auto"},
                "generation": {},
                "response_format": {"type": "text"},
                "context_epoch": 0,
                "renderer_version": 1
            },
            "input": [],
            "origin": {"kind": "fresh"}
        });
        if let Some(directives) = directives {
            started["config"]["turn_directives_v1"] = directives;
        }
        let started: WalRecord = serde_json::from_value(started).unwrap();
        wal.append(1, &started, true).unwrap();
        let registry = crate::LineageRegistry::new_with_reader(
            Arc::new(NeverOpen),
            wal_store.clone(),
            Some(store.clone()),
        );
        Self {
            _root: root,
            registry,
            key,
            wal_store,
            store,
            wal: Some(wal),
        }
    }

    fn append(&mut self, record: WalRecord) {
        self.wal
            .as_mut()
            .expect("writer is open")
            .append(1, &record, true)
            .unwrap();
    }

    fn close_writer(&mut self) {
        drop(self.wal.take());
    }

    fn write_tool_turn(&mut self, calls: &[(String, Value)], results: Option<&[String]>) {
        self.write_tool_turn_with("batch-1", 1, calls, results);
    }

    fn write_tool_turn_with(
        &mut self,
        batch_id: &str,
        step: u64,
        calls: &[(String, Value)],
        results: Option<&[String]>,
    ) {
        let step_id = StepId(step);
        let batch_id = BatchId(batch_id.into());
        let tool_calls: Vec<_> = calls
            .iter()
            .map(|(id, args)| ToolCall {
                tool_call_id: ToolCallId(id.clone()),
                tool_name: "write_file".into(),
                input: args.clone(),
                thought_signature: None,
            })
            .collect();
        self.append(WalRecord::StepStarted { step_id });
        self.append(WalRecord::ModelStepFinished {
            step_id,
            assistant_message: Arc::new(AssistantMessage {
                message_id: MessageId("assistant-1".into()),
                content: tool_calls
                    .iter()
                    .cloned()
                    .map(ContentBlock::ToolCall)
                    .collect(),
            }),
            usage: Usage::default(),
            finish_reason: Some(broca_protocol::content::FinishReason::ToolCalls),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
            hooks: None,
            tags: None,
        });
        self.append(WalRecord::ToolBatchStarted {
            batch_id: batch_id.clone(),
            step_id,
            planned_calls: calls.iter().map(|(id, _)| ToolCallId(id.clone())).collect(),
        });
        for (index, (id, args)) in calls.iter().enumerate() {
            let tool_call_id = ToolCallId(id.clone());
            self.append(WalRecord::ToolDispatchIntent {
                batch_id: batch_id.clone(),
                tool_call_id: tool_call_id.clone(),
                tool_name: Some("write_file".into()),
                args: Arc::new(args.clone()),
                module_id: Some("test-tools".into()),
                schema_pin: None,
                hooks: None,
                approval_digest: None,
                call_key: None,
            });
            if let Some(results) = results {
                self.append(WalRecord::ToolResult {
                    batch_id: batch_id.clone(),
                    result: Arc::new(ToolResult {
                        tool_call_id,
                        output: ToolOutput::Text {
                            text: results[index].clone(),
                        },
                        is_error: false,
                    }),
                    outcome: DispatchOutcome::Completed,
                    hooks: None,
                    reason: None,
                    withdraw_elicitation: None,
                    provider_held: None,
                    tags: None,
                });
            }
        }
        if results.is_some() {
            self.append(WalRecord::ToolBatchFinished { batch_id });
            self.append(WalRecord::TurnFinished { step_id });
        }
    }

    fn steer(&mut self, text: &str) {
        self.append(WalRecord::Steered {
            tags: None,
            retention: None,
            ts_ms: None,
            title: None,
            send_id: Some(format!("steer-{text}")),
            request_identity_digest: None,
            delivery: Some(broca_protocol::Delivery::Steer),
            input: Arc::new(vec![Message::new(
                Role::User,
                vec![ContentBlock::Text { text: text.into() }],
            )]),
            hooks: None,
        });
    }

    fn materialize_through_batch(&self, batch_id: &str) -> u64 {
        let records = self.wal_store.read_records(&self.key).unwrap();
        let batch_end = records
            .iter()
            .position(|framed| {
                matches!(
                    &framed.record,
                    WalRecord::ToolBatchFinished { batch_id: finished }
                        if finished.0 == batch_id
                )
            })
            .expect("the requested tool batch is finished");
        let end = records[batch_end + 1..]
            .iter()
            .position(|framed| matches!(&framed.record, WalRecord::TurnFinished { .. }))
            .map(|offset| batch_end + 1 + offset)
            .expect("the completed tool turn has a durable boundary");
        self.store.project(&self.key, &records[..=end]).unwrap()
    }

    fn write_duplicate_id_tool_turn(&mut self) {
        let call_id = ToolCallId("duplicate-id".into());
        let calls = [
            ToolCall {
                tool_call_id: call_id.clone(),
                tool_name: "write_file".into(),
                input: json!({"path":"first"}),
                thought_signature: None,
            },
            ToolCall {
                tool_call_id: call_id.clone(),
                tool_name: "write_file".into(),
                input: json!({"path":"duplicate"}),
                thought_signature: None,
            },
        ];
        let step_id = StepId(1);
        let batch_id = BatchId("batch-duplicate-id".into());
        self.append(WalRecord::ModelStepFinished {
            step_id,
            assistant_message: Arc::new(AssistantMessage {
                message_id: MessageId("assistant-duplicate-id".into()),
                content: calls.iter().cloned().map(ContentBlock::ToolCall).collect(),
            }),
            usage: Usage::default(),
            finish_reason: Some(broca_protocol::content::FinishReason::ToolCalls),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
            hooks: None,
            tags: None,
        });
        self.append(WalRecord::ToolBatchStarted {
            batch_id: batch_id.clone(),
            step_id,
            planned_calls: vec![call_id.clone(), call_id.clone()],
        });
        self.append(WalRecord::ToolDispatchIntent {
            batch_id: batch_id.clone(),
            tool_call_id: call_id.clone(),
            tool_name: Some("write_file".into()),
            args: Arc::new(calls[0].input.clone()),
            module_id: Some("test-tools".into()),
            schema_pin: None,
            hooks: None,
            approval_digest: None,
            call_key: None,
        });
        self.append(WalRecord::ToolResult {
            batch_id: batch_id.clone(),
            result: Arc::new(ToolResult {
                tool_call_id: call_id.clone(),
                output: ToolOutput::Text {
                    text: "first call ran".into(),
                },
                is_error: false,
            }),
            outcome: DispatchOutcome::Completed,
            hooks: None,
            reason: None,
            withdraw_elicitation: None,
            provider_held: None,
            tags: None,
        });
        self.append(WalRecord::ToolResult {
            batch_id: batch_id.clone(),
            result: Arc::new(ToolResult {
                tool_call_id: call_id,
                output: ToolOutput::Error {
                    message: "duplicate tool_call_id from provider".into(),
                },
                is_error: true,
            }),
            outcome: DispatchOutcome::Completed,
            hooks: None,
            reason: None,
            withdraw_elicitation: None,
            provider_held: None,
            tags: None,
        });
        self.append(WalRecord::ToolBatchFinished { batch_id });
    }

    fn finish(&mut self) {
        self.append(WalRecord::RunFinished {
            interrupt_reason: None,
            reason: RunFinishReason::Completed,
            ts_ms: None,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        });
    }

    async fn page(&self, request: ToolCallsRequest) -> ToolCallsPage {
        ServeHandler::run_tool_calls(&self.registry, &self.key, request)
            .await
            .unwrap_or_else(|error| panic!("run.tool_calls: {error}"))
    }
}

fn call(id: &str, args: Value) -> (String, Value) {
    (id.to_owned(), args)
}

#[tokio::test]
async fn run_tool_calls_pages_by_count_and_reads_a_closed_session_from_wal_only() {
    let mut session = ClosedSession::new("count");
    session.write_tool_turn(
        &[
            call("call-1", json!({"path":"a"})),
            call("call-2", json!({"path":"b"})),
            call("call-3", json!({"path":"c"})),
        ],
        Some(&["one".into(), "two".into(), "three".into()]),
    );
    session.finish();
    session.close_writer();

    let first = session
        .page(ToolCallsRequest::new("run-1").with_limit(1))
        .await;
    assert_eq!(first.calls.len(), 1);
    assert_eq!(first.calls[0].tool_call_id, "call-1");
    assert!(first.next.is_some());

    let second = session
        .page(
            ToolCallsRequest::new("run-1")
                .with_limit(1)
                .with_after(first.next.unwrap()),
        )
        .await;
    assert_eq!(second.calls[0].tool_call_id, "call-2");
    assert!(second.next.is_some());

    let third = session
        .page(
            ToolCallsRequest::new("run-1")
                .with_limit(1)
                .with_after(second.next.unwrap()),
        )
        .await;
    assert_eq!(third.calls[0].tool_call_id, "call-3");
    assert!(third.next.is_none());
    assert_eq!(first.calls[0].outcome, outcomes::SUCCEEDED);
    assert!(first.calls[0].result_bytes.unwrap() > 0);
}

#[tokio::test]
async fn run_tool_calls_lists_duplicate_id_with_its_synthetic_error_result() {
    let mut session = ClosedSession::new("duplicate-id");
    session.write_duplicate_id_tool_turn();
    session.finish();
    session.close_writer();

    let page = session.page(ToolCallsRequest::new("run-1")).await;
    assert_eq!(page.calls.len(), 2);
    assert_eq!(page.calls[0].tool_call_id, "duplicate-id");
    assert_eq!(page.calls[0].args, json!({"path":"first"}));
    assert_eq!(page.calls[0].outcome, outcomes::SUCCEEDED);
    assert!(page.calls[0].call_key.is_some());
    assert_eq!(page.calls[0].dispatched_to.as_deref(), Some("test-tools"));

    let duplicate = &page.calls[1];
    assert_eq!(duplicate.tool_call_id, "duplicate-id");
    assert_eq!(duplicate.args, json!({"path":"duplicate"}));
    assert_eq!(duplicate.outcome, outcomes::REFUSED);
    assert!(duplicate.call_key.is_none());
    assert!(duplicate.dispatched_to.is_none());
    assert!(duplicate.result_bytes.is_some_and(|bytes| bytes > 0));
}

#[tokio::test]
async fn run_tool_calls_pages_by_bytes() {
    let mut session = ClosedSession::new("bytes");
    let large = "a".repeat(1_700_000);
    session.write_tool_turn(
        &[
            call("call-1", json!({"content":large})),
            call("call-2", json!({"content":large})),
        ],
        Some(&["ok".into(), "ok".into()]),
    );
    session.finish();
    session.close_writer();

    let first = session.page(ToolCallsRequest::new("run-1")).await;
    assert_eq!(first.calls.len(), 1, "the byte cap must stop before call 2");
    assert_eq!(first.calls[0].tool_call_id, "call-1");
    assert_eq!(
        first.calls[0].args["content"].as_str().unwrap().len(),
        1_700_000
    );
    let second = session
        .page(ToolCallsRequest::new("run-1").with_after(first.next.unwrap()))
        .await;
    assert_eq!(second.calls.len(), 1);
    assert_eq!(second.calls[0].tool_call_id, "call-2");
}

#[tokio::test]
async fn run_tool_calls_serves_one_oversized_page_item_without_truncating_arguments() {
    let mut session = ClosedSession::new("oversized");
    let content = "b".repeat(3_200_000);
    session.write_tool_turn(&[call("large-call", json!({"content":content}))], None);
    session.close_writer();

    let page = session.page(ToolCallsRequest::new("run-1")).await;
    assert_eq!(page.calls.len(), 1);
    assert_eq!(page.calls[0].outcome, outcomes::INDETERMINATE);
    assert_eq!(
        page.calls[0].args["content"].as_str().unwrap().len(),
        3_200_000
    );
    assert!(page.calls[0].result_bytes.is_none());
    assert!(page.next.is_none());
}

#[tokio::test]
async fn run_tool_calls_refuses_a_cursor_that_was_modified_after_issue() {
    let mut session = ClosedSession::new("modified-cursor");
    session.write_tool_turn(
        &[
            call("call-1", json!({"path":"a"})),
            call("call-2", json!({"path":"b"})),
        ],
        Some(&["one".into(), "two".into()]),
    );
    session.finish();
    session.close_writer();

    let first = session
        .page(ToolCallsRequest::new("run-1").with_limit(1))
        .await;
    let mut cursor: Value = serde_json::from_str(first.next.as_deref().unwrap()).unwrap();
    cursor["ordinal"] = json!(cursor["ordinal"].as_u64().unwrap() + 1);
    let error = ServeHandler::run_tool_calls(
        &session.registry,
        &session.key,
        ToolCallsRequest::new("run-1").with_after(serde_json::to_string(&cursor).unwrap()),
    )
    .await
    .unwrap_err();
    assert!(
        matches!(error, ServeError::InvalidParams { ref field, .. } if field == "after"),
        "modified cursor should be refused by its request field, got {error:?}"
    );
}

#[tokio::test]
async fn run_tool_calls_reports_intent_without_result_as_indeterminate() {
    let mut session = ClosedSession::new("indeterminate");
    session.write_tool_turn(&[call("held-call", json!({"path":"large.txt"}))], None);
    session.close_writer();

    let page = session.page(ToolCallsRequest::new("run-1")).await;
    assert_eq!(page.calls.len(), 1);
    assert_eq!(page.calls[0].outcome, outcomes::INDETERMINATE);
    assert!(page.calls[0].result_bytes.is_none());
}

#[tokio::test]
async fn run_tool_calls_ordinals_match_session_read_across_a_materialized_prefix_and_open_batch() {
    let mut session = ClosedSession::new("open-batch-ordinal");
    session.write_tool_turn(
        &[
            call("first-call", json!({"path":"a"})),
            call("second-call", json!({"path":"b"})),
            call("third-call", json!({"path":"c"})),
        ],
        Some(&["one".into(), "two".into(), "three".into()]),
    );
    let watermark = session.materialize_through_batch("batch-1");
    let prefix = session
        .store
        .read_prefix_with_watermark(&session.key)
        .unwrap();
    assert_eq!(prefix.watermark_seq, watermark);
    assert_eq!(
        prefix.prefix.len(),
        2,
        "the store holds the committed assistant/tool pair"
    );

    session.steer("between turns");
    session.write_tool_turn_with(
        "batch-2",
        2,
        &[call("open-call", json!({"path":"d"}))],
        None,
    );
    session.close_writer();
    assert!(session
        .wal_store
        .read_records(&session.key)
        .unwrap()
        .iter()
        .any(|framed| framed.seq > watermark));

    let transcript = ServeHandler::read(
        &session.registry,
        SessionReadRequest::new(session.key.clone(), Some(0), Some(100)),
    )
    .await
    .unwrap();
    let transcript_calls: Vec<_> = transcript
        .messages
        .iter()
        .flat_map(|message| {
            message
                .message
                .content
                .iter()
                .filter_map(move |block| match block {
                    ContentBlock::ToolCall(call) => {
                        Some((message.ordinal, call.tool_call_id.0.clone()))
                    }
                    _ => None,
                })
        })
        .collect();
    let reported = session.page(ToolCallsRequest::new("run-1")).await;
    let reported_calls: Vec<_> = reported
        .calls
        .iter()
        .map(|call| (call.ordinal, call.tool_call_id.clone()))
        .collect();

    assert_eq!(
        transcript_calls,
        vec![
            (0, "first-call".into()),
            (0, "second-call".into()),
            (0, "third-call".into()),
            (3, "open-call".into()),
        ]
    );
    assert!(transcript.messages.iter().any(|message| {
        message.ordinal == 2
            && message.message.role == Role::User
            && message.message.content.iter().any(
                |block| matches!(block, ContentBlock::Text { text } if text == "between turns"),
            )
    }));
    assert_eq!(reported_calls, transcript_calls);
    for call in &reported.calls {
        let message = transcript
            .messages
            .iter()
            .find(|message| message.ordinal == call.ordinal)
            .unwrap();
        let calls: Vec<_> = message
            .message
            .content
            .iter()
            .filter_map(|block| match block {
                ContentBlock::ToolCall(call) => Some(call),
                _ => None,
            })
            .collect();
        let position = calls
            .iter()
            .position(|stored| stored.tool_call_id.0 == call.tool_call_id)
            .unwrap();
        assert_eq!(call.position, Some(position as u32));
        let result_message = transcript.messages.iter().find(|message| {
            message.message.content.iter().any(|block| matches!(block, ContentBlock::ToolResult(result) if result.tool_call_id.0 == call.tool_call_id))
        });
        assert_eq!(
            call.result_ordinal,
            result_message.map(|message| message.ordinal)
        );
    }

    // Pin the bytes a caller sees for this layout: the page as served and the
    // `session.read` ordinal of each tool-call block, with the random lineage id
    // in every `call_key` replaced so the file is stable.
    let lineage = reported
        .calls
        .iter()
        .find_map(|call| call.call_key.as_deref())
        .and_then(|key| key.rsplit_once(':'))
        .map(|(lineage, _)| lineage.to_owned())
        .expect("a dispatched call carries a call_key");
    let fixture = json!({
        "run_tool_calls_page": reported,
        "session_read_tool_call_ordinals": transcript_calls
            .iter()
            .map(|(ordinal, id)| json!({"ordinal": ordinal, "tool_call_id": id}))
            .collect::<Vec<_>>(),
    });
    let bytes = serde_json::to_string(&fixture)
        .unwrap()
        .replace(&lineage, "@LINEAGE@");
    let golden = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/run_tool_calls.open_batch.json");
    if std::env::var_os("BROCA_UPDATE_GOLDEN").is_some() {
        std::fs::write(&golden, &bytes).unwrap();
    }
    assert_eq!(
        bytes,
        std::fs::read_to_string(&golden).unwrap(),
        "the open-batch layout pinned in {}",
        golden.display()
    );
}

#[tokio::test]
async fn run_tool_calls_returns_empty_for_a_run_without_tool_calls() {
    let mut session = ClosedSession::new("empty");
    session.append(WalRecord::ModelStepFinished {
        step_id: StepId(1),
        assistant_message: Arc::new(AssistantMessage {
            message_id: MessageId("assistant-1".into()),
            content: vec![ContentBlock::Text {
                text: "finished without tools".into(),
            }],
        }),
        usage: Usage::default(),
        finish_reason: Some(broca_protocol::content::FinishReason::Stop),
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
        hooks: None,
        tags: None,
    });
    session.finish();
    session.close_writer();

    let page = session.page(ToolCallsRequest::new("run-1")).await;
    assert!(page.calls.is_empty());
    assert!(page.next.is_none());
}

#[tokio::test]
async fn run_tool_calls_reply_never_contains_stored_tool_result_content() {
    const SECRET: &str = "TOOL-RESULT-CONTENT-MUST-NOT-LEAK-7f52e3";
    let mut session = ClosedSession::new("result-content");
    session.write_tool_turn(
        &[call("call-1", json!({"path":"safe.txt"}))],
        Some(&[SECRET.into()]),
    );
    session.finish();
    session.close_writer();

    let page = session.page(ToolCallsRequest::new("run-1")).await;
    let bytes = serde_json::to_vec(&page).unwrap();
    let body = String::from_utf8(bytes).unwrap();
    assert!(!body.contains(SECRET));
    assert!(page.calls[0].result_bytes.unwrap() > 0);
}

#[tokio::test]
async fn run_tool_calls_unknown_run_is_not_an_empty_answer() {
    let mut session = ClosedSession::new("unknown");
    session.close_writer();
    let error = ServeHandler::run_tool_calls(
        &session.registry,
        &session.key,
        ToolCallsRequest::new("absent-run"),
    )
    .await
    .unwrap_err();
    assert!(matches!(error, ServeError::UnknownRun(run) if run == "absent-run"));
}

fn terminal(reason: RunFinishReason, interrupt_reason: Option<&str>) -> WalRecord {
    WalRecord::RunFinished {
        reason,
        interrupt_reason: interrupt_reason.map(str::to_owned),
        ts_ms: Some(42),
        usage: Usage::default(),
        error: None,
        retries_used: 0,
    }
}

#[tokio::test]
async fn interrupt_reason_surfaces_recorded_values_and_leaves_old_seals_absent() {
    for reason in [None, Some("runner_shutdown"), Some("runner_crash")] {
        let mut session = ClosedSession::new(&format!("interrupt-reason-{reason:?}"));
        session.append(terminal(RunFinishReason::Interrupted, reason));
        session.close_writer();
        let run = broca_protocol::RunId("run-1".into());
        let result = ServeHandler::run_result(&session.registry, &session.key, run.clone())
            .await
            .unwrap();
        let status = ServeHandler::run_status(&session.registry, &session.key, run)
            .await
            .unwrap();
        assert_eq!(result.reason.as_deref(), reason);
        let status = serde_json::to_value(status).unwrap();
        assert_eq!(status.get("reason").and_then(Value::as_str), reason);
        assert_eq!(status["state"], "interrupted");
        assert!(status.get("directives").is_none());
        assert!(result.directives.is_none());
    }
}

#[tokio::test]
async fn completed_directives_match_all_published_vectors_and_old_run_bytes() {
    use cortexkit_role_llm_runner::vectors;
    for (name, raw) in vectors::FILES
        .iter()
        .filter(|(name, _)| name.starts_with("directives/") && !name.ends_with("wire.json"))
    {
        let case: Value = serde_json::from_str(raw).unwrap();
        let mut session =
            ClosedSession::with_directives(&name.replace('/', "-"), Some(json!(["wake"])));
        let content: Vec<ContentBlock> = if let Some(parts) = case["parts"].as_array() {
            parts
                .iter()
                .map(|part| match part["type"].as_str().unwrap() {
                    "text" => ContentBlock::Text {
                        text: part["text"].as_str().unwrap().into(),
                    },
                    _ => ContentBlock::ToolCall(ToolCall {
                        tool_call_id: ToolCallId("ignored".into()),
                        tool_name: "ignored".into(),
                        input: json!({}),
                        thought_signature: None,
                    }),
                })
                .collect()
        } else {
            vec![ContentBlock::Text {
                text: case["text"].as_str().unwrap().into(),
            }]
        };
        let has_tool = content
            .iter()
            .any(|block| matches!(block, ContentBlock::ToolCall(_)));
        session.append(WalRecord::ModelStepFinished {
            step_id: StepId(1),
            assistant_message: Arc::new(AssistantMessage {
                message_id: MessageId("final".into()),
                content,
            }),
            usage: Usage::default(),
            finish_reason: Some(broca_protocol::content::FinishReason::Stop),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
            hooks: None,
            tags: None,
        });
        if has_tool {
            let batch_id = BatchId("directive-batch".into());
            session.append(WalRecord::ToolBatchStarted {
                batch_id: batch_id.clone(),
                step_id: StepId(1),
                planned_calls: vec![ToolCallId("ignored".into())],
            });
            session.append(WalRecord::ToolResult {
                batch_id: batch_id.clone(),
                result: Arc::new(ToolResult {
                    tool_call_id: ToolCallId("ignored".into()),
                    output: ToolOutput::Text {
                        text: "<wake off/>".into(),
                    },
                    is_error: false,
                }),
                outcome: DispatchOutcome::Completed,
                hooks: None,
                reason: None,
                withdraw_elicitation: None,
                provider_held: None,
                tags: None,
            });
            session.append(WalRecord::ToolBatchFinished { batch_id });
        }
        session.append(WalRecord::TurnFinished { step_id: StepId(1) });
        session.append(terminal(RunFinishReason::Completed, None));
        session.close_writer();
        let run = broca_protocol::RunId("run-1".into());
        let result = ServeHandler::run_result(&session.registry, &session.key, run.clone())
            .await
            .unwrap();
        let expected = if case["expected"].is_null() {
            Value::Null
        } else {
            json!([case["expected"].clone()])
        };
        assert_eq!(
            serde_json::to_value(&result.directives).unwrap(),
            expected,
            "{name}"
        );
        assert_eq!(
            result.final_message.as_ref().unwrap().text,
            case["text"].as_str().unwrap(),
            "{name}"
        );
        let status = ServeHandler::run_status(&session.registry, &session.key, run.clone())
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_value(status).unwrap()["directives"],
            expected,
            "{name}"
        );
        let bytes = serde_json::to_vec(&result).unwrap();
        let reopened = crate::LineageRegistry::new_with_reader(
            Arc::new(NeverOpen),
            session.wal_store.clone(),
            None,
        );
        let reread = ServeHandler::run_result(&reopened, &session.key, run)
            .await
            .unwrap();
        assert_eq!(
            serde_json::to_vec(&reread).unwrap(),
            bytes,
            "{name}: old frozen grammar and response bytes"
        );
    }
}

#[test]
fn wake_v1_frozen_run_answer_is_pinned_independently_of_current_vectors() {
    let mut session = ClosedSession::with_directives("wake-v1-pin", Some(json!(["wake"])));
    session.close_writer();
    let frames = session.wal_store.read_records(&session.key).unwrap();
    let answer = super::completed_directives(
        &frames,
        &broca_protocol::RunId("run-1".into()),
        "answer<wake in=\"000000001m\"> café </wake>",
    );
    assert_eq!(
        serde_json::to_string(&answer).unwrap(),
        r#"[{"name":"wake","in_ms":60000,"note":"café"}]"#
    );
}
