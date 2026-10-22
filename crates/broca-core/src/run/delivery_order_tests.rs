//! The run loop's half of sends with an ordered `delivery` list: a `steer` delivery lands at a
//! step boundary (never inside a tool batch), a `tool_result` delivery rides on exactly one result
//! of the batch it is decided at without changing that result's stored record, and replaying the
//! WAL renders exactly the prompt the live run sent.

use super::*;
use crate::delivery_order::OrderedSend;
use crate::{FileWalStore, SteerInbox, StubToolPlane};
use broca_protocol::provider::ProviderControl;
use broca_protocol::{DeliveryMode, DeliveryOrder, SubmissionId};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use tokio::sync::Notify;

fn request(name: &str) -> RunRequest {
    RunRequest::from_submission(serde_json::from_value(serde_json::json!({
        "model":{"provider_module_id":"stub","model_id":"m"},
        "input":[{"role":"user","content":[{"type":"text","text":"work"}]}],
        "policy_allowed":["work"],
        "tools":[{"name":"work","description":"work","input_schema":{"type":"object"},"execution_mode":"parallel"}]
    })).unwrap(), RunId(name.into()), BindIdentity::new("/ordered", "runner", name),
        SubmissionId("s".into()))
}

fn call(id: &str, n: u64) -> ContentBlock {
    ContentBlock::ToolCall(ToolCall {
        tool_call_id: ToolCallId(id.into()),
        tool_name: "work".into(),
        input: serde_json::json!({ "n": n }),
        thought_signature: None,
    })
}

/// A model that calls two tools on its first step and answers on its second, keeping every
/// prompt it was sent. It can hold its first step until released.
struct Model {
    prompts: Mutex<Vec<Vec<Message>>>,
    calls: AtomicUsize,
    hold: bool,
    entered: Notify,
    release: Notify,
}

impl Model {
    fn new(hold: bool) -> Self {
        Self {
            prompts: Mutex::new(Vec::new()),
            calls: AtomicUsize::new(0),
            hold,
            entered: Notify::new(),
            release: Notify::new(),
        }
    }

    fn prompt(&self, step: usize) -> Vec<Message> {
        self.prompts.lock().unwrap()[step].clone()
    }
}

#[async_trait::async_trait]
impl Provider for Model {
    async fn step(&self, _: StepId, opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        self.prompts.lock().unwrap().push(opts.prompt.to_vec());
        let first = self.calls.fetch_add(1, Ordering::SeqCst) == 0;
        if first && self.hold {
            self.entered.notify_one();
            self.release.notified().await;
        }
        let content = if first {
            vec![call("one", 1), call("two", 2)]
        } else {
            vec![ContentBlock::Text {
                text: "done".into(),
            }]
        };
        let (tx, rx) = mpsc::channel(8);
        tx.send(NormalizedEvent::Control(
            ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: broca_protocol::MessageId("m".into()),
                    content,
                },
            },
        ))
        .await
        .unwrap();
        tx.send(NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: if first {
                FinishReason::ToolCalls
            } else {
                FinishReason::Stop
            },
            retries_used: 0,
        }))
        .await
        .unwrap();
        rx
    }
    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        crate::render::canonical_request_bytes(opts)
    }
}

fn ordered(id: &str, modes: &[DeliveryMode]) -> OrderedSend {
    OrderedSend {
        submission_id: SubmissionId(id.into()),
        order: DeliveryOrder {
            modes: modes.to_vec(),
            dedupe_key: None,
            expires_at_ms: None,
            template: None,
        },
        principal: None,
        input: vec![Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "note".into(),
            }],
        )],
    }
}

fn bytes(messages: &[Message]) -> Vec<u8> {
    serde_json::to_vec(messages).unwrap()
}

fn position(records: &[WalRecord], wanted: impl Fn(&WalRecord) -> bool) -> usize {
    records.iter().position(wanted).expect("record present")
}

struct Ran {
    model: Model,
    records: Vec<WalRecord>,
}

async fn run(name: &str, model: Model, inbox: Arc<SteerInbox>, during: Option<OrderedSend>) -> Ran {
    run_with(name, model, inbox, during, None).await
}

/// A frozen plan manifest whose plan turns `render_tags` on.
fn rendering_manifest() -> broca_wal::PlanManifest {
    broca_wal::PlanManifest {
        plan: broca_wal::OpaqueJson::from_json_text(
            serde_json::json!({"step_transform_items":[],"render_tags":true}).to_string(),
        )
        .unwrap(),
        plan_identity: String::new(),
        composition_digest: String::new(),
        fetches: Vec::new(),
        tools: request("manifest").tools.into(),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: Default::default(),
        declarations: Vec::new(),
    }
}

async fn run_with(
    name: &str,
    model: Model,
    inbox: Arc<SteerInbox>,
    during: Option<OrderedSend>,
    manifest: Option<broca_wal::PlanManifest>,
) -> Ran {
    let root =
        broca_test_support::scratch_root().join(format!("ordered-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let store = FileWalStore::new(&root);
    let req = request(name);
    let writer = WalHandle::spawn(store.open(&req.session).unwrap(), 1).unwrap();
    let plane = StubToolPlane::new().with_canned_text("work", "done");
    let (out, _rx) = mpsc::channel(256);
    let mut running = Box::pin(run_loop_on_writer(
        req.clone(),
        &writer,
        StartMode::FreshOrResume,
        &model,
        &plane,
        &crate::AllowAll,
        &store,
        None,
        RunControl {
            steer: Some(inbox.clone()),
            manifest: manifest.map(Arc::new),
            ..Default::default()
        },
        &out,
    ));
    let summary = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        if let Some(send) = during {
            tokio::select! { biased; result = &mut running => panic!("model was not held: {result:?}"), _ = model.entered.notified() => {} }
            assert!(matches!(
                inbox.accept_ordered(send, |_| {}),
                crate::OrderedAcceptance::Queued { superseded: None }
            ));
            model.release.notify_one();
        }
        running.as_mut().await.unwrap()
    })
    .await
    .expect("the run ends");
    assert_eq!(summary.reason, RunFinishReason::Completed);
    drop(running);
    writer.shutdown().await.unwrap();
    let records = store
        .read_records(&req.session)
        .unwrap()
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    Ran { model, records }
}

/// A `tool_result` delivery is decided as the batch commits: its record sits between the batch's
/// `ToolBatchFinished` and `TurnFinished`, the next request renders exactly one result (the
/// batch's last text result) followed by the send's text, the stored `ToolResult` records keep
/// the tool's own output, and replaying the WAL renders the same prompt byte for byte.
#[tokio::test]
async fn a_tool_result_delivery_appends_to_exactly_one_result_and_resumes_identically() {
    let inbox = SteerInbox::new();
    // `tool_result` only: at the first step boundary no tool result exists, so the send waits
    // for the batch (a list naming `steer` too would be delivered there as a steer).
    inbox.seed_ordered(vec![ordered("sub-a", &[DeliveryMode::ToolResult])]);
    let Ran { model, records } = run("tool-result", Model::new(false), inbox.clone(), None).await;

    // Checked first: the tool's stored output is never changed by a delivery.
    for record in &records {
        if let WalRecord::ToolResult { result, .. } = record {
            assert_eq!(
                result.output,
                ToolOutput::Text {
                    text: "done".into()
                },
                "the stored result keeps the tool's own output"
            );
        }
    }
    let delivered = position(&records, |record| {
        matches!(record, WalRecord::SendDelivered { .. })
    });
    let WalRecord::SendDelivered {
        mode,
        tool_call_id,
        appended_text,
        input,
        ..
    } = &records[delivered]
    else {
        unreachable!()
    };
    assert_eq!(*mode, DeliveryMode::ToolResult);
    assert_eq!(tool_call_id.as_ref().map(|id| id.0.as_str()), Some("two"));
    assert_eq!(appended_text.as_deref(), Some("\n\nnote"));
    assert!(input.is_none());
    assert!(matches!(
        records[delivered - 1],
        WalRecord::ToolBatchFinished { .. }
    ));
    assert!(matches!(
        records[delivered + 1],
        WalRecord::TurnFinished { .. }
    ));
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::SendDelivered { .. }))
            .count(),
        1,
        "delivered once, and not again as a steer"
    );

    let sent = model.prompt(1);
    let carrying: Vec<_> = sent
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::ToolResult(result) => {
                Some((result.tool_call_id.0.as_str(), &result.output))
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        carrying,
        vec![
            (
                "one",
                &ToolOutput::Text {
                    text: "done".into()
                }
            ),
            (
                "two",
                &ToolOutput::Text {
                    text: "done\n\nnote".into()
                }
            ),
        ]
    );
    let replayed = crate::replay::replay(&records).unwrap().prompt;
    assert_eq!(
        bytes(&replayed[..sent.len()]),
        bytes(&sent),
        "resume renders what was sent"
    );
}

/// A `steer` delivery accepted while a step runs lands at the next step boundary: after the tool
/// batch and its turn have committed, immediately before the next `StepStarted`, never between a
/// tool call and its result. Replaying the WAL renders the prompt the run sent.
#[tokio::test]
async fn a_steer_delivery_lands_at_the_step_boundary_never_inside_a_tool_batch() {
    let inbox = SteerInbox::new();
    let Ran { model, records } = run(
        "steer",
        Model::new(true),
        inbox,
        Some(ordered(
            "sub-c",
            &[DeliveryMode::TurnSystem, DeliveryMode::Steer],
        )),
    )
    .await;

    let delivered = position(&records, |record| {
        matches!(record, WalRecord::SendDelivered { .. })
    });
    assert!(
        matches!(
            &records[delivered],
            WalRecord::SendDelivered {
                mode: DeliveryMode::Steer,
                input: Some(_),
                ..
            }
        ),
        "turn_system falls through to steer"
    );
    let batch_started = position(&records, |record| {
        matches!(record, WalRecord::ToolBatchStarted { .. })
    });
    let turn_finished = position(&records, |record| {
        matches!(record, WalRecord::TurnFinished { .. })
    });
    assert!(
        delivered > batch_started && delivered > turn_finished,
        "after the batch and its turn"
    );
    assert!(matches!(
        records[delivered + 1],
        WalRecord::StepStarted { step_id: StepId(2) }
    ));

    let sent = model.prompt(1);
    assert_eq!(sent.last().unwrap().role, Role::User);
    assert_eq!(
        sent.last().unwrap().content,
        vec![ContentBlock::Text {
            text: "note".into()
        }]
    );
    assert_eq!(
        sent[sent.len() - 2].role,
        Role::Tool,
        "after the tool results"
    );
    let replayed = crate::replay::replay(&records).unwrap().prompt;
    assert_eq!(
        bytes(&replayed[..sent.len()]),
        bytes(&sent),
        "resume renders what was sent"
    );
}

/// A run with no ordered sends writes no delivery-order record: its WAL is what it always was.
#[tokio::test]
async fn a_run_without_ordered_sends_writes_no_delivery_records() {
    let Ran { records, .. } = run("plain", Model::new(false), SteerInbox::new(), None).await;
    assert!(records
        .iter()
        .all(|record| record.required_features().is_empty()
            || !record
                .required_features()
                .contains(&broca_wal::framing::DELIVERY_ORDER_FEATURE)));
}

/// The prompt a replay of `records` rebuilds, with its tags rendered as a run frozen with
/// `render_tags` renders them on a family that keeps assistant tags as metadata.
fn rendered(records: &[WalRecord]) -> Vec<Message> {
    let mut prompt = crate::replay::replay(records).unwrap().prompt;
    broca_protocol::tags::render_tags_into(&mut prompt, Default::default());
    prompt
}

/// The number the replayed transcript shows for the first block of the first message that
/// `holds` matches, by the computed tag rules: the WAL itself stores none.
fn shown_number(records: &[WalRecord], holds: impl Fn(&ContentBlock) -> bool) -> u64 {
    assert!(
        records.iter().all(|record| record.runner_tags().is_empty()),
        "a run under the computed rules stores no tag number"
    );
    crate::replay::replay(records)
        .unwrap()
        .prompt
        .iter()
        .find_map(|message| {
            let index = message.content.iter().position(&holds)?;
            message.part_tags()?.parts[index]
        })
        .expect("the part shows a number")
}

/// With `render_tags` on, a `steer` delivery's message is a numbered part like any steer: the
/// transcript numbers it, the live request shows that number, and a replay of the WAL renders
/// the request the run sent.
#[tokio::test]
async fn a_steer_delivery_is_a_tagged_part_that_renders_as_its_rebuild() {
    let Ran { model, records } = run_with(
        "steer-tagged",
        Model::new(true),
        SteerInbox::new(),
        Some(ordered("sub-t", &[DeliveryMode::Steer])),
        Some(rendering_manifest()),
    )
    .await;
    let tag = shown_number(
        &records,
        |block| matches!(block, ContentBlock::Text { text } if text == "note"),
    );
    let sent = model.prompt(1);
    assert_eq!(
        sent.last().unwrap().content,
        vec![ContentBlock::Text {
            text: format!("§{tag}§ note")
        }]
    );
    assert_eq!(
        bytes(&rendered(&records)[..sent.len()]),
        bytes(&sent),
        "a rebuild renders what was sent"
    );
}

/// With `render_tags` on, a `tool_result` delivery's text is part of the tool result it rides
/// on: the result's own number leads the result and the appended text, and the delivery has no
/// number of its own. A replay of the WAL renders the request the run sent.
#[tokio::test]
async fn a_tool_result_delivery_renders_inside_the_results_tagged_part() {
    let inbox = SteerInbox::new();
    inbox.seed_ordered(vec![ordered("sub-r", &[DeliveryMode::ToolResult])]);
    let Ran { model, records } = run_with(
        "tool-result-tagged",
        Model::new(false),
        inbox,
        None,
        Some(rendering_manifest()),
    )
    .await;
    let carrier_tag = shown_number(
        &records,
        |block| matches!(block, ContentBlock::ToolResult(result) if result.tool_call_id.0 == "two"),
    );
    assert!(records.iter().any(|record| matches!(
        record,
        WalRecord::SendDelivered {
            mode: DeliveryMode::ToolResult,
            tags: None,
            ..
        }
    )));
    let sent = model.prompt(1);
    let carried = sent
        .iter()
        .flat_map(|message| &message.content)
        .find_map(|block| match block {
            ContentBlock::ToolResult(result) if result.tool_call_id.0 == "two" => {
                Some(result.output.clone())
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(
        carried,
        ToolOutput::Text {
            text: format!("§{carrier_tag}§ done\n\nnote")
        }
    );
    assert_eq!(
        bytes(&rendered(&records)[..sent.len()]),
        bytes(&sent),
        "a rebuild renders what was sent"
    );
}
