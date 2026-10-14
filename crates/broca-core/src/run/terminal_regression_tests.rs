use super::*;
use broca_protocol::{provider::NormalizedEvent, provider::ProviderControl, MessageId};
use broca_wal::{FramedRecord, Wal, WalError};
use std::sync::{Condvar, Mutex};
use tokio::sync::Notify;

#[derive(Default)]
struct TerminalGate {
    durable: Notify,
    released: Mutex<bool>,
    release: Condvar,
}

impl TerminalGate {
    async fn wait(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(10), self.durable.notified())
            .await
            .expect("terminal sync reached the gate");
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.release.notify_all();
    }
}

struct GatedWal {
    records: Arc<Mutex<Vec<FramedRecord>>>,
    gate: Arc<TerminalGate>,
}

impl Wal for GatedWal {
    fn append(&mut self, fence: u64, record: &WalRecord, sync: bool) -> Result<u64, WalError> {
        let mut records = self.records.lock().unwrap();
        let seq = records.len() as u64 + 1;
        records.push(FramedRecord {
            seq,
            fence,
            record: record.clone(),
        });
        drop(records);
        if sync {
            self.sync()?;
        }
        Ok(seq)
    }
    fn sync(&self) -> Result<(), WalError> {
        if self
            .records
            .lock()
            .unwrap()
            .iter()
            .any(|frame| matches!(frame.record, WalRecord::RunFinished { .. }))
        {
            // The terminal is durable before its acknowledgement can let the run return.
            self.gate.durable.notify_one();
            let mut released = self.gate.released.lock().unwrap();
            while !*released {
                released = self.gate.release.wait(released).unwrap();
            }
        }
        Ok(())
    }
    fn replay(&self) -> Result<Vec<FramedRecord>, WalError> {
        Ok(self.records.lock().unwrap().clone())
    }
    fn tail_after_seq(&self, seq: u64) -> Result<Vec<FramedRecord>, WalError> {
        Ok(self
            .records
            .lock()
            .unwrap()
            .iter()
            .filter(|frame| frame.seq > seq)
            .cloned()
            .collect())
    }
    fn last_seq(&self) -> u64 {
        self.records.lock().unwrap().len() as u64
    }
}

struct GatedStore(Arc<Mutex<Vec<FramedRecord>>>);
impl WalStore for GatedStore {
    fn open(&self, _: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
        unreachable!("the test runs on its already-open writer")
    }
    fn read_records(&self, _: &BindIdentity) -> Result<Vec<FramedRecord>, WalError> {
        Ok(self.0.lock().unwrap().clone())
    }
}

fn request() -> RunRequest {
    RunRequest::from_submission(
        serde_json::from_value(serde_json::json!({
            "model": {"provider_module_id": "stub", "model_id": "m"},
            "tools": [{"name": "lookup", "description": "lookup", "input_schema": {"type": "object"}}],
            "input": [{"role": "user", "content": [{"type": "text", "text": "hello"}]}]
        })).unwrap(),
        RunId("r1".into()), BindIdentity::new("/terminal-gate", "h", "s"),
        broca_protocol::SubmissionId("sub".into()),
    )
}

fn tool_step(id: &str, cut: bool) -> Vec<NormalizedEvent> {
    let mut events = Vec::new();
    if cut {
        events.push(NormalizedEvent::Control(
            ProviderControl::ToolArgsTrailingGarbage {
                cut: broca_protocol::provider::ToolArgsTrailingGarbage {
                    tool_call_id: ToolCallId(id.into()),
                    tool_name: "lookup".into(),
                    json_bytes: 2,
                    whitespace_bytes: 4097,
                    omitted_tool_calls: 1,
                },
            },
        ));
    }
    events.extend([
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId(id.into()),
                    tool_name: "lookup".into(),
                    input: serde_json::json!({}),
                })],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]);
    events
}

fn try_late_steer(inbox: &crate::SteerInbox, writer: &WalHandle) -> bool {
    let input = vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "late".into(),
        }],
    )];
    inbox.deliver(input.clone(), || {
        writer
            .record(WalRecord::Steered {
                retention: None,
                ts_ms: None,
                title: None,
                send_id: None,
                request_identity_digest: None,
                delivery: Some(broca_protocol::Delivery::Steer),
                input: input.into(),
                hooks: None,
            })
            .into_result()
            .expect("ample room for a steer; writer refusal cannot fence this test");
    })
}

#[tokio::test]
async fn budget_finalize_args_cut_closes_steer_inbox_before_terminal() {
    let records = Arc::new(Mutex::new(vec![]));
    let gate = Arc::new(TerminalGate::default());
    let writer = Arc::new(
        WalHandle::spawn(
            Box::new(GatedWal {
                records: records.clone(),
                gate: gate.clone(),
            }),
            1,
        )
        .unwrap(),
    );
    let inbox = crate::SteerInbox::new();
    let mut req = request();
    req.budget = Some(broca_protocol::request::BudgetConfig {
        nudges: vec![],
        finalize_at_tool_calls: 1,
        finalize_text: "Finish without tools".into(),
    });
    let running = {
        let writer = writer.clone();
        let inbox = inbox.clone();
        let records = records.clone();
        tokio::spawn(async move {
            let provider =
                crate::StubProvider::new(vec![tool_step("first", false), tool_step("bad", true)]);
            let plane = crate::StubToolPlane::new().with_canned_text("lookup", "ok");
            let (out, _events) = mpsc::channel(256);
            let result = run_loop_on_writer(
                req,
                &writer,
                StartMode::FreshOrResume,
                &provider,
                &plane,
                &crate::AllowAll,
                &GatedStore(records),
                None,
                RunControl {
                    steer: Some(inbox),
                    ..Default::default()
                },
                &out,
            )
            .await;
            assert_eq!(plane.exec_count(&ToolCallId("bad".into())), 0);
            result
        })
    };
    gate.wait().await;
    let delivered = try_late_steer(&inbox, &writer);
    gate.release();
    let summary = running.await.unwrap().unwrap();
    writer.sync().await.unwrap();
    assert_eq!(summary.steps, 2);
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert!(
        !delivered,
        "a durable budget terminal must refuse a live steer"
    );
    assert!(!records
        .lock()
        .unwrap()
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::Steered { .. })));
    let _ = Arc::try_unwrap(writer).ok().unwrap().shutdown().await;
}

#[tokio::test]
async fn size_refusal_closes_steer_inbox_before_terminal() {
    let records = Arc::new(Mutex::new(vec![FramedRecord {
        seq: 1,
        fence: 1,
        record: crate::retention::tests::started("r1", None),
    }]));
    let gate = Arc::new(TerminalGate::default());
    // No append cap is installed: a refusal to write cannot masquerade as inbox closure.
    let writer = Arc::new(
        WalHandle::spawn(
            Box::new(GatedWal {
                records: records.clone(),
                gate: gate.clone(),
            }),
            1,
        )
        .unwrap(),
    );
    let inbox = crate::SteerInbox::new();
    let running = {
        let writer = writer.clone();
        let inbox = inbox.clone();
        let records = records.clone();
        tokio::spawn(async move {
            let (out, _events) = mpsc::channel(32);
            seal_size_refusal(
                Err(RunError::RetainedSessionTooLarge { max_bytes: 1 << 30 }),
                &request(),
                &writer,
                &GatedStore(records),
                &RunControl {
                    steer: Some(inbox),
                    ..Default::default()
                },
                &out,
                false,
            )
            .await
        })
    };
    gate.wait().await;
    let delivered = try_late_steer(&inbox, &writer);
    gate.release();
    assert_eq!(
        running.await.unwrap().unwrap().reason,
        RunFinishReason::Error
    );
    writer.sync().await.unwrap();
    assert!(
        !delivered,
        "a durable size terminal must refuse a live steer"
    );
    assert!(!records
        .lock()
        .unwrap()
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::Steered { .. })));
    let _ = Arc::try_unwrap(writer).ok().unwrap().shutdown().await;
}

#[tokio::test]
async fn size_refusal_already_durable_owned_return_closes_steer_inbox() {
    let records = Arc::new(Mutex::new(vec![
        FramedRecord {
            seq: 1,
            fence: 1,
            record: crate::retention::tests::started("r1", None),
        },
        FramedRecord {
            seq: 2,
            fence: 1,
            record: WalRecord::RunFinished {
                ts_ms: None,
                reason: RunFinishReason::Completed,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        },
    ]));
    let gate = Arc::new(TerminalGate::default());
    gate.release();
    let writer = WalHandle::spawn(
        Box::new(GatedWal {
            records: records.clone(),
            gate,
        }),
        1,
    )
    .unwrap();
    let inbox = crate::SteerInbox::new();
    let (out, _events) = mpsc::channel(32);
    let summary = seal_size_refusal(
        Err(RunError::RetainedSessionTooLarge { max_bytes: 1 << 30 }),
        &request(),
        &writer,
        &GatedStore(records.clone()),
        &RunControl {
            steer: Some(inbox.clone()),
            ..Default::default()
        },
        &out,
        false,
    )
    .await
    .unwrap();
    let delivered = try_late_steer(&inbox, &writer);
    writer.sync().await.unwrap();
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert!(
        !delivered,
        "an owned durable return must refuse a live steer"
    );
    assert_eq!(
        records.lock().unwrap().len(),
        2,
        "no steer or replacement terminal"
    );
    let _ = writer.shutdown().await;
}
