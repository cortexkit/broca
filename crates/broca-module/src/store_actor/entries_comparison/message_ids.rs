use super::*;
use crate::serve::{ServeHandler, SessionReadRequest};
use crate::step_events::{StepEventPublisher, StepEvents};
use cortexkit_role_llm_runner::step_events::{decode_step_finished, StepFinished};

type Inbox = mpsc::UnboundedReceiver<(String, Vec<u8>)>;

struct Capture(mpsc::UnboundedSender<(String, Vec<u8>)>);

#[async_trait]
impl StepEventPublisher for Capture {
    async fn live_subject(&self, runner: &str) -> Result<String, String> {
        Ok(format!(
            "ck.synthetic-account.live.{runner}.step.finished.v1"
        ))
    }

    async fn publish(&self, subject: &str, body: Vec<u8>) -> Result<(), String> {
        self.0.send((subject.into(), body)).unwrap();
        Ok(())
    }
}

fn attempt(number: u32, outcome: &str) -> NormalizedEvent {
    NormalizedEvent::Control(ProviderControl::AttemptFinished {
        attempt: serde_json::from_value(json!({
            "attempt": number, "request_body_len": 10, "request_body_sha256": "0".repeat(64),
            "http_status": if outcome == "http_error" { 503 } else { 200 },
            "response_body_len": 20, "response_body_sha256": "1".repeat(64),
            "outcome": outcome, "ts_ms": 1100 + number * 100, "sent_at_ms": 1000 + number * 100,
        }))
        .unwrap(),
    })
}

fn measured(mut reply: Vec<NormalizedEvent>) -> Vec<NormalizedEvent> {
    reply.insert(0, attempt(0, "complete"));
    reply
}

async fn event_side(steps: Vec<Vec<NormalizedEvent>>) -> (Side, Inbox) {
    let (tx, inbox) = mpsc::unbounded_channel();
    let events = Arc::new(StepEvents::start_on_bus(Arc::new(Capture(tx))));
    let side = side_with_events(
        Build {
            runtime: true,
            new_sessions: true,
            gated: false,
            compaction: true,
            hooks: false,
            idle: Duration::from_secs(30),
        },
        steps,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        Some(events),
    )
    .await;
    (side, inbox)
}

async fn publication(inbox: &mut Inbox) -> (StepFinished, Vec<u8>) {
    let (subject, body) =
        broca_test_support::wait_for("scripted step publication", inbox.recv(), || {
            "synthetic step event".into()
        })
        .await
        .unwrap();
    assert_eq!(subject, "ck.synthetic-account.live.broca.step.finished.v1");
    let event = decode_step_finished(&subject, &body).unwrap();
    event.check().unwrap();
    (event, body)
}

fn sample(name: &str, body: &[u8]) {
    if let Some(dir) = std::env::var_os("BROCA_STEP_MESSAGE_ID_SAMPLES_DIR") {
        let dir = std::path::PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(name), body).unwrap();
    }
}

async fn send_planned(side: &Side, run: &str, first: bool) {
    let outcome = side
        .send_request(
            run,
            untooled("synthetic question"),
            Some(planned(
                "synthetic-plan",
                first.then(|| compacting_manifest(false)),
            )),
        )
        .await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    side.settled(run).await;
    side.assert_completed(run).await;
}

async fn assert_served_ids(side: &Side, run: &str, events: &[StepFinished]) {
    let read = side
        .registry
        .read(SessionReadRequest::new(
            side.key.clone(),
            Some(1),
            Some(100),
        ))
        .await
        .unwrap();
    let assistants: Vec<_> = read
        .messages
        .iter()
        .filter(|message| {
            message.message.role == broca_protocol::content::Role::Assistant
                && message.run.as_ref().is_some_and(|at| at.run_id == run)
        })
        .collect();
    assert_eq!(assistants.len(), events.len());
    let entries = entry_mids(side).await;
    for (index, (message, event)) in assistants.iter().zip(events).enumerate() {
        assert_eq!(event.run_id, run);
        assert_eq!(event.step_id, (index + 1).to_string());
        assert_eq!(event.message_id.as_deref(), Some(message.mid.as_str()));
        assert_eq!(entries.get(&message.ordinal), Some(&message.mid));
        assert_ne!(event.message_id, event.attempt_id);
    }
}

#[tokio::test]
async fn message_ids_join_two_tool_separated_steps_to_read_and_entries() {
    let (side, mut inbox) = event_side(vec![
        measured(tool_step("provider-first")),
        measured(text_step("provider-second", "synthetic answer")),
    ])
    .await;
    send_planned(&side, "synthetic-two-step", true).await;
    let (first, body) = publication(&mut inbox).await;
    let (second, _) = publication(&mut inbox).await;
    assert_ne!(first.message_id, second.message_id);
    assert_served_ids(&side, "synthetic-two-step", &[first, second]).await;
    sample("with-message-id.json", &body);
    assert!(side.registry.stop_and_seal().await);
    assert!(inbox.try_recv().is_err());
}

#[tokio::test]
async fn message_ids_only_the_successful_retry_places_a_message() {
    let mut reply = vec![attempt(0, "http_error"), attempt(1, "complete")];
    let mut response = text_step("provider-retry", "synthetic retry answer");
    for event in &mut response {
        if let NormalizedEvent::Control(ProviderControl::Finished { retries_used, .. }) = event {
            *retries_used = 1;
        }
    }
    reply.extend(response);
    let (side, mut inbox) = event_side(vec![reply]).await;
    send_planned(&side, "synthetic-retry", true).await;
    let (failed, body) = publication(&mut inbox).await;
    let (completed, _) = publication(&mut inbox).await;
    assert_eq!(failed.terminal_status, "failed");
    assert_eq!(failed.attempt.get(), 1);
    assert_eq!(failed.message_id, None);
    assert!(serde_json::from_slice::<Value>(&body)
        .unwrap()
        .get("message_id")
        .is_none());
    assert_eq!(completed.terminal_status, "completed");
    assert_eq!(completed.attempt.get(), 2);
    assert_served_ids(&side, "synthetic-retry", &[completed]).await;
    sample("failed-attempt.json", &body);
    assert!(side.registry.stop_and_seal().await);
}

#[tokio::test]
async fn message_ids_attempt_without_a_placed_reply_omits_the_member() {
    let mut reply = vec![attempt(0, "complete")];
    reply.extend(failing_step());
    let (side, mut inbox) = event_side(vec![reply]).await;
    assert!(matches!(
        side.send("synthetic-no-reply", "synthetic input").await,
        SendOutcome::Active { .. }
    ));
    side.settled("synthetic-no-reply").await;
    let (event, body) = publication(&mut inbox).await;
    assert_eq!(event.terminal_status, "interrupted");
    assert_eq!(event.message_id, None);
    assert!(serde_json::from_slice::<Value>(&body)
        .unwrap()
        .get("message_id")
        .is_none());
    assert!(side.registry.stop_and_seal().await);
}

#[tokio::test]
async fn message_ids_argument_cut_keeps_its_early_failed_hint_without_an_id() {
    let mut reply = tool_step("provider-cut");
    let terminal = reply.pop().unwrap();
    reply.insert(0, attempt(0, "tool_args_trailing_garbage"));
    reply.push(NormalizedEvent::Control(
        ProviderControl::ToolArgsTrailingGarbage {
            cut: broca_protocol::provider::ToolArgsTrailingGarbage {
                tool_call_id: broca_protocol::ToolCallId("c1".into()),
                tool_name: "read".into(),
                json_bytes: 12,
                whitespace_bytes: 4096,
                omitted_tool_calls: 0,
            },
        },
    ));
    reply.push(terminal);
    let (side, mut inbox) = event_side(vec![
        reply,
        measured(text_step("provider-after-cut", "synthetic answer")),
    ])
    .await;
    send_planned(&side, "synthetic-cut", true).await;
    let (cut, body) = publication(&mut inbox).await;
    let (completed, _) = publication(&mut inbox).await;
    assert_eq!(cut.terminal_status, "failed");
    assert_eq!(cut.message_id, None);
    assert!(serde_json::from_slice::<Value>(&body)
        .unwrap()
        .get("message_id")
        .is_none());
    assert_eq!(completed.terminal_status, "completed");
    let read = side
        .registry
        .read(SessionReadRequest::new(
            side.key.clone(),
            Some(1),
            Some(100),
        ))
        .await
        .unwrap();
    let assistants: Vec<_> = read
        .messages
        .iter()
        .filter(|message| message.message.role == broca_protocol::content::Role::Assistant)
        .collect();
    assert_eq!(
        assistants.len(),
        2,
        "the cut reply still placed an assistant message"
    );
    assert_ne!(Some(&assistants[0].mid), completed.message_id.as_ref());
    assert_eq!(Some(&assistants[1].mid), completed.message_id.as_ref());
    assert!(side.registry.stop_and_seal().await);
}

#[tokio::test]
async fn message_ids_use_served_facts_after_compaction_enrolment() {
    let (side, mut inbox) = event_side(vec![
        measured(text_step("provider-before", "synthetic before")),
        measured(text_step("provider-after", "synthetic after")),
    ])
    .await;
    let composition = json!({"providers": [], "step_transform_items": []});
    let mut plan = json!({
        "tool_items": [], "system_text_items": [], "step_transform_items": [],
        "composition_digest": cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
        "composition": composition,
    });
    let mut initial = compacting_manifest(false);
    initial.plan = broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap();
    initial.composition_digest = plan["composition_digest"].as_str().unwrap().into();
    initial.tools = Arc::new(Vec::new());
    initial.system_text.clear();
    initial.plan_identity = "synthetic-plan".into();
    assert!(matches!(
        side.send_request(
            "synthetic-before",
            untooled("synthetic input"),
            Some(planned("synthetic-plan", Some(initial)))
        )
        .await,
        SendOutcome::Active { .. }
    ));
    side.settled("synthetic-before").await;
    let (before, _) = publication(&mut inbox).await;
    broca_test_support::wait_until("the pre-enrollment actor is idle", || async {
        match side.registry.route_status(&side.key).await {
            Ok(crate::actor::LineageStatus::Idle) => Ok(()),
            other => Err(format!("{other:?}")),
        }
    })
    .await;
    plan["compaction_item"] = json!({"provider": "ck-mc", "preset": "default", "params": {}});
    plan["composition"]["compaction"] = json!({"provider": "ck-mc"});
    plan["composition"]["providers"] = json!([{"provider": "ck-mc", "tools": []}]);
    plan["composition_digest"] = json!(cortexkit_role_tool_provider::catalog::composition_digest(
        &plan["composition"]
    )
    .unwrap());
    let mut refreshed = compacting_manifest(false);
    refreshed.plan = broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap();
    refreshed.composition_digest = plan["composition_digest"].as_str().unwrap().into();
    refreshed.tools = Arc::new(Vec::new());
    refreshed.system_text.clear();
    refreshed.plan_identity = "synthetic-enrolled-plan".into();
    let answer = side
        .registry
        .route_session_refresh(&side.key, 1, "immediately".into(), Box::new(refreshed))
        .await
        .unwrap();
    assert!(
        format!("{answer:?}").contains("applies_from_next_run: true"),
        "{answer:?}"
    );
    let outcome = side
        .send_request(
            "synthetic-enrolled",
            untooled("synthetic question"),
            Some(planned("synthetic-enrolled-plan", None)),
        )
        .await;
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    side.settled("synthetic-enrolled").await;
    side.assert_completed("synthetic-enrolled").await;
    let (after, _) = publication(&mut inbox).await;
    assert_served_ids(&side, "synthetic-enrolled", &[after]).await;
    let read = side
        .registry
        .read(SessionReadRequest::new(
            side.key.clone(),
            Some(1),
            Some(100),
        ))
        .await
        .unwrap();
    assert!(read
        .messages
        .iter()
        .any(|message| Some(&message.mid) == before.message_id.as_ref()));
    let view = side
        .registry
        .store_runtime()
        .unwrap()
        .current_view(&side.key)
        .await
        .unwrap()
        .unwrap();
    assert!(crate::store_entries::on_contract(&view));
    assert!(view.operations.enrolled.is_some());
    assert!(side.registry.stop_and_seal().await);
}

struct Cancels;

#[async_trait]
impl broca_core::provider::Provider for Cancels {
    fn reports_attempt_evidence(&self) -> bool {
        true
    }

    async fn step(
        &self,
        _: broca_protocol::StepId,
        _: &broca_protocol::request::CallOptions,
    ) -> mpsc::Receiver<NormalizedEvent> {
        panic!("cancellation fixture requires the run's cancellation token")
    }

    async fn step_with_cancel(
        &self,
        _: broca_protocol::StepId,
        _: &broca_protocol::request::CallOptions,
        cancel: tokio_util::sync::CancellationToken,
        _: Option<tokio::time::Instant>,
    ) -> mpsc::Receiver<NormalizedEvent> {
        let (tx, rx) = mpsc::channel(1);
        // Cancel at the scripted send boundary, then let the loop drain the terminal evidence.
        cancel.cancel();
        tx.send(attempt(0, "cancelled")).await.unwrap();
        rx
    }
}

#[tokio::test]
async fn message_ids_cancelled_step_omits_the_member() {
    let (tx, mut inbox) = mpsc::unbounded_channel();
    let events = Arc::new(StepEvents::start_on_bus(Arc::new(Capture(tx))));
    let dir = broca_test_support::scratch_root()
        .join(format!("synthetic-step-cancel-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let provider = broca_core::provider::StubProvider::new(Vec::new());
    let executor: Arc<dyn RunExecutor> = Arc::new(Loops {
        store: Cancels,
        compaction: Some(Arc::new(ReadyProvider)),
        hooks: None,
    });
    let runtime = crate::store_open::StoreRuntime::open(
        &dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        executor.clone(),
        Duration::from_secs(30),
    )
    .await
    .unwrap();
    let opener = crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(dir.join("leases"))),
        wal.clone(),
        executor,
    )
    .with_store_runtime(Arc::new(runtime))
    .with_step_events(events);
    let side = Side {
        key: BindIdentity::new(
            dir.join("synthetic-project"),
            "synthetic-harness",
            "synthetic-cancel",
        ),
        dir,
        registry: LineageRegistry::new_with_reader(Arc::new(opener), wal, None)
            .with_store_new_sessions(),
        gate: None,
        provider,
        caller: Default::default(),
    };
    assert!(matches!(
        side.send("synthetic-cancelled-run", "synthetic input")
            .await,
        SendOutcome::Active { .. }
    ));
    side.settled("synthetic-cancelled-run").await;
    let read = side
        .registry
        .read(SessionReadRequest::new(
            side.key.clone(),
            Some(1),
            Some(100),
        ))
        .await
        .unwrap();
    assert_eq!(read.lineage_state.state, "cancelled");
    assert!(!read
        .messages
        .iter()
        .any(|message| message.message.role == broca_protocol::content::Role::Assistant));
    let (event, body) = publication(&mut inbox).await;
    assert_eq!(event.terminal_status, "cancelled");
    assert_eq!(event.message_id, None);
    assert!(serde_json::from_slice::<Value>(&body)
        .unwrap()
        .get("message_id")
        .is_none());
    assert!(side.registry.stop_and_seal().await);
}
