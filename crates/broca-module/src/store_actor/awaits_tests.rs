use broca_protocol::{BindIdentity, ControlUnit, RunId};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::{
    fanout::Delivery,
    registry::LineageRegistry,
    serve::{ServeHandler, SubscribeFrom},
};

pub(super) fn sample(name: &str, bytes: &[u8]) {
    if let Some(directory) = std::env::var_os("BROCA_RUN_AWAITS_SAMPLE_DIR") {
        std::fs::create_dir_all(&directory).unwrap();
        std::fs::write(std::path::PathBuf::from(directory).join(name), bytes).unwrap();
    }
}

pub(super) async fn prove(
    registry: &LineageRegistry,
    key: &BindIdentity,
    name: &str,
    expected: Option<(&str, &str, Option<&str>)>,
) -> Value {
    let status = registry.run_status(key, RunId("r1".into())).await.unwrap();
    let head = registry
        .head_for(key, None, &Default::default())
        .await
        .unwrap();
    let result = registry.run_result(key, RunId("r1".into())).await.unwrap();
    let body = serde_json::to_value(&status).unwrap();
    let head_body = serde_json::to_value(&head).unwrap();
    let result_body = serde_json::to_value(&result).unwrap();
    let wait = body.get("awaits").cloned().unwrap_or(Value::Null);
    if let Some((state, party, reason)) = expected {
        assert_eq!(body["state"], state, "{name}: {body}");
        assert_eq!(wait["party"], party, "{name}: {body}");
        assert!(wait["since_ms"].is_u64(), "{name}: {body}");
        assert_eq!(wait["reason"], json!(reason), "{name}: {body}");
        assert_eq!(
            head_body["last_run_state"]["state"], body["state"],
            "{name}"
        );
        assert_eq!(head_body["last_run_state"]["awaits"], wait, "{name}");
        if matches!(state, "active" | "paused") {
            assert_eq!(
                head_body["last_run_state"]["reason"], body["reason"],
                "{name}"
            );
            assert!(
                result_body.get("awaits").is_none(),
                "a nonterminal result has no wait"
            );
        } else {
            assert_eq!(result_body["awaits"], wait, "{name}");
        }
    } else {
        assert!(
            body.get("awaits").is_none(),
            "a legacy or unclassified error must not invent a wait: {body}"
        );
        assert!(head_body["last_run_state"].get("awaits").is_none());
        assert!(result_body.get("awaits").is_none());
    }
    sample(
        &format!("{name}.run.status.json"),
        &serde_json::to_vec(&status).unwrap(),
    );
    sample(
        &format!("{name}.session.head.json"),
        &serde_json::to_vec(&head).unwrap(),
    );
    sample(
        &format!("{name}.run.result.json"),
        &serde_json::to_vec(&result).unwrap(),
    );
    let (sink, mut events) = mpsc::channel(4096);
    ServeHandler::subscribe(registry, key, SubscribeFrom::Start, sink)
        .await
        .unwrap();
    broca_test_support::wait_for(
        "the run's subscribed wait",
        async {
            let mut seq = 0;
            while let Some(event) = events.recv().await {
                if let Delivery::Control(event) = event {
                    seq += 1;
                    let matching = match &event.1 {
                        ControlUnit::Status { status, .. } => !status.held_calls().is_empty(),
                        ControlUnit::Paused { reason, .. } => {
                            body["reason"].as_str() == Some(reason.as_str())
                        }
                        ControlUnit::Error { .. } | ControlUnit::RunFinished { .. } => {
                            body["state"] == "error"
                        }
                        _ => false,
                    };
                    if !matching {
                        continue;
                    }
                    let bytes = broca_wire::encode_live_subscribe_control_with_cursor(
                        event.0.into(),
                        &event.1,
                        seq,
                        event.2.as_ref(),
                    )
                    .unwrap();
                    assert!(broca_wire::decode_subscribe_event(&bytes).is_ok());
                    let frame: Value = serde_json::from_slice(&bytes).unwrap();
                    let (kind, actual) = match &event.1 {
                        ControlUnit::Status { .. } => ("status", &frame["awaits"]),
                        ControlUnit::Paused { .. } => ("paused", &frame["unit"]["awaits"]),
                        ControlUnit::Error { .. } => ("error", &frame["unit"]["awaits"]),
                        ControlUnit::RunFinished { .. } => {
                            ("run_finished", &frame["unit"]["awaits"])
                        }
                        _ => unreachable!(),
                    };
                    assert_eq!(actual, &wait, "{name} {kind}: {frame}");
                    sample(&format!("{name}.subscribe.{kind}.json"), &bytes);
                    if kind != "error" {
                        return;
                    }
                }
            }
            panic!("the subscriber ended without the run's wait");
        },
        || format!("{name}: no wait control"),
    )
    .await;
    body
}

type RetryLive = (
    broca_core::provider::LiveStepProgress,
    mpsc::Sender<broca_core::OutputEvent>,
);

struct Retrying {
    inner: super::tests::LoopExecutor,
    live: std::sync::Mutex<Option<RetryLive>>,
}

#[async_trait::async_trait]
impl crate::executor::RunExecutor for Retrying {
    async fn execute_on_store(
        &self,
        job: crate::executor::StoreRunJob,
        out: mpsc::Sender<broca_core::OutputEvent>,
    ) -> crate::executor::StoreRunEnd {
        *self.live.lock().unwrap() =
            Some((job.control.step_progress.clone().unwrap(), out.clone()));
        self.inner.execute_on_store(job, out).await
    }
}

async fn actor_read(
    commands: &mpsc::UnboundedSender<crate::actor::LineageCmd>,
    request: crate::store_reads::StoreReadRequest,
) -> Value {
    use crate::store_reads::{StoreReadOutcome, StoreReadReply};
    let (reply, answer) = tokio::sync::oneshot::channel();
    commands
        .send(crate::actor::LineageCmd::StoreRead {
            request: Box::new(request),
            reply,
        })
        .unwrap();
    let StoreReadOutcome::Served(reply) = answer.await.unwrap() else {
        panic!("the actor keeps this run in memory")
    };
    match *reply {
        StoreReadReply::RunStatus(Ok(status)) => serde_json::to_value(status).unwrap(),
        StoreReadReply::RunResult(Ok(result)) => serde_json::to_value(result).unwrap(),
        StoreReadReply::Head(Ok(head)) => serde_json::to_value(head.reply).unwrap(),
        other => panic!(
            "the actor did not serve the requested status/head: {}",
            std::any::type_name_of_val(&other)
        ),
    }
}

async fn subscribe_actor(
    commands: &mpsc::UnboundedSender<crate::actor::LineageCmd>,
) -> mpsc::Receiver<Delivery> {
    let (sink, events) = mpsc::channel(4096);
    let (reply, answer) = tokio::sync::oneshot::channel();
    commands
        .send(crate::actor::LineageCmd::Subscribe {
            from: crate::fanout::FromCursor::Start,
            live: false,
            sink,
            reply,
        })
        .unwrap();
    assert!(matches!(
        answer.await.unwrap(),
        crate::actor::SubscribeOutcome::Attached
    ));
    events
}

async fn display_status(events: &mut mpsc::Receiver<Delivery>) -> broca_protocol::DisplayEvent {
    broca_test_support::wait_for(
        "the retry's display status",
        async {
            while let Some(delivery) = events.recv().await {
                if let Delivery::Display(event) = delivery {
                    if matches!(*event, broca_protocol::DisplayEvent::RunStatus { .. }) {
                        return (*event).clone();
                    }
                }
            }
            panic!("the subscription ended before its retry status");
        },
        || "no retry status".into(),
    )
    .await
}

#[tokio::test]
async fn run_awaits_retry_is_actor_owned_and_replayed_to_a_mid_wait_subscriber() {
    use crate::store_reads::StoreReadRequest;
    use broca_session_store::{
        backend::{MemoryBackend, SessionAddr},
        session::Session,
    };
    use cortexkit_role_llm_runner::awaits::{AwaitParty, RunAwaits};
    use std::sync::Arc;
    let gate = Arc::new(tokio::sync::Notify::new());
    let executor = Arc::new(Retrying {
        inner: super::tests::executor(
            vec![super::tests::text_step("m1", "done")],
            Some(gate.clone()),
        ),
        live: Default::default(),
    });
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(super::tests::ADDR.into()),
        super::tests::triple(),
        &super::tests::deployment(),
        false,
    )
    .await
    .unwrap();
    let actor = super::StoreActor::new(
        super::tests::bind(),
        session,
        executor.clone(),
        Box::new(broca_session_loop::parts::MemoryParts::default()),
    )
    .with_readers(Arc::new(move || backend));
    let (commands, receive) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receive));
    let commands = super::tests::Commands::over(commands, backend);
    super::tests::send(&commands, "r1", "hello").await;
    let (progress, output) =
        broca_test_support::wait_until("the actor's in-flight provider step", || async {
            executor
                .live
                .lock()
                .unwrap()
                .clone()
                .filter(|(progress, _)| progress.lock().unwrap().is_some())
                .ok_or_else(|| "the step has not started".to_owned())
        })
        .await;
    let mut original = subscribe_actor(&commands).await;
    let before_display = broca_test_support::wait_for(
        "the initial display snapshot",
        async {
            while let Some(delivery) = original.recv().await {
                if let Delivery::Display(event) = delivery {
                    return event;
                }
            }
            panic!("the subscription ended before its display snapshot");
        },
        || "no initial display reset".into(),
    )
    .await;
    sample(
        "retry.before.subscribe.display.json",
        &broca_wire::encode_live_subscribe_display(&before_display, 0).unwrap(),
    );
    for (name, request) in [
        (
            "run.status",
            StoreReadRequest::RunStatus(RunId("r1".into())),
        ),
        ("session.head", StoreReadRequest::Head),
        (
            "run.result",
            StoreReadRequest::RunResult(RunId("r1".into())),
        ),
    ] {
        let body = actor_read(&commands, request).await;
        sample(
            &format!("retry.before.{name}.json"),
            &serde_json::to_vec(&body).unwrap(),
        );
    }
    let mut wait = RunAwaits::new(AwaitParty::Runner, crate::store_awaits::now_ms());
    wait.reason = Some("provider_retry".to_owned().into());
    wait.next_ms = Some(wait.since_ms + 60_000);
    wait.attempt = Some(2);
    progress.lock().unwrap().as_mut().unwrap().retry_awaits = Some(Box::new(wait.clone()));
    let event = broca_protocol::DisplayEvent::RunStatus {
        run_id: RunId("r1".into()),
        state: "active".into(),
        reason: Some("provider_retry".into()),
        deadline_ms: None,
        awaits: Some(Box::new(wait.clone())),
    };
    output
        .send(broca_core::OutputEvent::Display(event.clone()))
        .await
        .unwrap();
    assert_eq!(display_status(&mut original).await, event);
    let status = actor_read(&commands, StoreReadRequest::RunStatus(RunId("r1".into()))).await;
    let head = actor_read(&commands, StoreReadRequest::Head).await;
    assert_eq!(status["awaits"], json!(wait));
    assert_eq!(head["last_run_state"]["awaits"], status["awaits"]);
    assert_eq!(head["last_run_state"]["reason"], status["reason"]);
    let mut joined = subscribe_actor(&commands).await;
    assert_eq!(display_status(&mut joined).await, event);
    let bytes = broca_wire::encode_live_subscribe_display(&event, 4).unwrap();
    assert!(broca_wire::decode_subscribe_event(&bytes).is_ok());
    sample("retry.after.subscribe.display.run_status.json", &bytes);
    sample(
        "retry.after.run.status.json",
        &serde_json::to_vec(&status).unwrap(),
    );
    sample(
        "retry.after.session.head.json",
        &serde_json::to_vec(&head).unwrap(),
    );
    let result = actor_read(&commands, StoreReadRequest::RunResult(RunId("r1".into()))).await;
    assert!(result.get("awaits").is_none());
    sample(
        "retry.after.run.result.json",
        &serde_json::to_vec(&result).unwrap(),
    );
    let mut expired = wait.clone();
    let now = crate::store_awaits::now_ms();
    expired.since_ms = now - 2000;
    expired.next_ms = Some(now - 1000);
    progress.lock().unwrap().as_mut().unwrap().retry_awaits = Some(Box::new(expired.clone()));
    let current = actor_read(&commands, StoreReadRequest::RunStatus(RunId("r1".into()))).await;
    assert_eq!(current["awaits"]["since_ms"], expired.since_ms);
    assert!(current["awaits"].get("next_ms").is_none());
    let mut rejoined = subscribe_actor(&commands).await;
    let fresh = display_status(&mut rejoined).await;
    let broca_protocol::DisplayEvent::RunStatus {
        awaits: Some(replayed),
        ..
    } = &fresh
    else {
        panic!("the current retry is visible")
    };
    assert_eq!(
        replayed.since_ms, expired.since_ms,
        "replay must read the current actor slot, not the captured frame"
    );
    assert_eq!(replayed.next_ms, None);
    assert_eq!(serde_json::to_value(replayed).unwrap(), current["awaits"]);
    let current_head = actor_read(&commands, StoreReadRequest::Head).await;
    assert_eq!(current_head["last_run_state"]["awaits"], current["awaits"]);
    sample(
        "retry.past-floor.run.status.json",
        &serde_json::to_vec(&current).unwrap(),
    );
    sample(
        "retry.past-floor.session.head.json",
        &serde_json::to_vec(&current_head).unwrap(),
    );
    sample(
        "retry.past-floor.subscribe.display.run_status.json",
        &broca_wire::encode_live_subscribe_display(&fresh, 4).unwrap(),
    );
    progress.lock().unwrap().as_mut().unwrap().retry_awaits = None;
    let cleared = broca_protocol::DisplayEvent::RunStatus {
        run_id: RunId("r1".into()),
        state: "active".into(),
        reason: None,
        deadline_ms: None,
        awaits: None,
    };
    output
        .send(broca_core::OutputEvent::Display(cleared.clone()))
        .await
        .unwrap();
    assert_eq!(display_status(&mut original).await, cleared);
    sample(
        "retry.after.subscribe.display.cleared.json",
        &broca_wire::encode_live_subscribe_display(&cleared, 5).unwrap(),
    );
    assert!(
        actor_read(&commands, StoreReadRequest::RunStatus(RunId("r1".into())))
            .await
            .get("awaits")
            .is_none()
    );
    gate.notify_one();
    super::tests::idle(&commands).await;
    drop(original);
    drop(joined);
    drop(rejoined);
    drop(output);
    drop(commands);
    served.await.unwrap();
}
