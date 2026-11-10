use super::*;
use crate::step_events::{StepEventPublisher, StepEvents};
use broca_protocol::ProviderControl;
use broca_session_store::keys::Cipher;
use serde_json::{json, Value};

// Each storage independently allocates its durable identity and log position. The assertions
// below check those against that storage before comparing every other serialized member.
const INDEPENDENT_DURABLE_FIELDS: &[&str] = &[
    "attempt_id",
    "log_incarnation",
    "seq",
    "started_at_ms",
    "finished_at_ms",
    "session_ref",
];

pub(super) struct Capture {
    pub(super) calls: mpsc::UnboundedSender<(String, Vec<u8>)>,
    pub(super) fail: bool,
}

#[async_trait]
impl StepEventPublisher for Capture {
    async fn live_subject(&self, runner: &str) -> Result<String, String> {
        Ok(format!("ck.fixture.live.{runner}.step.finished.v1"))
    }

    async fn publish(&self, subject: &str, body: Vec<u8>) -> Result<(), String> {
        self.calls.send((subject.into(), body)).unwrap();
        if self.fail {
            Err("bus unavailable".into())
        } else {
            Ok(())
        }
    }
}

fn script(interrupted: bool) -> Vec<Vec<NormalizedEvent>> {
    let attempt = serde_json::from_value(json!({
        "attempt": 0, "request_body_len": 10, "request_body_sha256": "0".repeat(64),
        "http_status": 200, "response_body_len": 20, "response_body_sha256": "1".repeat(64),
        "outcome": if interrupted { "sealed_in_flight" } else { "complete" },
        "ts_ms": 1100, "sent_at_ms": 1000,
    }))
    .unwrap();
    let mut events = vec![NormalizedEvent::Control(ProviderControl::AttemptFinished {
        attempt,
    })];
    let mut response = super::super::tests::text_step("a1", "hello");
    for event in &mut response {
        if let NormalizedEvent::Control(ProviderControl::Finished { usage, .. }) = event {
            *usage = serde_json::from_value(
                json!({"input_tokens": 100, "cached_input_tokens": 30, "output_tokens": 20}),
            )
            .unwrap();
        }
    }
    events.extend(response);
    vec![events]
}

pub(super) fn normalized(body: &[u8]) -> Value {
    let mut value: Value = serde_json::from_slice(body).unwrap();
    for key in INDEPENDENT_DURABLE_FIELDS {
        value.as_object_mut().unwrap().remove(*key);
    }
    value
}

pub(super) fn sample(name: &str, body: &[u8]) {
    if let Some(dir) = std::env::var_os("BROCA_STEP_EVENT_SAMPLES_DIR") {
        let dir = std::path::PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(name), body).unwrap();
    }
}

async fn scenario(cipher: Cipher, with_bus: bool, fail: bool, interrupted: bool) {
    let mut bodies = Vec::new();
    for new_storage in [false, true] {
        let (calls, mut inbox) = mpsc::unbounded_channel();
        let events =
            with_bus.then(|| Arc::new(StepEvents::start_on_bus(Arc::new(Capture { calls, fail }))));
        let module = module_step_events(
            new_storage,
            script(interrupted),
            Some(cipher),
            events.clone(),
        )
        .await;
        let outcome = module.send("step-run", "hello", None).await;
        assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
        broca_test_support::wait_until("the scripted run is idle", || async {
            match module.registry.route_status(&module.key).await {
                Ok(LineageStatus::Idle) => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
        assert!(module.registry.stop_and_seal().await);
        if !with_bus {
            assert!(inbox.try_recv().is_err());
            continue;
        }
        let (subject, body) =
            broca_test_support::wait_for("durable step publication", inbox.recv(), || {
                format!("new={new_storage} interrupted={interrupted}")
            })
            .await
            .unwrap();
        assert_eq!(subject, "ck.fixture.live.broca.step.finished.v1");
        let event =
            cortexkit_role_llm_runner::step_events::decode_step_finished(&subject, &body).unwrap();
        assert_eq!(event.runner, "broca");
        assert_eq!(event.harness, module.key.harness);
        assert_eq!(event.session, module.key.session);
        assert_eq!(
            event.session_ref,
            broca_core::agent_session_ref(&module.key)
        );
        assert_eq!(event.started_at_ms, 1000);
        assert_eq!(event.finished_at_ms, 1100);
        assert!(serde_json::from_slice::<Value>(&body)
            .unwrap()
            .get("harness_step_ref")
            .is_none());
        if interrupted {
            assert_eq!(event.terminal_status, "interrupted");
            for key in [
                "input_tokens",
                "cached_input_tokens",
                "cache_write_tokens",
                "output_tokens",
                "reasoning_tokens",
                "cache_write_5m_tokens",
                "cache_write_1h_tokens",
            ] {
                assert!(
                    serde_json::from_slice::<Value>(&body)
                        .unwrap()
                        .get(key)
                        .is_none(),
                    "{key}"
                );
            }
        } else {
            assert_eq!(event.input_tokens, Some(100));
            assert_eq!(event.output_tokens, Some(20));
        }
        if new_storage {
            let session = module.load().await;
            let records = session.open_log_records().await.unwrap();
            assert!(records.iter().any(|(_, record)| matches!(record,
                broca_session_store::record::Record::RunEnd(end) if end.reason == broca_protocol::RunFinishReason::Completed)));
            let attempts: Vec<_> = records
                .into_iter()
                .filter_map(|(seq, record)| match record {
                    broca_session_store::record::Record::Attempt(attempt) => Some((seq, attempt)),
                    _ => None,
                })
                .collect();
            assert_eq!(attempts.len(), 1);
            assert_eq!(event.seq, attempts[0].0);
            assert_eq!(event.attempt_id, attempts[0].1.attempt_id);
            let incarnation: String = session
                .head()
                .incarnation
                .0
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            assert_eq!(event.log_incarnation, incarnation);
        } else {
            let records = module.wal.read_records(&module.key).unwrap();
            assert!(records.iter().any(|frame| matches!(
                &frame.record,
                broca_wal::WalRecord::RunFinished {
                    reason: broca_protocol::RunFinishReason::Completed,
                    ..
                }
            )));
            let attempts: Vec<_> = records
                .iter()
                .filter_map(|frame| match &frame.record {
                    broca_wal::WalRecord::ModelAttemptFinished {
                        attempt_id: Some(id),
                        ..
                    } => Some((frame.seq, id)),
                    _ => None,
                })
                .collect();
            assert_eq!(attempts.len(), 1);
            assert_eq!(event.seq, attempts[0].0);
            assert_eq!(event.attempt_id.as_deref(), Some(attempts[0].1.as_str()));
            let writer = records
                .iter()
                .find_map(|frame| match &frame.record {
                    broca_wal::WalRecord::WriterOpened { writer_id, .. } => Some(writer_id),
                    _ => None,
                })
                .unwrap();
            let writer: String = writer.iter().map(|b| format!("{b:02x}")).collect();
            let lineage = module.wal.open(&module.key).unwrap().lineage_id().unwrap();
            assert_eq!(event.log_incarnation, format!("{lineage}.{writer}"));
        }
        tokio::task::yield_now().await;
        assert!(inbox.try_recv().is_err(), "an attempt published twice");
        if fail {
            assert_eq!(events.as_ref().unwrap().dropped(), 1);
        }
        let storage = if new_storage { "store" } else { "today" };
        let mode = if cipher == Cipher::None {
            "plain"
        } else {
            "encrypted"
        };
        let status = if interrupted {
            "interrupted"
        } else {
            "completed"
        };
        sample(&format!("{status}-{storage}-{mode}.json"), &body);
        bodies.push(body);
    }
    if with_bus {
        let mut expected = normalized(&bodies[0]);
        // Only today's reservation measures physical bytes. The new attempt's canonical
        // length cannot stand in for it, so pin this documented evidence gap explicitly.
        assert_eq!(
            expected
                .as_object_mut()
                .unwrap()
                .remove("request_body_bytes"),
            Some(json!(10))
        );
        assert!(normalized(&bodies[1]).get("request_body_bytes").is_none());
        assert_eq!(expected, normalized(&bodies[1]));
    }
}

async fn durable_step_events_match(cipher: Cipher) {
    scenario(cipher, true, false, false).await;
    scenario(cipher, true, false, true).await;
}

async fn no_bus_publishes_no_step_events(cipher: Cipher) {
    scenario(cipher, false, false, false).await;
}

async fn run_publishes_step_events_without_configuration(cipher: Cipher) {
    scenario(cipher, true, false, false).await;
}

async fn failing_step_publisher_does_not_fail_runs(cipher: Cipher) {
    scenario(cipher, true, true, false).await;
}

async fn a_model_attempt_without_send_time_emits_no_step_event(cipher: Cipher) {
    let (calls, mut inbox) = mpsc::unbounded_channel();
    let events = Arc::new(
        StepEvents::start("fixture", "broca", Arc::new(Capture { calls, fail: false })).unwrap(),
    );
    let mut script = script(false);
    let NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) = &mut script[0][0]
    else {
        unreachable!()
    };
    attempt.sent_at_ms = None;
    let module = module_step_events(true, script, Some(cipher), Some(events.clone())).await;
    assert!(matches!(
        module.send("no-send-time", "hello", None).await,
        SendOutcome::Active { .. }
    ));
    broca_test_support::wait_until("the unknown-start attempt is recorded", || async {
        if events.dropped() == 1 {
            Ok(())
        } else {
            Err(format!("dropped={}", events.dropped()))
        }
    })
    .await;
    assert!(module.registry.stop_and_seal().await);
    assert!(inbox.try_recv().is_err());
}

cipher_tests! { durable_step_events_match, no_bus_publishes_no_step_events, run_publishes_step_events_without_configuration, failing_step_publisher_does_not_fail_runs, a_model_attempt_without_send_time_emits_no_step_event }
