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
    // The session store reports the assistant id; the comparison leaves it out.
    "message_id",
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
    for new_storage in [true] {
        let (calls, mut inbox) = mpsc::unbounded_channel();
        let events =
            with_bus.then(|| Arc::new(StepEvents::start_on_bus(Arc::new(Capture { calls, fail }))));
        let module = module_step_events(script(interrupted), Some(cipher), events.clone()).await;
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
            if !interrupted {
                let entries = session.entries(1, 100).await.unwrap();
                let assistant = entries
                    .iter()
                    .find(|entry| entry.message.role == broca_protocol::content::Role::Assistant)
                    .unwrap();
                assert_eq!(
                    event.message_id.as_deref(),
                    Some(assistant.state.facts.as_ref().unwrap().mid.as_str())
                );
            } else {
                assert_eq!(event.message_id, None);
            }
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
        // The session store's attempt records no physical request length, so the event leaves
        // `request_body_bytes` out rather than stand a canonical length in for it.
        assert!(normalized(&bodies[0]).get("request_body_bytes").is_none());
        broca_test_support::pin!("the step event", normalized(&bodies[0]));
    }
}

async fn durable_step_events_match(cipher: Cipher) {
    scenario(cipher, true, false, false).await;
    scenario(cipher, true, false, true).await;
}

async fn handoff_evidence(new_storage: bool) {
    let (calls, mut inbox) = mpsc::unbounded_channel();
    let events = Arc::new(StepEvents::start_on_bus(Arc::new(Capture {
        calls,
        fail: false,
    })));
    let module = module_step_events(script(false), Some(Cipher::None), Some(events)).await;
    let session_ref = broca_core::agent_session_ref(&module.key);
    let capture = broca_test_support::LogCapture::observe(move |event| {
        event.target == "step-events.evidence"
            && event.fields.get("session_ref") == Some(&session_ref)
    });
    let sent = module.send("evidence-run", "hello", None).await;
    assert!(matches!(sent, SendOutcome::Active { .. }), "{sent:?}");
    let (subject, bytes) =
        broca_test_support::wait_for("step-event evidence publication", inbox.recv(), || {
            format!("new_storage={new_storage}")
        })
        .await
        .unwrap();
    let published =
        cortexkit_role_llm_runner::step_events::decode_step_finished(&subject, &bytes).unwrap();
    let evidence = capture
        .events()
        .into_iter()
        .find(|event| {
            event.fields.get("attempt_id").map(String::as_str) == published.attempt_id.as_deref()
        })
        .expect("a real publication has writer-side evidence");
    assert_eq!(evidence.level, tracing::Level::DEBUG);
    assert_eq!(
        evidence.fields["log_incarnation"],
        published.log_incarnation
    );
    assert_eq!(
        evidence.fields["step_seq"].parse::<u64>().unwrap(),
        published.seq
    );
    if new_storage {
        assert_eq!(evidence.fields["storage"], "store");
        let record_end = evidence.fields["record_end"].parse::<u64>().unwrap();
        let synced = evidence.fields["synced_through"].parse::<u64>().unwrap();
        assert!(record_end > 0);
        assert!(synced >= record_end, "published before sync: {evidence:?}");
    }
    assert!(module.registry.stop_and_seal().await);
}

#[tokio::test]
async fn store_step_event_handoff_logs_the_actual_synced_position() {
    handoff_evidence(true).await;
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
    let module = module_step_events(script, Some(cipher), Some(events.clone())).await;
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

async fn cached_anthropic_auto_step_events_match(cipher: Cipher) {
    let mut bodies = Vec::new();
    for new_storage in [true] {
        let (calls, mut inbox) = mpsc::unbounded_channel();
        let events = Arc::new(StepEvents::start_on_bus(Arc::new(Capture {
            calls,
            fail: false,
        })));
        let module = module_options_with_render(
            script(false),
            Some(cipher),
            Some(events),
            IDLE,
            None,
            Some(serde_json::to_value(super::warm::warmable()).unwrap()),
        )
        .await;
        let mut request = request("hello");
        request.model.provider_module_id = "anthropic".into();
        request.model.model_id = "claude-opus-4-8".into();
        request.cache =
            Some(serde_json::from_value(serde_json::json!({"keep_warm": "auto"})).unwrap());
        assert!(matches!(
            module.send_request("cached-step", request, None).await,
            SendOutcome::Active { .. }
        ));
        broca_test_support::wait_until("the cached run is idle", || async {
            match module.registry.route_status(&module.key).await {
                Ok(LineageStatus::Idle) => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
        assert!(module.registry.stop_and_seal().await);
        let (subject, body) =
            broca_test_support::wait_for("cached step publication", inbox.recv(), || {
                format!("new={new_storage}")
            })
            .await
            .unwrap();
        let event =
            cortexkit_role_llm_runner::step_events::decode_step_finished(&subject, &body).unwrap();
        assert_eq!(
            serde_json::to_value(event.cache).unwrap(),
            json!({"ttl_ms":3_600_000, "warming":{"on":true}})
        );
        let mut body = normalized(&body);
        body.as_object_mut().unwrap().remove("request_body_bytes");
        bodies.push(body);
    }
    broca_test_support::pin!("alike", bodies[0]);
}

cipher_tests! { durable_step_events_match, no_bus_publishes_no_step_events, run_publishes_step_events_without_configuration, failing_step_publisher_does_not_fail_runs, a_model_attempt_without_send_time_emits_no_step_event, cached_anthropic_auto_step_events_match }
