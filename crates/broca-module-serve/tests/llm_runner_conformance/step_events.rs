//! Observe real bus publications and join them to independent native log and sync evidence.
use super::*;
#[cfg(feature = "test-clock")]
use cortexkit_role_llm_runner::step_events::StepRef;
use cortexkit_role_llm_runner_conformance::step_events::StepEventObservation;
#[cfg(feature = "test-clock")]
use cortexkit_role_llm_runner_conformance::step_events::{PublishedStep, RecordedStep};
use std::collections::BTreeMap;

pub(super) fn fixture_session(session: &str) -> bool {
    session.contains("step-events") || session.starts_with("step_events_")
}

#[cfg(feature = "test-clock")]
async fn recorded(
    subject: &BrocaSubject,
    live: &Live,
    session: &str,
) -> Result<Vec<RecordedStep>, HarnessError> {
    use broca_session_store::{
        backend::{LocalBackend, SessionAddr},
        head::SessionTriple,
        keys::DeploymentKey,
        record::Record,
        session::Session,
    };
    let identity = subject.bind_identity(session);
    let address = broca_core::session_addr(&identity);
    let class = broca_wal::session_storage_class(&live.state_root, &address)
        .map_err(|error| HarnessError::new(error.to_string()))?;
    let Some(class) = class else {
        return Err(HarnessError::new(
            "step-event fixture is not a new-storage head",
        ));
    };
    let stored = Session::view(
        LocalBackend::new(class.store_root(&live.state_root)),
        SessionAddr(address),
        &SessionTriple {
            project_root: identity.project_root.display().to_string(),
            harness: identity.harness.clone(),
            session: identity.session.clone(),
        },
        &DeploymentKey::unloaded(1),
    )
    .await
    .map_err(|error| HarnessError::new(format!("read the plain step fixture: {error:?}")))?;
    let incarnation = stored
        .head()
        .incarnation
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let frames = stored
        .open_log_records()
        .await
        .map_err(|error| HarnessError::new(format!("read native attempt log: {error:?}")))?;
    let mut recorded = Vec::new();
    for (seq, record) in &frames {
        if let Record::Attempt(attempt) = record {
            let id = attempt
                .attempt_id
                .as_deref()
                .ok_or_else(|| HarnessError::new("native attempt omitted its identity"))?;
            let completes_step = frames.iter().any(|(_, record)| matches!(record, Record::StepEnd(end) if end.run_id == attempt.run_id && end.step_id == attempt.step_id))
                && !frames.iter().any(|(_, record)| matches!(record, Record::Attempt(later) if later.run_id == attempt.run_id && later.step_id == attempt.step_id && later.attempt > attempt.attempt));
            recorded.push(
                RecordedStep::new(
                    StepRef::new(
                        "broca",
                        broca_core::agent_session_ref(&identity),
                        &incarnation,
                        *seq,
                    ),
                    &attempt.run_id,
                    attempt.step_id.to_string(),
                    std::num::NonZeroU64::new(u64::from(attempt.attempt) + 1).unwrap(),
                    completes_step,
                )
                .with_attempt_id(id),
            );
        }
    }
    Ok(recorded)
}

#[cfg(feature = "test-clock")]
fn handoffs(logs: &Path) -> Result<Vec<BTreeMap<String, String>>, String> {
    let mut handoffs = Vec::new();
    for entry in std::fs::read_dir(logs).map_err(|error| error.to_string())? {
        let path = entry.map_err(|error| error.to_string())?.path();
        if !path.extension().is_some_and(|extension| extension == "log") {
            continue;
        }
        let text = std::fs::read_to_string(path).map_err(|error| error.to_string())?;
        for line in text
            .lines()
            .filter(|line| line.contains("step event durable handoff"))
        {
            handoffs.push(
                line.split_whitespace()
                    .filter_map(|field| {
                        let (name, value) = field.split_once('=')?;
                        Some((name.to_owned(), value.trim_matches('"').to_owned()))
                    })
                    .collect(),
            );
        }
    }
    Ok(handoffs)
}

fn durability(body: &Value, evidence: &[BTreeMap<String, String>]) -> Option<bool> {
    let matching = evidence.iter().find(|fields| {
        ["session_ref", "log_incarnation", "attempt_id"]
            .iter()
            .all(|name| fields.get(*name).map(String::as_str) == body[*name].as_str())
            && fields
                .get("step_seq")
                .and_then(|seq| seq.parse::<u64>().ok())
                == body["seq"].as_u64()
    })?;
    let number = |name| {
        matching
            .get(name)
            .and_then(|value| value.parse::<u64>().ok())
    };
    Some(match matching.get("storage").map(String::as_str) {
        Some("wal") => number("synced_seq")
            .zip(body["seq"].as_u64())
            .is_some_and(|(synced, seq)| synced >= seq),
        Some("store") => number("synced_through")
            .zip(number("record_end"))
            .is_some_and(|(synced, end)| end > 0 && synced >= end),
        _ => false,
    })
}

pub(super) async fn observe(
    subject: &BrocaSubject,
    live: &Live,
    session: &str,
) -> Result<StepEventObservation, HarnessError> {
    #[cfg(not(feature = "test-clock"))]
    {
        let _ = (subject, live, session);
        Err(HarnessError::new(
            "native step-event capture requires test-clock",
        ))
    }
    #[cfg(feature = "test-clock")]
    {
        let routed_live = routed_accounts::session_live(subject, live, session).await?;
        let live = routed_live.as_deref().unwrap_or(live);
        let bus = subject
            .bus
            .as_ref()
            .ok_or_else(|| HarnessError::new("step-event fixture has no private bus"))?;
        let recorded = recorded(subject, live, session).await?;
        if recorded.is_empty() {
            return Err(HarnessError::new(
                "step-event fixture has no durable attempt records",
            ));
        }
        let identity = subject.bind_identity(session);
        Ok(broca_test_support::wait_until(
            "native step publications and their handoff evidence",
            || async {
                let evidence = handoffs(&live.logs_dir)?;
                let mut published = Vec::new();
                for (topic, bytes) in bus.step_publications() {
                    let body: Value =
                        serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
                    if body["session"].as_str() != Some(identity.session.as_str())
                        || body["harness"].as_str() != Some(identity.harness.as_str())
                    {
                        continue;
                    }
                    let durable = durability(&body, &evidence)
                        .ok_or_else(|| format!("no handoff evidence for {body}"))?;
                    published.push(PublishedStep::new(topic, body, durable));
                }
                if published.len() < recorded.len() {
                    return Err(format!(
                        "{} publications for {} durable attempts",
                        published.len(),
                        recorded.len()
                    ));
                }
                Ok(StepEventObservation::new(recorded.clone(), published))
            },
        )
        .await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "test-clock")]
    use cortexkit_role_llm_runner_conformance::ScriptedTurn;

    #[test]
    fn durability_requires_matching_identity_and_an_actual_sync_bound() {
        let body = json!({"session_ref":"ref", "log_incarnation":"lineage.writer", "attempt_id":"attempt", "seq":7});
        let mut fields = BTreeMap::from([
            ("session_ref".into(), "ref".into()),
            ("log_incarnation".into(), "lineage.writer".into()),
            ("attempt_id".into(), "attempt".into()),
            ("step_seq".into(), "7".into()),
            ("storage".into(), "wal".into()),
            ("synced_seq".into(), "6".into()),
        ]);
        assert_eq!(durability(&body, &[fields.clone()]), Some(false));
        fields.insert("synced_seq".into(), "8".into());
        assert_eq!(durability(&body, &[fields.clone()]), Some(true));
        fields.insert("attempt_id".into(), "another".into());
        assert_eq!(durability(&body, &[fields]), None);
        assert_eq!(durability(&body, &[]), None);
    }

    #[test]
    fn store_durability_requires_the_writer_byte_bound() {
        let body = json!({"session_ref":"ref", "log_incarnation":"incarnation", "attempt_id":"attempt", "seq":7});
        let mut fields = BTreeMap::from([
            ("session_ref".into(), "ref".into()),
            ("log_incarnation".into(), "incarnation".into()),
            ("attempt_id".into(), "attempt".into()),
            ("step_seq".into(), "7".into()),
            ("storage".into(), "store".into()),
            ("record_end".into(), "1200".into()),
            ("synced_through".into(), "1199".into()),
        ]);
        assert_eq!(durability(&body, &[fields.clone()]), Some(false));
        fields.insert("synced_through".into(), "1200".into());
        assert_eq!(durability(&body, &[fields.clone()]), Some(true));
        fields.remove("record_end");
        assert_eq!(durability(&body, &[fields]), Some(false));
    }

    #[cfg(feature = "test-clock")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn native_step_publications_match_durable_records_and_handoff_positions() {
        if !session_class::rigs_present() {
            return;
        }
        let scratch = session_class::scratch("step-events-native");
        let mut subject = BrocaSubject::new(&scratch).await;
        attach_notice_bus(&mut subject, &scratch).await;
        subject
            .install_script(
                "native-step-events",
                Script {
                    turns: vec![
                        ScriptedTurn::new(vec![ScriptedPart::Text("first".into())]),
                        ScriptedTurn::new(vec![ScriptedPart::Text("second".into())]),
                    ],
                },
            )
            .await
            .unwrap();
        let mut live = subject.start(&scratch.join("rig")).await.unwrap();
        let route = subject
            .session_route(&live, "native-step-events", &subject.owner_stamp())
            .await
            .unwrap();
        for creating in [true, false] {
            let mut fields = subject.send_fields("native-step-events", creating);
            fields.insert("prompt".into(), json!("hello"));
            let sent = match route
                .request("session.send", Value::Object(fields))
                .await
                .unwrap()
            {
                Reply::Response(sent) => sent,
                other => panic!("step fixture send refused: {other:?}"),
            };
            broca_test_support::wait_until("the step fixture run finished", || async {
                match route
                    .request("run.status", json!({"run_id":sent["run_id"]}))
                    .await
                    .unwrap()
                {
                    Reply::Response(status)
                        if status["state"] != "active" && status["state"] != "pending" =>
                    {
                        assert_eq!(
                            status["state"], "completed",
                            "step fixture run did not complete: {status}"
                        );
                        Ok(())
                    }
                    other => Err(format!("{other:?}")),
                }
            })
            .await;
        }
        let observation = observe(&subject, &live, "native-step-events")
            .await
            .unwrap();
        assert_eq!(observation.recorded.len(), 2);
        assert_eq!(observation.published.len(), 2);
        for record in &observation.recorded {
            assert!(record.completes_step);
            assert_eq!(
                observation
                    .published
                    .iter()
                    .filter(|publication| {
                        publication.body["attempt_id"].as_str() == record.attempt_id.as_deref()
                    })
                    .count(),
                1
            );
        }
        for publication in &observation.published {
            assert!(publication.record_durable_at_publish);
            let event = cortexkit_role_llm_runner::step_events::decode_step_finished(
                &publication.subject,
                &serde_json::to_vec(&publication.body).unwrap(),
            )
            .unwrap();
            let matches: Vec<_> = observation
                .recorded
                .iter()
                .filter(|record| record.attempt_id == event.attempt_id)
                .collect();
            assert_eq!(matches.len(), 1);
            let record = matches[0];
            assert_eq!(event.step_ref(), record.position);
            assert_eq!(event.run_id, record.run_id);
            assert_eq!(event.step_id, record.step_id);
            assert_eq!(event.attempt, record.attempt);
        }
        live.kill_all();
    }
}
