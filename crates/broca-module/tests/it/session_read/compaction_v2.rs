use super::*;
use serde_json::{json, Value};

fn metadata() -> Vec<WalRecord> {
    include_str!("../../../../broca-wal/tests/fixtures/compaction_v2_records.jsonl")
        .lines()
        .take(3)
        .map(|bytes| serde_json::from_str(bytes).unwrap())
        .collect()
}

async fn responses(rig: &Rig, key: &BindIdentity) -> Vec<Value> {
    let page = rig
        .registry
        .read(read_request(key, Some(0), Some(100)))
        .await
        .unwrap();
    let mut page = serde_json::to_value(page).unwrap();
    // Control cursors name physical WAL sequences, which interleaved frames necessarily move.
    page.as_object_mut().unwrap().remove("head");
    let mut responses = vec![page];
    for form in ["raw", "display"] {
        let request =
            serde_json::from_value(json!({"from_mid":"m0","limit":100,"form":form})).unwrap();
        let mut history = broca_module::history::read(&rig.registry, key, &request)
            .await
            .unwrap();
        history.as_object_mut().unwrap().remove("head");
        responses.push(history);
    }
    for run in ["r1", "r2"] {
        responses.push(
            serde_json::to_value(
                rig.registry
                    .run_status(key, RunId(run.into()))
                    .await
                    .unwrap(),
            )
            .unwrap(),
        );
        responses.push(
            serde_json::to_value(
                rig.registry
                    .run_result(key, RunId(run.into()))
                    .await
                    .unwrap(),
            )
            .unwrap(),
        );
    }
    assert_eq!(
        rig.registry.slot_state(key),
        None,
        "reads never start an executor"
    );
    responses
}

#[tokio::test]
async fn compaction_v2_is_invisible_to_read_history_status_result_and_store_projection() {
    for terminal in ["completed", "paused", "zero-step-interrupted"] {
        let plain = Rig::new("mode3-plain", Arc::new(NoopExec));
        let with = Rig::new("mode3-with", Arc::new(NoopExec));
        let key = plain.key("mode3");
        let mut records = vec![
            started(&key, "r1", "first question"),
            WalRecord::StepStarted { step_id: StepId(1) },
            assistant_text(1, "first answer"),
            WalRecord::TurnFinished { step_id: StepId(1) },
            finished_ok(),
            started(&key, "r2", "second question"),
        ];
        if terminal != "zero-step-interrupted" {
            records.extend([
                WalRecord::StepStarted { step_id: StepId(1) },
                assistant_text(1, "second answer"),
                WalRecord::TurnFinished { step_id: StepId(1) },
            ]);
        }
        if terminal == "paused" {
            records.push(WalRecord::RunPaused {
                account_limit: None,
                run_id: RunId("r2".into()),
                ts_ms: None,
                reason: PauseReason::AuthRequired,
                error: None,
            });
        } else {
            let mut finished = finished_ok();
            if terminal == "zero-step-interrupted" {
                if let WalRecord::RunFinished { reason, usage, .. } = &mut finished {
                    *reason = RunFinishReason::Interrupted;
                    *usage = Usage::default();
                }
            }
            records.push(finished);
        }
        for record in &mut records {
            match record {
                WalRecord::RunStarted { written_at_ms, .. }
                | WalRecord::ModelStepFinished { written_at_ms, .. } => *written_at_ms = Some(100),
                _ => {}
            }
        }
        append_seed(&plain.wal_store, &key, &records);
        let mut interleaved = metadata();
        for record in &records {
            let mut record = record.clone();
            if let WalRecord::RunStarted {
                compaction_plan_version,
                ..
            } = &mut record
            {
                *compaction_plan_version = Some(u32::MAX);
            }
            interleaved.push(record);
            interleaved.extend(metadata());
        }
        let plain_bytes = std::fs::read(plain.wal_store.wal_path(&key)).unwrap();
        // Equivalent logs share their lineage identity, so public history cursors can compare
        // byte-for-byte. Only their framed records and physical sequence positions differ.
        let mut bytes = plain_bytes[..broca_wal::HEADER_LEN + 16].to_vec();
        for (i, record) in interleaved.iter().enumerate() {
            bytes.extend(broca_wal::framing::encode(i as u64 + 1, 1, record).unwrap());
        }
        let with_path = with.wal_store.wal_path(&key);
        std::fs::create_dir_all(with_path.parent().unwrap()).unwrap();
        std::fs::write(&with_path, bytes).unwrap();

        for phase in ["full WAL", "projected prefix and tail", "fully projected"] {
            if phase == "projected prefix and tail" {
                for (rig, cut) in [(&plain, 5), (&with, 23)] {
                    let frames = rig.wal_store.read_records(&key).unwrap();
                    rig.store.project(&key, &frames[..cut]).unwrap();
                }
            } else if phase == "fully projected" {
                materialize_all(&plain, &key);
                materialize_all(&with, &key);
            }
            let expected = responses(&plain, &key).await;
            let actual = responses(&with, &key).await;
            assert_eq!(actual, expected, "{terminal}: {phase}");
            let messages = expected[0]["messages"].as_array().unwrap();
            assert!(!messages.is_empty());
            for (i, message) in messages.iter().enumerate() {
                assert_eq!(message["ordinal"], i as u64);
                assert_eq!(message["mid"], format!("m{i}"));
            }
        }
    }
}

#[tokio::test]
async fn compaction_v2_refuses_send_steer_and_restart_resume_without_writes_but_allows_reads() {
    let rig = Rig::new("mode3-read-only", Arc::new(NoopExec));
    let key = rig.key("mode3");
    let mut start = started(&key, "r", "question");
    if let WalRecord::RunStarted {
        on_restart,
        written_at_ms,
        ..
    } = &mut start
    {
        *on_restart = Some(broca_protocol::request::OnRestart::Resume);
        *written_at_ms = Some(100);
    }
    let records = [
        start,
        WalRecord::RunPaused {
            account_limit: None,
            run_id: RunId("r".into()),
            ts_ms: None,
            reason: PauseReason::Restart,
            error: None,
        },
    ];
    append_seed(&rig.wal_store, &key, &records);
    let path = rig.wal_store.wal_path(&key);
    let original = std::fs::read(&path).unwrap();
    let mut bytes = original[..broca_wal::HEADER_LEN + 16].to_vec();
    for (i, record) in records.iter().enumerate() {
        let requires: &[&str] = if i == 1 {
            &[broca_wal::COMPACTION_V2_FEATURE]
        } else {
            &[]
        };
        bytes.extend(
            broca_wal::framing::encode_with_requires(i as u64 + 1, 1, requires, record).unwrap(),
        );
    }
    std::fs::write(&path, &bytes).unwrap();

    for delivery in [
        broca_protocol::Delivery::default(),
        broca_protocol::Delivery::Steer,
    ] {
        let request = req(&key, "next", "do not run");
        let error = rig
            .registry
            .send(broca_module::serve::SendRequest {
                session_class: None,
                retention: None,
                stop_sequences_named: false,
                title: None,
                conversation_key: None,
                delivery,
                session: key.clone(),
                run_id: request.run_id.clone(),
                send_id: Some("next".into()),
                request: Box::new(request.to_submission()),
                skip_materialization: false,
                kind: RunKind::FreshOrResume,
                plan: None,
                order: None,
            })
            .await
            .expect_err("reader-only session must refuse before admission");
        assert!(
            matches!(error, broca_module::serve::ServeError::WalNewerWriter { missing, reason }
            if missing == ["compaction-v2/v1"] && reason.contains("needs a newer build to run"))
        );
        assert_eq!(std::fs::read(&path).unwrap(), bytes);
    }
    let counts = rig
        .registry
        .resume_restart_pauses(rig.wal_store.clone(), vec![("r".into(), key.clone())])
        .await;
    assert_eq!(
        counts,
        (0, 0),
        "restart neither resumes nor seals the read-only run"
    );
    let held = rig.registry.resume_held_run(&key).await.unwrap_err();
    assert!(held.contains("compaction-v2/v1"), "{held}");
    assert!(held.contains("needs a newer build to run"), "{held}");
    assert_eq!(std::fs::read(&path).unwrap(), bytes);

    let page = rig
        .registry
        .read(read_request(&key, Some(0), Some(100)))
        .await
        .unwrap();
    assert_eq!(page.messages.len(), 1);
    assert_eq!(page.messages[0].ordinal, 0);
    let head = rig.registry.head(&key).await.unwrap();
    assert!(serde_json::to_value(head).unwrap().is_object());
    let history = serde_json::from_value(json!({"limit":100})).unwrap();
    assert_eq!(
        broca_module::history::read(&rig.registry, &key, &history)
            .await
            .unwrap()["messages"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    rig.registry
        .run_status(&key, RunId("r".into()))
        .await
        .unwrap();
    rig.registry
        .run_result(&key, RunId("r".into()))
        .await
        .unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    assert_eq!(rig.registry.slot_state(&key), None);
}

#[tokio::test]
async fn compaction_v2_actorless_reads_share_interruption_rule_and_preserve_durable_pauses() {
    for pause in [
        None,
        Some(PauseReason::AuthRequired),
        Some(PauseReason::AccountLimit),
        Some(PauseReason::ToolHeld),
        Some(PauseReason::Restart),
    ] {
        let mut rig = Rig::new("mode3-unowned", Arc::new(NoopExec));
        let key = rig.key("mode3");
        let mut start = started(&key, "r", "question");
        if let WalRecord::RunStarted { written_at_ms, .. } = &mut start {
            *written_at_ms = Some(100);
        }
        let mut records = vec![start];
        if let Some(reason) = pause {
            records.push(WalRecord::RunPaused {
                account_limit: None,
                run_id: RunId("r".into()),
                ts_ms: None,
                reason,
                error: None,
            });
        }
        append_seed(&rig.wal_store, &key, &records);
        let path = rig.wal_store.wal_path(&key);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend(
            broca_wal::framing::encode(records.len() as u64 + 1, 1, &metadata()[0]).unwrap(),
        );
        std::fs::write(&path, &bytes).unwrap();
        let expected = if pause.is_some() {
            "paused"
        } else {
            "interrupted"
        };
        for projected in [false, true] {
            if projected {
                materialize_all(&rig, &key);
                let opener = Arc::new(
                    DefaultOpener::new(
                        Arc::new(FileLeaseStore::new(&rig.lease_dir)),
                        rig.wal_store.clone(),
                        Arc::new(NoopExec),
                    )
                    .with_run_index(rig.store.clone())
                    .with_materialized_store(rig.store.clone()),
                );
                rig.registry = LineageRegistry::new_with_reader(
                    opener,
                    rig.wal_store.clone(),
                    Some(rig.store.clone()),
                );
            }
            let status = serde_json::to_value(
                rig.registry
                    .run_status(&key, RunId("r".into()))
                    .await
                    .unwrap(),
            )
            .unwrap();
            let page = rig
                .registry
                .read(read_request(&key, Some(0), Some(100)))
                .await
                .unwrap();
            let result = serde_json::to_value(
                rig.registry
                    .run_result(&key, RunId("r".into()))
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert_eq!(
                page.lineage_state.state, expected,
                "{pause:?}/{projected}: session.read"
            );
            assert_eq!(
                status["state"], expected,
                "{pause:?}/{projected}: run.status"
            );
            assert_eq!(
                result["state"], expected,
                "{pause:?}/{projected}: run.result"
            );
            if let Some(reason) = pause {
                let reason = serde_json::to_value(reason).unwrap();
                assert_eq!(
                    serde_json::to_value(&page.lineage_state.reason).unwrap(),
                    reason
                );
                assert_eq!(status["reason"], reason);
                assert_eq!(result["reason"], reason);
            }
            assert_eq!(
                std::fs::read(&path).unwrap(),
                bytes,
                "reads never seal or resume"
            );
            assert_eq!(rig.registry.slot_state(&key), None);
        }
    }
}
