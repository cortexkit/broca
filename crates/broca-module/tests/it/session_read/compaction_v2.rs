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
    // A mode-3 log's message ids carry its lineage prefix, so read history from the session's
    // own first message id.
    let first_mid = page["messages"][0]["mid"].clone();
    let mut responses = vec![page];
    for form in ["raw", "display"] {
        let request =
            serde_json::from_value(json!({"from_mid":first_mid,"limit":100,"form":form})).unwrap();
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
    // The two logs differ only in their message-id form (`m<prefix>.N` in mode 3, `mN` in a
    // legacy log); compare everything else.
    responses
        .into_iter()
        .map(|response| {
            let text = serde_json::to_string(&response).unwrap();
            let mut out = String::with_capacity(text.len());
            let mut rest = text.as_str();
            while let Some(at) = rest.find("\"m") {
                out.push_str(&rest[..at + 2]);
                rest = &rest[at + 2..];
                let prefix = rest.as_bytes();
                if prefix.len() > 9
                    && prefix[..8].iter().all(u8::is_ascii_hexdigit)
                    && prefix[8] == b'.'
                {
                    rest = &rest[9..];
                }
            }
            out.push_str(rest);
            let mut value: Value = serde_json::from_str(&out).unwrap();
            // Each session allocates its own history version from its first served read, and an
            // output-reference set id (`h1.<incarnation>.<history version>/<mid>`) repeats it.
            if let Some(version) = value.get_mut("history_version") {
                *version = json!("@HISTORY_VERSION@");
            }
            mask_set_id_versions(&mut value);
            value
        })
        .collect()
}

/// Replaces the history-version segment of every output-reference `set_id`, which the two rigs
/// allocate independently from the wall clock, so equal histories compare equal.
fn mask_set_id_versions(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, field) in map.iter_mut() {
                if key == "set_id" {
                    if let Some(id) = field.as_str() {
                        if let Some((scope, mid)) = id.split_once('/') {
                            if let Some((prefix, _version)) = scope.rsplit_once('.') {
                                *field = json!(format!("{prefix}.@HISTORY_VERSION@/{mid}"));
                            }
                        }
                    }
                } else {
                    mask_set_id_versions(field);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(mask_set_id_versions),
        _ => {}
    }
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
                // Public ordinals start at 1; message ids keep the stored position.
                assert_eq!(message["ordinal"], i as u64 + 1);
                assert_eq!(message["mid"], format!("m{i}"));
            }
        }
    }
}
