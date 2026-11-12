//! The same recorded sends read through today's WAL and the new session storage.

use broca_module::serve::{ServeHandler, SessionReadRequest};
use broca_module::store_read::history_sides::TodaysHistory;
use broca_session_loop::parts::MemoryParts;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use broca_wal::{FramedRecord, WalRecord};
use futures::executor::block_on;
use serde_json::{json, Value};

use crate::counting::Counting;
use crate::stored_history::StoredHistory;

const LINEAGE: &str = "0b3c3c1e-1f2a-4c1d-9e57-5a0e5f0c0004";

fn message(role: &str, text: &str) -> Value {
    json!({"role": role, "content": [{"type": "text", "text": text}]})
}

fn start(run: &str, text: &str, extra: Value) -> Value {
    let mut value = json!({
        "type": "run_started", "run_id": run,
        "session": {"project_root": "/synthetic", "harness": "send-ids", "session": "sample"},
        "config": {"model": {"provider_module_id": "p", "model_id": "m"},
            "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
            "response_format": {"type": "text"}, "context_epoch": 0, "renderer_version": 1},
        "input": [message("user", text)], "origin": {"kind": "fresh"}, "written_at_ms": 10,
    });
    value
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    value
}

fn queued(id: &str, text: &str, key: Option<&str>) -> Value {
    json!({"type": "queued", "submission_id": id, "send_id": key,
        "principal": {"kind": "reserved", "module_id": "sample-sender"}, "written_at_ms": 10,
        "request": {"model": {"provider_module_id": "p", "model_id": "m"},
            "input": [message("user", text)], "policy_allowed": [], "tool_choice": {"type": "auto"},
            "generation": {}, "stop_when": []}})
}

fn finish(records: &mut Vec<Value>, step: u64) {
    records.extend([
        json!({"type": "step_started", "step_id": step}),
        json!({"type": "model_step_finished", "step_id": step, "written_at_ms": 11,
            "assistant_message": {"message_id": format!("answer-{step}"),
                "content": [{"type": "text", "text": "synthetic answer"}]},
            "usage": {}, "finish_reason": "stop", "retries_used": 0}),
        json!({"type": "run_finished", "reason": "completed", "usage": {}, "retries_used": 0}),
    ]);
}

/// Each input has a distinct body so an id attributed to the wrong send cannot pass by equality.
fn sends() -> Vec<FramedRecord> {
    let sender = json!({"kind": "reserved", "module_id": "sample-sender"});
    let mut records = vec![start(
        "plain",
        "plain user turn",
        json!({"principal": {"kind": "direct"}}),
    )];
    finish(&mut records, 1);
    records.push(start(
        "keyed-run",
        "keyed module send",
        json!({"principal": sender, "send_id": "keyed-send"}),
    ));
    records.push(json!({"type": "step_started", "step_id": 1}));
    records.push(queued("sub-unkeyed", "unkeyed queued module send", None));
    records.push(queued(
        "sub-queued",
        "keyed queued module send",
        Some("queued-send"),
    ));
    records.push(
        json!({"type": "steered", "send_id": "steer-send", "sent_by": sender,
        "input": [message("user", "module steer")], "written_at_ms": 10}),
    );
    for (submission, key, text, mode, role) in [
        (
            "sub-ordered-steer",
            "ordered-steer-send",
            "ordered module steer",
            "steer",
            "user",
        ),
        (
            "sub-system",
            "system-send",
            "turn system delivery",
            "turn_system",
            "system",
        ),
    ] {
        records.push(json!({"type": "delivery_ordered", "submission_id": submission, "order": {"modes": [mode]}}));
        records.push(queued(submission, text, Some(key)));
        let mut input = message(role, text);
        if mode == "turn_system" {
            input["turn_system"] = json!(true);
        }
        records.push(
            json!({"type": "send_delivered", "submission_id": submission,
            "run_id": "keyed-run", "mode": mode, "input": [input], "written_at_ms": 10}),
        );
    }
    finish(&mut records, 2);
    for (run, submission, text, key) in [
        (
            "unkeyed-run",
            "sub-unkeyed",
            "unkeyed queued module send",
            None,
        ),
        (
            "queued-run",
            "sub-queued",
            "keyed queued module send",
            Some("queued-send"),
        ),
    ] {
        records.push(start(
            run,
            text,
            json!({"principal": sender, "submission_id": submission, "send_id": key}),
        ));
        finish(&mut records, 1);
    }
    records.push(json!({"type": "delivery_ordered", "submission_id": "sub-idle", "order": {"modes": ["idle_start"]}}));
    records.push(queued("sub-idle", "idle start delivery", Some("idle-send")));
    records.push(start(
        "idle-run",
        "idle start delivery",
        json!({"principal": sender, "submission_id": "sub-idle", "send_id": "idle-send"}),
    ));
    finish(&mut records, 1);
    records.push(start(
        "unkeyed-direct-run",
        "unkeyed direct module send",
        json!({"principal": sender}),
    ));
    finish(&mut records, 1);
    records
        .into_iter()
        .enumerate()
        .map(|(index, record)| FramedRecord {
            seq: index as u64 + 1,
            fence: 0,
            record: serde_json::from_value::<WalRecord>(record).unwrap(),
        })
        .collect()
}

#[test]
fn paged_raw_history_keeps_one_sealed_send_id_index() {
    let old = sends();
    let mut converted = crate::convert(&old, Some(LINEAGE)).unwrap();
    converted.start_at(crate::FIRST_SEQ);
    let backend = Counting::new(MemoryBackend::default());
    block_on(async {
        let addr = SessionAddr("paged-send-ids".into());
        let triple = SessionTriple {
            project_root: "/synthetic".into(),
            harness: "send-ids".into(),
            session: "sample".into(),
        };
        let deployment = DeploymentKey::new(1, &[0x5a; 32]);
        let mut session =
            Session::create(&backend, addr.clone(), triple.clone(), &deployment, false)
                .await
                .unwrap();
        session.append(&converted.records, false).await.unwrap();
        session.seal(None, 0).await.unwrap();
        session.close().await.unwrap();
        let view = Session::view(&backend, addr, &triple, &deployment)
            .await
            .unwrap();
        let entries = view.entries(1, 100).await.unwrap();
        assert!(entries.iter().any(|entry| entry
            .state
            .facts
            .as_ref()
            .is_some_and(|facts| facts.sender.is_some())));
        backend.take();
        let mut all_ids = std::collections::BTreeMap::new();
        for entry in &entries {
            all_ids.extend(
                view.entry_send_ids(std::slice::from_ref(entry))
                    .await
                    .unwrap(),
            );
        }
        let first = backend.take();
        assert!(
            !first.objects.is_empty(),
            "the first sender page must reach sealed history"
        );
        for entry in &entries {
            let ids = view
                .entry_send_ids(std::slice::from_ref(entry))
                .await
                .unwrap();
            for (ordinal, id) in ids {
                assert_eq!(all_ids.get(&ordinal), Some(&id));
            }
        }
        let repeated = backend.take();
        assert_eq!(repeated.log_reads, 0, "{repeated:?}");
        assert!(
            repeated.objects.is_empty(),
            "later pages must reuse the send index: {repeated:?}"
        );
        eprintln!(
            "sealed send index: first log_reads={} objects={}, later log_reads={} objects={}",
            first.log_reads,
            first.objects.len(),
            repeated.log_reads,
            repeated.objects.len()
        );

        let parts = MemoryParts {
            parts: converted.parts,
        };
        let store = crate::stored_history::StoredHistoryWalk::new(&view, &parts)
            .await
            .unwrap();
        let head = view.head();
        let today = TodaysHistory::new(
            &old,
            Some(LINEAGE),
            broca_session_store::codec::hex(&head.incarnation.0),
            broca_module::store_exposure::served_history_version(head.history_version),
        )
        .unwrap();
        let key = broca_protocol::BindIdentity::new("/synthetic", "send-ids", "sample");
        for newest in [false, true] {
            let pages = crate::verify::history_pages(&store, &key, "raw", newest).await;
            assert!(pages.len() > 1);
            assert!(pages.iter().all(|page| page.get("refused").is_none()));
            assert_eq!(
                pages,
                crate::verify::history_pages(&today, &key, "raw", newest).await
            );
        }
    });
}

#[test]
fn a_requested_send_id_index_stays_current_across_appends() {
    let mut converted = crate::convert(&sends(), Some(LINEAGE)).unwrap();
    converted.start_at(crate::FIRST_SEQ);
    let backend = Counting::new(MemoryBackend::default());
    block_on(async {
        let mut session = Session::create(
            &backend,
            SessionAddr("appended-send-ids".into()),
            SessionTriple {
                project_root: "/synthetic".into(),
                harness: "send-ids".into(),
                session: "sample".into(),
            },
            &DeploymentKey::new(1, &[0x5a; 32]),
            false,
        )
        .await
        .unwrap();
        // Start with one complete run, then request the index before later starts discard
        // that run's records from the writer's retained suffix.
        let split = converted
            .records
            .iter()
            .position(|record| matches!(record, Record::RunEnd(_)))
            .unwrap()
            + 1;
        session
            .append(&converted.records[..split], true)
            .await
            .unwrap();
        let entries = session.entries(1, 100).await.unwrap();
        // The first run is a direct user turn, so add a sender only to the read copy to
        // request the same admission index that a module-authored input would need.
        let mut entries = entries;
        std::sync::Arc::make_mut(entries[0].state.facts.as_mut().unwrap()).sender =
            Some("sample-sender".into());
        assert_eq!(
            session.entry_send_ids(&entries).await.unwrap().get(&1),
            Some(&None)
        );
        session
            .append(&converted.records[split..], true)
            .await
            .unwrap();
        let entries = session.entries(1, 100).await.unwrap();
        backend.take();
        let memory = session.in_memory();
        let ids = session.entry_send_ids(&entries).await.unwrap();
        assert!(!memory.refused());
        assert!(ids.values().any(|id| id.as_deref() == Some("queued-send")));
        assert!(
            ids.values().any(Option::is_none),
            "known absence must survive append"
        );
        let reads = backend.take();
        assert_eq!(reads.log_reads, 0, "{reads:?}");
        assert!(reads.objects.is_empty(), "{reads:?}");
    });
}

struct Bodies {
    wal_read: Value,
    store_read: Value,
    wal_default: Value,
    store_default: Value,
    wal_history: Value,
    store_history: Value,
    compaction: Value,
    descriptor_digests: Vec<Value>,
    reads_after_warmup: crate::counting::Reads,
}

fn bodies(change: impl Fn(&mut Vec<Record>)) -> Bodies {
    let old = sends();
    let mut converted = crate::convert(&old, Some(LINEAGE)).unwrap();
    converted.start_at(crate::FIRST_SEQ);
    assert!(converted.lost.is_empty(), "{:?}", converted.lost);
    change(&mut converted.records);
    let backend = Counting::new(MemoryBackend::default());
    block_on(async {
        let mut session = Session::create(
            &backend,
            SessionAddr("send-ids-sample".into()),
            SessionTriple {
                project_root: "/synthetic".into(),
                harness: "send-ids".into(),
                session: "sample".into(),
            },
            &DeploymentKey::new(1, &[0x5a; 32]),
            false,
        )
        .await
        .unwrap();
        for record in &converted.records {
            session
                .append(std::slice::from_ref(record), false)
                .await
                .unwrap();
        }
        session.seal(None, 0).await.unwrap();
        session.close().await.unwrap();
        let session = Session::load(
            &backend,
            SessionAddr("send-ids-sample".into()),
            &SessionTriple {
                project_root: "/synthetic".into(),
                harness: "send-ids".into(),
                session: "sample".into(),
            },
            &DeploymentKey::new(1, &[0x5a; 32]),
        )
        .await
        .unwrap();
        let parts = MemoryParts {
            parts: converted.parts,
        };
        let store = StoredHistory {
            session: &session,
            parts: &parts,
        };
        let today = TodaysHistory::new(&old, Some(LINEAGE), "synthetic-wal".into(), 1).unwrap();
        let key = broca_protocol::BindIdentity::new("/synthetic", "send-ids", "sample");
        let mut req = SessionReadRequest::new(key.clone(), Some(1), Some(100));
        let wal_default = serde_json::to_value(today.read(req.clone()).await.unwrap()).unwrap();
        let store_default = serde_json::to_value(store.read(req.clone()).await.unwrap()).unwrap();
        req.include_display_facts = true;
        let wal_read = serde_json::to_value(today.read(req.clone()).await.unwrap()).unwrap();
        let store_read = serde_json::to_value(store.read(req.clone()).await.unwrap()).unwrap();
        let again = serde_json::to_value(store.read(req.clone()).await.unwrap()).unwrap();
        assert_eq!(again, store_read);
        let entries = session.entries(1, 100).await.unwrap();
        backend.take();
        session.older_entry_send_ids(&entries).await.unwrap();
        let reads_after_warmup = backend.take();
        req.media = cortexkit_role_llm_runner::read::MediaMode::Descriptors;
        let descriptors = serde_json::to_value(store.read(req).await.unwrap()).unwrap();
        let descriptor_digests = descriptors["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|entry| json!([entry["digest"], entry["part_digests"]]))
            .collect();
        let compaction = serde_json::to_value(
            broca_module::store_entries::all_entries(&session)
                .await
                .unwrap(),
        )
        .unwrap();
        let history_req = serde_json::from_value(json!({"limit": 100})).unwrap();
        let wal_history = broca_module::history::read(&today, &key, &history_req)
            .await
            .unwrap();
        let store_history = broca_module::history::read(&store, &key, &history_req)
            .await
            .unwrap();
        Bodies {
            wal_read,
            store_read,
            wal_default,
            store_default,
            wal_history,
            store_history,
            compaction,
            descriptor_digests,
            reads_after_warmup,
        }
    })
}

#[test]
#[ignore = "requires BROCA_SEND_ID_SAMPLES output directory"]
fn send_id_serializer_samples() {
    let root = std::env::var_os("BROCA_SEND_ID_SAMPLES").expect("sample output directory");
    let bodies = bodies(|_| {});
    let root = std::path::PathBuf::from(root);
    std::fs::create_dir_all(&root).unwrap();
    for (file, body) in [
        ("wal.session.read.json", bodies.wal_read),
        ("store.session.read.json", bodies.store_read),
        ("wal.session.history.json", bodies.wal_history),
        ("store.session.history.json", bodies.store_history),
    ] {
        std::fs::write(root.join(file), serde_json::to_vec_pretty(&body).unwrap()).unwrap();
    }
}

fn assert_ids(body: &Value) {
    for (text, id) in [
        ("plain user turn", None),
        ("keyed module send", Some("keyed-send")),
        ("unkeyed queued module send", Some("sub-unkeyed")),
        ("keyed queued module send", Some("queued-send")),
        ("module steer", Some("steer-send")),
        ("ordered module steer", Some("ordered-steer-send")),
        ("turn system delivery", Some("system-send")),
        ("idle start delivery", Some("idle-send")),
        ("unkeyed direct module send", None),
    ] {
        let entries: Vec<_> = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|entry| {
                entry["message"]["content"]
                    .as_array()
                    .is_some_and(|parts| parts.iter().any(|part| part["text"] == text))
            })
            .collect();
        assert_eq!(entries.len(), 1, "{text}: {body:#}");
        let entry = entries[0];
        assert_eq!(entry.get("send_id").and_then(Value::as_str), id, "{text}");
        assert_eq!(
            entry.get("sender").and_then(Value::as_str),
            (text != "plain user turn").then_some("sample-sender"),
            "{text}"
        );
        assert!(entry["message"].get("send_id").is_none());
    }
}

#[test]
fn send_ids_match_across_storages_for_every_placement() {
    let bodies = bodies(|_| {});
    for body in [
        &bodies.wal_read,
        &bodies.store_read,
        &bodies.wal_history,
        &bodies.store_history,
    ] {
        assert_ids(body);
    }
    for (default, facts) in [
        (&bodies.wal_default, &bodies.wal_read),
        (&bodies.store_default, &bodies.store_read),
    ] {
        assert_eq!(default["history_version"], facts["history_version"]);
        for (default, facts) in default["messages"]
            .as_array()
            .unwrap()
            .iter()
            .zip(facts["messages"].as_array().unwrap())
        {
            assert!(default.get("send_id").is_none());
            assert!(default.get("sender").is_none());
            assert_eq!(default["message"], facts["message"]);
        }
    }
}

#[test]
fn older_send_ids_are_exact_cached_and_do_not_change_content_digests() {
    let current = bodies(|_| {});
    let older = bodies(|records| {
        for record in records {
            if let Record::EntryFacts(facts) = record {
                std::sync::Arc::make_mut(facts).send_id = None;
            }
        }
    });
    let mut expected = current.store_read["messages"].clone();
    for entry in expected.as_array_mut().unwrap() {
        if entry["send_id"] == "steer-send" {
            entry.as_object_mut().unwrap().remove("send_id");
        }
    }
    assert_eq!(
        expected, older.store_read["messages"],
        "converted steer receipts alone cannot prove their send id"
    );
    assert_eq!(
        current.store_read["history_version"],
        older.store_read["history_version"]
    );
    assert!(!current.descriptor_digests.is_empty());
    assert!(current
        .descriptor_digests
        .iter()
        .all(|digests| digests[0].is_string() && digests[1].is_array()));
    assert!(current
        .compaction
        .as_array()
        .unwrap()
        .iter()
        .all(|entry| entry["digest"].is_string()));
    assert_eq!(
        current.compaction, older.compaction,
        "compaction entries and entry_digest are content identities"
    );
    assert_eq!(
        current.descriptor_digests, older.descriptor_digests,
        "read digests exclude send metadata"
    );
    assert!(older.reads_after_warmup.objects.is_empty());
    assert_eq!(
        older.reads_after_warmup.log_reads, 0,
        "another page must reuse the send index"
    );
}
