mod delivery_rig;

use broca_core::{MaterializedStore, Persister, WalStore};
use delivery_rig::{result, Rig, Step};
use serde_json::json;

#[tokio::test]
async fn recorded_pending_marks_survive_checkpoint_actor_reopen_and_delivery() {
    for ride in [false, true] {
        let mut rig = Rig::start(if ride {
            "checkpoint-mark-ride"
        } else {
            "checkpoint-mark-drain"
        })
        .await;
        rig.script.push(&[Step::Text("baseline")]);
        let baseline = rig.send("baseline", "baseline", None).await;
        assert!(result(&baseline)["run_id"].is_string());
        rig.wait_for_finished(1).await;
        rig.wait_idle().await;
        assert!(rig.registry().stop_and_seal().await);
        let store = broca_core::FileWalStore::new(rig.dir.join("wal"));
        let original = store.read_records(&rig.session).unwrap();
        std::fs::remove_file(&rig.wal_path).unwrap();
        let mut wal = store.open(&rig.session).unwrap();
        let marks = if ride {
            json!({"speaks_for":{"agent_id":"Ada"}})
        } else {
            json!({"notice":true})
        };
        let mut queued: serde_json::Value = serde_json::from_str(include_str!(
            "../../broca-wal/tests/golden/pre_step_transforms/queued.json"
        ))
        .unwrap();
        queued["message_marks"] = marks.clone();
        let queued = serde_json::from_value(queued).unwrap();
        // A completed model step co-commits queued work in its checkpoint.
        for frame in original {
            if matches!(frame.record, broca_wal::WalRecord::ModelStepFinished { .. }) {
                if ride {
                    wal.append(1, &serde_json::from_value(json!({"type":"delivery_ordered","submission_id":"sub-run-8","order":{"modes":["tool_result"]}})).unwrap(), true).unwrap();
                }
                wal.append(1, &queued, true).unwrap();
            }
            wal.append(1, &frame.record, true).unwrap();
        }
        drop(wal);
        let range = store
            .read_records_range(&rig.session, 0, None, None)
            .unwrap();
        let through = Persister::project_range(rig.store.as_ref(), &rig.session, &range).unwrap();
        let snapshot = rig
            .store
            .read_admission_snapshot(&rig.session)
            .unwrap()
            .unwrap();
        assert_eq!(snapshot.watermark_seq, through);
        assert_eq!(snapshot.anchor, range.anchor_at(through));
        let encoded = serde_json::to_value(snapshot.checkpoint.unwrap()).unwrap();
        assert_eq!(
            encoded["pending_fifo"][0]["message_marks"], marks,
            "checkpoint {encoded}"
        );
        let seeded = broca_core::load_prior_wal_snapshot(
            &store,
            &rig.session,
            through,
            Some(rig.store.as_ref()),
        )
        .unwrap();
        assert_eq!(
            seeded.base_watermark, through,
            "the actor has a valid checkpoint, not a full-replay repair"
        );
        assert!(seeded.tail.is_empty());
        let mut rig = rig.restart().await;
        if ride {
            rig.script
                .push(&[Step::Call("carrier"), Step::Text("done")]);
            let wake = rig.send("wake", "wake", None).await;
            assert!(result(&wake)["run_id"].is_string());
        } else {
            rig.script.push(&[Step::Text("done"), Step::Text("done")]);
            let wake = rig.request(7, json!({"method":"session.send","params":{"send_id":"wake","prompt":"wake","model":{"provider":"fake","model":"fake"}}})).await;
            assert!(matches!(
                result(&wake)["state"].as_str(),
                Some("pending" | "active")
            ));
        }
        let records = rig.wait_for_finished(if ride { 2 } else { 3 }).await;
        rig.wait_idle().await;
        if ride {
            let stored = records
                .iter()
                .find_map(|record| match record {
                    broca_wal::WalRecord::SendDelivered {
                        ride: Some(ride), ..
                    } => Some(ride),
                    _ => None,
                })
                .expect("queued send rode the tool result");
            assert_eq!(stored.speaks_for.as_ref().unwrap().agent_id, "Ada");
        } else {
            let stored = records
                .iter()
                .find_map(|record| match record {
                    broca_wal::WalRecord::RunStarted {
                        submission_id: Some(id),
                        message_marks,
                        ..
                    } if id.0 == "sub-run-8" => Some(message_marks),
                    _ => None,
                })
                .expect("queued send started a run");
            assert_eq!(serde_json::to_value(stored).unwrap(), marks);
        }
        rig.finish().await;
    }
}
