use super::*;

async fn closed_run() -> Module {
    let m = module_with_runtime(
        |gate| Arc::new(executor(vec![text_step("a1", "one")], Some(gate))),
        |runtime| runtime.with_view_bytes_for_test(1),
    )
    .await;
    active(
        m.send_keyed("r1", "hello", SendKey::SendId("first".into()))
            .await,
    );
    m.gate.notify_one();
    m.ended("r1", "completed").await;
    m.close().await;
    m
}

#[tokio::test]
async fn closed_run_admission_repeat_does_not_load_whole_view() {
    let m = closed_run().await;
    let request = StoreReadRequest::RunAdmission(RunId("r1".into()));
    let bytes = |reply| match reply {
        StoreReadReply::RunAdmission(Ok(Some(admission))) => {
            serde_json::to_vec(&admission).unwrap()
        }
        _ => panic!("expected a frozen admission"),
    };
    let fresh = bytes(m.fresh(&request).await);
    assert_eq!(bytes(m.served(&request).await), fresh);
    let runtime = m.runtime();
    let before = runtime.views().memory(|_| 0).loads.unwrap();
    assert_eq!(bytes(m.served(&request).await), fresh);
    let after = runtime.views().memory(|_| 0).loads.unwrap();
    assert_eq!(after, before, "repeat admission loaded the whole view");
}

#[tokio::test]
async fn closed_run_status_repeat_does_not_load_whole_view() {
    let m = closed_run().await;
    let request = StoreReadRequest::RunStatusReply(RunId("r1".into()));
    let bytes = |reply| match reply {
        StoreReadReply::RunStatusReply(Ok(status)) => serde_json::to_vec(&status).unwrap(),
        _ => panic!("expected a run status"),
    };
    let fresh = bytes(m.fresh(&request).await);
    assert_eq!(bytes(m.served(&request).await), fresh);
    let runtime = m.runtime();
    let before = runtime.views().memory(|_| 0).loads.unwrap();
    assert_eq!(bytes(m.served(&request).await), fresh);
    let after = runtime.views().memory(|_| 0).loads.unwrap();
    assert_eq!(after, before, "repeat run status loaded the whole view");
    let internal = StoreReadRequest::RunStatus(RunId("r1".into()));
    let internal_bytes = |reply| match reply {
        StoreReadReply::RunStatus(Ok(status)) => serde_json::to_vec(&status).unwrap(),
        _ => panic!("expected an internal run status"),
    };
    assert_eq!(
        internal_bytes(m.served(&internal).await),
        internal_bytes(m.fresh(&internal).await)
    );
    assert_eq!(runtime.views().memory(|_| 0).loads.unwrap(), before);
}

#[tokio::test]
async fn closed_run_status_provisional_ids_match_fresh_bytes() {
    use broca_session_store::record::{Delivery, Retraction};

    let m = closed_run().await;
    let mut writer = m.writer().await;
    writer
        .append(
            &[
                Record::Delivery(Delivery {
                    submission_id: "sub-provisional-delivered".into(),
                    run_id: "r1".into(),
                    mode: "steer".into(),
                    input: Vec::new(),
                    tool_call_id: None,
                    appended_text: None,
                    written_at_ms: None,
                    tags_v1: None,
                    input_listed: None,
                }),
                Record::Retraction(Retraction {
                    submission_id: "sub-provisional-retracted".into(),
                    failure: None,
                }),
            ],
            true,
        )
        .await
        .unwrap();
    writer.close().await.unwrap();
    for (id, state) in [
        ("provisional-delivered", "completed"),
        ("provisional-retracted", "retracted"),
        ("not-a-run", "unknown"),
    ] {
        let request = StoreReadRequest::RunStatusReply(RunId(id.into()));
        let bytes = |reply| match reply {
            StoreReadReply::RunStatusReply(Ok(status)) => serde_json::to_vec(&status).unwrap(),
            _ => panic!("expected a run status for {id}"),
        };
        let fresh = bytes(m.fresh(&request).await);
        let value: Value = serde_json::from_slice(&fresh).unwrap();
        assert_eq!(value["state"], state);
        if id == "provisional-delivered" {
            assert_eq!(value["delivered"]["ref"], "r1");
        }
        for _ in 0..2 {
            assert_eq!(bytes(m.served(&request).await), fresh);
        }
    }
}
