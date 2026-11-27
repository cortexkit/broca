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
