mod delivery_rig;

use delivery_rig::{result, Rig, Step};
use serde_json::{json, Value};
use subc_protocol::{session::ModuleControlRequest, FrameType, Principal};

async fn census(rig: &mut Rig) -> Value {
    let frame = rig
        .request(
            0,
            serde_json::to_value(ModuleControlRequest::HealthCheck {}).unwrap(),
        )
        .await;
    assert_eq!(frame.header.ty, FrameType::Response);
    let health: Value = serde_json::from_slice(&frame.body).unwrap();
    health["metrics"]["session_readers"].clone()
}

#[tokio::test]
async fn serve_counts_attested_readers_and_keeps_cold_ownership_unknown() {
    let mut rig = Rig::start("session-readers").await;
    let owner = "reader-owner";
    let outside = "reader-observer";
    rig.bind_as(
        7,
        Some(Principal::Reserved {
            module_id: owner.into(),
        }),
    )
    .await;
    rig.bind_as(
        8,
        Some(Principal::Reserved {
            module_id: outside.into(),
        }),
    )
    .await;

    let before = census(&mut rig).await;
    assert!(before["since_ms"].is_i64());
    assert_eq!(before["overflow"], 0);
    let cold = rig
        .request(7, json!({"method":"session.read","params":{}}))
        .await;
    assert_eq!(cold.header.ty, FrameType::Response);
    let cold_census = census(&mut rig).await;
    let entries: Vec<_> = cold_census["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e["principal"] == "reserved:reader-owner")
        .cloned()
        .collect();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0]["principal"], "reserved:reader-owner");
    assert_eq!(entries[0]["op"], "session.read");
    assert!(
        entries[0]["owner"].is_null(),
        "a cold cache must not misreport a non-owner"
    );
    assert_eq!(entries[0]["count"], 1);

    rig.script.push(&[Step::Text("hello")]);
    let sent = rig.send("reader-send", "hello", None).await;
    assert_eq!(sent.header.ty, FrameType::Response, "{}", result(&sent));
    let run = result(&sent)["run_id"].as_str().unwrap().to_owned();
    rig.wait_for_finished(1).await;
    rig.wait_idle().await;
    // Cancel authorization learns a validated ownership summary without a telemetry lookup.
    let _ = rig.cancel(&run).await;
    let baseline = census(&mut rig).await;
    let ops = [
        "session.read",
        "session.history",
        "session.head",
        "run.status",
        "run.result",
        "run.tool_calls",
        "send.status",
        "session.send_status",
        "session.media_read",
    ];
    for channel in [7, 8] {
        for op in ops {
            let params = match op {
                "run.status" | "run.result" | "run.tool_calls" => json!({"run_id":run}),
                "send.status" | "session.send_status" => json!({"send_id":"reader-send"}),
                _ => json!({}),
            };
            let reply = rig
                .request(channel, json!({"method":op,"params":params}))
                .await;
            // A media id is deliberately absent; send status is private to its original caller.
            let expected = if op == "session.media_read"
                || (matches!(op, "send.status" | "session.send_status") && channel == 8)
            {
                FrameType::Error
            } else {
                FrameType::Response
            };
            assert_eq!(
                reply.header.ty,
                expected,
                "{op}: {:?}",
                String::from_utf8_lossy(&reply.body)
            );
        }
        rig.subscribe_collect_bound(channel, json!({}), std::time::Duration::from_millis(20))
            .await;
    }
    let after = census(&mut rig).await;
    assert_eq!(after["since_ms"], before["since_ms"]);
    assert_eq!(after["overflow"], 0);
    for (principal, owner) in [
        ("reserved:reader-owner", true),
        ("reserved:reader-observer", false),
    ] {
        for op in ops.into_iter().chain(["session.subscribe"]) {
            let find = |census: &Value| {
                census["entries"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|e| e["principal"] == principal && e["op"] == op && e["owner"] == owner)
                    .cloned()
            };
            let entry = find(&after)
                .unwrap_or_else(|| panic!("missing {principal} {op} owner={owner}: {after}"));
            let old_count = find(&baseline).map_or(0, |e| e["count"].as_u64().unwrap());
            assert_eq!(entry["count"].as_u64().unwrap(), old_count + 1, "{entry}");
            assert!(entry["last_ms"].as_i64().unwrap() >= before["since_ms"].as_i64().unwrap());
        }
    }
    assert!(
        after["entries"].as_array().unwrap().contains(&entries[0]),
        "unknown reads retain their separate key"
    );
    rig.finish().await;
}
