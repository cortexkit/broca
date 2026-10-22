//! Compare module and owner transcript reads without a compaction process.
use super::*;
use cortexkit_role_llm_runner_conformance::ScriptedTurn;

async fn request(
    connection: &SubcConnection,
    route: &RouteHandle,
    method: &str,
    params: Value,
) -> Value {
    let bytes = connection
        .route_request_raw(
            route,
            serde_json::to_vec(&json!({"method":method,"params":params})).unwrap(),
        )
        .await
        .unwrap();
    let reply: Value = serde_json::from_slice(&bytes).unwrap();
    assert!(reply.get("error").is_none(), "{method}: {reply}");
    if method == "session.read" {
        assert_eq!(reply.as_object().unwrap().len(), 1, "{reply}");
        assert!(reply["result"].is_object(), "{reply}");
        eprintln!(
            "session.read raw reply: {}",
            String::from_utf8_lossy(&bytes)
        );
    }
    reply["result"].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reserved_reader_same_triple_gets_written_lineage() {
    if !super::session_class::rigs_present() {
        return;
    }
    tokio::time::timeout(Duration::from_secs(90), async {
        let scratch = super::session_class::scratch("reader-identity");
        let mut subject = BrocaSubject::new(&scratch).await;
        subject.reserved_reader = true;
        let session = "mc-e2e";
        subject
            .install_script(
                session,
                Script {
                    turns: vec![ScriptedTurn::new(vec![ScriptedPart::Text(
                        "written answer".into(),
                    )])],
                },
            )
            .await
            .unwrap();
        let live = subject.start(&scratch.join("rig")).await.unwrap();
        let identity = broca_protocol::BindIdentity::new(&subject.project_root, "broca", session);
        let owner = subject
            .connection(&live, &subject.owner_stamp())
            .await
            .unwrap();
        let owner_route = subject.open(&owner, &identity).await.unwrap();
        let mut fields = subject.send_fields(session, true);
        fields.insert("prompt".into(), json!("written question"));
        let sent = request(&owner, &owner_route, "session.send", Value::Object(fields)).await;
        loop {
            let status = request(
                &owner,
                &owner_route,
                "run.status",
                json!({"run_id":sent["run_id"]}),
            )
            .await;
            if status["state"] != "active" && status["state"] != "pending" {
                assert_eq!(status["state"], "completed", "{status}");
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
        let params = json!({"from_ordinal":0,"max_bytes":1048576});
        let written = request(&owner, &owner_route, "session.read", params.clone()).await;
        assert_eq!(
            written["messages"].as_array().unwrap().len(),
            3,
            "{written}"
        );
        assert!(
            written["lineage_id"]
                .as_str()
                .is_some_and(|id| !id.is_empty()),
            "{written}"
        );
        assert_eq!(
            written["messages"][1]["message"]["content"][0]["text"],
            "written question"
        );
        assert_eq!(
            written["messages"][2]["message"]["content"][0]["text"],
            "written answer"
        );
        let reader = subject
            .connection(
                &live,
                &RouteStamp {
                    principal: "reserved:magic-context".into(),
                    scope: None,
                },
            )
            .await
            .unwrap();
        let reader_route = subject.open(&reader, &identity).await.unwrap();
        let read = request(&reader, &reader_route, "session.read", params).await;
        eprintln!("same-triple reserved reader: identity={identity:?} reply={read}");
        assert_eq!(
            read, written,
            "same-triple module reader must not look never-written"
        );
        let end = request(
            &reader,
            &reader_route,
            "session.read",
            json!({
                "from_ordinal":16,"max_bytes":1048576,
            }),
        )
        .await;
        assert_eq!(end["messages"], json!([]));
        assert_eq!(
            end["lineage_id"], written["lineage_id"],
            "past-end read must retain the written lineage"
        );

        // Tool-provider routes present the synthetic harness `runner`, but transcript reads must
        // use the session's own harness. The other harness addresses a different WAL.
        let other = broca_protocol::BindIdentity::new(&subject.project_root, "runner", session);
        let other_route = subject.open(&reader, &other).await.unwrap();
        let never_written = request(
            &reader,
            &other_route,
            "session.read",
            json!({
                "from_ordinal":0,"max_bytes":1048576,
            }),
        )
        .await;
        eprintln!("different-harness reserved reader: identity={other:?} reply={never_written}");
        assert_eq!(never_written["messages"], json!([]));
        assert!(never_written.get("lineage_id").is_none());
    })
    .await
    .expect("reserved-reader reproduction timed out");
}
