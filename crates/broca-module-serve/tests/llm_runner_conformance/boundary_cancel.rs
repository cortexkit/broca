use super::*;
use cortexkit_role_llm_runner_conformance::{ScriptedToolCall, ScriptedTurn};

fn response(reply: Reply) -> Value {
    match reply {
        Reply::Response(value) => value,
        other => panic!("expected response: {other:?}"),
    }
}

async fn terminal(route: &BrocaRoute, run_id: &Value, state: &str) {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let value = response(
                route
                    .request("run.result", json!({"run_id":run_id}))
                    .await
                    .unwrap(),
            );
            if value["state"] == state {
                break;
            }
            assert!(
                matches!(value["state"].as_str(), Some("active" | "paused")),
                "unexpected terminal: {value}"
            );
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("run must reach its expected terminal");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boundary_cancel_during_tool_then_send_is_provider_valid_and_byte_stable() {
    assert!(
        sibling_bin("ck-subc", "SUBC_CORE_BIN").exists(),
        "real daemon required"
    );
    assert!(
        sibling_bin("fake-aft-stub", "FAKE_AFT_STUB_BIN").exists(),
        "real provider rig required"
    );
    let scratch =
        broca_test_support::scratch_root().join(format!("boundary-daemon-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("root");
    std::fs::create_dir_all(&root).unwrap();
    let session = "boundary-send";
    let call = ScriptedToolCall {
        tool_call_id: "boundary_tool".into(),
        tool: SCRIPTED_TOOL.into(),
        arguments: json!({"marker":"boundary-args"}),
        result: json!({"marker":"boundary-result"}),
        hold: true,
    };
    subject
        .install_script(
            session,
            Script {
                turns: vec![
                    ScriptedTurn {
                        parts: vec![ScriptedPart::ToolCall(call.clone())],
                    },
                    ScriptedTurn {
                        parts: vec![ScriptedPart::Text("continued".into())],
                    },
                ],
            },
        )
        .await
        .unwrap();
    let live = subject.spawn(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let mut fields = subject.send_fields(session, true);
    fields.insert("prompt".into(), json!("work"));
    fields.insert("send_id".into(), json!("boundary-first"));
    let first = response(
        route
            .request("session.send", Value::Object(fields))
            .await
            .unwrap(),
    );
    subject.await_tool_call(&call.arguments).await.unwrap();
    let raw = route.connection.route_request_terminal(&route.route,
        serde_json::to_vec(&json!({"method":"run.cancel","params":{"run_id":first["run_id"],"at":"step_boundary"}})).unwrap())
        .await.unwrap().unwrap();
    #[derive(serde::Deserialize)]
    struct Raw<'a> {
        #[serde(borrow)]
        result: &'a serde_json::value::RawValue,
    }
    let reply: Raw<'_> = serde_json::from_slice(&raw).unwrap();
    let golden = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../broca-wire/tests/golden/run_cancel.step_boundary.json");
    if std::env::var_os("BROCA_REGENERATE_ROLE_DESCRIBE_GOLDEN").is_some() {
        std::fs::write(&golden, reply.result.get()).unwrap();
    }
    assert_eq!(
        reply.result.get().as_bytes(),
        std::fs::read(golden).unwrap()
    );
    assert_eq!(reply.result.get(), "\"ack\"");
    assert!(
        subject
            .records(&live, session)
            .iter()
            .any(|record| matches!(record, broca_wal::WalRecord::RunCancelRequested { .. })),
        "ack must follow the durable marker"
    );
    subject.release_tool_call(&call.arguments).await.unwrap();
    terminal(&route, &first["run_id"], "cancelled").await;
    assert_eq!(subject.model_invocations(session).await.unwrap(), 1);
    let mut fields = subject.send_fields(session, false);
    fields.insert("prompt".into(), json!("continue"));
    fields.insert("send_id".into(), json!("boundary-second"));
    let second = response(
        route
            .request("session.send", Value::Object(fields))
            .await
            .unwrap(),
    );
    terminal(&route, &second["run_id"], "completed").await;
    assert_eq!(subject.tool_invocations(&call.arguments).await, 1);
    let records = subject.records(&live, session);
    let start = records
        .iter()
        .rposition(|record| matches!(record, broca_wal::WalRecord::StepStarted { .. }))
        .unwrap();
    let config = records[..start]
        .iter()
        .rev()
        .find_map(|record| match record {
            broca_wal::WalRecord::RunStarted { config, .. } => Some(config.as_ref()),
            _ => None,
        })
        .unwrap();
    let (_, opts, expected) = broca_module_serve::wal_render::render_step(
        &records[..=start],
        config,
        second["run_id"].as_str().unwrap(),
        1,
    )
    .unwrap();
    let calls: Vec<_> = opts
        .prompt
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|block| match block {
            broca_protocol::ContentBlock::ToolCall(call) => Some(call.tool_call_id.clone()),
            _ => None,
        })
        .collect();
    let results: Vec<_> = opts
        .prompt
        .iter()
        .flat_map(|message| message.content.iter())
        .filter_map(|block| match block {
            broca_protocol::ContentBlock::ToolResult(result) => Some(result.tool_call_id.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        calls, results,
        "every tool use must have exactly its tool result"
    );
    assert_eq!(calls.len(), 1);
    let actual = subject.shared.raw_bodies.lock().unwrap()[session][1].clone();
    assert_eq!(
        actual, expected,
        "the actual provider request must match WAL replay byte for byte"
    );
    let body: Value = serde_json::from_slice(&actual).unwrap();
    assert_eq!(
        body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|message| message["role"] == "tool")
            .count(),
        1
    );
    drop(route);
    drop(live);
}
