use super::*;
use broca_provider::{families::AnthropicMessages, wire::WireFamily};

fn note(body: &serde_json::Value) -> (usize, Vec<u8>) {
    let (at, message) = body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .enumerate()
        .find(|(_, message)| {
            message["role"] == "system" && message["content"].to_string().contains("tool_addition")
        })
        .expect("a native declaration stays a separate conversation message");
    (at, serde_json::to_vec(message).unwrap())
}

#[tokio::test]
async fn a_sealed_anchor_redelivers_the_same_native_note_at_the_next_run_start() {
    use broca_core::compaction_v2::{
        protocol::view::{Edit, EditOp, View},
        received::ReceivedView,
    };
    let compacted = |text, tools: &[&str], id| {
        let mut value = plan(text, tools, None, None);
        value["compaction_item"] = serde_json::json!({"provider":"compactor", "preset":"default"});
        manifest(&value, id)
    };
    let first = compacted("before", &["read"], "plan-1");
    let pending = compacted("after", &["read", "search"], "plan-2");
    let mut exec = claude(vec![
        confirmed(text_step("a1", "ready")),
        confirmed(text_step("a2", "noted")),
        text_step("a3", "done"),
    ]);
    exec.planes.compaction = Some(Arc::new(Compactor));
    let (module, executor) = module_with_executor(exec, true, true).await;
    send_limited(&module, "r1", Some(&first)).await;
    refresh(&module, 1, pending.clone(), Some(NOTE)).await;
    send_limited(&module, "r2", None).await;
    let mut session = module.load().await;
    let old = session.operations.plan.as_ref().unwrap().notes[0].clone();
    let entry = session.entries(old.anchor, 1).await.unwrap();
    let through = entry[0].state.facts.as_ref().unwrap().mid.clone();
    let view = ReceivedView::from(View::seal(1, vec![Edit::new(1, EditOp::Cut { through })]));
    session
        .append(
            &[broca_session_loop::compaction::view_record(None, &view, None).unwrap()],
            true,
        )
        .await
        .unwrap();
    drop(session);
    send_limited(&module, "r3", None).await;
    let calls = executor.provider.inner.calls();
    let frozen = serde_json::from_value(executor.provider.render_config.clone().unwrap()).unwrap();
    let (earlier, old_bytes) = note(&AnthropicMessages.shape(&frozen, &calls[1]));
    let sent = AnthropicMessages.shape(&frozen, &calls[2]);
    let (at, bytes) = note(&sent);
    assert!(earlier > 0);
    assert_eq!(at + 1, sent["messages"].as_array().unwrap().len());
    assert_eq!(bytes, old_bytes);
    assert_eq!(names(&calls[2]), ["read"]);
    let session = module.load().await;
    let state = session.operations.plan.as_ref().unwrap();
    assert!(state.pending.is_some());
    assert_eq!(state.notes.len(), 2);
    assert_ne!(state.notes[1].anchor, old.anchor);
    assert_eq!(state.notes[1].notes, old.notes);
}

struct Compactor;

#[async_trait::async_trait]
impl broca_core::CompactionPlane for Compactor {
    async fn setup(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::CompactionUnavailable,
    > {
        panic!("the fixture uses the current compaction contract")
    }

    async fn call_v2(
        &self,
        _: &str,
        _: &BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        method: &str,
        _: serde_json::Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        let answer = match method {
            "compaction.setup" => "ready",
            "compaction.notice" => "received",
            "compaction.ride" => "nothing",
            _ => panic!("unexpected method {method}"),
        };
        Ok(serde_json::value::to_raw_value(&serde_json::json!({"answer":answer})).unwrap())
    }
}

async fn send_limited(
    module: &Module,
    run: &str,
    first: Option<&broca_records::record::PlanManifest>,
) {
    let mut req = request(run);
    req.model = ModelHandle::new("anthropic", "claude-opus-5-5");
    req.context_limit = Some(200_000);
    send_request(module, run, first, &mut req).await;
}
