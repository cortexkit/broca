//! broca keeps no input-token budget and leaves `input_token_cap` undeclared: a send naming
//! `budget.max_input_tokens` is refused through the real serving path before any session exists.
use crate::delivery_rig::*;
use serde_json::json;

fn refusal(frame: &subc_protocol::Frame) -> serde_json::Value {
    assert_eq!(
        frame.header.ty,
        subc_protocol::FrameType::Error,
        "{}",
        String::from_utf8_lossy(&frame.body)
    );
    serde_json::from_slice(&frame.body).unwrap()
}

#[tokio::test]
async fn a_send_naming_an_input_cap_is_refused_before_its_session_exists() {
    let mut rig = Rig::start("input-cap-refused").await;
    let reply = rig
        .send_with(
            "cap",
            "hello",
            None,
            json!({"budget":{"max_input_tokens":10}}),
        )
        .await;
    let error = refusal(&reply);
    assert_eq!(error["code"], "invalid_params", "{error}");
    assert_eq!(
        error["detail"]["field"], "budget.max_input_tokens",
        "{error}"
    );
    assert!(
        rig.records().await.is_empty(),
        "a refused send creates no session"
    );
    assert!(rig.script.dispatched.lock().unwrap().is_empty());
    rig.finish().await;
}

#[tokio::test]
async fn closed_budget_and_nudges_refuse_unknown_members_by_name() {
    let mut rig = Rig::start("input-cap-closed").await;
    for (budget, field) in [
        (json!({"max_input_tokens":10,"unknown":1}), "budget.unknown"),
        (
            json!({"nudges":[{"at_tool_calls":1,"text":"stop","unknown":1}]}),
            "budget.nudges[0].unknown",
        ),
        (json!({"max_input_tokens":0}), "budget.max_input_tokens"),
    ] {
        let reply = rig
            .send_with("bad", "hello", None, json!({"budget":budget}))
            .await;
        let error = refusal(&reply);
        assert_eq!(error["code"], "invalid_params");
        assert_eq!(error["detail"]["field"], field);
        assert!(error.to_string().contains(field), "{error}");
    }
    rig.finish().await;
}
