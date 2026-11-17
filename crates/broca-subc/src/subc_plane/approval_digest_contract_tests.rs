use super::*;

#[tokio::test]
async fn approved_connection_omits_approval_digest_and_preserves_saved_bytes() {
    let (conn, route, mut module) = SubcConnection::in_process_for_test(5, 3);
    let saved = r#"{"n":1e+30,"a":2}"#;
    let call = ToolCall {
        tool_call_id: broca_protocol::ToolCallId("approved".into()),
        tool_name: "edit".into(),
        input: serde_json::from_str(saved).unwrap(),
        thought_signature: None,
    };
    let args = ApprovedArgs {
        bytes: saved.into(),
        key: "lineage:7".into(),
        signals: Default::default(),
    };
    let (result, body) = tokio::join!(
        ToolPlaneConnection::tool_call_approved(
            &conn,
            &route,
            &call,
            crate::connection::ToolReplyDeadline {
                duration: Duration::from_secs(1),
                declared: false,
                yield_provider: None,
            },
            Some("head"),
            &args,
            None
        ),
        async {
            let request = module.next_request().await;
            let body = request.body.clone();
            assert!(module.reply(&request, br#"{"content":[]}"#.to_vec()));
            body
        }
    );
    result.unwrap();
    assert!(String::from_utf8(body.clone())
        .unwrap()
        .contains(&format!("\"arguments\":{saved}")));
    let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(value["call_key"], "lineage:7");
    assert_eq!(value["preset"], "head");
    assert!(
        value.get("approval_digest").is_none(),
        "consent execution must omit approval_digest rather than sending an empty or legacy digest"
    );
}
