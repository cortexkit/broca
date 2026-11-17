use super::*;

#[tokio::test]
async fn raw_envelope_has_parity_and_preserves_saved_argument_bytes() {
    let (conn, route, mut module) = SubcConnection::in_process_for_test(5, 3);
    let arguments = serde_json::json!({"a":1,"b":"x"});
    let (result, ordinary) = tokio::join!(
        conn.tool_call_with_preset(
            &route,
            "edit",
            &arguments,
            Some("c"),
            Duration::from_secs(1),
            Some("head")
        ),
        async {
            let request = module.next_request().await;
            let body = request.body.clone();
            assert!(module.reply(&request, br#"{"content":[]}"#.to_vec()));
            body
        }
    );
    result.unwrap();
    let argument_bytes = arguments.to_string();
    let (result, raw) = tokio::join!(
        conn.tool_call_raw_with_preset(
            &route,
            "edit",
            &argument_bytes,
            Some("c"),
            Duration::from_secs(1),
            Some("head"),
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
    assert_eq!(
        ordinary, raw,
        "the raw carrier changes no other envelope field"
    );

    let saved = r#"{"n":1e+30,"a":2}"#;
    let parsed: serde_json::Value = serde_json::from_str(saved).unwrap();
    assert_ne!(parsed.to_string(), saved);
    let (result, ()) = tokio::join!(
        conn.tool_call_raw_with_preset(
            &route,
            "edit",
            saved,
            Some("c"),
            Duration::from_secs(1),
            Some("head"),
            Some("lineage:7"),
        ),
        async {
            let request = module.next_request().await;
            let body = String::from_utf8(request.body.clone()).unwrap();
            assert!(
                body.contains(&format!("\"arguments\":{saved}")),
                "provider receives saved bytes verbatim"
            );
            let value: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(value["preset"], "head");
            assert_eq!(value["call_key"], "lineage:7");
            assert!(value.get("approval_digest").is_none());
            assert!(module.reply(&request, br#"{"content":[]}"#.to_vec()));
        }
    );
    result.unwrap();
}

#[tokio::test]
async fn malformed_saved_arguments_fail_closed_before_a_frame_is_sent() {
    let (conn, route, mut module) = SubcConnection::in_process_for_test(5, 3);
    for saved in ["{", "{} {}", "null,42"] {
        assert!(matches!(
            conn.tool_call_raw_with_preset(
                &route,
                "edit",
                saved,
                Some("c"),
                Duration::from_secs(1),
                None,
                None,
            )
            .await,
            Err(ConnError::Protocol(_))
        ));
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(10), module.next_request())
            .await
            .is_err()
    );
}
