use broca_module_serve::wire::SendParamsExt;
use broca_protocol::{BindIdentity, RunId};
use serde_json::json;

fn pin_values() -> [(&'static str, Vec<u8>, &'static str); 4] {
    let params: broca_wire::SendParams = serde_json::from_value(json!({
        "send_id":"hintless", "prompt":"hello", "model":{"provider":"anthropic","model":"fixture"},
        "auth":{"method":"oauth","account":"work"}
    }))
    .unwrap();
    let send = serde_json::to_vec(&params).unwrap();
    let session = BindIdentity::new("/private", "runner", "session");
    let request = params
        .into_send_request(session, RunId("run".into()))
        .unwrap();
    // What a queued send stores of its request, and the run config a run starts with.
    let submission = serde_json::to_vec(&request.request).unwrap();
    let start: serde_json::Value = serde_json::from_str(
        include_str!("../../broca-records/tests/fixtures/before_message_times.jsonl")
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    let mut config = start["config"].clone();
    config["route_decision_id"] = json!("existing-decision");
    config["auth_selection"] = json!({"credential_id":"oauth:anthropic:work","auth_method":"oauth",
        "account_ref":"work-account","account_decision":{"path":"routed","admission_id":"run","decision_id":"allocation"}});
    let config: broca_records::record::RunConfig = serde_json::from_value(config).unwrap();
    let candidates = broca_module_serve::account_routing::Candidates {
        view_digest: "view".into(),
        accounts: vec![broca_module_serve::account_routing::Candidate {
            account_id: "account".into(),
            credential_id: "credential".into(),
        }],
    };
    let account_for = broca_module_serve::account_routing::account_for_params(
        &broca_module_serve::account_routing::AccountForRequest {
            account_route: None,
            pool: None,
            credential_id: None,
            decision_id: None,
            service_tier: None,
            provider: "anthropic",
            model: "fixture",
            conversation_key: "conversation",
            admission_id: "run",
            initial_input_bytes: 5,
            candidates: &candidates,
            hold_cap: Default::default(),
            head_turn: false,
            warm_credential: None,
        },
    );
    [
        (
            "send.json",
            send,
            include_str!("fixtures/auth-hints/send.json"),
        ),
        (
            "submission.json",
            submission,
            include_str!("fixtures/auth-hints/submission.json"),
        ),
        (
            "run-config.json",
            serde_json::to_vec(&config).unwrap(),
            include_str!("fixtures/auth-hints/run-config.json"),
        ),
        (
            "account-for.json",
            serde_json::to_vec(&account_for).unwrap(),
            include_str!("fixtures/auth-hints/account-for.json"),
        ),
    ]
}

#[test]
fn auth_hintless_send_submission_and_run_config_bytes_are_pinned() {
    let values = pin_values();
    if let Some(dir) = std::env::var_os("BROCA_AUTH_HINT_PIN_CAPTURE") {
        let dir = std::path::PathBuf::from(dir);
        std::fs::create_dir_all(&dir).unwrap();
        for (name, actual, _) in &values {
            std::fs::write(dir.join(name), actual).unwrap();
        }
    }
    for (name, actual, expected) in values {
        assert_eq!(actual.as_slice(), expected.trim_end().as_bytes(), "{name}");
    }
}
