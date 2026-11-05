use broca_module_serve::wire::SendParamsExt;
use broca_protocol::{BindIdentity, RunId, SubmissionId};
use broca_wal::WalRecord;
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
        .into_send_request(session.clone(), RunId("run".into()))
        .unwrap();
    let queued = WalRecord::Queued {
        sent_by: None,
        submission_id: SubmissionId("queued".into()),
        request: request.request,
        request_identity_digest: None,
        principal: None,
        send_id: Some("hintless".into()),
        title: None,
        delivery: Some(broca_protocol::Delivery::Queue),
        retention: None,
        conversation_key: None,
        hooks: None,
        keep_warm: None,
        written_at_ms: None,
        ts_ms: None,
    };
    let mut start: serde_json::Value = serde_json::from_str(
        include_str!("../../broca-wal/tests/fixtures/before_message_times.jsonl")
            .lines()
            .next()
            .unwrap(),
    )
    .unwrap();
    start["session"] = serde_json::to_value(session).unwrap();
    start["run_id"] = json!("run");
    start["config"]["route_decision_id"] = json!("existing-decision");
    start["config"]["auth_selection"] = json!({"credential_id":"oauth:anthropic:work","auth_method":"oauth",
        "account_ref":"work-account","account_decision":{"path":"routed","admission_id":"run","decision_id":"allocation"}});
    let start: WalRecord = serde_json::from_value(start).unwrap();
    let candidates = broca_module_serve::account_routing::Candidates {
        view_digest: "view".into(),
        accounts: vec![broca_module_serve::account_routing::Candidate {
            account_id: "account".into(),
            credential_id: "credential".into(),
        }],
    };
    let account_for = broca_module_serve::account_routing::account_for_params(
        &broca_module_serve::account_routing::AccountForRequest {
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
        },
    );
    [
        (
            "send.json",
            send,
            include_str!("fixtures/auth-hints/send.json"),
        ),
        (
            "queued.json",
            serde_json::to_vec(&queued).unwrap(),
            include_str!("fixtures/auth-hints/queued.json"),
        ),
        (
            "run-started.json",
            serde_json::to_vec(&start).unwrap(),
            include_str!("fixtures/auth-hints/run-started.json"),
        ),
        (
            "account-for.json",
            serde_json::to_vec(&account_for).unwrap(),
            include_str!("fixtures/auth-hints/account-for.json"),
        ),
    ]
}

#[test]
fn auth_hintless_send_queue_and_run_start_bytes_are_pinned() {
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

// Both auth input types in 0.3.315 used this derived shape without deny_unknown_fields.
// Keep the old shape independent of additions to the current input types.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct AuthReader0315 {
    method: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    account: Option<String>,
}

#[derive(serde::Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum RecordAuthReader0315 {
    Queued { request: SubmissionAuthReader0315 },
}

#[derive(serde::Deserialize)]
struct SubmissionAuthReader0315 {
    auth: Option<AuthReader0315>,
}

#[test]
fn auth_hints_are_ignored_by_the_0315_auth_decoder_inside_queued_records() {
    let (_, bytes, _) = pin_values()
        .into_iter()
        .find(|(name, _, _)| *name == "queued.json")
        .unwrap();
    let mut queued: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    queued["request"]["auth"]["pool"] = json!("selected-pool");
    queued["request"]["auth"]["credential_id"] = json!("preferred-credential");
    let RecordAuthReader0315::Queued { request } = serde_json::from_value(queued.clone()).unwrap();
    let old = request.auth.unwrap();
    assert_eq!(old.method, "oauth");
    assert_eq!(old.account.as_deref(), Some("work"));
    assert_eq!(
        serde_json::to_value(old).unwrap(),
        json!({"method":"oauth","account":"work"})
    );
    let decoded: WalRecord = serde_json::from_value(queued).unwrap();
    assert!(
        matches!(decoded, WalRecord::Queued { request, .. } if request.auth.as_ref().unwrap().method == "oauth")
    );
    assert!(serde_json::from_value::<AuthReader0315>(
        json!({"pool":"pool","credential_id":"preferred"})
    )
    .is_err());
}
