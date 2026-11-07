//! Pin run and queue records before adding optional account-routing names.

use broca_wal::WalRecord;

const STARTED: &str = r#"{"type":"run_started","run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"route_decision_id":"caller-decision","model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"frozen_render_config":{"model":"vendor-model","provider":"vendor"}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
const QUEUED: &str = r#"{"type":"queued","submission_id":"q","request":{"route_decision_id":"caller-decision","model":{"provider_module_id":"p","model_id":"m"},"input":[],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}"#;

#[test]
fn records_without_account_route_keep_their_bytes() {
    for bytes in [STARTED, QUEUED] {
        let record: WalRecord = serde_json::from_str(bytes).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), bytes);
    }
}

#[test]
fn old_reader_ignores_additive_account_route() {
    for (bytes, member) in [(STARTED, "config"), (QUEUED, "request")] {
        let mut value: serde_json::Value = serde_json::from_str(bytes).unwrap();
        value[member]["account_route"] =
            serde_json::json!({"provider": "routing-provider", "model": "routing-model"});
        let record: WalRecord = serde_json::from_value(value).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), bytes);
    }
}
