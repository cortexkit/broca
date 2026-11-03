//! Pin the pre-routing-decision record bytes before adding optional routing metadata.

use broca_wal::WalRecord;

const QUEUED: &str = r#"{"type":"queued","submission_id":"q","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}"#;

fn started() -> &'static str {
    include_str!("fixtures/before_message_times.jsonl")
        .lines()
        .next()
        .unwrap()
}

#[test]
fn run_started_and_queued_without_routing_metadata_keep_their_bytes() {
    for bytes in [started(), QUEUED] {
        let record: WalRecord = serde_json::from_str(bytes).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), bytes);
    }
}

#[test]
fn record_decoders_accept_additive_route_decision_metadata() {
    for (bytes, member) in [(started(), "config"), (QUEUED, "request")] {
        let mut value: serde_json::Value = serde_json::from_str(bytes).unwrap();
        value[member]["route_decision_id"] = "caller-decision".into();
        let record: WalRecord = serde_json::from_value(value).unwrap();
        match record {
            WalRecord::RunStarted { config, .. } => assert_eq!(config.model.model_id, "m"),
            WalRecord::Queued { request, .. } => assert_eq!(request.model.model_id, "m"),
            _ => panic!("fixture must name a run or queued request"),
        }
    }
}
