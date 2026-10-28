use broca_core::compaction_provider::{
    lineage::MessageRef, setup::SetupRequest, OpRequest as CompactionOpRequest,
};
use cortexkit_role_step_transform_provider::{
    hook::{HookCall, Subject},
    subscription::DeclareRequest,
    OpRequest as TransformOpRequest,
};
use serde_json::{json, Map, Value};
use subc_protocol::tool_call::ToolCallRequest;

fn map(value: Value) -> Map<String, Value> {
    value.as_object().expect("object fixture").clone()
}

#[test]
fn compaction_setup_request_bytes_are_pinned() {
    let mut request = SetupRequest::new(
        "session-1",
        "broca",
        "lineage-1:setup:9",
        map(json!({"provider": "compactor"})),
        "provider/model",
        1_700_000_000_000,
    )
    .with_item(Some("default".into()), map(json!({"budget": 3})))
    .with_model_details(Some("fast".into()), Some(128_000), Some(8_192));
    request.lineage_id = Some("lineage-1".into());
    request.newest = Some(MessageRef {
        ordinal: 4,
        mid: "m4".into(),
    });

    let bytes =
        serde_json::to_vec(&CompactionOpRequest::new("compaction.setup", &request)).unwrap();
    assert_eq!(
        bytes,
        br#"{"method":"compaction.setup","params":{"session":"session-1","harness":"broca","request_id":"lineage-1:setup:9","lineage_id":"lineage-1","preset":"default","params":{"budget":3},"composition":{"provider":"compactor"},"model":"provider/model","variant":"fast","context_window":128000,"output_limit":8192,"newest":{"ordinal":4,"mid":"m4"},"now":1700000000000}}"#
    );
}

#[test]
fn transform_declare_request_bytes_are_pinned() {
    let request = DeclareRequest::new(map(json!({"mode": "safe"})))
        .with_preset("default")
        .with_composition(map(json!({"provider": "transformer"})));
    let bytes =
        serde_json::to_vec(&TransformOpRequest::new("transform.declare", &request)).unwrap();
    assert_eq!(
        bytes,
        br#"{"method":"transform.declare","params":{"preset":"default","params":{"mode":"safe"},"composition":{"provider":"transformer"}}}"#
    );
}

#[test]
fn transform_hook_request_bytes_are_pinned() {
    let request = HookCall::new(
        "session-1",
        "broca",
        Subject::PreUser {
            blocks: vec!["hello".into()],
            mark: None,
            delivery: None,
        },
    )
    .with_item(Some("default".into()), map(json!({"mode": "safe"})));
    let bytes = serde_json::to_vec(&TransformOpRequest::new("transform.hook", &request)).unwrap();
    assert_eq!(
        bytes,
        br#"{"method":"transform.hook","params":{"session":"session-1","harness":"broca","preset":"default","params":{"mode":"safe"},"hook":"pre_user","blocks":["hello"]}}"#
    );
}

#[test]
fn tool_call_request_bytes_are_pinned() {
    let request = ToolCallRequest {
        name: "read_file".into(),
        arguments: json!({"path": "README.md"}),
        tool_call_id: Some("call-1".into()),
        progress_token: None,
        call_key: None,
        schema_pin: None,
        preset: Some("default".into()),
        origin: None,
        output_refs: None,
    };
    assert_eq!(
        serde_json::to_vec(&request).unwrap(),
        br#"{"name":"read_file","arguments":{"path":"README.md"},"tool_call_id":"call-1","preset":"default"}"#
    );
}
