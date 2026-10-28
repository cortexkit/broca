//! broca's tool error results against the runner contract's published tool-outage vectors
//! (`fixtures/owned-broca-tool-outage/`, copied from `cortexkit-role-llm-runner`; see the
//! README there). The vectors include broca's own producer captures, so these tests pin
//! that broca still builds, encodes and renders an outage result exactly as captured.

use broca_protocol::content::ToolOutput;
use cortexkit_role_llm_runner::serializer_profile::error_output::ErrorOutput;
use serde_json::Value;

fn fixture(name: &str) -> Value {
    let text = match name {
        "valid" => include_str!("fixtures/owned-broca-tool-outage/valid-error-outputs.json"),
        "display" => {
            include_str!("fixtures/owned-broca-tool-outage/session_history.display.row.json")
        }
        "display_plain" => include_str!(
            "fixtures/owned-broca-tool-outage/session_history.display.plain-error.row.json"
        ),
        "record" => include_str!("fixtures/owned-broca-tool-outage/tool_result.record.json"),
        other => panic!("no fixture {other}"),
    };
    serde_json::from_str(text).unwrap()
}

/// The text the model reads for `output`, as every provider family renders it.
fn model_text(output: &ToolOutput) -> String {
    let ToolOutput::Error {
        message,
        module,
        code,
        detail,
        nothing_sent,
        retry,
    } = output
    else {
        panic!("expected an error output, got {output:?}");
    };
    ToolOutput::error_model_text(
        message,
        module.as_deref(),
        code.as_deref(),
        detail.as_deref(),
        *nothing_sent,
        retry.as_deref(),
    )
}

/// The result broca builds for the captured outage: `aft` refused to open the route with
/// `target_unavailable`.
fn captured_outage() -> ToolOutput {
    ToolOutput::module_unavailable("aft", "target_unavailable", "supervised_not_registered")
}

/// The display rows were captured from broca's history path: the model text broca renders
/// today is byte-identical to the captured content, for the outage and for a plain error.
#[test]
fn rendered_text_matches_the_captured_display_rows() {
    let content = |name: &str| {
        fixture(name)["row"]["parts"][0]["content"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    assert_eq!(model_text(&captured_outage()), content("display"));
    assert_eq!(
        model_text(&ToolOutput::error("plain failure")),
        content("display_plain")
    );
}

/// The stored record was captured from broca's write-ahead log (WAL): the outage result broca builds today
/// equals the captured output, and encodes to the same JSON.
#[test]
fn the_outage_result_matches_the_captured_record() {
    let captured = &fixture("record")["record"]["result"]["output"];
    let decoded: ToolOutput = serde_json::from_value(captured.clone()).unwrap();
    assert_eq!(decoded, captured_outage());
    assert_eq!(serde_json::to_value(captured_outage()).unwrap(), *captured);
    assert!(ErrorOutput::decode(captured).is_ok());
}

/// Every output a contract reader accepts decodes into broca's `ToolOutput`, renders to the
/// vector's block, and re-encodes to the same bytes as the contract's own encoder (members
/// in the fixed wire order, whatever order the vector wrote them in).
#[test]
fn every_valid_vector_round_trips_and_renders_as_published() {
    let valid = fixture("valid");
    let cases = valid["valid"].as_array().unwrap();
    assert!(!cases.is_empty());
    for case in cases {
        let name = &case["name"];
        let output: ToolOutput = serde_json::from_value(case["output"].clone())
            .unwrap_or_else(|error| panic!("{name}: {error}"));
        assert_eq!(
            model_text(&output),
            case["rendered"].as_str().unwrap(),
            "{name}"
        );
        let contract = ErrorOutput::decode(&case["output"]).unwrap();
        assert_eq!(
            serde_json::to_string(&output).unwrap(),
            serde_json::to_string(&contract).unwrap(),
            "{name}"
        );
        assert_eq!(output.contract_error_output(), Some(contract), "{name}");
    }
}
