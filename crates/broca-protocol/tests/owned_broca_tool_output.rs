//! broca's model text for a tool output against the runner contract's `owned-broca` conversion
//! (`serializer_profile::owned_broca::tool_output`) and its published vectors
//! (`fixtures/owned-broca-tool-output/`, copied from `cortexkit-role-llm-runner` 0.8.61; see
//! the README there). A reader works out what the model was given from a stored output with
//! that conversion, so broca's text must be the same text, apart from where a media part
//! stands: the conversion lists media apart, and a family without media carries a marker in
//! the part's place.

use broca_protocol::content::{media_not_shown_marker, ToolOutput, ToolOutputPart};
use broca_protocol::MediaMarkerReason;
use cortexkit_role_llm_runner::serializer_profile::owned_broca;
use serde_json::Value;

fn cases() -> Vec<Value> {
    serde_json::from_str(include_str!("fixtures/owned-broca-tool-output/cases.json")).unwrap()
}

/// Every case is a stored output broca reads back unchanged.
#[test]
fn every_case_is_an_output_broca_stores_as_written() {
    let cases = cases();
    assert_eq!(cases.len(), 10);
    for case in &cases {
        let output: ToolOutput = serde_json::from_value(case["output"].clone())
            .unwrap_or_else(|error| panic!("{}: {error}", case["id"]));
        assert_eq!(
            serde_json::to_value(&output).unwrap(),
            case["output"],
            "{}",
            case["id"]
        );
    }
}

/// broca's text is the contract's for every output without media, and the contract's view is
/// the one each case records.
#[test]
fn an_output_without_media_reads_as_the_contracts_text() {
    for case in cases() {
        let is_error = case["is_error"].as_bool().unwrap();
        let view = owned_broca::tool_output(&case["output"], is_error).unwrap();
        assert_eq!(
            serde_json::to_value(&view).unwrap(),
            case["expected"],
            "{}",
            case["id"]
        );
        if !view.media.is_empty() {
            continue;
        }
        let output: ToolOutput = serde_json::from_value(case["output"].clone()).unwrap();
        assert_eq!(output.model_text(), view.text, "{}", case["id"]);
    }
}

/// With media, broca's text is the contract's text parts with a marker for each media part in
/// its place, and the media parts are the contract's references, in order.
#[test]
fn an_output_with_media_has_the_contracts_parts_and_markers_in_their_place() {
    let mut seen = 0;
    for case in cases() {
        let view = owned_broca::tool_output(&case["output"], false).unwrap();
        if view.media.is_empty() {
            continue;
        }
        seen += 1;
        let output: ToolOutput = serde_json::from_value(case["output"].clone()).unwrap();
        let ToolOutput::Parts { parts } = &output else {
            panic!("{}: media outside parts", case["id"]);
        };
        let mut texts = Vec::new();
        let mut media = Vec::new();
        let mut expected = Vec::new();
        for part in parts {
            match part {
                ToolOutputPart::Text { text } => {
                    texts.push(text.to_string());
                    expected.push(text.to_string());
                }
                ToolOutputPart::Media { media: reference } => {
                    media.push(serde_json::to_value(reference).unwrap());
                    expected.push(media_not_shown_marker(
                        reference,
                        MediaMarkerReason::Carrier,
                    ));
                }
            }
        }
        assert_eq!(texts.join("\n"), view.text, "{}", case["id"]);
        assert_eq!(
            Value::Array(media),
            serde_json::to_value(&view.media).unwrap(),
            "{}",
            case["id"]
        );
        assert_eq!(output.model_text(), expected.join("\n"), "{}", case["id"]);
    }
    assert_eq!(seen, 2, "the vectors hold two media cases");
}
