//! Ordinary plan catalog used by the runner conformance adapter.
use super::*;

pub(super) fn catalog(request: &Value) -> Value {
    let args = &request["arguments"];
    let composition = &args["composition"];
    let digest = cortexkit_role_tool_provider::catalog::composition_digest(composition).unwrap();
    let schema = json!({"type":"object"});
    let tool_free = composition["providers"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|entry| entry["provider"] == TOOL_MODULE)
        .is_some_and(|entry| entry["tools"].as_array().is_some_and(Vec::is_empty));
    let tools = if tool_free {
        json!([])
    } else {
        json!([{"name":SCRIPTED_TOOL,"schema_digest":cortexkit_role_tool_provider::catalog::schema_digest(&schema).unwrap(),
            "semantics":1,"result_format":"tool-result/v1","capabilities":[],"description":"Answers each call with its scripted result.","input_schema":schema,
            "reference_slots":[{"name":"data","kind":"token"},{"name":"program","kind":"source:rich-ui/1"}]}])
    };
    let mut answer = json!({"generation":"1","catalog_digest":"conformance", "composition_digest":digest,"tools":tools});
    // Most planned cases put their script marker in the catalog text. Plan-note cases instead
    // identify their script by model so they can replace the text without relying on that marker.
    let item = &args["system_text"];
    let text = plan_notes::system_text(item)
        .map(str::to_owned)
        .or_else(|| {
            item["params"]["session"]
                .as_str()
                .map(|session| format!("{SESSION_MARKER}{session}"))
        });
    if let Some(text) = text {
        answer["system_text"] = json!({
            "text":text,
            "item_digest":cortexkit_role_tool_provider::catalog::system_text_digest(&text),
            "preflight_digest":"conformance", "composition_digest":digest,
            "tool_names":if tool_free { json!([]) } else { json!([SCRIPTED_TOOL]) },
        });
    }
    answer
}
