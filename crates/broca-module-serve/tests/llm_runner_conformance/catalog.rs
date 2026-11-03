//! Ordinary plan catalog used by the runner conformance adapter.
use super::*;

pub(super) fn catalog(request: &Value) -> Value {
    let args = &request["arguments"];
    let composition = &args["composition"];
    let digest = cortexkit_role_tool_provider::catalog::composition_digest(composition).unwrap();
    let schema = json!({"type":"object"});
    let tool_free = composition["providers"].as_array().into_iter().flatten()
        .find(|entry| entry["provider"] == TOOL_MODULE)
        .is_some_and(|entry| entry["tools"].as_array().is_some_and(Vec::is_empty));
    let tools = if tool_free { json!([]) } else {
        json!([{"name":SCRIPTED_TOOL,"schema_digest":cortexkit_role_tool_provider::catalog::schema_digest(&schema).unwrap(),
            "semantics":1,"result_format":"tool-result/v1","capabilities":[],"description":"Answers each call with its scripted result.","input_schema":schema,
            "reference_slots":[{"name":"data","kind":"token"},{"name":"program","kind":"source:rich-ui/1"}]}])
    };
    let mut answer = json!({"generation":"1","catalog_digest":"conformance", "composition_digest":digest,"tools":tools});
    // A plan replaces system text, so its system-text item carries the scripted model's marker.
    if let Some(session) = args["system_text"]["params"]["session"].as_str() {
        let text = format!("{SESSION_MARKER}{session}");
        answer["system_text"] = json!({
            "text":text,
            "item_digest":cortexkit_role_tool_provider::catalog::system_text_digest(&text),
            "preflight_digest":"conformance", "composition_digest":digest,
            "tool_names":if tool_free { json!([]) } else { json!([SCRIPTED_TOOL]) },
        });
    }
    answer
}
