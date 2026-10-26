//! Tracks per-connection continuation without changing canonical rendering or tool dispatch.

use serde_json::Value;

#[derive(Clone)]
pub(crate) struct CompletedResponse {
    pub request: Value,
    pub output: Vec<Value>,
    pub response_id: String,
}

impl CompletedResponse {
    /// Conservatively charge owned receipt storage, including spare string/array capacity.
    /// Map entries get extra room for tree/hash nodes and unused node slots; this is a
    /// retention budget, not a claim to measure allocator or process RSS exactly.
    #[cfg(feature = "live")]
    pub(crate) fn retained_bytes(&self) -> usize {
        fn allocations(value: &Value) -> usize {
            match value {
                Value::String(text) => text.capacity(),
                Value::Array(items) => {
                    items.capacity() * std::mem::size_of::<Value>()
                        + items.iter().map(allocations).sum::<usize>()
                }
                Value::Object(items) => {
                    items.len()
                        * 12
                        * (std::mem::size_of::<String>()
                            + std::mem::size_of::<Value>()
                            + 4 * std::mem::size_of::<usize>())
                        + items
                            .iter()
                            .map(|(key, value)| key.capacity() + allocations(value))
                            .sum::<usize>()
                }
                _ => 0,
            }
        }
        std::mem::size_of::<Self>()
            + self.response_id.capacity()
            + self.output.capacity() * std::mem::size_of::<Value>()
            + allocations(&self.request)
            + self.output.iter().map(allocations).sum::<usize>()
    }

    /// Codex 0.163.0 compares these eleven request properties before reusing a response.
    /// Delivery options, client metadata and access programs deliberately do not participate.
    pub fn suffix_start(&self, current: &Value) -> Result<usize, &'static str> {
        for (field, reason) in [
            ("model", "model_changed"),
            ("tools", "tools_changed"),
            ("tool_choice", "tool_choice_changed"),
            ("parallel_tool_calls", "parallel_tool_calls_changed"),
            ("reasoning", "reasoning_changed"),
            ("store", "store_changed"),
            ("stream", "stream_changed"),
            ("include", "include_changed"),
            ("service_tier", "service_tier_changed"),
            ("prompt_cache_key", "prompt_cache_key_changed"),
            ("text", "text_changed"),
        ] {
            if self.request.get(field) != current.get(field) {
                return Err(reason);
            }
        }
        let previous = self.request["input"].as_array().ok_or("input_shape")?;
        let current = current["input"].as_array().ok_or("input_shape")?;
        let prefix_len = previous.len() + self.output.len();
        if current.len() < prefix_len {
            return Err("input_shortened");
        }
        if !previous
            .iter()
            .chain(&self.output)
            .zip(current)
            .all(|(a, b)| items_equal(a, b))
        {
            return Err("input_mismatch");
        }
        if self.response_id.is_empty() {
            return Err("empty_response_id");
        }
        Ok(prefix_len)
    }
}

fn items_equal(previous: &Value, current: &Value) -> bool {
    if previous == current {
        return true;
    }
    // Appending input cannot change a tool result already sent in an earlier request.
    // Compare its position, tool name, arguments and result metadata before ignoring
    // the other internal annotations.
    if result_metadata(previous) != result_metadata(current) {
        return false;
    }
    let mut previous = previous.clone();
    let mut current = current.clone();
    for item in [&mut previous, &mut current] {
        if let Some(object) = item.as_object_mut() {
            object.remove("internal_chat_message_metadata_passthrough");
        }
    }
    previous == current
}

fn result_metadata(item: &Value) -> Vec<(usize, &Value, &Value, &Value)> {
    item["internal_chat_message_metadata_passthrough"]["executed_tool_calls"]
        .as_array()
        .into_iter()
        .flatten()
        .enumerate()
        .filter(|(_, call)| call.get("tool_result_metadata").is_some())
        .map(|(index, call)| {
            (
                index,
                &call["name"],
                &call["arguments"],
                &call["tool_result_metadata"],
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn fixture() -> (CompletedResponse, Value) {
        let request = json!({"model":"m","tools":[],"tool_choice":"auto","parallel_tool_calls":true,"reasoning":{},"store":false,"stream":true,"include":["reasoning.encrypted_content"],"prompt_cache_key":"scope","input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"Hi"}]}]});
        let output = vec![
            json!({"type":"message","role":"assistant","content":[{"type":"output_text","text":"Hello"}]}),
        ];
        let mut current = request.clone();
        current["input"]
            .as_array_mut()
            .unwrap()
            .extend(output.clone());
        (
            CompletedResponse {
                request,
                output,
                response_id: "resp_one".into(),
            },
            current,
        )
    }
    #[test]
    fn matching_prefix_and_empty_suffix_are_eligible_with_store_false() {
        let (previous, mut current) = fixture();
        assert_eq!(previous.suffix_start(&current), Ok(2));
        current["input"]
            .as_array_mut()
            .unwrap()
            .push(json!({"type":"function_call_output","call_id":"c","output":"ok"}));
        assert_eq!(previous.suffix_start(&current), Ok(2));
        current["stream_options"] = json!({"delivery":"changed"});
        current["client_metadata"] = json!({"turn":"new"});
        current["access_programs"] = json!(["changed"]);
        assert_eq!(previous.suffix_start(&current), Ok(2));
    }
    #[test]
    fn shorter_and_mismatching_inputs_are_full() {
        let (previous, mut current) = fixture();
        current["input"].as_array_mut().unwrap().pop();
        assert_eq!(previous.suffix_start(&current), Err("input_shortened"));
        let (_, mut current) = fixture();
        current["input"][0]["content"][0]["text"] = json!("changed");
        assert_eq!(previous.suffix_start(&current), Err("input_mismatch"));
    }
    #[test]
    fn empty_completed_response_id_is_full() {
        let (mut previous, current) = fixture();
        previous.response_id.clear();
        assert_eq!(previous.suffix_start(&current), Err("empty_response_id"));
    }
    #[test]
    fn metadata_only_difference_is_ignored_but_late_result_bindings_are_not() {
        let (mut previous, mut current) = fixture();
        previous.output[0]["internal_chat_message_metadata_passthrough"] =
            json!({"content_item_kinds":["old"]});
        current["input"][1]["internal_chat_message_metadata_passthrough"] =
            json!({"content_item_kinds":["new"]});
        assert_eq!(previous.suffix_start(&current), Ok(2));
        let call =
            json!({"name":"read","arguments":{"path":"p"},"tool_result_metadata":{"source":"one"}});
        previous.output[0]["internal_chat_message_metadata_passthrough"]["executed_tool_calls"] =
            json!([call.clone()]);
        current["input"][1]["internal_chat_message_metadata_passthrough"]["executed_tool_calls"] =
            json!([call.clone()]);
        assert_eq!(previous.suffix_start(&current), Ok(2));
        for changed in [
            json!({"name":"other"}),
            json!({"arguments":{"path":"other"}}),
            json!({"tool_result_metadata":{"source":"other"}}),
        ] {
            let mut mismatch = current.clone();
            let object = mismatch["input"][1]["internal_chat_message_metadata_passthrough"]
                ["executed_tool_calls"][0]
                .as_object_mut()
                .unwrap();
            object.extend(changed.as_object().unwrap().clone());
            assert_eq!(previous.suffix_start(&mismatch), Err("input_mismatch"));
        }
        current["input"][1]["internal_chat_message_metadata_passthrough"]["executed_tool_calls"] =
            json!([{"name":"ignored","arguments":{}},call]);
        assert_eq!(previous.suffix_start(&current), Err("input_mismatch"));
        let (mut previous, mut current) = fixture();
        previous.output[0]["internal_chat_message_metadata_passthrough"] =
            json!({"executed_tool_calls":[{"name":"read","arguments":{}}]});
        current["input"][1]["internal_chat_message_metadata_passthrough"] = json!({"executed_tool_calls":[{"name":"read","arguments":{},"tool_result_metadata":null}]});
        assert_eq!(previous.suffix_start(&current), Err("input_mismatch"));
    }
    fn property(field: &str, value: Value, reason: &'static str) {
        let (previous, mut current) = fixture();
        current[field] = value;
        assert_eq!(previous.suffix_start(&current), Err(reason));
    }
    #[test]
    fn model_change_is_full() {
        property("model", json!("other"), "model_changed");
    }
    #[test]
    fn tools_change_is_full() {
        property(
            "tools",
            json!([{"type":"function","name":"f"}]),
            "tools_changed",
        );
    }
    #[test]
    fn tool_choice_change_is_full() {
        property("tool_choice", json!("none"), "tool_choice_changed");
    }
    #[test]
    fn parallel_tool_calls_change_is_full() {
        property(
            "parallel_tool_calls",
            json!(false),
            "parallel_tool_calls_changed",
        );
    }
    #[test]
    fn reasoning_change_is_full() {
        property("reasoning", json!({"effort":"high"}), "reasoning_changed");
    }
    #[test]
    fn store_change_is_full() {
        property("store", json!(true), "store_changed");
    }
    #[test]
    fn stream_change_is_full() {
        property("stream", json!(false), "stream_changed");
    }
    #[test]
    fn include_change_is_full() {
        property("include", json!([]), "include_changed");
    }
    #[test]
    fn service_tier_change_is_full() {
        property("service_tier", json!("priority"), "service_tier_changed");
    }
    #[test]
    fn prompt_cache_key_change_is_full() {
        property(
            "prompt_cache_key",
            json!("other"),
            "prompt_cache_key_changed",
        );
    }
    #[test]
    fn text_change_is_full() {
        property("text", json!({"verbosity":"low"}), "text_changed");
    }
}
