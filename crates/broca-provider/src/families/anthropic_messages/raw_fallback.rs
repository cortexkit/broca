//! Preserve opaque handoff blocks without changing the serialization of the rest of a request.

use serde::Deserialize;
use serde_json::{json, value::RawValue, Value};

pub(super) fn shape(from: &str, to: &str, raw: Option<&str>) -> Value {
    raw.and_then(|raw| serde_json::from_str::<Value>(raw).ok())
        .filter(|block| {
            block["type"] == "fallback"
                && block["from"]["model"] == from
                && block["to"]["model"] == to
        })
        .unwrap_or_else(|| json!({"type":"fallback", "from":{"model":from}, "to":{"model":to}}))
}

#[derive(Deserialize)]
struct Body<'a> {
    #[serde(borrow, default)]
    messages: Vec<Message<'a>>,
}

#[derive(Deserialize)]
struct Message<'a> {
    role: &'a str,
    #[serde(borrow)]
    content: &'a RawValue,
}

pub(super) fn blocks(body: &[u8]) -> Vec<&str> {
    let body: Body<'_> = serde_json::from_slice(body).expect("rendered Anthropic body is JSON");
    body.messages
        .into_iter()
        .filter(|message| message.role == "assistant")
        .flat_map(|message| {
            serde_json::from_str::<Vec<&RawValue>>(message.content.get()).unwrap_or_default()
        })
        .filter(|block| {
            serde_json::from_str::<Value>(block.get())
                .is_ok_and(|block| block["type"] == "fallback")
        })
        .map(RawValue::get)
        .collect()
}

/// Splice only direct assistant content blocks, not matching objects inside tool arguments
/// or strings. Borrowed RawValue slices identify their exact ranges in the serialized body.
pub(super) fn splice(body: Vec<u8>, fragments: &[Option<&str>]) -> Vec<u8> {
    if fragments.iter().all(Option::is_none) {
        return body;
    }
    let blocks = blocks(&body);
    if blocks.len() != fragments.len() {
        return body;
    }
    let mut output = Vec::with_capacity(body.len());
    let mut end = 0;
    for (block, fragment) in blocks.into_iter().zip(fragments) {
        let Some(fragment) = fragment else { continue };
        let Ok(value) = serde_json::from_str::<Value>(fragment) else {
            continue;
        };
        let rendered: Value =
            serde_json::from_str(block).expect("a rendered content block is JSON");
        // The shape and raw fragment must describe the same object. The persona
        // serializer normalizes integral floats to integers, so use its number spelling
        // for both comparisons before restoring the original bytes.
        if crate::claude_code_persona::to_persona_json(&value)
            != crate::claude_code_persona::to_persona_json(&rendered)
        {
            continue;
        }
        let start = block.as_ptr() as usize - body.as_ptr() as usize;
        output.extend_from_slice(&body[end..start]);
        output.extend_from_slice(fragment.as_bytes());
        end = start + block.len();
    }
    output.extend_from_slice(&body[end..]);
    output
}
