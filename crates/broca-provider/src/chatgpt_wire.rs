//! Durable opt-in to the released Codex wire policy. Missing profiles remain legacy.

use broca_protocol::wire_node::{Node, NodeRef};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChatgptWireProfile {
    #[serde(rename = "codex-0.163.0")]
    Codex0163,
}

/// Identity captured at admission, never rediscovered while rendering or resuming.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChatgptWire {
    pub profile: ChatgptWireProfile,
    pub user_agent: String,
    pub originator: String,
    pub session_id: String,
    pub thread_id: String,
    pub installation_id: String,
    pub window_id: String,
    pub turn_id: String,
    pub turn_started_at_unix_ms: u64,
}

impl ChatgptWire {
    pub const VERSION: &str = "0.163.0";
    pub const ORIGINATOR: &str = "codex_cli_rs";

    pub fn instruction_id(&self, text: &str) -> String {
        let namespace = Uuid::new_v5(&Uuid::NAMESPACE_OID, self.thread_id.as_bytes());
        format!("msg_{}", Uuid::new_v5(&namespace, text.as_bytes()))
    }

    fn turn_metadata(&self) -> String {
        // This compact ASCII JSON is also a header. Only admitted identity and turn time
        // belong here; Codex-only workspace and analytics facts have no authority in broca.
        #[derive(Serialize)]
        struct TurnMetadata<'a> {
            installation_id: &'a str,
            session_id: &'a str,
            thread_id: &'a str,
            turn_id: &'a str,
            window_id: &'a str,
            request_kind: &'a str,
            turn_started_at_unix_ms: u64,
        }
        let text = serde_json::to_string(&TurnMetadata {
            installation_id: &self.installation_id,
            session_id: &self.session_id,
            thread_id: &self.thread_id,
            turn_id: &self.turn_id,
            window_id: &self.window_id,
            request_kind: "turn",
            turn_started_at_unix_ms: self.turn_started_at_unix_ms,
        })
        .expect("string metadata serializes");
        let mut ascii = String::new();
        for character in text.chars() {
            if character.is_ascii() {
                ascii.push(character);
            } else {
                for unit in character.encode_utf16(&mut [0; 2]) {
                    use std::fmt::Write;
                    write!(ascii, "\\u{unit:04x}").expect("writing to a String succeeds");
                }
            }
        }
        ascii
    }

    pub(crate) fn apply_with_provenance(
        &self,
        whole: &mut crate::families::openai_responses::ResponsesBody<'_>,
        opts: &broca_protocol::CallOptions,
        aliases: &std::collections::BTreeMap<String, String>,
        provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
    ) {
        let body = &mut whole.rest;
        let instructions = body.as_object_mut().unwrap().remove("instructions");
        if let Some(text) = instructions
            .as_ref()
            .and_then(Value::as_str)
            .filter(|text| !text.is_empty())
        {
            whole.input.insert(0, Node::value(json!({
                "type":"message", "id":self.instruction_id(text), "role":"developer",
                "content":[{"type":"input_text","text":text}],
                "internal_chat_message_metadata_passthrough":{"content_item_kinds":["model.base_instructions"]}
            })));
            if let Some(sink) = provenance {
                sink.prepend_synthetic_message();
            }
        }
        typed_message_nodes(&mut whole.input);
        body["parallel_tool_calls"] = json!(true);
        let includes = body
            .as_object_mut()
            .unwrap()
            .entry("include")
            .or_insert_with(|| json!([]));
        if !includes.is_array() {
            *includes = json!([]);
        }
        if let Some(includes) = includes.as_array_mut() {
            if !includes
                .iter()
                .any(|value| value == "reasoning.encrypted_content")
            {
                includes.push(json!("reasoning.encrypted_content"));
            }
        }
        let object = body.as_object_mut().unwrap();
        object.entry("tools").or_insert_with(|| json!([]));
        object.entry("tool_choice").or_insert_with(|| {
            let names = crate::tool_name_alias::ToolNameMap::build_with_aliases(
                &opts.tools,
                &opts.prompt,
                aliases,
            );
            crate::families::openai_responses::convert_tool_choice(&opts.tool_choice, &names)
        });
        // The catalog has no authoritative summary/default effort/verbosity facts. Keep
        // explicit caller choices, including schema output, instead of guessing defaults.
        object.entry("reasoning").or_insert_with(|| json!({}));
        body["client_metadata"] = json!({
            "x-codex-installation-id":self.installation_id,
            "session_id":self.session_id, "thread_id":self.thread_id,
            "x-codex-window-id":self.window_id, "turn_id":self.turn_id,
            "x-codex-turn-metadata":self.turn_metadata()
        });
    }

    pub(crate) fn headers(&self, cache_key: Option<&str>) -> Vec<(String, String)> {
        vec![
            ("originator".into(), self.originator.clone()),
            ("x-client-request-id".into(), self.thread_id.clone()),
            (
                "session-id".into(),
                cache_key.unwrap_or(&self.session_id).into(),
            ),
            ("thread-id".into(), self.thread_id.clone()),
            ("x-codex-window-id".into(), self.window_id.clone()),
            ("x-codex-turn-metadata".into(), self.turn_metadata()),
            ("user-agent".into(), self.user_agent.clone()),
        ]
    }

    /// Separate installation and conversation namespaces; the conversation source includes
    /// the full bind identity so equal short session names cannot share a thread.
    pub fn scoped_id(domain: &str, source: &str) -> String {
        Uuid::new_v5(
            &Uuid::NAMESPACE_OID,
            format!("broca:{domain}:{source}").as_bytes(),
        )
        .to_string()
    }
}

pub(crate) fn typed_message_content(items: &mut [Value]) {
    for item in items {
        if item["type"] == "message" {
            if let Some(text) = item["content"].as_str() {
                let kind = if item["role"] == "assistant" {
                    "output_text"
                } else {
                    "input_text"
                };
                item["content"] = json!([{"type":kind,"text":text}]);
            }
        }
    }
}

/// [`typed_message_content`] for input items written as nodes. A message's string content is
/// moved into its typed part, not copied.
pub(crate) fn typed_message_nodes(items: &mut [Node<'_>]) {
    for item in items {
        if let Node::Value(value) = item {
            typed_message_content(std::slice::from_mut(value.to_mut()));
            continue;
        }
        if item.get("type").and_then(NodeRef::as_str) != Some("message") {
            continue;
        }
        let kind = if item.get("role").and_then(NodeRef::as_str) == Some("assistant") {
            "output_text"
        } else {
            "input_text"
        };
        let Some(content) = item.get_mut("content") else {
            continue;
        };
        if content.as_str().is_none() {
            continue;
        }
        let text = std::mem::replace(content, Node::Array(Vec::new()));
        *content = Node::Array(vec![Node::object([
            ("type", Node::str(kind)),
            ("text", text),
        ])]);
    }
}

/// Routing fields precede large input only in the transport encoding. Canonical rendering
/// still uses sorted JSON, so retry and resume hashes do not acquire framing policy.
#[cfg(any(feature = "live", test))]
const ROUTING_ORDER: [&str; 18] = [
    "type",
    "model",
    "stream",
    "service_tier",
    "previous_response_id",
    "input",
    "tools",
    "tool_choice",
    "parallel_tool_calls",
    "reasoning",
    "store",
    "stream_options",
    "include",
    "prompt_cache_key",
    "text",
    "generate",
    "client_metadata",
    "access_programs",
];

/// A request's members, each a key name and its value's canonical text, written as one JSON
/// object: the routing fields first in [`ROUTING_ORDER`], the rest by key. With
/// `routing: false`, every member by key: the canonical object.
#[cfg(any(feature = "live", test))]
pub(crate) fn object_bytes(
    members: Vec<(std::borrow::Cow<'_, str>, std::borrow::Cow<'_, str>)>,
    routing: bool,
) -> Vec<u8> {
    crate::json_text::object_bytes(members, if routing { &ROUTING_ORDER } else { &[] })
}

/// A rendered request's canonical `body` in the backend's routing order, written from the
/// body's text without parsing it. `None` when `body` is not a JSON object.
#[cfg(any(feature = "live", test))]
pub(crate) fn request_bytes(body: &[u8]) -> Option<Vec<u8>> {
    let members = crate::json_text::top_level_members(std::str::from_utf8(body).ok()?)?;
    Some(object_bytes(
        members
            .into_iter()
            .map(|(key, value)| (crate::json_text::key_name(key), value.into()))
            .collect(),
        true,
    ))
}

#[cfg(any(feature = "live", test))]
pub(crate) fn websocket_headers(headers: &[(String, String)]) -> Vec<(String, String)> {
    let order = [
        "originator",
        "x-client-request-id",
        "session-id",
        "thread-id",
        "x-codex-window-id",
        "x-codex-turn-metadata",
        "openai-beta",
        "user-agent",
        "authorization",
        "chatgpt-account-id",
    ];
    let mut result = Vec::new();
    for name in order {
        if name == "openai-beta" {
            result.push((name.into(), "responses_websockets=2026-02-06".into()));
        } else {
            result.extend(
                headers
                    .iter()
                    .filter(|(key, _)| key.eq_ignore_ascii_case(name))
                    .cloned(),
            );
        }
    }
    result.extend(
        headers
            .iter()
            .filter(|(name, _)| {
                !order.iter().any(|key| name.eq_ignore_ascii_case(key))
                    && !["content-type", "accept"]
                        .iter()
                        .any(|key| name.eq_ignore_ascii_case(key))
            })
            .cloned(),
    );
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::families::{openai_responses_chatgpt_frozen_config, OpenAiResponsesChatgpt};
    use crate::wire::{AuthHandle, FrozenRenderConfig, WireFamily};
    use broca_protocol::CallOptions;

    fn fixture() -> (FrozenRenderConfig, CallOptions) {
        let mut frozen = openai_responses_chatgpt_frozen_config("gpt-5.3-codex");
        frozen.chatgpt_wire = Some(
            serde_json::from_str(include_str!(
                "../tests/fixtures/chatgpt_codex_0_163_identity.json"
            ))
            .unwrap(),
        );
        frozen.prompt_cache_key = Some("caller-cache-key".into());
        let opts = serde_json::from_value(json!({"prompt":[
            {"role":"system","content":[{"type":"text","text":"Be concise."}]},
            {"role":"user","content":[{"type":"text","text":"Hello"}]}
        ]}))
        .unwrap();
        (frozen, opts)
    }
    fn body() -> Value {
        let (frozen, opts) = fixture();
        OpenAiResponsesChatgpt.shape(&frozen, &opts)
    }

    #[test]
    fn parallel_tools_are_explicit() {
        assert_eq!(body()["parallel_tool_calls"], true);
    }
    #[test]
    fn empty_tools_are_explicit() {
        assert_eq!(body()["tools"], json!([]));
    }
    #[test]
    fn empty_tool_choice_defaults_to_auto() {
        assert_eq!(body()["tool_choice"], "auto");
    }
    #[test]
    fn encrypted_reasoning_is_unconditional() {
        assert_eq!(body()["include"], json!(["reasoning.encrypted_content"]));
    }
    #[test]
    fn instructions_are_developer_input() {
        let body = body();
        assert!(body.get("instructions").is_none());
        assert_eq!(body["input"][0]["role"], "developer");
        assert_eq!(
            body["input"][0]["content"],
            json!([{"type":"input_text","text":"Be concise."}])
        );
        assert_eq!(
            body["input"][0]["internal_chat_message_metadata_passthrough"],
            json!({"content_item_kinds":["model.base_instructions"]})
        );
    }
    #[test]
    fn instruction_id_matches_codex_uuid_v5() {
        assert_eq!(
            body()["input"][0]["id"],
            "msg_98c6451b-474b-504e-978b-d4e4fedad369"
        );
    }
    #[test]
    fn text_messages_are_typed_content_arrays() {
        let (frozen, mut opts) = fixture();
        let assistant = serde_json::from_value(
            json!({"role":"assistant","content":[{"type":"text","text":"Answer"}]}),
        )
        .unwrap();
        opts.prompt = vec![opts.prompt[1].clone(), assistant];
        let body = OpenAiResponsesChatgpt.shape(&frozen, &opts);
        assert_eq!(
            body["input"][0]["content"],
            json!([{"type":"input_text","text":"Hello"}])
        );
        assert_eq!(
            body["input"][1]["content"],
            json!([{"type":"output_text","text":"Answer"}])
        );
    }
    #[test]
    fn no_unknown_reasoning_or_verbosity_defaults() {
        let body = body();
        assert_eq!(body["reasoning"], json!({}));
        assert!(body.get("text").is_none());
    }
    #[test]
    fn caller_controls_win() {
        let (mut frozen, mut opts) = fixture();
        frozen.reasoning_effort = Some("high".into());
        frozen.service_tier = Some("priority".into());
        opts.tool_choice = broca_protocol::request::ToolChoice::None.into();
        let body = OpenAiResponsesChatgpt.shape(&frozen, &opts);
        assert_eq!(body["tool_choice"], "none");
        assert_eq!(body["reasoning"], json!({"effort":"high"}));
        assert_eq!(body["service_tier"], "priority");
    }
    #[test]
    fn sourced_metadata_and_frozen_headers_agree() {
        let (frozen, _) = fixture();
        let wire = frozen.chatgpt_wire.as_ref().unwrap();
        let headers = OpenAiResponsesChatgpt
            .endpoint(
                &frozen,
                &AuthHandle::Headers(vec![
                    ("user-agent".into(), "changed-host".into()),
                    ("thread-id".into(), "wrong".into()),
                ]),
                Vec::new(),
            )
            .headers;
        for (name, expected) in wire.headers(Some("caller-cache-key")) {
            let actual: Vec<_> = headers.iter().filter(|(key, _)| key == &name).collect();
            assert_eq!(actual.len(), 1);
            assert_eq!(actual[0].1, expected);
        }
        let body = body();
        assert_eq!(
            body["client_metadata"]["thread_id"],
            "11111111-2222-5333-8444-555555555555"
        );
        assert_eq!(body["client_metadata"]["session_id"], "session-fixture");
        assert_eq!(
            body["client_metadata"]["x-codex-installation-id"],
            "aaaaaaaa-bbbb-5ccc-8ddd-eeeeeeeeeeee"
        );
        assert_eq!(
            body["client_metadata"]["x-codex-window-id"],
            "12345678-1234-4234-8234-123456789abc"
        );
        assert_eq!(body["client_metadata"]["turn_id"], "run-fixture");
        let nested: Value = serde_json::from_str(
            body["client_metadata"]["x-codex-turn-metadata"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            nested,
            json!({"installation_id":wire.installation_id,"session_id":wire.session_id,"thread_id":wire.thread_id,"turn_id":wire.turn_id,"window_id":wire.window_id,"request_kind":"turn","turn_started_at_unix_ms":1234567890000_u64})
        );
    }
    #[test]
    fn routing_order_does_not_change_canonical_rendering() {
        let (frozen, opts) = fixture();
        let body = body();
        let canonical = OpenAiResponsesChatgpt.render(&frozen, &opts);
        assert_eq!(canonical, broca_protocol::canonical_json_bytes(&body));
        assert!(canonical.starts_with(b"{\"client_metadata\":"));
        assert!(request_bytes(&canonical)
            .unwrap()
            .starts_with(b"{\"model\":\"gpt-5.3-codex\",\"stream\":true,\"input\":"));
        let mut frame = body;
        frame["type"] = json!("response.create");
        assert!(request_bytes(&broca_protocol::canonical_json_bytes(&frame))
            .unwrap()
            .starts_with(b"{\"type\":\"response.create\",\"model\":"));
    }
}
