use broca_protocol::{
    tool_addition::{AdditionPart, ToolAddition, ToolAdditionForm},
    CallOptions,
};

use crate::{spec::WireFamilyId, wire::FrozenRenderConfig};

/// The native form of a resolved route. Claude takes inline definitions only if its catalog
/// declares mid-conversation system support. Other forms use the wire family and provider flags.
pub fn frozen_form(frozen: &FrozenRenderConfig, provider: &str) -> Option<ToolAdditionForm> {
    use ToolAdditionForm::*;
    match (frozen.family_id, provider) {
        (WireFamilyId::OpenAiResponses, "openai") => Some(ResponsesAdditionalTools),
        (WireFamilyId::OpenAiChat, "kimi-code-plan-global") if frozen.moonshot_schema_types => {
            Some(KimiSystemTools)
        }
        (WireFamilyId::AnthropicMessages, "anthropic") if super::supported(frozen) => {
            Some(AnthropicToolAddition)
        }
        _ => None,
    }
}

pub fn carries_form(frozen: &FrozenRenderConfig, provider: &str, native: Option<&str>) -> bool {
    match native {
        Some(form) => frozen_form(frozen, provider).is_some_and(|known| known.as_str() == form),
        None => super::supported(frozen),
    }
}

/// A retry cannot apply a different plan, so its route must support the same note forms.
pub fn route_carries(frozen: &FrozenRenderConfig, provider: &str, opts: &CallOptions) -> bool {
    opts.prompt.iter().all(|message| {
        if let Some(addition) = &message.tool_addition {
            carries_form(frozen, provider, Some(addition.form.as_str()))
        } else {
            !message.tags.plan_note || carries_form(frozen, provider, None)
        }
    })
}

use broca_protocol::wire_node::Node;

use crate::tool_name_alias::ToolNameMap;

pub(crate) fn render<'a>(addition: &'a ToolAddition, names: &ToolNameMap) -> Node<'a> {
    use ToolAdditionForm::*;
    let parts = addition
        .parts
        .iter()
        .map(|part| {
            let tool = match part {
                AdditionPart::Tool(tool) => tool,
                AdditionPart::Text(text) => {
                    return Node::object([
                        ("type", Node::str("text")),
                        ("text", Node::str(text.as_str())),
                    ])
                }
            };
            let name = Node::string(names.wire_name(&tool.name).to_owned());
            let description = Node::str(&tool.description);
            let schema = if addition.form == KimiSystemTools {
                let mut schema = tool.input_schema.parse();
                crate::tool_schema_enum_types::add_enum_types(&mut schema);
                Node::value(schema)
            } else {
                Node::Json(&tool.input_schema)
            };
            match addition.form {
                ResponsesAdditionalTools => Node::object([
                    ("type", Node::str("function")),
                    ("name", name),
                    ("description", description),
                    ("parameters", schema),
                ]),
                KimiSystemTools => Node::object([
                    ("type", Node::str("function")),
                    (
                        "function",
                        Node::object([
                            ("name", name),
                            ("description", description),
                            ("parameters", schema),
                        ]),
                    ),
                ]),
                AnthropicToolAddition => Node::object([
                    ("type", Node::str("tool_addition")),
                    (
                        "tool",
                        Node::object([
                            ("type", Node::str("tool_definition")),
                            (
                                "definition",
                                Node::object([
                                    ("name", name),
                                    ("description", description),
                                    ("input_schema", schema),
                                ]),
                            ),
                        ]),
                    ),
                ]),
            }
        })
        .collect();
    match addition.form {
        ResponsesAdditionalTools => Node::object([
            ("type", Node::str("additional_tools")),
            ("role", Node::str("developer")),
            ("tools", Node::Array(parts)),
        ]),
        KimiSystemTools => {
            Node::object([("role", Node::str("system")), ("tools", Node::Array(parts))])
        }
        AnthropicToolAddition => Node::object([
            ("role", Node::str("system")),
            ("content", Node::Array(parts)),
        ]),
    }
}

pub(crate) const BETA: &str = "inline-tools-2026-09-15";

#[cfg(feature = "live")]
pub(crate) fn renders_native(opts: &CallOptions) -> bool {
    opts.prompt
        .iter()
        .any(|message| message.tool_addition.is_some())
}

#[cfg(feature = "live")]
pub(crate) fn has_beta(headers: &[(String, String)]) -> bool {
    headers.iter().any(|(name, value)| {
        name.eq_ignore_ascii_case("anthropic-beta")
            && value.split(',').any(|beta| beta.trim() == BETA)
    })
}

/// Cache warms can start from persisted body bytes, without a live request view.
#[cfg(feature = "live")]
pub(crate) fn warm_has_native(body: &[u8]) -> bool {
    if memchr::memmem::find(body, b"\"tool_addition\"").is_none() {
        return false;
    }
    #[derive(serde::Deserialize)]
    struct HeaderHint {
        messages: Vec<serde_json::Value>,
    }
    serde_json::from_slice::<HeaderHint>(body).is_ok_and(|hint| {
        hint.messages.iter().any(|message| {
            message["role"] == "system"
                && message["content"]
                    .as_array()
                    .is_some_and(|parts| parts.iter().any(|part| part["type"] == "tool_addition"))
        })
    })
}

/// Add the Anthropic beta when the request view emits an inline declaration. Authentication
/// retries carry this header across without inspecting the body again.
pub(crate) fn add_anthropic_beta(request: &mut crate::wire::HttpRequest, inline: bool) {
    if inline {
        if let Some((_, value)) = request
            .headers
            .iter_mut()
            .find(|(name, _)| name == "anthropic-beta")
        {
            if !value.split(',').any(|beta| beta.trim() == BETA) {
                value.push(',');
                value.push_str(BETA);
            }
        } else {
            request.headers.push(("anthropic-beta".into(), BETA.into()));
        }
    }
}
