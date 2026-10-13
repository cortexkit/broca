//! `lift` (owned canonical → CK) and `project` (CK → owned canonical).
//!
//! broca is the producer: it `lift`s its canonical prompt into a `CkMessage` array,
//! an external transform reduces it, and broca `project`s the result back to canonical
//! `Message`s and renders them. The load-bearing contract is RENDER byte-identity: for
//! untransformed content, `render(project(lift(prompt)))` MUST equal `render(prompt)` — else
//! merely WIRING the transform (zero reductions) would bust the prompt cache. The
//! pass-through identity conformance test pins exactly this across the render goldens.
//!
//! The round-trip is RENDER-lossless, not necessarily TYPE-lossless: the owned canonical has
//! two ways to express an error string (a `Text` output with `is_error` set, and a distinct
//! `Error` output) that render to identical bytes, so both lift to CK `ErrorText` and project
//! back to one canonical form — different type, identical render. That is exactly what the
//! byte-identity contract permits.
//!
//! The owned canonical carries no `Opaque` or `Media` blocks, so `lift` never produces them.
//! `project` FAILS LOUD on an `Opaque`/`Media`/nested-`Content`/`ErrorContent` block rather
//! than dropping a lossy projection: the owned v1 render has no path for those, and silently
//! discarding one would corrupt the conversation. This intentionally exposes unsupported blocks;
//! an intermediary that forwards provider content must preserve `Opaque` data without loss.

use std::collections::BTreeMap;

use broca_protocol::content::{
    ContentBlock as CanonBlock, Message as CanonMessage, Role as CanonRole,
    ToolCall as CanonToolCall, ToolOutput as CanonToolOutput, ToolResult as CanonToolResult,
};
use broca_protocol::request::ModelHandle;

use crate::types::{
    CkMessage, ContentBlock, ContentKind, MessageOrigin, ProviderExtras, ToolResultOutput,
    ToolResultOutputKind,
};

/// A block that the owned v1 render cannot emit was found while projecting a transformed
/// array back to the owned canonical. Fail loud rather than drop it (see the module docs).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectError(pub String);

impl std::fmt::Display for ProjectError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "cannot project CK block to owned canonical: {}", self.0)
    }
}

impl std::error::Error for ProjectError {}

/// Lift an owned canonical prompt into a `CkMessage` array. A `ToolResult` carries no tool
/// name in the owned canonical, so lift recovers it from the immediately preceding assistant
/// message. The lookup remains valid across that assistant message's contiguous tool-result
/// group and is replaced or cleared at the next non-tool boundary.
///
/// `MessageOrigin.api` is the wire family of the producing `(provider, model)`. The owned
/// `ModelHandle` does not carry it, and it is NOT render-load-bearing (the render reads the
/// projected canonical `origin`, never CK's `api`; the transform's same-model check keys on
/// `(provider, model)`, which deterministically imply the family). `api_for` supplies it from
/// the producer's frozen family knowledge; pass a provider-derived stand-in when the exact
/// family string is unavailable — consistent population keeps the same-model check correct.
pub fn lift(prompt: &[CanonMessage], api_for: &dyn Fn(&ModelHandle) -> String) -> Vec<CkMessage> {
    let mut current_tool_call_names = BTreeMap::new();
    let mut out = Vec::with_capacity(prompt.len());

    for message in prompt {
        match message.role {
            CanonRole::Assistant => {
                current_tool_call_names = tool_call_names_in_message(message);
            }
            CanonRole::Tool => {}
            CanonRole::System | CanonRole::User => current_tool_call_names.clear(),
        }
        out.push(lift_message(message, &current_tool_call_names, api_for));
    }

    out
}

/// Project a (possibly transformed) `CkMessage` array back to the owned canonical prompt.
/// Fails loud on any block the owned v1 render cannot emit.
pub fn project(ck: &[CkMessage]) -> Result<Vec<CanonMessage>, ProjectError> {
    ck.iter()
        .map(|m| {
            let content = m
                .content
                .iter()
                .map(project_block)
                .collect::<Result<Vec<_>, _>>()?;
            Ok(CanonMessage {
                role: m.role,
                content,
                cache_prefix_blocks: None,
                origin: m.origin.as_ref().map(|o| ModelHandle {
                    provider_module_id: o.provider.clone(),
                    model_id: o.model.clone(),
                }),
                // Ordinal is a render-invisible reduction key, re-derived by position when the
                // projected prompt is next finalized — never carried through the CK round-trip.
                ordinal: None,
            })
        })
        .collect()
}

fn tool_call_names_in_message(message: &CanonMessage) -> BTreeMap<String, String> {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            CanonBlock::ToolCall(call) => {
                Some((call.tool_call_id.0.clone(), call.tool_name.clone()))
            }
            _ => None,
        })
        .collect()
}

fn lift_message(
    message: &CanonMessage,
    names: &BTreeMap<String, String>,
    api_for: &dyn Fn(&ModelHandle) -> String,
) -> CkMessage {
    CkMessage {
        role: message.role,
        content: message
            .content
            .iter()
            .map(|block| lift_block(block, names))
            .collect(),
        origin: message.origin.as_ref().map(|handle| MessageOrigin {
            provider: handle.provider_module_id.clone(),
            model: handle.model_id.clone(),
            api: api_for(handle),
        }),
        provider_extras: ProviderExtras::new(),
        meta: Default::default(),
    }
}

fn lift_block(b: &CanonBlock, names: &BTreeMap<String, String>) -> ContentBlock {
    let kind = match b {
        CanonBlock::Text { text } => ContentKind::Text { text: text.clone() },
        CanonBlock::Reasoning { text, signature } => ContentKind::Reasoning {
            text: text.clone(),
            signature: signature.clone(),
        },
        CanonBlock::RedactedReasoning {
            data,
            provider_format,
        } => ContentKind::RedactedReasoning {
            data: data.clone(),
            provider_format: *provider_format,
        },
        CanonBlock::ToolCall(tc) => ContentKind::ToolCall {
            id: tc.tool_call_id.0.clone(),
            name: tc.tool_name.clone(),
            input: tc.input.clone(),
            thought_signature: tc.thought_signature.clone(),
            // The owned loop dispatches every tool via the host tool plane (client-executed),
            // so a tool call is never provider-executed on this leg.
            provider_executed: false,
        },
        CanonBlock::ToolResult(tr) => ContentKind::ToolResult {
            id: tr.tool_call_id.0.clone(),
            tool_name: names.get(&tr.tool_call_id.0).cloned().unwrap_or_default(),
            output: lift_output(&tr.output, tr.is_error),
            provider_executed: false,
        },
    };
    ContentBlock::bare(kind)
}

/// Lift a canonical tool output + its `is_error` flag into the CK typed output shape.
fn lift_output(out: &CanonToolOutput, is_error: bool) -> ToolResultOutput {
    let kind = match (out, is_error) {
        (CanonToolOutput::Text { text }, false) => {
            ToolResultOutputKind::Text { text: text.clone() }
        }
        (CanonToolOutput::Text { text }, true) => {
            ToolResultOutputKind::ErrorText { text: text.clone() }
        }
        (CanonToolOutput::Json { value }, false) => ToolResultOutputKind::Json {
            value: value.clone(),
        },
        (CanonToolOutput::Json { value }, true) => ToolResultOutputKind::ErrorJson {
            value: value.clone(),
        },
        // An `Error` output is an error string regardless of the (always-true) flag.
        (CanonToolOutput::Error { message }, _) => ToolResultOutputKind::ErrorText {
            text: message.clone(),
        },
        (CanonToolOutput::Denied { reason }, _) => ToolResultOutputKind::ExecutionDenied {
            reason: Some(reason.clone()),
        },
    };
    ToolResultOutput::bare(kind)
}

fn project_block(b: &ContentBlock) -> Result<CanonBlock, ProjectError> {
    match &b.kind {
        ContentKind::Text { text } => Ok(CanonBlock::Text { text: text.clone() }),
        ContentKind::Reasoning { text, signature } => Ok(CanonBlock::Reasoning {
            text: text.clone(),
            signature: signature.clone(),
        }),
        ContentKind::RedactedReasoning {
            data,
            provider_format,
        } => Ok(CanonBlock::RedactedReasoning {
            data: data.clone(),
            provider_format: *provider_format,
        }),
        ContentKind::ToolCall {
            id,
            name,
            input,
            thought_signature,
            ..
        } => Ok(CanonBlock::ToolCall(CanonToolCall {
            tool_call_id: broca_protocol::ToolCallId(id.clone()),
            tool_name: name.clone(),
            input: input.clone(),
            thought_signature: thought_signature.clone(),
        })),
        ContentKind::ToolResult { id, output, .. } => {
            let (out, is_error) = project_output(output)?;
            Ok(CanonBlock::ToolResult(CanonToolResult {
                tool_call_id: broca_protocol::ToolCallId(id.clone()),
                output: out,
                is_error,
            }))
        }
        ContentKind::Media(_) => Err(ProjectError(
            "a Media block has no owned-canonical form in v1 (owned render supports text/tool \
             blocks only)"
                .into(),
        )),
        ContentKind::Opaque(o) => Err(ProjectError(format!(
            "an Opaque block ({kind:?}) has no owned-canonical form in v1 — full Opaque carriage \
             is the MITM leg's prerequisite",
            kind = o.kind
        ))),
    }
}

/// Project a CK typed output back to a canonical output + `is_error` flag. The
/// error-string variants collapse to the canonical `Error` output (they render identically
/// to a `Text` output with `is_error` set — the render-lossless-not-type-lossless point).
fn project_output(o: &ToolResultOutput) -> Result<(CanonToolOutput, bool), ProjectError> {
    Ok(match &o.kind {
        ToolResultOutputKind::Text { text } => {
            (CanonToolOutput::Text { text: text.clone() }, false)
        }
        ToolResultOutputKind::Json { value } => (
            CanonToolOutput::Json {
                value: value.clone(),
            },
            false,
        ),
        ToolResultOutputKind::ErrorText { text } => (
            CanonToolOutput::Error {
                message: text.clone(),
            },
            true,
        ),
        ToolResultOutputKind::ErrorJson { value } => (
            CanonToolOutput::Json {
                value: value.clone(),
            },
            true,
        ),
        ToolResultOutputKind::ExecutionDenied { reason } => (
            CanonToolOutput::Denied {
                reason: reason.clone().unwrap_or_default(),
            },
            true,
        ),
        ToolResultOutputKind::Content { .. } | ToolResultOutputKind::ErrorContent { .. } => {
            return Err(ProjectError(
                "a nested Content/ErrorContent tool output has no owned-canonical form in v1 \
                 (owned tool results are a single text/json/denied/error output)"
                    .into(),
            ))
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{
        CkMessage, ContentBlock as CkBlock, ContentKind, MediaBlock, MediaKind, MediaSource,
        OpaqueBlock, OpaqueSource, Role,
    };
    use broca_protocol::content::{
        ContentBlock, EncryptedReasoningFormat, Message, ToolOutput, ToolResult,
    };
    use broca_protocol::ToolCallId;

    fn ck_msg(role: Role, kind: ContentKind) -> CkMessage {
        CkMessage {
            role,
            content: vec![CkBlock::bare(kind)],
            origin: None,
            provider_extras: Default::default(),
            meta: Default::default(),
        }
    }

    fn assistant_tool_call(id: &str, name: &str) -> Message {
        Message::assistant_from(
            vec![ContentBlock::ToolCall(broca_protocol::content::ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId(id.into()),
                tool_name: name.into(),
                input: serde_json::json!({}),
            })],
            ModelHandle::new("anthropic", "m"),
        )
    }

    fn tool_result(id: &str, text: &str) -> Message {
        Message::new(
            CanonRole::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId(id.into()),
                output: ToolOutput::Text { text: text.into() },
                is_error: false,
            })],
        )
    }

    fn lifted_tool_result_names(messages: &[CkMessage]) -> Vec<String> {
        messages
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|block| match &block.kind {
                ContentKind::ToolResult { tool_name, .. } => Some(tool_name.clone()),
                _ => None,
            })
            .collect()
    }

    fn legacy_prompt_scoped_lift(
        prompt: &[Message],
        api_for: &dyn Fn(&ModelHandle) -> String,
    ) -> Vec<CkMessage> {
        let names = prompt
            .iter()
            .flat_map(|message| &message.content)
            .filter_map(|block| match block {
                ContentBlock::ToolCall(call) => {
                    Some((call.tool_call_id.0.clone(), call.tool_name.clone()))
                }
                _ => None,
            })
            .collect();
        prompt
            .iter()
            .map(|message| lift_message(message, &names, api_for))
            .collect()
    }

    #[test]
    fn project_fails_loud_on_opaque() {
        let ck = vec![ck_msg(
            Role::Assistant,
            ContentKind::Opaque(OpaqueBlock {
                source: OpaqueSource::Wire {
                    wire: "anthropic".into(),
                },
                kind: "server_tool_use".into(),
                raw: b"{}".to_vec(),
                arc: None,
            }),
        )];
        let err = project(&ck).expect_err("an Opaque block must fail projection in v1");
        assert!(
            err.0.contains("Opaque"),
            "error names the offending kind: {err}"
        );
    }

    #[test]
    fn project_fails_loud_on_media() {
        let ck = vec![ck_msg(
            Role::User,
            ContentKind::Media(MediaBlock {
                kind: MediaKind::Image,
                media_type: "image/png".into(),
                filename: None,
                source: MediaSource::Url {
                    url: "https://x/y.png".into(),
                },
            }),
        )];
        let err = project(&ck).expect_err("a Media block must fail projection in v1");
        assert!(err.0.contains("Media"), "error names Media: {err}");
    }

    #[test]
    fn project_fails_loud_on_error_content_without_an_owned_form() {
        let ck = vec![ck_msg(
            Role::Tool,
            ContentKind::ToolResult {
                id: "c1".into(),
                tool_name: "read".into(),
                output: ToolResultOutput::bare(ToolResultOutputKind::ErrorContent {
                    blocks: Vec::new(),
                }),
                provider_executed: false,
            },
        )];
        let err = project(&ck).expect_err("ErrorContent cannot project to owned v1");
        assert!(
            err.0.contains("ErrorContent"),
            "error names the missing owned form: {err}"
        );
    }

    #[test]
    fn lift_recovers_tool_name_from_the_paired_call() {
        // A tool RESULT carries no name in the owned canonical; lift recovers it from the
        // matching call in the immediately preceding assistant turn.
        let prompt = vec![
            Message::assistant_from(
                vec![ContentBlock::ToolCall(broca_protocol::content::ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId("c1".into()),
                    tool_name: "read".into(),
                    input: serde_json::json!({}),
                })],
                ModelHandle::new("anthropic", "m"),
            ),
            Message::new(
                CanonRole::Tool,
                vec![ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("c1".into()),
                    output: ToolOutput::Text { text: "ok".into() },
                    is_error: false,
                })],
            ),
        ];
        let ck = lift(&prompt, &|_| "test-api".into());
        // The tool-role message's result carries the recovered name.
        let result_msg = &ck[1];
        match &result_msg.content[0].kind {
            ContentKind::ToolResult { tool_name, .. } => assert_eq!(tool_name, "read"),
            other => panic!("expected a ToolResult, got {other:?}"),
        }
    }

    #[test]
    fn repeated_tool_call_ids_resolve_within_their_own_turns() {
        let prompt = vec![
            assistant_tool_call("call_0", "search"),
            tool_result("call_0", "search-result"),
            assistant_tool_call("call_0", "read"),
            tool_result("call_0", "read-result"),
        ];

        let lifted = lift(&prompt, &|_| "test-api".into());
        assert_eq!(lifted_tool_result_names(&lifted), ["search", "read"]);

        let legacy = legacy_prompt_scoped_lift(&prompt, &|_| "test-api".into());
        assert_eq!(
            lifted_tool_result_names(&legacy),
            ["read", "read"],
            "a prompt-global last-write-wins map renames the first result"
        );
    }

    #[test]
    fn distinct_tool_call_ids_keep_byte_identical_lift_output() {
        let prompt = vec![
            assistant_tool_call("call_0", "search"),
            tool_result("call_0", "search-result"),
            assistant_tool_call("call_1", "read"),
            tool_result("call_1", "read-result"),
        ];

        let lifted = lift(&prompt, &|_| "test-api".into());
        let legacy = legacy_prompt_scoped_lift(&prompt, &|_| "test-api".into());
        assert_eq!(
            serde_json::to_vec(&lifted).unwrap(),
            serde_json::to_vec(&legacy).unwrap(),
            "narrowing the lookup scope must not change non-colliding lift bytes"
        );
    }

    #[test]
    fn tool_call_thought_signature_survives_lift_and_project() {
        let prompt = vec![Message::assistant_from(
            vec![ContentBlock::ToolCall(broca_protocol::content::ToolCall {
                tool_call_id: ToolCallId("c1".into()),
                tool_name: "read".into(),
                input: serde_json::json!({"path": "x"}),
                thought_signature: Some("gemini-signed-call".into()),
            })],
            ModelHandle::new("google", "gemini-3.5-flash-low"),
        )];

        let ck = lift(&prompt, &|_| "gemini".into());
        let projected = project(&ck).expect("signed tool call projects");

        assert_eq!(projected, prompt);
    }

    #[test]
    fn encrypted_reasoning_format_survives_lift_and_project() {
        let prompt = vec![Message::assistant_from(
            vec![ContentBlock::RedactedReasoning {
                data: r#"{"type":"reasoning","encrypted_content":"cipher"}"#.into(),
                provider_format: Some(EncryptedReasoningFormat::OpenAiResponses),
            }],
            ModelHandle::new("openai", "gpt-5.5"),
        )];

        let ck = lift(&prompt, &|_| "openai_responses_chatgpt".into());
        let projected = project(&ck).expect("encrypted reasoning projects");

        assert_eq!(projected, prompt);
    }

    #[test]
    fn owned_tool_calls_are_never_provider_executed() {
        let prompt = vec![Message::assistant_from(
            vec![ContentBlock::ToolCall(broca_protocol::content::ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("c1".into()),
                tool_name: "read".into(),
                input: serde_json::json!({}),
            })],
            ModelHandle::new("anthropic", "m"),
        )];
        let ck = lift(&prompt, &|_| "test-api".into());
        match &ck[0].content[0].kind {
            ContentKind::ToolCall {
                provider_executed, ..
            } => assert!(!provider_executed, "owned loop is client-executed"),
            other => panic!("expected a ToolCall, got {other:?}"),
        }
    }
}
