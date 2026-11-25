use broca_module_serve::{cache_routing::default_cache_policy, claude_code_identity, state_root};
use broca_module_serve::{AuthMethod, CredentialId};
use broca_protocol::{request::Tool, BindIdentity};
use broca_provider::{
    claude_code_persona::{sign_body_in_place, BILLING_HEADER_PREFIX},
    families::{anthropic_apikey_frozen_config, anthropic_frozen_config},
    wire::{AuthHandle, ClaudeCodePersona, FrozenRenderConfig, HttpRequest},
};
use serde_json::{json, value::RawValue, Value};

const INLINE_TOOLS_BETA: &str = "inline-tools-2026-09-15";

pub(super) fn frozen(model: &str, persona: Option<ClaudeCodePersona>) -> FrozenRenderConfig {
    let mut frozen = if persona.is_some() {
        anthropic_frozen_config(model)
    } else {
        anthropic_apikey_frozen_config(model)
    };
    frozen.base_url = Some("https://api.anthropic.com".into());
    frozen.cache = default_cache_policy(frozen.family_id, "anthropic");
    frozen.claude_code_persona = persona;
    frozen
}

pub(super) fn method(id: &CredentialId) -> Result<AuthMethod, &'static str> {
    if id.provider == "anthropic" && matches!(id.method, AuthMethod::ApiKey | AuthMethod::Oauth) {
        Ok(id.method)
    } else {
        Err("--provider anthropic requires an apikey:anthropic or oauth:anthropic credential")
    }
}

pub(super) fn auth(method: AuthMethod, token: String) -> AuthHandle {
    match method {
        AuthMethod::Oauth => AuthHandle::Bearer(token),
        AuthMethod::ApiKey => AuthHandle::ApiKeyHeader {
            header: "x-api-key".into(),
            value: token,
        },
        _ => unreachable!("validated Anthropic credential method"),
    }
}

pub(super) fn persona(
    id: &CredentialId,
    account: Option<&str>,
    bind: &BindIdentity,
) -> Result<ClaudeCodePersona, &'static str> {
    let root = state_root::resolve_state_root().map_err(|_| "state root unavailable")?;
    let install = claude_code_identity::load_or_create_install_id(&root)
        .map_err(|_| "installation identity unavailable")?;
    claude_code_identity::fresh_persona(&install, account, &id.to_id_string(), bind, None, 1)
        .map_err(|_| "Claude Code persona unavailable")
}

/// The system message that adds `tool`; with `grouped`, a text note comes first in the same
/// message, the shape broca sends when a plan note and an added tool share one turn.
pub(super) fn tool_addition(tool: &Tool, grouped: bool) -> Value {
    let addition = json!({"type":"tool_addition", "tool":{
        "type":"tool_definition", "definition":{
            "name":tool.name, "description":tool.description, "input_schema":tool.input_schema,
        }
    }});
    let content = if grouped {
        // The text a plan note would carry for this change.
        let note = format!(
            "Your tools changed: a new tool, {}, is now available.",
            tool.name
        );
        json!([{"type":"text", "text":note}, addition])
    } else {
        json!([addition])
    };
    json!({"role":"system", "content":content})
}

pub(super) fn append_tool(body: Vec<u8>, tool: &Tool, signed: bool, grouped: bool) -> Vec<u8> {
    #[derive(serde::Deserialize)]
    struct Transcript<'a> {
        #[serde(borrow)]
        messages: &'a RawValue,
    }

    // Find the closing bracket of the rendered array without reserializing any existing bytes.
    // Re-sign a Claude subscription body because its billing signature covers the appended item.
    let mut body = String::from_utf8(body).expect("Anthropic family renders UTF-8");
    let transcript: Transcript<'_> = serde_json::from_str(&body).expect("Anthropic messages");
    let raw = transcript.messages.get();
    let end = raw.as_ptr() as usize - body.as_ptr() as usize + raw.len() - 1;
    assert_eq!(body.as_bytes()[end], b']');
    body.insert_str(end, &format!(",{}", tool_addition(tool, grouped)));
    if signed {
        sign_body_in_place(&mut body);
    }
    body.into_bytes()
}

pub(super) fn add_beta(request: &mut HttpRequest) {
    if let Some((_, betas)) = request
        .headers
        .iter_mut()
        .find(|(key, _)| key.eq_ignore_ascii_case("anthropic-beta"))
    {
        if !betas
            .split(',')
            .any(|beta| beta.trim() == INLINE_TOOLS_BETA)
        {
            if !betas.is_empty() {
                betas.push(',');
            }
            betas.push_str(INLINE_TOOLS_BETA);
        }
    } else {
        request
            .headers
            .push(("anthropic-beta".into(), INLINE_TOOLS_BETA.into()));
    }
}

pub(super) fn redact_persona(body: &mut Value) {
    if let Some(object) = body.as_object_mut() {
        object.remove("metadata");
    }
    if let Some(system) = body.get_mut("system").and_then(Value::as_array_mut) {
        system.retain(|block| {
            !block["text"]
                .as_str()
                .is_some_and(|text| text.starts_with(BILLING_HEADER_PREFIX))
        });
    }
}
