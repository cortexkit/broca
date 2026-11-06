//! The rules that start an episode under a session's fetch plan. They live apart from the session
//! actor (`crate::actor::LineageActor`), which applies them on every path that starts an episode,
//! so any other actor that starts episodes under a plan freezes it the same way.

use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::request::SubmissionRequest;
use broca_protocol::SessionScope;
use broca_wal::PlanManifest;

/// The refusal for a send carrying `tools` into a session whose first episode froze a fetch plan:
/// the plan owns the session's tools for its whole life, so the send is refused before any retry,
/// queue, steer or interrupt path could ignore the tools or freeze them.
pub(crate) fn tools_frozen_refusal() -> crate::actor::SendOutcome {
    crate::actor::SendOutcome::DeliveryRefused {
        code: "invalid_params",
        field: "tools",
        message: "tools are frozen by the session's fetch plan".into(),
    }
}

/// Refuses an episode whose plan asks a provider to confine its tools
/// (`broca_fetch_plan::CONFINED_TOOL_PRESET`) when the episode carries no scope, since the
/// provider keys that confinement on the scope. Every path that starts an episode checks this.
pub(crate) fn require_confining_scope(
    manifest: Option<&PlanManifest>,
    scope: Option<&SessionScope>,
) -> Result<(), broca_core::ScopeRefusal> {
    let confines = manifest.is_some_and(|manifest| {
        serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text())
            .is_ok_and(|plan| broca_fetch_plan::names_confined_preset(&plan))
    });
    if scope.is_none() && confines {
        return Err(broca_core::ScopeRefusal::ScopeRequired {
            preset: broca_fetch_plan::CONFINED_TOOL_PRESET,
        });
    }
    Ok(())
}

/// Gives a starting episode's `request` the tools `manifest` froze, and, in the session's first
/// episode (`first_episode_input`), the manifest's system text as its first message.
///
/// A send carries neither tools nor system text: the manifest supplies both. An episode re-driven
/// from its own start carries the manifest's tools and, in the first episode, the manifest's
/// system text first; exactly those are accepted. Anything else is refused. `tools_were_omitted`
/// says the send itself named no tools, before anything filled them in.
pub(crate) fn freeze_manifest(
    manifest: &PlanManifest,
    request: &mut SubmissionRequest,
    first_episode_input: bool,
    tools_were_omitted: bool,
) -> Result<(), broca_core::RunError> {
    let is_system = |message: &&Message| message.role == Role::System;
    let frozen_system = vec![ContentBlock::Text {
        text: manifest.system_text.clone(),
    }];
    let systems: Vec<_> = request.input.iter().filter(is_system).collect();
    let system_frozen = match systems.as_slice() {
        [] => true,
        [only] => {
            first_episode_input
                && std::ptr::eq(*only, &request.input[0])
                && only.content == frozen_system
        }
        _ => false,
    };
    let tools_frozen =
        tools_were_omitted || request.tools.as_deref() == Some(manifest.tools.as_slice());
    if !system_frozen || !tools_frozen {
        return Err(broca_core::RunError::Config(
            "this session's tools and system text are frozen from its fetch plan; a later send \
             carries neither `tools` nor `system`"
                .into(),
        ));
    }
    let needs_system =
        first_episode_input && systems.is_empty() && !manifest.system_text.is_empty();
    request.tools = Some(manifest.tools.as_ref().clone());
    request.policy_allowed = manifest
        .tools
        .iter()
        .map(|tool| tool.name.clone())
        .collect();
    if needs_system {
        request
            .input
            .insert(0, Message::new(Role::System, frozen_system));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use broca_protocol::content::{ContentBlock, Message, Role};
    use broca_protocol::request::{SubmissionRequest, Tool};
    use broca_wal::{OpaqueJson, PlanManifest};
    use serde_json::json;

    use super::{freeze_manifest, require_confining_scope};

    fn tool(name: &str) -> Tool {
        serde_json::from_value(json!({
            "name": name,
            "description": "a tool",
            "input_schema": {"type": "object"},
        }))
        .unwrap()
    }

    fn manifest(plan: serde_json::Value, system_text: &str) -> PlanManifest {
        PlanManifest {
            rich_ui: None,
            plan: OpaqueJson::from_json_text(plan.to_string()).unwrap(),
            plan_identity: "plan".into(),
            composition_digest: "composition".into(),
            tools: Arc::new(vec![tool("read"), tool("edit")]),
            system_text: system_text.into(),
            system_digest: "system".into(),
            join_version: 1,
            role_versions: Default::default(),
            fetches: vec![],
            declarations: vec![],
        }
    }

    fn request(input: Vec<Message>, tools: Option<Vec<Tool>>) -> SubmissionRequest {
        let mut request: SubmissionRequest = serde_json::from_value(json!({
            "model": {"provider_module_id": "stub", "model_id": "m"},
            "input": [],
        }))
        .unwrap();
        request.input = input;
        request.tools = tools;
        request
    }

    fn user(text: &str) -> Message {
        Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
    }

    fn system(text: &str) -> Message {
        Message::new(Role::System, vec![ContentBlock::Text { text: text.into() }])
    }

    #[test]
    fn a_confining_plan_needs_a_scope() {
        let confining = manifest(json!({"tool_items": [{"preset": "worker"}]}), "");
        let scope = broca_protocol::SessionScope {
            owner: broca_protocol::BindPrincipal::Reserved {
                module_id: "prefrontal-core".into(),
            },
            agent_id_frozen: false,
            scope_ref: "head-1".into(),
            scope_epoch: 1,
            flow_id: None,
            sender: None,
            agent_id: None,
            run_id: None,
        };
        assert!(matches!(
            require_confining_scope(Some(&confining), None),
            Err(broca_core::ScopeRefusal::ScopeRequired { preset: "worker" })
        ));
        assert!(require_confining_scope(Some(&confining), Some(&scope)).is_ok());
        let open = manifest(json!({"tool_items": [{"preset": "head"}]}), "");
        assert!(require_confining_scope(Some(&open), None).is_ok());
        assert!(require_confining_scope(None, None).is_ok());
    }

    /// The first episode of a planned session gets the manifest's tools and, first in its
    /// input, the manifest's system text.
    #[test]
    fn the_first_episode_gets_the_manifests_tools_and_system_text() {
        let frozen = manifest(json!({}), "you are a worker");
        let mut first = request(vec![user("go")], None);
        freeze_manifest(&frozen, &mut first, true, true).unwrap();
        assert_eq!(first.input, [system("you are a worker"), user("go")]);
        assert_eq!(first.tools.as_deref(), Some(frozen.tools.as_slice()));
        let allowed: std::collections::BTreeSet<&str> =
            first.policy_allowed.iter().map(String::as_str).collect();
        assert_eq!(allowed, ["edit", "read"].into());
        // A later episode gets the tools and no system text.
        let mut later = request(vec![user("again")], None);
        freeze_manifest(&frozen, &mut later, false, true).unwrap();
        assert_eq!(later.input, [user("again")]);
        assert_eq!(later.tools.as_deref(), Some(frozen.tools.as_slice()));
    }

    /// The first episode re-driven from its own start carries exactly the manifest's system
    /// text first and its tools, and is accepted unchanged.
    #[test]
    fn a_re_driven_first_episode_keeps_its_frozen_input() {
        let frozen = manifest(json!({}), "you are a worker");
        let input = vec![system("you are a worker"), user("go")];
        let mut redriven = request(input.clone(), Some(frozen.tools.as_ref().clone()));
        freeze_manifest(&frozen, &mut redriven, true, false).unwrap();
        assert_eq!(redriven.input, input);
    }

    /// A send carrying its own system text or other tools into a planned session is refused.
    #[test]
    fn a_send_carrying_its_own_system_text_or_tools_is_refused() {
        let frozen = manifest(json!({}), "you are a worker");
        for (input, tools, first) in [
            (vec![system("another"), user("go")], None, true),
            (vec![system("you are a worker"), user("go")], None, false),
            (vec![user("go"), system("you are a worker")], None, true),
            (vec![user("go")], Some(vec![tool("read")]), true),
        ] {
            let tools_were_omitted = tools.is_none();
            let mut send = request(input, tools);
            assert!(
                freeze_manifest(&frozen, &mut send, first, tools_were_omitted).is_err(),
                "{:?}",
                send.input
            );
        }
    }
}
