//! A run's identity inside its frozen render config: the members whose values belong to one run
//! or one session, such as the prompt cache key or a Claude Code persona's prompt id.
//!
//! A run start stores its render config as a setup part, which every run with the same bytes
//! shares, and keeps these members in its own record (`RunStart::render_identity`). Splitting
//! them out is what lets runs on one route share a part: left in, every run of a Claude
//! subscription, ChatGPT login or Code Assist route would store a part of its own.

use serde_json::{Map, Value};

/// The top-level members of a [`FrozenRenderConfig`](crate::wire::FrozenRenderConfig) whose
/// values depend on the run or its session. A new member that does belongs here too, or every
/// run stores its own render config part again.
pub const RENDER_IDENTITY_MEMBERS: [&str; 4] = [
    "prompt_cache_key",
    "claude_code_persona",
    "chatgpt_wire",
    "codeassist_identity",
];

/// Splits `config`, a frozen render config as JSON, into the part runs share and the run's
/// identity: an object holding whichever [`RENDER_IDENTITY_MEMBERS`] it has. The identity is
/// `None` when it has none, and a config that isn't an object comes back whole.
pub fn split(config: &Value) -> (Value, Option<Value>) {
    let Value::Object(members) = config else {
        return (config.clone(), None);
    };
    let mut shared = members.clone();
    let mut identity = Map::new();
    for member in RENDER_IDENTITY_MEMBERS {
        if let Some(value) = shared.remove(member) {
            identity.insert(member.into(), value);
        }
    }
    let identity = (!identity.is_empty()).then_some(Value::Object(identity));
    (Value::Object(shared), identity)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chatgpt_wire::ChatgptWire;
    use crate::families::{anthropic_frozen_config, openai_responses_frozen_config};
    use crate::wire::{ClaudeCodePersona, CodeAssistIdentity, FrozenRenderConfig};

    /// The configs of two runs of each identity-bearing route: the same route, model and
    /// options, with the identity a run and its session give it.
    fn runs(run: u64, session: &str) -> Vec<FrozenRenderConfig> {
        let mut claude = anthropic_frozen_config("claude-opus-4-8");
        claude.claude_code_persona = Some(ClaudeCodePersona {
            version: "2.1.280".into(),
            device_id: "0".repeat(64),
            account_uuid: Some("acct1".into()),
            session_id: ClaudeCodePersona::derive_session_id(session),
            prompt_id: Some(format!("prompt-{run}")),
            cc_version_suffix: Some("abc".into()),
            prompt_ordinal: Some(run),
        });
        let mut chatgpt = openai_responses_frozen_config("gpt-6.2-sol");
        chatgpt.prompt_cache_key = Some(format!("cache-{session}"));
        chatgpt.chatgpt_wire = Some(ChatgptWire {
            profile: crate::chatgpt_wire::ChatgptWireProfile::Codex0163,
            user_agent: "codex_cli_rs/0.163.0".into(),
            originator: ChatgptWire::ORIGINATOR.into(),
            session_id: format!("session-{session}"),
            thread_id: format!("thread-{session}"),
            installation_id: "install".into(),
            window_id: format!("window-{session}"),
            turn_id: format!("turn-{run}"),
            turn_started_at_unix_ms: 1_000 + run,
            reasoning_summary: Some("auto".into()),
        });
        let mut code_assist = anthropic_frozen_config("gemini-3-pro");
        code_assist.codeassist_identity = Some(CodeAssistIdentity::for_session(
            session,
            run * 10,
            5_000 + run,
        ));
        vec![claude, chatgpt, code_assist]
    }

    fn json(config: &FrozenRenderConfig) -> Value {
        serde_json::to_value(config).unwrap()
    }

    /// Runs on one route share one part, across runs of a session and across sessions; only
    /// their identities differ. A member that varies by run or session and isn't listed in
    /// [`RENDER_IDENTITY_MEMBERS`] fails this.
    #[test]
    fn runs_on_one_route_share_the_split_part() {
        let configs: Vec<Vec<FrozenRenderConfig>> = vec![
            runs(1, "session-a"),
            runs(2, "session-a"),
            runs(3, "session-b"),
        ];
        for route in 0..configs[0].len() {
            let parts: Vec<(Value, Option<Value>)> = configs
                .iter()
                .map(|runs| split(&json(&runs[route])))
                .collect();
            assert_ne!(json(&configs[0][route]), json(&configs[1][route]));
            for (part, identity) in &parts {
                assert_eq!(part, &parts[0].0, "route {route}");
                assert!(identity.is_some(), "route {route}");
            }
            assert_ne!(parts[0].1, parts[1].1, "route {route}");
        }
    }

    /// Putting the identity back on top of the part gives the config that was split, member
    /// for member, and it still reads as the same frozen render config.
    #[test]
    fn the_identity_joined_onto_the_part_is_the_whole_config() {
        for config in runs(4, "session-c") {
            let whole = json(&config);
            let (part, identity) = split(&whole);
            let mut joined = part.as_object().unwrap().clone();
            for (member, value) in identity.unwrap().as_object().unwrap() {
                joined.insert(member.clone(), value.clone());
            }
            assert_eq!(Value::Object(joined.clone()), whole);
            let read: FrozenRenderConfig = serde_json::from_value(Value::Object(joined)).unwrap();
            assert_eq!(read, config);
        }
    }

    /// A config without an identity member keeps no identity.
    #[test]
    fn a_config_without_identity_members_splits_into_itself() {
        let config = json(&openai_responses_frozen_config("gpt-5"));
        assert_eq!(split(&config), (config.clone(), None));
        assert_eq!(split(&Value::Null), (Value::Null, None));
    }
}
