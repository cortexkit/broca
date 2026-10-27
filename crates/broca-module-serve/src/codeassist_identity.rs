//! Admission-only identity derivation. A resumed episode restores its recorded envelope,
//! including a recorded absence, rather than adopting the current identity derivation.

use broca_protocol::BindIdentity;
use broca_provider::wire::CodeAssistIdentity;
use serde_json::Value;

pub fn for_episode(
    session: &BindIdentity,
    recorded: Option<&Value>,
    head_seq: u64,
    admitted_at_ms: u64,
) -> Result<Option<CodeAssistIdentity>, serde_json::Error> {
    if let Some(recorded) = recorded {
        return recorded
            .get("codeassist_identity")
            .filter(|value| !value.is_null())
            .cloned()
            .map(serde_json::from_value)
            .transpose();
    }
    let session_ref = broca_wire::agent_data::session_ref(
        &session.harness,
        &session.project_root.to_string_lossy(),
        &session.session,
    );
    Ok(Some(CodeAssistIdentity::for_session(
        &session_ref,
        head_seq,
        admitted_at_ms,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::{CallOptions, ContentBlock, Message, Role, StepContext};
    use broca_provider::{families::GoogleCodeAssist, wire::WireFamily};

    fn render(identity: Option<CodeAssistIdentity>, run: &str, step: u64) -> Vec<u8> {
        let mut frozen =
            broca_provider::families::google_codeassist_frozen_config("gemini-2.5-flash");
        frozen.project_id = Some("test-project".into());
        frozen.codeassist_identity = identity;
        let mut opts = CallOptions::new(vec![Message::new(
            Role::User,
            vec![ContentBlock::Text { text: "hi".into() }],
        )]);
        opts.step_context = Some(StepContext {
            session_address: None,
            credential_id: None,
            account_ref: None,
            run_id: run.into(),
            step_index: step,
            max_output_tokens: None,
        });
        GoogleCodeAssist.render(&frozen, &opts)
    }

    #[test]
    fn same_session_keeps_identity_across_runs_and_steps_increase() {
        let session = BindIdentity::new("/project", "harness", "session");
        let first = for_episode(&session, None, 0, 1000).unwrap().unwrap();
        let second = for_episode(&session, None, 20, 2000).unwrap().unwrap();
        let a: Value = serde_json::from_slice(&render(Some(first.clone()), "run-a", 2)).unwrap();
        let b: Value = serde_json::from_slice(&render(Some(second), "run-b", 1)).unwrap();
        assert_eq!(a["request"]["sessionId"], b["request"]["sessionId"]);
        let a_id: Vec<_> = a["requestId"].as_str().unwrap().split('/').collect();
        let b_id: Vec<_> = b["requestId"].as_str().unwrap().split('/').collect();
        assert_eq!(a_id[1], b_id[1], "agent identity survives a new user turn");
        assert_eq!(a_id[3], b_id[3], "trajectory survives a new user turn");
        assert!(a_id[4].parse::<u64>().unwrap() < b_id[4].parse::<u64>().unwrap());
        assert_ne!(a_id, b_id);
        assert_eq!(
            render(Some(first.clone()), "run-a", 2),
            render(Some(first), "run-a", 2)
        );

        for other in [
            BindIdentity::new("/other-project", "harness", "session"),
            BindIdentity::new("/project", "other-harness", "session"),
            BindIdentity::new("/project", "harness", "other-session"),
        ] {
            let different = for_episode(&other, None, 20, 2000).unwrap().unwrap();
            assert_ne!(b["request"]["sessionId"], different.session_id);
        }
    }

    #[test]
    fn resume_restores_recorded_identity_and_legacy_bytes() {
        let session = BindIdentity::new("/project", "harness", "session");
        let identity = for_episode(&session, None, 0, 1000).unwrap();
        let recorded = serde_json::json!({"codeassist_identity": identity});
        let other_session = BindIdentity::new("/moved", "harness", "session");
        let restored = for_episode(&other_session, Some(&recorded), 90, 9000).unwrap();
        assert_eq!(render(identity, "run-a", 2), render(restored, "run-a", 2));

        let old: Value = serde_json::json!({});
        let restored = for_episode(&session, Some(&old), 90, 9000).unwrap();
        assert_eq!(
            restored, None,
            "old episodes must not acquire a new identity"
        );
        assert_eq!(render(restored, "old-run", 2), render(None, "old-run", 2));
        let body: Value = serde_json::from_slice(&render(None, "old-run", 2)).unwrap();
        assert!(body["request"].get("sessionId").is_none());
        assert!(body["request"].get("labels").is_none());
        assert!(for_episode(
            &session,
            Some(&serde_json::json!({"codeassist_identity": {}})),
            0,
            0
        )
        .is_err());
    }
}
