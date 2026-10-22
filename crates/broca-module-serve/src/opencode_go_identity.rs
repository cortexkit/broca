use broca_protocol::BindIdentity;
use broca_provider::wire::ClaudeCodePersona;

/// Derive the Go routing id from the same opaque bind reference used by Claude Code identities.
pub fn for_session(session: &BindIdentity) -> String {
    let reference = broca_wire::agent_data::session_ref(
        &session.harness,
        &session.project_root.to_string_lossy(),
        &session.session,
    );
    ClaudeCodePersona::derive_session_id(&reference)
}

#[cfg(test)]
mod tests {
    use broca_protocol::BindIdentity;

    use super::for_session;

    #[test]
    fn session_identity_is_repeatable_opaque_and_scoped_to_the_bind() {
        let session = BindIdentity::new("/private/project", "harness", "my-session-name");
        let id = for_session(&session);
        assert_eq!(id, for_session(&session));
        assert_eq!(id.len(), 36);
        assert!(!id.contains("private"));
        assert!(!id.contains("project"));
        assert!(!id.contains("my-session-name"));

        assert_ne!(
            id,
            for_session(&BindIdentity::new(
                "/other/project",
                "harness",
                "my-session-name"
            ))
        );
        assert_ne!(
            id,
            for_session(&BindIdentity::new(
                "/private/project",
                "harness",
                "other-session"
            ))
        );
    }
}
