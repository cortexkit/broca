//! `agent-data/v1` wire types for moving broca's session WALs between machines.
//!
//! Entorhinal, the project registry and move driver, calls the participant's
//! internal `agent-data` service. Re-export the shared contract's ten operations,
//! strict decoding, refusal bodies, and step, idempotency and ref-set checks so
//! the participant and driver use identical rules. This module defines wire
//! shapes only; it does not register a service or handle requests.

pub use cortexkit_role_agent_data::{
    abort, activate, busy, caller_admitted, describe, diverged, errors, export, idempotency,
    import, ops, prepare, refs, release, request, status, step, EmptyReply, Op, ADMITTED_CALLER,
    MAX_REFS, PROVIDES, REQUIRED_OPS, ROLE, SERVICE_ID, VERSION,
};
pub fn session_ref(harness: &str, _project_root: &str, session: &str) -> String {
    cortexkit_role_llm_runner::session_ref(&cortexkit_role_llm_runner::SessionIdentity {
        runner: "broca",
        harness,
        session,
    })
}

/// Accept old folder-based handles as well as the current external identity.
pub fn matches_session_ref(
    reference: &str,
    harness: &str,
    project_root: &str,
    session: &str,
) -> bool {
    reference == session_ref(harness, project_root, session)
        || reference == broca_core::wal_writer::legacy_session_ref(harness, project_root, session)
}

/// The kind entorhinal uses to route a registered broca session to this participant.
pub const BROCA_SESSION_REF_KIND: &str = "broca-session";

/// A session's external reference, independent of its folder spelling.
pub fn session_reference(harness: &str, project_root: &str, session: &str) -> refs::Ref {
    refs::Ref::new(
        BROCA_SESSION_REF_KIND,
        session_ref(harness, project_root, session),
    )
}

/// Describe the session-WAL store as data belonging to one agent. broca transfers
/// these bytes through the session door, rather than asking the operator to copy
/// files owned by a different program, so `external` is false.
pub fn describe_reply() -> describe::DescribeReply {
    describe::DescribeReply::new(
        vec![BROCA_SESSION_REF_KIND.to_owned()],
        vec![describe::Store::new(
            "broca/session-wals",
            describe::sync_class::AGENT,
        )],
        false,
    )
}
