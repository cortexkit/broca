//! What the send that started a run recorded about itself, which the run's start keeps.

/// What the send that started a run recorded about itself.
#[derive(Debug, Clone, Default)]
pub struct SendFacts {
    /// The queued send the run consumes, when it started from the queue.
    pub submission_id: Option<String>,
    /// The caller's idempotency key, and the digest of the request it was sent with: a retry
    /// under the key is compared with the digest.
    pub send_id: Option<String>,
    pub request_identity_digest: Option<String>,
    /// The caller's label for the send.
    pub title: Option<String>,
    pub session_class: Option<String>,
    /// How long the session is kept after its last activity, in seconds.
    pub retention_secs: Option<u64>,
    /// The storage policy this send freezes (llm-runner `session_retention`), as admission
    /// decoded it: set only when it is the session's first send carrying one. The start's batch
    /// records it as a `Retention` `Set` after the run's input.
    pub retention_policy: Option<serde_json::Value>,
    /// Who sent it, as the daemon attested; and, for a send another module made on the owner's
    /// behalf, that module.
    pub principal: Option<broca_protocol::BindPrincipal>,
    pub sent_by: Option<broca_protocol::BindPrincipal>,
    /// The scope the session's runs are frozen to.
    pub scope: Option<broca_protocol::SessionScope>,
    /// What the input hooks did to the run's input, as evidence: the input itself is already
    /// the hooks' final value.
    pub hooks: Option<broca_records::record::Hooks>,
    /// The send's display marks, which the run's start records for its input.
    pub message_marks: Option<broca_protocol::MessageMarks>,
}
