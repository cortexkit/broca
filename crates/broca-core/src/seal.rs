//! Interrupted run seals and their terminal diagnostics.

/// Why a broca process ends a run it didn't finish: it is stopping while the run runs, or it
/// found, on opening the session, a run that a crashed process left running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SealCause {
    Shutdown,
    CrashOrphan,
}

/// The diagnostic a crash-orphan seal records on its `RunFinished`. The run's reason stays
/// `Interrupted`: recovery knows the old process left the run unsealed, not how it ended.
pub fn crash_orphan_error() -> broca_protocol::ProviderError {
    broca_protocol::ProviderError {
        provider_details: None,
        cause: None,
        provider_subcode: None,
        class: broca_protocol::ErrorClass::Transient,
        message: "the previous broca process ended without sealing this run: host crash, kill, \
                  or power loss"
            .into(),
        provider_code: Some("previous_process_ended_unsealed".into()),
        status: None,
        retry_after_secs: None,
        refusal: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}
