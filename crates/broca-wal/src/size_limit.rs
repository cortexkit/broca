use broca_protocol::{ErrorClass, ProviderError};

pub const CODE: &str = "retained_session_too_large";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SizeScope {
    Session,
    RetainedSession,
}

pub fn scoped_error(max_bytes: u64, scope: SizeScope) -> ProviderError {
    let (subject, code) = match scope {
        SizeScope::Session => ("session", "session_too_large"),
        SizeScope::RetainedSession => ("retained session", CODE),
    };
    ProviderError {
        provider_details: None,
        cause: None,
        provider_subcode: None,
        class: ErrorClass::Permanent,
        message: format!("{subject} log cannot exceed {max_bytes} bytes"),
        provider_code: Some(code.into()),
        status: None,
        retry_after_secs: None,
        refusal: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
    }
}

pub fn error(max_bytes: u64) -> ProviderError {
    scoped_error(max_bytes, SizeScope::RetainedSession)
}
