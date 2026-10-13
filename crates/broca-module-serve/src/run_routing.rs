use broca_core::StartMode;
use broca_module::executor::RunKind;

/// Select the core start mode after the credential source has made its authoritative decision.
///
/// A successful credential resolution upgrades the ordinary public send mode so a durable
/// `AuthRequired` pause can continue. Core consults the resulting auth-resolved flag only for
/// that replay resolution; fresh runs and every other resume resolution retain their normal
/// behavior.
pub fn start_mode_after_auth(kind: RunKind, credential_ready: bool) -> StartMode {
    match kind {
        RunKind::AppendEpisode => StartMode::AppendEpisode,
        RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
        RunKind::FreshOrResume if credential_ready => StartMode::ResumeAfterAuth,
        RunKind::FreshOrResume => StartMode::FreshOrResume,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn successful_auth_upgrades_only_the_public_resume_mode() {
        assert_eq!(
            start_mode_after_auth(RunKind::FreshOrResume, false),
            StartMode::FreshOrResume
        );
        assert_eq!(
            start_mode_after_auth(RunKind::FreshOrResume, true),
            StartMode::ResumeAfterAuth
        );
        assert_eq!(
            start_mode_after_auth(RunKind::AppendEpisode, true),
            StartMode::AppendEpisode
        );
        assert_eq!(
            start_mode_after_auth(RunKind::ResumeAfterAuth, false),
            StartMode::ResumeAfterAuth
        );
    }
}
