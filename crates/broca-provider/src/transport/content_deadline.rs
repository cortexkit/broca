//! How long a provider stream may stay alive without content before it is abandoned.

use std::time::Duration;

use broca_protocol::error::{ErrorClass, ProviderError};

/// How long a stream may carry ONLY keepalive records (SSE `: keepalive` comments, socket
/// pings) before it is abandoned, unless the wire family sets its own limit through
/// `StreamNormalizer::content_deadline`. Applies only to grammars that report a liveness
/// marker; for them the 300-second idle deadline is reset by any complete record, keepalives
/// included, and this one is reset only by content.
///
/// MEASURED, NOT GUESSED. xai/grok-4.7, replaying a failing request byte for byte, reasons
/// without streaming its reasoning and sends nothing but `: keepalive` comments every 15 s
/// while it does. The silent span varied from 138 s to 579 s across samples for about 45k
/// reasoning tokens, and every sample then completed normally. A 300-second content limit cut
/// those healthy streams and the retry was cut the same way. A maximum-effort reasoning step
/// can run several times the longest measured span, so 1800 s leaves roughly 3x headroom over
/// 579 s while still bounding a peer that keeps the connection alive and never answers.
pub(crate) const STREAM_CONTENT_TIMEOUT: Duration = Duration::from_secs(1800);

/// The error for a stream that carried only keepalives for `limit`.
pub(super) fn content_timeout_error(limit: Duration) -> ProviderError {
    ProviderError {
        provider_details: None,
        cause: None,
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Transient,
        message: format!(
            "provider stream idle: waiting for content: only keepalive comments for {}s",
            limit.as_secs()
        ),
        status: None,
        retry_after_secs: None,
        provider_code: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        account_continue: None,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::wire::{NormalizeError, StreamNormalizer};
    use broca_protocol::NormalizedEvent;

    /// A family that sets its own content limit.
    struct OwnLimit;

    impl StreamNormalizer for OwnLimit {
        fn push(&mut self, _: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            Ok(Vec::new())
        }

        fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
            Ok(Vec::new())
        }

        fn content_deadline(&self) -> Option<Duration> {
            Some(Duration::from_secs(600))
        }
    }

    /// Every live attempt reads its stream through the tool-name wrapper, so a family's own
    /// content limit (ChatGPT's 600 s) only applies if the wrapper passes it on.
    #[test]
    fn the_live_wrapper_keeps_the_family_content_limit() {
        let wrapped = super::super::ToolNameRestoringNormalizer {
            inner: Box::new(OwnLimit),
            names: Arc::default(),
            live_progress: None,
        };
        assert_eq!(wrapped.content_deadline(), Some(Duration::from_secs(600)));
    }
}
