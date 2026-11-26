//! How long a provider stream may stay alive without content before it is abandoned.

use std::time::Duration;

use broca_protocol::error::{ErrorClass, ProviderError};

/// How long an attempt may go without real content, unless the wire family sets its own
/// limit through `StreamNormalizer::content_deadline`. Unlike the idle deadline, comments,
/// socket pings, and protocol bookkeeping do not reset this timer.
///
/// MEASURED, NOT GUESSED. xai/grok-4.7, replaying a failing request byte for byte, reasons
/// without streaming its reasoning and sends nothing but `: keepalive` comments every 15 s
/// while it does. The silent span varied from 138 s to 579 s across samples for about 45k
/// reasoning tokens, and every sample then completed normally. A 300-second content limit cut
/// those healthy streams and the retry was cut the same way. A maximum-effort reasoning step
/// can run several times the longest measured span, so 1800 s leaves roughly 3x headroom over
/// 579 s while still bounding a peer that keeps the connection alive and never answers.
pub(crate) const STREAM_CONTENT_TIMEOUT: Duration = Duration::from_secs(1800);

/// Local models can finish prompt evaluation before writing any response headers. This extra
/// time applies only before the response, not to gaps once the stream starts.
pub(super) fn first_response_timeout(url: &str, default: Duration) -> Duration {
    if default != super::STREAM_IDLE_TIMEOUT {
        return default;
    }
    use url::Host;
    let local = url::Url::parse(url).is_ok_and(|url| match url.host() {
        Some(Host::Domain(host)) => host.trim_end_matches('.') == "localhost",
        Some(Host::Ipv4(ip)) => ip.is_loopback() || ip.is_private(),
        Some(Host::Ipv6(ip)) => ip.to_ipv4_mapped().map_or_else(
            || ip.is_loopback() || ip.is_unique_local(),
            |ip| ip.is_loopback() || ip.is_private(),
        ),
        None => false,
    });
    if local {
        STREAM_CONTENT_TIMEOUT
    } else {
        default
    }
}

pub(super) fn header_timeout_error(limit: Duration) -> ProviderError {
    let mut error = super::stream_timeout_error("waiting for response headers");
    if limit == STREAM_CONTENT_TIMEOUT {
        error.message = format!(
            "provider stream idle: waiting for response headers for {}s",
            limit.as_secs()
        );
    }
    error
}

/// The error for a stream that carried only keepalives for `limit`.
pub(super) fn content_timeout_error(limit: Duration) -> ProviderError {
    ProviderError {
        provider_details: None,
        cause: None,
        provider_subcode: None,
        refusal: None,
        class: ErrorClass::Transient,
        message: format!(
            "provider stream idle: no model content for {}s",
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
    /// content limit only applies if the wrapper passes it on.
    #[test]
    fn the_live_wrapper_keeps_the_family_content_limit() {
        let wrapped = super::super::ToolNameRestoringNormalizer {
            inner: Box::new(OwnLimit),
            names: Arc::default(),
        };
        assert_eq!(wrapped.content_deadline(), Some(Duration::from_secs(600)));
    }

    #[test]
    fn first_byte_policy_classifies_private_and_loopback_hosts() {
        for (url, secs) in [
            ("http://localhost:8000", 1800),
            ("http://127.0.0.2", 1800),
            ("http://[::1]", 1800),
            ("https://192.168.1.2", 1800),
            ("https://[fd00::1]", 1800),
            ("https://[::ffff:10.0.0.1]", 1800),
            ("https://192.0.2.1", 300),
            ("https://localhost.example", 300),
        ] {
            assert_eq!(
                super::first_response_timeout(url, Duration::from_secs(300)),
                Duration::from_secs(secs),
                "{url}"
            );
        }
    }
}
