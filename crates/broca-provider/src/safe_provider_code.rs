/// Only closed, known codes may enter diagnostics for retention-bearing sessions.
/// Provider-supplied strings can otherwise echo arbitrary request content.
pub fn safe_provider_code(code: &str) -> bool {
    matches!(
        code,
        "account_limit_exhausted"
            | "upstream_api_key_rejected"
            | "rate_limit_exceeded"
            | "rate_limited"
            | "invalid_api_key"
            | "incorrect_api_key"
            | "context_length_exceeded"
            | "context_window_exceeded"
            | "billing_exhausted"
            | "credit_insufficient"
            | "credit_balance_exhausted"
            | "organization_spend_limit_exceeded"
            | "project_spend_limit_exceeded"
            | "usage_not_included"
            | "invalid_prompt"
            | "cyber_policy"
            | "bio_policy"
            | "misalignment_policy_violation"
            | "usage_limit_reached"
            | "content_filter"
            | "server_error"
            | "stream_too_slow"
            | "response_deadline"
            | "output_ceiling"
            | "previous_process_ended_unsealed"
            | "websocket_auth_rejected"
            | "request_invalid"
    )
}

#[cfg(test)]
mod tests {
    use super::safe_provider_code;

    #[test]
    fn allowlist_rejects_arbitrary_provider_content() {
        for code in [
            "PRIVATE-PROVIDER-CODE",
            "rate_limit_exceeded PRIVATE-PROVIDER-CODE",
            "rate_limit_exceeded\n",
            "RATE_LIMIT_EXCEEDED",
            "",
        ] {
            assert!(!safe_provider_code(code), "unexpected safe code: {code:?}");
        }
        for code in [
            "rate_limit_exceeded",
            "rate_limited",
            "context_length_exceeded",
            "context_window_exceeded",
            "billing_exhausted",
            "credit_insufficient",
            "output_ceiling",
            "previous_process_ended_unsealed",
            "websocket_auth_rejected",
            "request_invalid",
        ] {
            assert!(safe_provider_code(code), "known safe code rejected: {code}");
        }
    }
}
