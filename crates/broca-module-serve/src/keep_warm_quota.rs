//! Advisory quota checks for automatic cache maintenance, never account selection.

use std::collections::BTreeMap;

use broca_catalog::live::{AuthMethod, WireFamilyId};
use serde::Deserialize;

use crate::account_routing::{RoutingClient, RoutingReply, ADMISSION_BUDGET};

pub const ACCOUNT_STATUS_OP: &str = "route.account_status";

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct AccountStatus {
    pub provider: String,
    pub account_id: String,
    pub observed_at: Option<String>,
    pub age_band: Option<String>,
    pub windows: Vec<StatusWindow>,
    pub warm_ok: bool,
    pub reason: StatusReason,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct StatusWindow {
    pub kind: String,
    pub used_percent: Option<f64>,
    pub resets_at: Option<String>,
    pub window_minutes: Option<u64>,
}

/// The account-routing module in prefrontal, which chooses accounts and reports quota, may
/// return reason codes this build does not know. An unknown code is logged and never
/// authorizes automatic warming.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct StatusReason {
    pub code: String,
    pub window: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum StatusOutcome {
    Answer(AccountStatus),
    /// Only an unknown operation, unreachable route, or elapsed budget permits falling back to
    /// the run's own Anthropic rate-limit headers: stop warming if any window is at least 90%.
    Unavailable {
        cause: String,
    },
    Stop {
        code: String,
    },
}

pub fn classify_status_reply(reply: Result<Vec<u8>, Vec<u8>>) -> StatusOutcome {
    match reply {
        Ok(body) => {
            #[derive(Deserialize)]
            struct Envelope {
                result: AccountStatus,
            }
            match serde_json::from_slice::<Envelope>(&body) {
                Ok(reply) => StatusOutcome::Answer(reply.result),
                Err(error) => {
                    tracing::info!(target: "cache_warm", "route.account_status unreadable: {error}");
                    StatusOutcome::Stop {
                        code: "reading_missing".into(),
                    }
                }
            }
        }
        Err(body) => {
            let code = serde_json::from_slice::<serde_json::Value>(&body)
                .ok()
                .and_then(|body| body["code"].as_str().map(str::to_owned));
            match code.as_deref() {
                Some("unknown_method") => StatusOutcome::Unavailable {
                    cause: "unknown_method".into(),
                },
                _ => StatusOutcome::Stop {
                    code: code.unwrap_or_else(|| "reading_missing".into()),
                },
            }
        }
    }
}

pub fn classify_status_transport(reply: RoutingReply) -> StatusOutcome {
    match reply {
        Ok(reply) => classify_status_reply(reply),
        Err(cause) => StatusOutcome::Unavailable { cause },
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaDecision {
    Allow,
    Stop { code: String },
}

/// Query the same routing client and time budget as admission, but never rotate or infer an
/// account. Metered credentials do not consume subscription quota and do not query routing.
pub async fn automatic_warm_quota(
    client: &RoutingClient,
    provider: &str,
    account_ref: Option<&str>,
    method: AuthMethod,
    family: WireFamilyId,
    latest_headers: &BTreeMap<String, String>,
) -> QuotaDecision {
    if !matches!(method, AuthMethod::Oauth | AuthMethod::Chatgpt) {
        return QuotaDecision::Allow;
    }
    let Some(account_id) = account_ref.filter(|id| !id.is_empty()) else {
        return QuotaDecision::Stop {
            code: "account_unknown".into(),
        };
    };
    let deadline = tokio::time::Instant::now() + ADMISSION_BUDGET;
    match client.account_status(provider, account_id, deadline).await {
        StatusOutcome::Answer(status) => {
            if status.provider != provider || status.account_id != account_id {
                return QuotaDecision::Stop {
                    code: "account_unknown".into(),
                };
            }
            // Quota windows (for example, 5-hour and 7-day) are decoded but not used for this
            // decision: Broca never applies a second quota reserve policy. Only the account-
            // routing module in prefrontal may authorize warming, with warm_ok: true and
            // reason "headroom".
            if status.warm_ok && status.reason.code == "headroom" {
                QuotaDecision::Allow
            } else {
                QuotaDecision::Stop {
                    code: status.reason.code,
                }
            }
        }
        StatusOutcome::Stop { code } => QuotaDecision::Stop { code },
        StatusOutcome::Unavailable { cause } => {
            client.counters().bump("warm_quota_fallback");
            tracing::info!(target: "cache_warm", "route.account_status fallback: {cause}");
            headers_fallback(family, latest_headers)
        }
    }
}

/// Anthropic reports utilization as a fraction, not a percent. With no routing answer, only
/// an explicitly reported window at ninety percent stops warming; Codex has no interim stop.
pub fn headers_fallback(family: WireFamilyId, headers: &BTreeMap<String, String>) -> QuotaDecision {
    if family == WireFamilyId::AnthropicMessages
        && headers.iter().any(|(name, value)| {
            let name = name.to_ascii_lowercase();
            name.starts_with("anthropic-ratelimit-unified-")
                && name.ends_with("-utilization")
                && value.parse::<f64>().is_ok_and(|fraction| fraction >= 0.9)
        })
    {
        QuotaDecision::Stop {
            code: "headers_90_percent".into(),
        }
    } else {
        QuotaDecision::Allow
    }
}

/// With no routing connection, use the same headers-based quota fallback as a transport
/// failure. Retain one client so its warm_quota_fallback counter accumulates across calls.
pub fn unavailable_routing_client() -> &'static RoutingClient {
    use std::sync::{Arc, OnceLock};
    struct Unavailable;
    #[async_trait::async_trait]
    impl crate::account_routing::RoutingTransport for Unavailable {
        async fn request(&self, _body: Vec<u8>) -> RoutingReply {
            Err("routing connection unavailable".into())
        }
    }
    static CLIENT: OnceLock<RoutingClient> = OnceLock::new();
    CLIENT.get_or_init(|| {
        RoutingClient::new(
            Arc::new(Unavailable),
            crate::account_routing::AccountSelectionConfig::default(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::account_routing::{AccountSelectionConfig, RoutingTransport};
    use std::sync::{Arc, Mutex};

    struct Stub {
        reply: RoutingReply,
        requests: Mutex<Vec<serde_json::Value>>,
    }

    #[async_trait::async_trait]
    impl RoutingTransport for Stub {
        async fn request(&self, body: Vec<u8>) -> RoutingReply {
            self.requests
                .lock()
                .unwrap()
                .push(serde_json::from_slice(&body).unwrap());
            self.reply.clone()
        }
    }

    struct Slow;

    #[async_trait::async_trait]
    impl RoutingTransport for Slow {
        async fn request(&self, _body: Vec<u8>) -> RoutingReply {
            std::future::pending().await
        }
    }

    #[tokio::test]
    async fn warm_quota_over_budget_uses_the_headers_fallback() {
        let client = RoutingClient::new(Arc::new(Slow), AccountSelectionConfig::default());
        let headers = BTreeMap::from([(
            "anthropic-ratelimit-unified-7d-utilization".into(),
            "0.91".into(),
        )]);
        let started = tokio::time::Instant::now();
        assert_eq!(
            automatic_warm_quota(
                &client,
                "anthropic",
                Some(ACCOUNT),
                AuthMethod::Oauth,
                WireFamilyId::AnthropicMessages,
                &headers
            )
            .await,
            QuotaDecision::Stop {
                code: "headers_90_percent".into()
            }
        );
        assert!(started.elapsed() >= ADMISSION_BUDGET);
        assert_eq!(client.counters().get("warm_quota_fallback"), 1);
    }

    fn client(reply: RoutingReply) -> (RoutingClient, Arc<Stub>) {
        let stub = Arc::new(Stub {
            reply,
            requests: Mutex::new(Vec::new()),
        });
        (
            RoutingClient::new(stub.clone(), AccountSelectionConfig::default()),
            stub,
        )
    }

    const WARM: &str = include_str!("../tests/fixtures/route-account-status-v1/warm_ok.json");
    const RESERVE: &str =
        include_str!("../tests/fixtures/route-account-status-v1/within_reserve.json");
    const STALE: &str =
        include_str!("../tests/fixtures/route-account-status-v1/reading_stale.json");
    const ACCOUNT: &str = "00000000-0000-4000-8000-00000000000a";

    #[tokio::test]
    async fn warm_quota_decodes_vendored_routing_vectors_and_uses_their_permission() {
        for (fixture, expected) in [
            (WARM, QuotaDecision::Allow),
            (
                RESERVE,
                QuotaDecision::Stop {
                    code: "within_reserve".into(),
                },
            ),
            (
                STALE,
                QuotaDecision::Stop {
                    code: "reading_stale".into(),
                },
            ),
        ] {
            let vector: serde_json::Value = serde_json::from_str(fixture).unwrap();
            let (client, transport) = client(Ok(Ok(serde_json::to_vec(&vector["reply"]).unwrap())));
            let result = automatic_warm_quota(
                &client,
                "anthropic",
                Some(ACCOUNT),
                AuthMethod::Oauth,
                WireFamilyId::AnthropicMessages,
                &BTreeMap::new(),
            )
            .await;
            assert_eq!(result, expected);
            assert_eq!(
                transport.requests.lock().unwrap().as_slice(),
                &[vector["request"].clone()]
            );
            assert_eq!(client.counters().get("warm_quota_fallback"), 0);
        }
    }

    #[tokio::test]
    async fn warm_quota_never_rederives_windows_and_stops_on_an_unknown_reason() {
        let mut vector: serde_json::Value = serde_json::from_str(WARM).unwrap();
        vector["reply"]["result"]["windows"][0]["used_percent"] = serde_json::json!(100.0);
        let (known, _) = client(Ok(Ok(serde_json::to_vec(&vector["reply"]).unwrap())));
        assert_eq!(
            automatic_warm_quota(
                &known,
                "anthropic",
                Some(ACCOUNT),
                AuthMethod::Oauth,
                WireFamilyId::AnthropicMessages,
                &BTreeMap::new()
            )
            .await,
            QuotaDecision::Allow
        );
        vector["reply"]["result"]["reason"]["code"] = serde_json::json!("future_quota_policy");
        let (unknown, _) = client(Ok(Ok(serde_json::to_vec(&vector["reply"]).unwrap())));
        assert_eq!(
            automatic_warm_quota(
                &unknown,
                "anthropic",
                Some(ACCOUNT),
                AuthMethod::Oauth,
                WireFamilyId::AnthropicMessages,
                &BTreeMap::new()
            )
            .await,
            QuotaDecision::Stop {
                code: "future_quota_policy".into()
            }
        );
        assert_eq!(unknown.counters().get("warm_quota_fallback"), 0);
    }

    #[tokio::test]
    async fn warm_quota_method_unknown_and_unreachable_use_the_counted_headers_fallback() {
        for reply in [
            Ok(Err(br#"{"code":"unknown_method"}"#.to_vec())),
            Err("routing unreachable".into()),
        ] {
            for (fraction, expected) in [
                ("0.89", QuotaDecision::Allow),
                (
                    "0.90",
                    QuotaDecision::Stop {
                        code: "headers_90_percent".into(),
                    },
                ),
            ] {
                let (client, _) = client(reply.clone());
                let headers = BTreeMap::from([(
                    "anthropic-ratelimit-unified-5h-utilization".into(),
                    fraction.into(),
                )]);
                assert_eq!(
                    automatic_warm_quota(
                        &client,
                        "anthropic",
                        Some(ACCOUNT),
                        AuthMethod::Oauth,
                        WireFamilyId::AnthropicMessages,
                        &headers
                    )
                    .await,
                    expected
                );
                assert_eq!(client.counters().get("warm_quota_fallback"), 1);
                assert_eq!(
                    automatic_warm_quota(
                        &client,
                        "openai",
                        Some(ACCOUNT),
                        AuthMethod::Chatgpt,
                        WireFamilyId::OpenAiResponsesChatgpt,
                        &headers
                    )
                    .await,
                    QuotaDecision::Allow
                );
                assert_eq!(client.counters().get("warm_quota_fallback"), 2);
            }
        }
    }

    #[tokio::test]
    async fn warm_quota_skips_metered_credentials_and_does_not_infer_missing_accounts() {
        let (client, transport) = client(Err("must not be reached".into()));
        assert_eq!(
            automatic_warm_quota(
                &client,
                "anthropic",
                None,
                AuthMethod::ApiKey,
                WireFamilyId::AnthropicMessages,
                &BTreeMap::new()
            )
            .await,
            QuotaDecision::Allow
        );
        assert_eq!(
            automatic_warm_quota(
                &client,
                "anthropic",
                None,
                AuthMethod::Oauth,
                WireFamilyId::AnthropicMessages,
                &BTreeMap::new()
            )
            .await,
            QuotaDecision::Stop {
                code: "account_unknown".into()
            }
        );
        assert!(transport.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn warm_quota_refusals_malformed_replies_and_wrong_accounts_do_not_fall_back() {
        for reply in [
            Ok(Err(br#"{"code":"account_selection_disabled"}"#.to_vec())),
            Ok(Ok(br#"{"result":{}}"#.to_vec())),
        ] {
            let (client, _) = client(reply);
            assert!(matches!(
                automatic_warm_quota(
                    &client,
                    "anthropic",
                    Some(ACCOUNT),
                    AuthMethod::Oauth,
                    WireFamilyId::AnthropicMessages,
                    &BTreeMap::new()
                )
                .await,
                QuotaDecision::Stop { .. }
            ));
            assert_eq!(client.counters().get("warm_quota_fallback"), 0);
        }
        let vector: serde_json::Value = serde_json::from_str(WARM).unwrap();
        let (client, _) = client(Ok(Ok(serde_json::to_vec(&vector["reply"]).unwrap())));
        assert_eq!(
            automatic_warm_quota(
                &client,
                "anthropic",
                Some("another-account"),
                AuthMethod::Oauth,
                WireFamilyId::AnthropicMessages,
                &BTreeMap::new()
            )
            .await,
            QuotaDecision::Stop {
                code: "account_unknown".into()
            }
        );
    }
}
