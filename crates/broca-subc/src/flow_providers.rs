//! Asking Basal which providers a flow may send private content to
//! (`flow.private_content_providers`).
//!
//! A flow that read the person's private content may still call local models and the providers
//! on broca's configured list. Every other provider needs the approval recorded on the flow's
//! approved version, which Basal answers for a flow id. Basal admits only `reserved:broca`, so
//! the question goes over broca's own module connection, like
//! [`private_content`](crate::private_content).
//!
//! A flow never raises a consent card (there is nobody to answer one mid-flow), so a refused
//! provider ends the call. The answer is cached for the life of the flow's scope identity, and
//! a rejection is not cached: the flow's approved version can change, and broca only learns a
//! provider is still refused by asking again.
//!
//! An answer broca cannot use (no Basal, a timeout, an operation Basal does not serve) is not an
//! error: it narrows the flow to the base set, exactly as an approval that named no extra
//! providers would.

use std::sync::Arc;
use std::time::Duration;

use broca_protocol::BindIdentity;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::connection::{ConnError, RouteOpenRetry, SubcConnection};

/// The module whose management surface answers `flow.private_content_providers`.
pub const BASAL_MODULE: &str = "basal";

/// The one operation broca calls on Basal's management surface, and the code Basal answers
/// when a flow has no approved version that could allow a provider.
pub const FLOW_PROVIDERS_OP: &str = "flow.private_content_providers";
pub const FLOW_NOT_APPROVED: &str = "flow_not_approved";

/// How long one question waits for its reply.
const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// Provider ids are lowercase letters, digits and hyphens, 1-64 bytes each, at most 16 per
/// list. The same grammar bounds the configured private-content list.
pub fn valid_provider_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 64
        && id
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
}

/// The approved version and its declared providers, as named by Basal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovedFlowProviders {
    pub version: u64,
    pub code_hash: String,
    pub providers: Vec<String>,
}

/// The providers a flow may call, or the reason its call is refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowPolicy {
    /// The provider ids Basal listed for the flow's approved version.
    Allowed(ApprovedFlowProviders),
    /// Basal reports that the flow has no approved version. Broca refuses this call with
    /// Basal's own code and does not cache the answer: approval can change between calls.
    NotApproved { message: String },
}

/// Why Basal could not name a flow's providers. Every variant narrows the flow to the base set
/// and is not cached.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlowProvidersError {
    Route(String),
    Refused {
        code: String,
        message: String,
        detail: Option<Value>,
    },
    Unanswered(String),
    Protocol(String),
}

impl FlowProvidersError {
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Refused { code, .. } => Some(code),
            _ => None,
        }
    }
}

impl std::fmt::Display for FlowProvidersError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Route(message) => write!(f, "{FLOW_PROVIDERS_OP} route: {message}"),
            Self::Refused {
                code,
                message,
                detail,
            } => match detail {
                Some(detail) => write!(
                    f,
                    "{FLOW_PROVIDERS_OP} refused {code}: {message} ({detail})"
                ),
                None => write!(f, "{FLOW_PROVIDERS_OP} refused {code}: {message}"),
            },
            Self::Unanswered(message) => write!(f, "{FLOW_PROVIDERS_OP} unanswered: {message}"),
            Self::Protocol(message) => write!(f, "decode {FLOW_PROVIDERS_OP}: {message}"),
        }
    }
}

impl std::error::Error for FlowProvidersError {}

/// Asks Basal one `flow.private_content_providers {flow_id}`.
pub struct FlowProvidersClient {
    conn: Arc<SubcConnection>,
    identity: BindIdentity,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Answer {
    flow_id: String,
    version: u64,
    code_hash: String,
    providers: Vec<String>,
}

impl FlowProvidersClient {
    pub fn new(conn: Arc<SubcConnection>, identity: BindIdentity) -> Self {
        Self { conn, identity }
    }

    pub async fn query(&self, flow_id: &str) -> Result<FlowPolicy, FlowProvidersError> {
        let route = self
            .conn
            .route_open_management_within(
                BASAL_MODULE,
                &self.identity,
                RouteOpenRetry::within(Duration::ZERO, Duration::ZERO),
            )
            .await
            .map_err(|error| FlowProvidersError::Route(error.to_string()))?;
        let body = serde_json::to_vec(&request_body(flow_id))
            .map_err(|error| FlowProvidersError::Protocol(error.to_string()));
        let reply = match body {
            Ok(body) => match tokio::time::timeout(
                QUERY_TIMEOUT,
                self.conn.route_unary_reply(&route, body, QUERY_TIMEOUT),
            )
            .await
            {
                Ok(result) => decode(result, flow_id),
                Err(_) => Err(FlowProvidersError::Unanswered(format!(
                    "no reply within {QUERY_TIMEOUT:?}"
                ))),
            },
            Err(error) => Err(error),
        };
        let _ = self.conn.route_close(&route).await;
        reply
    }
}

fn request_body(flow_id: &str) -> Value {
    json!({"method": FLOW_PROVIDERS_OP, "params": {"flow_id": flow_id}})
}

fn decode(
    reply: Result<Result<Vec<u8>, subc_protocol::ErrorBody>, ConnError>,
    flow_id: &str,
) -> Result<FlowPolicy, FlowProvidersError> {
    let bytes = match reply {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(error)) if error.code == FLOW_NOT_APPROVED => {
            return Ok(FlowPolicy::NotApproved {
                message: error.message,
            })
        }
        Ok(Err(error)) => {
            return Err(FlowProvidersError::Refused {
                code: error.code,
                message: error.message,
                detail: error.detail,
            })
        }
        Err(error) => {
            return Err(match error {
                ConnError::Rejected(message) => FlowProvidersError::Unanswered(message.to_string()),
                other => FlowProvidersError::Route(other.to_string()),
            })
        }
    };
    #[derive(Deserialize)]
    struct Envelope {
        result: Answer,
    }
    let Envelope { result } = serde_json::from_slice::<Envelope>(&bytes)
        .map_err(|error| FlowProvidersError::Protocol(error.to_string()))?;
    if result.flow_id != flow_id {
        return Err(FlowProvidersError::Protocol(format!(
            "answer names flow {} instead of {flow_id}",
            result.flow_id
        )));
    }
    if result.providers.len() > 16 || !result.providers.iter().all(|id| valid_provider_id(id)) {
        return Err(FlowProvidersError::Protocol(
            "providers must be at most 16 ids of lowercase letters, digits or hyphens".into(),
        ));
    }
    if result.code_hash.len() != 64
        || !result
            .code_hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(FlowProvidersError::Protocol(
            "code_hash must be 64 lowercase hexadecimal characters".into(),
        ));
    }
    Ok(FlowPolicy::Allowed(ApprovedFlowProviders {
        version: result.version,
        code_hash: result.code_hash,
        providers: result.providers,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASAL_SUCCESS: &[u8] = br#"{"result":{"flow_id":"flow","version":7,"code_hash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","providers":["typesafe","openai"]}}"#;
    const BASAL_EMPTY: &[u8] = br#"{"result":{"flow_id":"flow","version":7,"code_hash":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","providers":[]}}"#;

    #[test]
    fn basal_approved_version_success_fixture_decodes() {
        for (bytes, providers) in [
            (BASAL_SUCCESS, vec!["typesafe".into(), "openai".into()]),
            (BASAL_EMPTY, vec![]),
        ] {
            assert_eq!(
                decode(Ok(Ok(bytes.to_vec())), "flow").unwrap(),
                FlowPolicy::Allowed(ApprovedFlowProviders {
                    version: 7,
                    code_hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                        .into(),
                    providers,
                })
            );
        }
    }

    #[test]
    fn request_body_contains_only_flow_id() {
        assert_eq!(
            request_body("flow"),
            json!({
                "method": "flow.private_content_providers", "params": {"flow_id": "flow"}
            })
        );
    }

    fn answer(
        flow_id: &str,
        providers: Value,
    ) -> Result<Result<Vec<u8>, subc_protocol::ErrorBody>, ConnError> {
        Ok(Ok(serde_json::to_vec(
            &json!({"result": {"flow_id": flow_id, "version": 7,
                "code_hash": "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
                "providers": providers}}),
        )
        .unwrap()))
    }

    #[test]
    fn a_listed_flow_allows_exactly_its_providers() {
        let policy = decode(answer("flow", json!(["cloudflare-workers-ai"])), "flow").unwrap();
        assert_eq!(
            policy,
            FlowPolicy::Allowed(ApprovedFlowProviders {
                version: 7,
                code_hash: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .into(),
                providers: vec!["cloudflare-workers-ai".into()]
            })
        );
    }

    #[test]
    fn the_not_approved_refusal_is_a_policy_answer_not_a_lookup_failure() {
        let policy = decode(
            Ok(Err(subc_protocol::ErrorBody::new(
                FLOW_NOT_APPROVED,
                "this flow has no approved version",
            ))),
            "flow",
        )
        .unwrap();
        assert_eq!(
            policy,
            FlowPolicy::NotApproved {
                message: "this flow has no approved version".into()
            }
        );
    }

    #[test]
    fn an_unusable_answer_is_an_error_and_never_a_provider_set() {
        for providers in [json!(["Not A Provider"]), json!(vec!["a"; 17])] {
            assert!(matches!(
                decode(answer("flow", providers), "flow"),
                Err(FlowProvidersError::Protocol(_))
            ));
        }
        // An answer for another flow would apply one flow's approval to this one.
        assert!(matches!(
            decode(answer("other-flow", json!([])), "flow"),
            Err(FlowProvidersError::Protocol(_))
        ));
    }

    #[test]
    fn basal_error_frame_fixtures_keep_their_codes() {
        for (bytes, code) in [
            (
                br#"{"code":"flow_not_approved","message":"flow has no approved version"}"#
                    .as_slice(),
                FLOW_NOT_APPROVED,
            ),
            (
                br#"{"code":"not_permitted","message":"caller is not permitted"}"#,
                "not_permitted",
            ),
            (
                br#"{"code":"invalid_params","message":"unknown parameter"}"#,
                "invalid_params",
            ),
            (
                br#"{"code":"module_warming","message":"module is warming"}"#,
                "module_warming",
            ),
            (
                br#"{"code":"storage_unavailable","message":"storage is unavailable"}"#,
                "storage_unavailable",
            ),
        ] {
            let error: subc_protocol::ErrorBody = serde_json::from_slice(bytes).unwrap();
            assert_eq!(error.detail, None);
            let policy = decode(Ok(Err(error)), "flow");
            if code == FLOW_NOT_APPROVED {
                assert_eq!(
                    policy.unwrap(),
                    FlowPolicy::NotApproved {
                        message: "flow has no approved version".into()
                    }
                );
            } else {
                let error = policy.unwrap_err();
                assert_eq!(error.code(), Some(code));
                assert!(error.to_string().contains(code));
            }
        }
    }

    #[test]
    fn approval_metadata_is_required_and_strictly_validated() {
        let fixture: Value = serde_json::from_slice(BASAL_SUCCESS).unwrap();
        for hash in [
            "a".repeat(63),
            "a".repeat(65),
            "A".repeat(64),
            "g".repeat(64),
            "é".repeat(32),
        ] {
            let mut value = fixture.clone();
            value["result"]["code_hash"] = json!(hash);
            assert!(matches!(
                decode(Ok(Ok(serde_json::to_vec(&value).unwrap())), "flow"),
                Err(FlowProvidersError::Protocol(_))
            ));
        }
        for version in [json!(-1), json!(1.5), json!("7"), json!(null)] {
            let mut value = fixture.clone();
            value["result"]["version"] = version;
            assert!(matches!(
                decode(Ok(Ok(serde_json::to_vec(&value).unwrap())), "flow"),
                Err(FlowProvidersError::Protocol(_))
            ));
        }
        for field in ["version", "code_hash"] {
            let mut value = fixture.clone();
            value["result"].as_object_mut().unwrap().remove(field);
            assert!(matches!(
                decode(Ok(Ok(serde_json::to_vec(&value).unwrap())), "flow"),
                Err(FlowProvidersError::Protocol(_))
            ));
        }
        let mut value = fixture;
        value["result"]["future_field"] = json!(true);
        assert!(matches!(
            decode(Ok(Ok(serde_json::to_vec(&value).unwrap())), "flow"),
            Err(FlowProvidersError::Protocol(_))
        ));
    }
}
