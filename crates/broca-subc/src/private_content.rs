//! Asking cerebellum whether a scope has read the person's private content
//! (`private_content.query`).
//!
//! Cerebellum marks a run that read the person's Chrome, a desktop app or a site where they are
//! signed in, and marks the head that started a program. A module that is about to send that
//! content to a third-party provider asks this question first and gets `{private_content,
//! sources}` back, where the sources name which kinds of private content the scope touched.
//!
//! The question goes over broca's own module connection: the daemon answers it only for a
//! caller it knows as a module, and cerebellum admits `reserved:broca` on its management
//! surface. [`PrivateContentClient`] opens that route, asks once and closes it, so a caller can
//! run the ask on the call's own task without keeping a route open between calls.
//!
//! A failure to answer is deliberately not an error the caller may treat as "not marked": every
//! [`PrivateContentError`] leaves the caller in the same place as a "marked" answer, because
//! broca must never send private content to a third party on a guess.

use std::sync::Arc;
use std::time::Duration;

use broca_protocol::BindIdentity;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::connection::{ConnError, RouteOpenRetry, SubcConnection};

/// The module whose management surface answers `private_content.query`.
pub const CEREBELLUM_MODULE: &str = "cerebellum";

/// The one operation broca calls on cerebellum's management surface. Cerebellum refuses every
/// other operation on a route bound as `reserved:broca`; broca never asks for one.
pub const PRIVATE_CONTENT_QUERY: &str = "private_content.query";

/// How long one query waits for its reply. The daemon and cerebellum answer from their own
/// memory, so a slower reply means that side is in trouble and the caller should hear "no
/// answer" rather than block a model call on it.
const QUERY_TIMEOUT: Duration = Duration::from_secs(10);

/// One kind of private content a scope read. A kind cerebellum adds later decodes as
/// [`SourceKind::Other`] and is kept here, so a caller can tell an answer it does not
/// understand from one that names no source at all.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SourceKind {
    PersonBrowser,
    DesktopApp,
    SignedInSite,
    Other(String),
}

impl SourceKind {
    fn from_wire(token: &str) -> Self {
        match token {
            "person_browser" => Self::PersonBrowser,
            "desktop_app" => Self::DesktopApp,
            "signed_in_site" => Self::SignedInSite,
            other => Self::Other(other.to_owned()),
        }
    }
}

/// What cerebellum said about one scope.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PrivateContent {
    /// Whether the scope read any of the person's private content.
    pub marked: bool,
    /// The kinds of private content it read. A marked scope with no recognized kind is a scope
    /// broca must not draw a consent card for: there is no source it could show.
    pub sources: Vec<SourceKind>,
}

impl PrivateContent {
    /// What an unmarked scope answers.
    pub fn unmarked() -> Self {
        Self::default()
    }

    /// Whether both answers name the same kinds, in the same order, with the same mark.
    pub fn same_as(&self, other: &Self) -> bool {
        self.marked == other.marked && self.sources == other.sources
    }
}

/// Why the question could not be answered. Every variant means the caller must treat the scope
/// as marked and must not cache anything.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PrivateContentError {
    /// The route to cerebellum could not be opened (the daemon refused, there is no module
    /// connection, or the connection is down).
    Route(String),
    /// Cerebellum (or the daemon) refused the question, or answered with `unknown_method`.
    Refused {
        code: String,
        message: String,
        detail: Option<Value>,
    },
    /// No reply within [`QUERY_TIMEOUT`], or the connection failed mid-question.
    Unanswered(String),
    /// A reply arrived but was not the published answer shape.
    Protocol(String),
}

impl PrivateContentError {
    /// The refusal's code, when cerebellum refused the question.
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Refused { code, .. } => Some(code),
            _ => None,
        }
    }
}

impl std::fmt::Display for PrivateContentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Route(message) => write!(f, "private_content.query route: {message}"),
            Self::Refused {
                code,
                message,
                detail,
            } => match detail {
                Some(detail) => {
                    write!(
                        f,
                        "private_content.query refused {code}: {message} ({detail})"
                    )
                }
                None => write!(f, "private_content.query refused {code}: {message}"),
            },
            Self::Unanswered(message) => write!(f, "private_content.query unanswered: {message}"),
            Self::Protocol(message) => write!(f, "decode private_content.query: {message}"),
        }
    }
}

impl std::error::Error for PrivateContentError {}

impl From<ConnError> for PrivateContentError {
    fn from(error: ConnError) -> Self {
        match error {
            ConnError::Rejected(message) => Self::Unanswered(message.to_string()),
            other => Self::Route(other.to_string()),
        }
    }
}

/// The bare answer on cerebellum's management surface.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Answer {
    private_content: bool,
    sources: Vec<String>,
}

/// Asks cerebellum one `private_content.query`.
pub struct PrivateContentClient {
    conn: Arc<SubcConnection>,
    identity: BindIdentity,
}

impl PrivateContentClient {
    pub fn new(conn: Arc<SubcConnection>, identity: BindIdentity) -> Self {
        Self { conn, identity }
    }

    /// Ask whether `scope_ref` read the person's private content.
    ///
    /// `parent_scope_ref` is sent only when the caller has one: a program run whose head read
    /// the person's Chrome pastes that content into itself, so cerebellum unions the head's
    /// marks into the run's answer. A flow has no parent and omits it.
    pub async fn query(
        &self,
        scope_ref: &str,
        parent_scope_ref: Option<&str>,
    ) -> Result<PrivateContent, PrivateContentError> {
        // One attempt: broca waits up to ten seconds for the reply and does not retry a failed
        // question, because every failure is answered in the safe direction anyway.
        let route = self
            .conn
            .route_open_management_within(
                CEREBELLUM_MODULE,
                &self.identity,
                RouteOpenRetry::within(Duration::ZERO, Duration::ZERO),
            )
            .await
            .map_err(|error| PrivateContentError::Route(error.to_string()))?;
        // Close the route once the question is answered or timed out. A cancelled question
        // relies on the connection ending, since this code never reaches the close.
        let body = request_body(scope_ref, parent_scope_ref)
            .map_err(|error| PrivateContentError::Protocol(error.to_string()));
        let reply = match body {
            Ok(body) => match tokio::time::timeout(
                QUERY_TIMEOUT,
                self.conn.route_unary_reply(&route, body, QUERY_TIMEOUT),
            )
            .await
            {
                Ok(result) => decode(result),
                Err(_) => Err(PrivateContentError::Unanswered(format!(
                    "no reply within {QUERY_TIMEOUT:?}"
                ))),
            },
            Err(error) => Err(error),
        };
        let _ = self.conn.route_close(&route).await;
        reply
    }
}

fn request_body(
    scope_ref: &str,
    parent_scope_ref: Option<&str>,
) -> Result<Vec<u8>, serde_json::Error> {
    // Cerebellum's private-content-query-v1 contract puts method before params; within params,
    // scope_ref precedes the parent ref, when present (`parent_scope_ref`). Struct serialization
    // preserves that order; a JSON map sorts parent_scope_ref before scope_ref instead.
    #[derive(Serialize)]
    struct Params<'a> {
        scope_ref: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        parent_scope_ref: Option<&'a str>,
    }
    #[derive(Serialize)]
    struct Request<'a> {
        method: &'static str,
        params: Params<'a>,
    }
    serde_json::to_vec(&Request {
        method: PRIVATE_CONTENT_QUERY,
        params: Params {
            scope_ref,
            parent_scope_ref,
        },
    })
}

fn decode(
    reply: Result<Result<Vec<u8>, subc_protocol::ErrorBody>, ConnError>,
) -> Result<PrivateContent, PrivateContentError> {
    let bytes = match reply {
        Ok(Ok(bytes)) => bytes,
        Ok(Err(error)) => {
            return Err(PrivateContentError::Refused {
                code: error.code,
                message: error.message,
                detail: error.detail,
            })
        }
        Err(error) => return Err(PrivateContentError::from(error)),
    };
    let value: Value = serde_json::from_slice(&bytes).map_err(|_| {
        PrivateContentError::Protocol("invalid JSON; top-level keys unavailable".into())
    })?;
    if let Some(outcome) = value.get("outcome") {
        // Refusal bodies can carry context and session data. Keep only diagnostic tokens,
        // since this error is printed in a warning rather than returned as private content.
        let detail = value.get("detail").map(|detail| {
            let mut safe = serde_json::Map::new();
            for key in ["reason_code", "field"] {
                if let Some(token) = detail.get(key).and_then(Value::as_str) {
                    safe.insert(key.into(), json!(token));
                }
            }
            Value::Object(safe)
        });
        return Err(PrivateContentError::Refused {
            code: outcome.as_str().unwrap_or("unknown_outcome").into(),
            message: "request refused".into(),
            detail,
        });
    }
    let keys = value
        .as_object()
        .map(|object| object.keys().cloned().collect::<Vec<_>>());
    let result: Answer = serde_json::from_value(value).map_err(|_| {
        PrivateContentError::Protocol(format!("unexpected reply shape; top-level keys: {keys:?}"))
    })?;
    Ok(PrivateContent {
        marked: result.private_content,
        // Preserve Cerebellum's source order, rather than sorting token names alphabetically.
        sources: result
            .sources
            .iter()
            .map(|token| SourceKind::from_wire(token))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    // Cerebellum fbcc6766, docs/contracts/private-content-query-v1.md.
    // These are reply bodies from the vectors below. Hashes identify the whole golden-file
    // documents for provenance only, not the body bytes:
    // crates/cerebellum/tests/vectors/private-content-query-v1/success-marked.json
    // whole-file SHA-256 eb4c452cadd0b5d48e8855db366c599c3dcccb3408da1ea403fa5d8703032d1c
    // crates/cerebellum/tests/vectors/private-content-query-v1/success-unmarked.json
    // whole-file SHA-256 e860efcd7db12298f7af82758e8a72bc66b5e0ede81ada0c7fba40c1aa29acb2
    // crates/cerebellum/tests/vectors/private-content-query-v1/unknown-field.json
    // whole-file SHA-256 7cc2ec06859a700a91c1e6a1a57643c66ab9af848debcf9b4beb6ecaa7aeb2bc
    const CEREBELLUM_MARKED: &[u8] = br#"{"private_content":true,"sources":["signed_in_site"]}"#;
    const CEREBELLUM_UNMARKED: &[u8] = br#"{"private_content":false,"sources":[]}"#;
    const CEREBELLUM_REFUSAL: &[u8] = br#"{"context":null,"detail":{"field":"owner","reason_code":"unknown_field"},"outcome":"invalid_request","session":null}"#;

    #[test]
    fn cerebellum_bare_success_fixtures_decode() {
        assert_eq!(CEREBELLUM_MARKED.len(), 53);
        assert_eq!(CEREBELLUM_UNMARKED.len(), 38);
        let marked = decode(Ok(Ok(CEREBELLUM_MARKED.to_vec()))).unwrap();
        assert!(marked.marked);
        assert_eq!(marked.sources, vec![SourceKind::SignedInSite]);
        assert_eq!(
            decode(Ok(Ok(CEREBELLUM_UNMARKED.to_vec()))).unwrap(),
            PrivateContent::unmarked()
        );
    }

    #[test]
    fn query_request_bytes_follow_cerebellums_contract() {
        assert_eq!(request_body("test-session", None).unwrap().len(), 72);
        assert_eq!(request_body("unmarked-scope", None).unwrap().len(), 74);
        assert_eq!(
            request_body("test-session", None).unwrap(),
            br#"{"method":"private_content.query","params":{"scope_ref":"test-session"}}"#
        );
        assert_eq!(
            request_body("unmarked-scope", None).unwrap(),
            br#"{"method":"private_content.query","params":{"scope_ref":"unmarked-scope"}}"#
        );
        assert_eq!(
            request_body("program", Some("head")).unwrap(),
            br#"{"method":"private_content.query","params":{"scope_ref":"program","parent_scope_ref":"head"}}"#
        );
    }

    #[test]
    fn answers_decode_marks_and_keep_unknown_sources() {
        let answer = decode(Ok(Ok(serde_json::to_vec(&json!({
            "private_content": true, "sources": ["person_browser", "desktop_app", "signed_in_site", "future_kind"]
        }))
        .unwrap())))
        .unwrap();
        assert!(answer.marked);
        assert_eq!(
            answer.sources,
            vec![
                SourceKind::PersonBrowser,
                SourceKind::DesktopApp,
                SourceKind::SignedInSite,
                SourceKind::Other("future_kind".into())
            ]
        );
    }

    #[test]
    fn cerebellum_refusal_names_only_diagnostic_tokens() {
        assert_eq!(CEREBELLUM_REFUSAL.len(), 116);
        let error = decode(Ok(Ok(CEREBELLUM_REFUSAL.to_vec()))).unwrap_err();
        assert_eq!(error.code(), Some("invalid_request"));
        assert!(error.to_string().contains("unknown_field"));
        assert!(error.to_string().contains("owner"));
        let error = decode(Ok(Ok(br#"{"outcome":"invalid_request","context":"PRIVATE_CONTEXT","session":"PRIVATE_SESSION","detail":{"field":"scope_ref","message":"PRIVATE_DETAIL"}}"#.to_vec()))).unwrap_err();
        assert!(error.to_string().contains("scope_ref"));
        assert!(!error.to_string().contains("PRIVATE_"));
    }

    #[test]
    fn protocol_errors_name_keys_without_values() {
        for bytes in [
            br#"{"result":{"private_content":false},"secret":"PRIVATE_VALUE"}"#.as_slice(),
            br#"{"private_content":"PRIVATE_VALUE","sources":[]}"#,
            br#"{"private_content":false}"#,
            br#"["PRIVATE_VALUE"]"#,
            b"not JSON PRIVATE_VALUE",
        ] {
            let error = decode(Ok(Ok(bytes.to_vec()))).unwrap_err();
            assert!(matches!(error, PrivateContentError::Protocol(_)));
            let diagnostic = error.to_string();
            assert!(diagnostic.contains("top-level keys"));
            assert!(!diagnostic.contains("PRIVATE_VALUE"));
        }
        let error = decode(Ok(Ok(br#"{"unexpected":"PRIVATE_VALUE"}"#.to_vec()))).unwrap_err();
        assert!(error.to_string().contains("unexpected"));
    }

    #[test]
    fn a_refusal_is_not_an_unmarked_answer() {
        let error = decode(Ok(Err(subc_protocol::ErrorBody::new(
            "private_content_unavailable",
            "cerebellum cannot tell",
        ))))
        .unwrap_err();
        assert_eq!(error.code(), Some("private_content_unavailable"));
    }
}
