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
use serde::Deserialize;
use serde_json::{json, Value};
use subc_protocol::Principal;

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

/// The published answer: `private_content`, plus `sources`, which may be omitted and then
/// decodes as an empty list.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Answer {
    private_content: bool,
    sources: Option<Vec<String>>,
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

    /// Ask whether the scope `owner`/`scope_ref` read the person's private content.
    ///
    /// `parent_scope_ref` is sent only when the caller has one: a program run whose head read
    /// the person's Chrome pastes that content into itself, so cerebellum unions the head's
    /// marks into the run's answer. A flow has no parent and omits it.
    pub async fn query(
        &self,
        owner: &Principal,
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
        let body = serde_json::to_vec(&json!({
            "method": PRIVATE_CONTENT_QUERY,
            "params": request_params(owner, scope_ref, parent_scope_ref),
        }))
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

fn request_params(owner: &Principal, scope_ref: &str, parent_scope_ref: Option<&str>) -> Value {
    // `owner` is the daemon's scope identity, in the same shape `scope.describe` takes, so the
    // receiving module never has to parse a display string broca invented.
    let mut params = json!({"owner": owner, "scope_ref": scope_ref});
    if let Some(parent) = parent_scope_ref {
        params["parent_scope_ref"] = json!(parent);
    }
    params
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
    #[derive(Deserialize)]
    struct Envelope {
        result: Answer,
    }
    let Envelope { result } = serde_json::from_slice::<Envelope>(&bytes)
        .map_err(|error| PrivateContentError::Protocol(error.to_string()))?;
    Ok(PrivateContent {
        marked: result.private_content,
        sources: result
            .sources
            .unwrap_or_default()
            .iter()
            .map(|token| SourceKind::from_wire(token))
            .collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_request_omits_a_parent_it_was_not_given() {
        let owner = Principal::Reserved {
            module_id: "owner".into(),
        };
        let run = request_params(&owner, "run", None);
        assert_eq!(
            run,
            json!({"owner":{"kind":"reserved","module_id":"owner"},"scope_ref":"run"})
        );
        let program = request_params(&owner, "program", Some("head"));
        assert_eq!(
            program,
            json!({
                "owner":{"kind":"reserved","module_id":"owner"},
                "scope_ref":"program",
                "parent_scope_ref":"head"
            })
        );
    }

    #[test]
    fn answers_decode_marks_and_keep_unknown_sources() {
        let answer = decode(Ok(Ok(serde_json::to_vec(&json!({
            "result": {"private_content": true, "sources": ["person_browser", "future_kind"]}
        }))
        .unwrap())))
        .unwrap();
        assert!(answer.marked);
        assert_eq!(
            answer.sources,
            vec![
                SourceKind::PersonBrowser,
                SourceKind::Other("future_kind".into())
            ]
        );
        let unmarked = decode(Ok(Ok(serde_json::to_vec(&json!({
            "result": {"private_content": false}
        }))
        .unwrap())))
        .unwrap();
        assert_eq!(unmarked, PrivateContent::unmarked());
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
