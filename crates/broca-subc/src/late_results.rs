//! The consumer half of the tool-provider `late_results` feed.
//!
//! broca is the custodian of every call it dispatches: it holds the call's `call_key`, so a
//! provider settles that call later and broca pulls the entry. The provider never pushes. Each
//! entry is served only to the connection whose verified principal is its custodian, so a feed
//! belongs to one (provider module, broca's principal) pair, with one cursor and one ack stream.
//!
//! This module is only the wire: one unscoped route per provider module, the two operations, and
//! the typed decoding of their replies. Which entry is delivered, acked or retried is decided by
//! the intake that drives it.

use std::{sync::Arc, time::Duration};

use broca_protocol::BindIdentity;
use cortexkit_role_tool_provider::late_results::{
    AckRequest, Cursor, LateResultsReply, LateResultsRequest,
};
use serde::Serialize;

use crate::connection::{ConnError, RouteHandle, SubcConnection};

/// Pull once. Served by a provider that declares the `late_results` session capability.
pub const LATE_RESULTS: &str = cortexkit_role_tool_provider::ops::LATE_RESULTS;
/// Acknowledge entries through a cursor the provider returned.
pub const LATE_RESULTS_ACK: &str = cortexkit_role_tool_provider::ops::LATE_RESULTS_ACK;

/// The daemon refused the `since` cursor: it is from another provider incarnation, or past the
/// entries this one has. The reader then re-reads from the start and dedupes on `IntakeKey`.
pub const CURSOR_INCARNATION_CHANGED: &str =
    cortexkit_role_tool_provider::late_results::CURSOR_INCARNATION_CHANGED;

/// A pull or ack may make the provider scan its whole log, which legitimately takes longer than
/// a tool call's route silence bound.
const REPLY_TIMEOUT: Duration = Duration::from_secs(20);

#[derive(Debug)]
pub enum LateResultsError {
    /// The provider or the daemon answered with a typed refusal carrying `code`.
    Refused { code: String, message: String },
    /// The request may have reached the provider; nothing can be concluded from the failure.
    Transport(String),
    /// The provider or daemon answered something this build cannot read.
    Protocol(String),
}

impl LateResultsError {
    pub fn is_cursor_incarnation_changed(&self) -> bool {
        matches!(self, LateResultsError::Refused { code, .. } if code == CURSOR_INCARNATION_CHANGED)
    }

    /// Only a `Protocol` answer is permanent: a refusal may lift, and a transport failure says
    /// nothing about the feed.
    pub fn is_permanent(&self) -> bool {
        matches!(self, LateResultsError::Protocol(_))
    }
}

impl std::fmt::Display for LateResultsError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LateResultsError::Refused { code, message } => write!(f, "{code} — {message}"),
            LateResultsError::Transport(message) => write!(f, "late_results transport: {message}"),
            LateResultsError::Protocol(message) => write!(f, "late_results protocol: {message}"),
        }
    }
}

impl std::error::Error for LateResultsError {}

impl From<ConnError> for LateResultsError {
    fn from(error: ConnError) -> Self {
        match error {
            ConnError::Rejected(refusal) => LateResultsError::Refused {
                code: refusal.code().to_owned(),
                message: refusal.to_string(),
            },
            ConnError::Protocol(message) => LateResultsError::Protocol(message),
            other => LateResultsError::Transport(other.to_string()),
        }
    }
}

#[derive(Serialize)]
struct Request<'a, P> {
    method: &'a str,
    params: P,
}

/// One provider module's feed on the consumer-identity connection.
pub struct LateResultsFeed {
    conn: Arc<SubcConnection>,
    route: RouteHandle,
    module: String,
    reply_timeout: Duration,
}

impl LateResultsFeed {
    /// Open the feed's one unscoped route. The custodian comes from this connection's consumer
    /// identity, not from `identity`, which only names the bind this route is opened under.
    pub async fn open(
        conn: Arc<SubcConnection>,
        module: &str,
        identity: &BindIdentity,
    ) -> Result<Self, LateResultsError> {
        if conn.consumer_module_id().is_none() {
            return Err(LateResultsError::Refused {
                code: "consumer_identity_required".into(),
                message:
                    "a late-results feed is served only to a connection with a consumer identity"
                        .into(),
            });
        }
        let route = conn
            .route_open_tool_provider_v1(module, identity, None)
            .await?;
        Ok(Self {
            conn,
            route,
            module: module.to_owned(),
            reply_timeout: REPLY_TIMEOUT,
        })
    }

    pub fn module(&self) -> &str {
        &self.module
    }

    /// Give the feed its own reply budget, so a test does not wait the production one.
    pub fn with_reply_timeout(mut self, reply_timeout: Duration) -> Self {
        self.reply_timeout = reply_timeout;
        self
    }

    /// Pull the entries after `since`; `None` reads from the start of this incarnation.
    pub async fn pull(
        &self,
        since: Option<&Cursor>,
        limit: Option<u32>,
    ) -> Result<LateResultsReply, LateResultsError> {
        let mut request = LateResultsRequest::new();
        if let Some(since) = since {
            request = request.with_since(since.clone());
        }
        if let Some(limit) = limit {
            request = request.with_limit(limit);
        }
        let body = self.body(LATE_RESULTS, &request).await?;
        let reply: LateResultsReply = serde_json::from_slice(&body).map_err(|error| {
            LateResultsError::Protocol(format!("decode late_results reply: {error}"))
        })?;
        Ok(reply)
    }

    /// Acknowledge every entry up to and including a cursor a reply returned.
    pub async fn ack(&self, through: &Cursor) -> Result<(), LateResultsError> {
        let request = AckRequest {
            through: through.clone(),
        };
        let body = self.body(LATE_RESULTS_ACK, &request).await?;
        if !body.is_empty() {
            if let Ok(value) = serde_json::from_slice::<serde_json::Value>(&body) {
                if let Some(value) = value.as_object() {
                    if !value.is_empty() {
                        return Err(LateResultsError::Protocol(format!(
                            "late_results.ack answered {value:?} where an empty object was expected"
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn close(&self) -> Result<(), LateResultsError> {
        self.conn.route_close(&self.route).await.map_err(Into::into)
    }

    async fn body<P: Serialize>(
        &self,
        method: &str,
        params: &P,
    ) -> Result<Vec<u8>, LateResultsError> {
        let body = serde_json::to_vec(&Request { method, params })
            .map_err(|error| LateResultsError::Protocol(error.to_string()))?;
        let terminal = self
            .conn
            .route_request_terminal_with_deadline(&self.route, body, self.reply_timeout)
            .await?;
        match terminal {
            Ok(body) => Ok(body),
            Err(error) => Err(crate::connection::ConnError::Rejected(
                crate::connection::decode_error_body(&error),
            )
            .into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::InProcessModuleEnd;
    use cortexkit_role_tool_provider::late_results::{kinds, LateEntry};
    use serde_json::json;

    fn entry(key: &str, event: &str) -> LateEntry {
        LateEntry::new(
            kinds::RESULT,
            "owner",
            "ref-1",
            3,
            "reserved:broca",
            key,
            event,
            1_761_220_000_000,
        )
        .with_result(json!({"content": [{"type": "text", "text": "late"}]}))
    }

    fn reply(entries: &[LateEntry], seq: u64, more: bool) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "entries": entries,
            "cursor": {"provider_incarnation": "inc-1", "seq": seq},
            "more": more,
        }))
        .unwrap()
    }

    fn feed(reply_timeout: Duration) -> (LateResultsFeed, InProcessModuleEnd) {
        let (conn, route, module) = SubcConnection::in_process_for_test(5, 3);
        (
            LateResultsFeed {
                conn: Arc::new(conn),
                route,
                module: "core".into(),
                reply_timeout,
            },
            module,
        )
    }

    #[tokio::test]
    async fn a_pull_sends_the_operation_and_decodes_the_reply_verbatim() {
        let (feed, mut module) = feed(Duration::from_millis(500));
        let provider = tokio::spawn(async move {
            let request = module.next_request().await;
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
                json!({"method": "late_results", "params": {"since": null}})
            );
            module.reply(&request, reply(&[entry("lin_9:4", "ev-1")], 1, false));
            module
        });
        let pulled = feed.pull(None, None).await.unwrap();
        assert_eq!(pulled.entries.len(), 1);
        assert_eq!(pulled.cursor.seq, 1);
        assert!(!pulled.more);
        provider.await.unwrap();
    }

    #[tokio::test]
    async fn a_limited_re_pull_names_its_cursor_and_limit_and_the_ack_sends_that_cursor() {
        let (feed, mut module) = feed(Duration::from_millis(500));
        let provider = tokio::spawn(async move {
            let request = module.next_request().await;
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
                json!({
                    "method": "late_results",
                    "params": {"since": {"provider_incarnation": "inc-1", "seq": 1}, "limit": 1}
                })
            );
            module.reply(&request, reply(&[entry("lin_9:4", "ev-1")], 2, false));
            let request = module.next_request().await;
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
                json!({
                    "method": "late_results.ack",
                    "params": {"through": {"provider_incarnation": "inc-1", "seq": 2}}
                })
            );
            module.reply(&request, b"{}".to_vec());
            module
        });
        let pulled = feed
            .pull(
                Some(&Cursor {
                    provider_incarnation: "inc-1".into(),
                    seq: 1,
                }),
                Some(1),
            )
            .await
            .unwrap();
        feed.ack(&pulled.cursor).await.unwrap();
        provider.await.unwrap();
    }

    #[tokio::test]
    async fn a_refused_cursor_is_reported_as_an_incarnation_change() {
        let (feed, mut module) = feed(Duration::from_millis(500));
        let provider = tokio::spawn(async move {
            let request = module.next_request().await;
            module.refuse(&request, CURSOR_INCARNATION_CHANGED);
            module
        });
        let error = feed
            .pull(
                Some(&Cursor {
                    provider_incarnation: "inc-0".into(),
                    seq: 9,
                }),
                None,
            )
            .await
            .unwrap_err();
        assert!(error.is_cursor_incarnation_changed(), "{error}");
        assert!(!error.is_permanent());
        provider.await.unwrap();
    }

    #[tokio::test]
    async fn an_ack_that_answers_a_body_is_not_taken_for_success() {
        let (feed, mut module) = feed(Duration::from_millis(500));
        let provider = tokio::spawn(async move {
            let request = module.next_request().await;
            module.reply(&request, br#"{"acknowledged": false}"#.to_vec());
            module
        });
        let error = feed
            .ack(&Cursor {
                provider_incarnation: "inc-1".into(),
                seq: 2,
            })
            .await
            .unwrap_err();
        assert!(error.is_permanent(), "{error}");
        provider.await.unwrap();
    }

    #[tokio::test]
    async fn an_unreadable_pull_reply_is_a_protocol_error_not_a_retry() {
        let (feed, mut module) = feed(Duration::from_millis(500));
        let provider = tokio::spawn(async move {
            let request = module.next_request().await;
            module.reply(&request, br#"{"entries": "nope"}"#.to_vec());
            module
        });
        let error = feed.pull(None, None).await.unwrap_err();
        assert!(error.is_permanent(), "{error}");
        provider.await.unwrap();
    }
}
