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

/// Withdraw on the original call's frozen scope and carrier connection, not the unscoped
/// late-results route. The provider rejects a scoped record reached through another scope.
pub async fn withdraw_held_call(
    conn: &SubcConnection,
    call: &broca_core::tool_plane::LateResultRegistration,
) -> (
    cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
    serde_json::Value,
) {
    if conn.consumer_module_id().is_none() {
        return withdraw_error(ConnError::Protocol(
            "tool.withdraw requires the original carrier's consumer identity".into(),
        ));
    }
    let selector = match call
        .scope
        .as_ref()
        .map(crate::subc_plane::frozen_scope_selector)
        .transpose()
    {
        Ok(selector) => selector,
        Err(message) => return withdraw_error(ConnError::Protocol(message)),
    };
    let route = match conn
        .route_open_tool_provider_v1(&call.module, &call.bind, selector)
        .await
    {
        Ok(route) => route,
        Err(error) => return withdraw_error(error),
    };
    let result = withdraw_on_route(conn, &route, &call.call_key).await;
    let _ = conn.route_close(&route).await;
    result
}

async fn withdraw_on_route(
    conn: &SubcConnection,
    route: &RouteHandle,
    call_key: &str,
) -> (
    cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
    serde_json::Value,
) {
    use cortexkit_role_tool_provider::withdraw::{caller_verdict, WithdrawArguments};
    let body = serde_json::to_vec(&WithdrawArguments::new(call_key).into_request())
        .expect("withdraw request serializes");
    match conn
        .route_request_terminal_with_deadline(route, body, REPLY_TIMEOUT)
        .await
    {
        Ok(Ok(bytes)) => {
            let body = serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| serde_json::json!({"unreadable_body": bytes}));
            (caller_verdict(Ok(&body)), body)
        }
        Ok(Err(bytes)) => {
            let raw = serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| serde_json::json!({"unreadable_body": bytes}));
            let error = serde_json::from_slice::<subc_protocol::ErrorBody>(&bytes).unwrap_or_else(
                |error| subc_protocol::ErrorBody::new("invalid_withdraw_reply", error.to_string()),
            );
            (caller_verdict(Err(&error)), raw)
        }
        Err(error) => withdraw_error(error),
    }
}

fn withdraw_error(
    error: ConnError,
) -> (
    cortexkit_role_tool_provider::withdraw::WithdrawVerdict,
    serde_json::Value,
) {
    use cortexkit_role_tool_provider::withdraw::caller_verdict;
    let body = match error {
        ConnError::Rejected(refusal) => {
            subc_protocol::ErrorBody::new(refusal.code(), refusal.to_string())
        }
        ConnError::Protocol(message) => {
            subc_protocol::ErrorBody::new("invalid_withdraw_reply", message)
        }
        other => subc_protocol::ErrorBody::new("target_unavailable", other.to_string()),
    };
    let raw = serde_json::to_value(&body).expect("error serializes");
    (caller_verdict(Err(&body)), raw)
}

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

/// A role op on a tool route travels as a call-shaped body naming the op, exactly as the
/// tool-provider conformance kit sends `late_results` and `late_results.ack`.
#[derive(Serialize)]
struct Request<'a, P> {
    name: &'a str,
    arguments: P,
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
        let body = serde_json::to_vec(&Request {
            name: method,
            arguments: params,
        })
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

/// The tool result a late entry settles its held call with, or `None` for an entry that is not
/// a settlement (`progress`, or a kind this build does not know).
///
/// The entry IS the call's answer: a held call is never dispatched again to obtain it.
///
/// - `result` is the reply the held call's synchronous answer would have been. The held call
///   was sent on a route declaring `content/v1`, to a tool that opted into it, so the body is
///   read with the same `content/v1` mapper a synchronous reply goes through. A body that does
///   not validate, or that is itself held again, closes the call with an error naming the
///   problem rather than letting interim content reach the model.
/// - `reduced: true` means the provider kept only the call's outcome code (contract §7), so the
///   model is told the call finished and that only its outcome survives.
/// - `not_started` and `expired` are the contract's terminal answers: the call never ran, or its
///   result was dropped before broca collected it. Both close the call as an error result;
///   neither is guessed from the absence of an entry.
pub fn late_entry_tool_result(
    tool_call_id: &broca_protocol::ToolCallId,
    entry: &cortexkit_role_tool_provider::late_results::LateEntry,
) -> Option<broca_protocol::ToolResult> {
    use cortexkit_role_tool_provider::late_results::EntryKind;
    let error = |message: String| broca_protocol::ToolResult {
        tool_call_id: tool_call_id.clone(),
        output: broca_protocol::ToolOutput::error(message),
        is_error: true,
    };
    match entry.kind() {
        EntryKind::Result if entry.reduced => Some(error(format!(
            "This call finished, but its provider kept only its outcome ({}), not its result.",
            entry.outcome.as_deref().unwrap_or("unknown")
        ))),
        EntryKind::Result => Some(match &entry.result {
            None => error("Its provider settled this call without a result.".into()),
            Some(body) => {
                match serde_json::from_value::<cortexkit_role_tool_provider::result::ToolResult>(
                    body.clone(),
                ) {
                    Ok(reply) => crate::content_v1::map_content_v1_result(tool_call_id, &reply)
                        .unwrap_or_else(|problem| {
                            error(format!(
                                "Its provider settled this call with an unreadable result: {problem}"
                            ))
                        }),
                    Err(problem) => error(format!(
                        "Its provider settled this call with an unreadable result: {problem}"
                    )),
                }
            }
        }),
        EntryKind::NotStarted => Some(error(format!(
            "This call never ran: its provider settled it as not started ({}).",
            entry.reason.as_deref().unwrap_or("no reason given")
        ))),
        EntryKind::Expired => Some(error(
            "This call's result expired at its provider before it was collected, so the result \
             is lost. The call may have run."
                .into(),
        )),
        _ => None,
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
    async fn withdraw_route_sends_the_pinned_request_and_reads_a_bare_answer() {
        let (feed, mut module) = feed(Duration::from_millis(500));
        let provider = tokio::spawn(async move {
            let request = module.next_request().await;
            let body: serde_json::Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(
                body,
                json!({"name": "tool.withdraw", "arguments": {"call_key": "lineage:4"}})
            );
            module.reply(&request, br#"{"answer":"withdrawn"}"#.to_vec());
        });
        let (verdict, raw) = withdraw_on_route(&feed.conn, &feed.route, "lineage:4").await;
        assert_eq!(
            verdict,
            cortexkit_role_tool_provider::withdraw::WithdrawVerdict::NeverRan
        );
        assert_eq!(raw, json!({"answer": "withdrawn"}));
        provider.await.unwrap();
    }

    #[tokio::test]
    async fn withdraw_route_preserves_final_unclassified_and_refused_answers() {
        for raw in [
            json!({"answer": "future_answer", "detail": 1}),
            json!({"answer": "refused", "reason": "denied"}),
            json!({"result": {"answer": "withdrawn"}}),
        ] {
            let (feed, mut module) = feed(Duration::from_millis(500));
            let answer = raw.clone();
            let provider = tokio::spawn(async move {
                let request = module.next_request().await;
                module.reply(&request, serde_json::to_vec(&answer).unwrap());
            });
            let (verdict, body) = withdraw_on_route(&feed.conn, &feed.route, "lineage:4").await;
            assert!(!verdict.retries());
            assert_eq!(body, raw);
            provider.await.unwrap();
        }
    }

    #[tokio::test]
    async fn withdraw_route_retries_only_transient_refusals() {
        for (code, retries) in [
            ("scope_not_synced", true),
            ("target_unavailable", true),
            ("withdraw_not_permitted", false),
            ("unknown_operation", false),
        ] {
            let (feed, mut module) = feed(Duration::from_millis(500));
            let provider = tokio::spawn(async move {
                let request = module.next_request().await;
                module.refuse(&request, code);
            });
            let (verdict, raw) = withdraw_on_route(&feed.conn, &feed.route, "lineage:4").await;
            assert_eq!(verdict.retries(), retries, "{code}");
            assert_eq!(raw["code"], code);
            provider.await.unwrap();
        }
    }

    #[tokio::test]
    async fn a_pull_sends_the_operation_and_decodes_the_reply_verbatim() {
        let (feed, mut module) = feed(Duration::from_millis(500));
        let provider = tokio::spawn(async move {
            let request = module.next_request().await;
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
                json!({"name": "late_results", "arguments": {"since": null}})
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
                    "name": "late_results",
                    "arguments": {"since": {"provider_incarnation": "inc-1", "seq": 1}, "limit": 1}
                })
            );
            module.reply(&request, reply(&[entry("lin_9:4", "ev-1")], 2, false));
            let request = module.next_request().await;
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
                json!({
                    "name": "late_results.ack",
                    "arguments": {"through": {"provider_incarnation": "inc-1", "seq": 2}}
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

    fn settled(kind: &str) -> LateEntry {
        LateEntry::new(
            kind,
            "owner",
            "ref-1",
            3,
            "reserved:broca",
            "lin_9:4",
            "ev-1",
            1_761_220_000_000,
        )
    }

    /// The model-facing text of every terminal a late entry can write, pinned byte for byte:
    /// the result body through the `content/v1` mapper, the reduced outcome, and the two
    /// terminal kinds. Progress and unknown kinds settle nothing.
    #[test]
    fn a_late_entry_becomes_its_held_calls_result_with_pinned_text() {
        let id = broca_protocol::ToolCallId("tc_1".into());
        let render = |entry: &LateEntry| {
            late_entry_tool_result(&id, entry).map(|result| serde_json::to_value(result).unwrap())
        };
        // A result body is read exactly as the synchronous reply would have been.
        let body = json!({"content": [{"type": "text", "text": "the answer"}], "isError": true, "errorCode": "not_found"});
        let synchronous = crate::content_v1::map_content_v1_result(
            &id,
            &serde_json::from_value(body.clone()).unwrap(),
        )
        .unwrap();
        assert_eq!(
            render(&settled(kinds::RESULT).with_result(body)),
            Some(serde_json::to_value(synchronous).unwrap())
        );
        assert_eq!(
            render(
                &settled(kinds::RESULT)
                    .with_reduced(true)
                    .with_outcome("completed")
            ),
            Some(
                json!({"tool_call_id": "tc_1", "output": {"kind": "error", "message": "This call finished, but its provider kept only its outcome (completed), not its result."}, "is_error": true})
            )
        );
        assert_eq!(
            render(&settled(kinds::NOT_STARTED).with_reason("restart_before_dispatch")),
            Some(
                json!({"tool_call_id": "tc_1", "output": {"kind": "error", "message": "This call never ran: its provider settled it as not started (restart_before_dispatch)."}, "is_error": true})
            )
        );
        assert_eq!(
            render(&settled(kinds::EXPIRED)),
            Some(
                json!({"tool_call_id": "tc_1", "output": {"kind": "error", "message": "This call's result expired at its provider before it was collected, so the result is lost. The call may have run."}, "is_error": true})
            )
        );
        // A late body that is itself held again never reaches the model as interim content.
        let rehold = render(&settled(kinds::RESULT).with_result(json!({
            "content": [{"type": "text", "text": "STILL WAITING"}],
            "held": {"reason": "running"}
        })))
        .unwrap();
        assert_eq!(rehold["is_error"], true);
        assert!(!rehold.to_string().contains("STILL WAITING"), "{rehold}");
        assert_eq!(
            render(&settled(kinds::PROGRESS).with_text("half way")),
            None
        );
        assert_eq!(render(&settled("later_kind")), None);
    }
}
