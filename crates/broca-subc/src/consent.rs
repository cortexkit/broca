//! The client of Cingulate's consent requester routes: it files, reads and settles consent
//! cards, in the request and reply shapes of the consent contract version broca pins.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use broca_protocol::{BindIdentity, SessionScope};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use subc_protocol::scope::CAP_SCOPES_V1;

use crate::connection::{ConnError, RouteHandle, RouteOpenRetry, SubcConnection};

pub const CONSENT_MODULE: &str = "cingulate";
const REPLY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct FilingReply {
    pub elicitation_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AnswersPage {
    pub records: Vec<Value>,
    pub cursor: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub continuation: Option<u64>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConsentState {
    Pending,
    Answered,
    Expired,
    Withdrawn,
}

/// Preserve the argument hash, tool/module/schema binding and all requester metadata,
/// including legacy choices on expired records. broca-core checks that binding
/// against the saved call before authorizing execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConsentRecord {
    pub elicitation_id: String,
    pub state: ConsentState,
    #[serde(flatten)]
    pub fields: BTreeMap<String, Value>,
}

#[derive(Debug)]
pub enum ConsentError {
    Refused {
        code: String,
        message: String,
        detail: Option<Value>,
    },
    Transport(String),
    AcceptedFilingReplyUnreadable(String),
    Protocol(String),
}

impl ConsentError {
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Refused { code, .. } => Some(code),
            Self::AcceptedFilingReplyUnreadable(_) => Some("consent_ack_unreadable"),
            _ => None,
        }
    }

    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Transport(_) | Self::AcceptedFilingReplyUnreadable(_) => true,
            Self::Protocol(_) => false,
            Self::Refused { code, .. } => {
                subc_protocol::error_codes::is_retryable_route_open(code)
                    || matches!(
                        code.as_str(),
                        "unknown_channel"
                            | "transient"
                            | "consent_unavailable"
                            | "elicitation_store_error"
                            | "cancelled"
                    )
            }
        }
    }
}

impl std::fmt::Display for ConsentError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused { code, message, .. } => write!(f, "{code} — {message}"),
            Self::Transport(message) => write!(f, "consent transport: {message}"),
            Self::Protocol(message) => write!(f, "consent protocol: {message}"),
            Self::AcceptedFilingReplyUnreadable(message) => {
                write!(f, "consent acknowledgement: {message}")
            }
        }
    }
}
impl std::error::Error for ConsentError {}
impl From<ConnError> for ConsentError {
    fn from(error: ConnError) -> Self {
        match error {
            ConnError::Rejected(message) => Self::Refused {
                code: message.code().to_owned(),
                message: message.to_string(),
                detail: None,
            },
            ConnError::Protocol(message) => Self::Protocol(message),
            other => Self::Transport(other.to_string()),
        }
    }
}

#[derive(Debug)]
pub enum WithdrawalReply {
    Acknowledged(ConsentRecord),
    NoLongerOpen { code: String },
    Retryable(ConsentError),
    Refused(ConsentError),
}

pub struct ConsentClient {
    conn: Arc<SubcConnection>,
    route: RouteHandle,
    reply_timeout: Duration,
}

impl ConsentClient {
    /// The caller must already have bound this route to the owner scope captured for the session.
    pub fn on_route(
        conn: Arc<SubcConnection>,
        route: RouteHandle,
        reply_timeout: Duration,
    ) -> Self {
        Self {
            conn,
            route,
            reply_timeout,
        }
    }

    pub async fn open(
        conn: Arc<SubcConnection>,
        identity: &BindIdentity,
        scope: &SessionScope,
        retry: RouteOpenRetry,
    ) -> Result<Self, ConsentError> {
        if !conn
            .server_capabilities()
            .await?
            .iter()
            .any(|c| c == CAP_SCOPES_V1)
        {
            return Err(ConsentError::Refused {
                code: "scope_unsupported".into(),
                message: "consent requires a daemon-attested scope".into(),
                detail: None,
            });
        }
        let selector = crate::subc_plane::frozen_scope_selector(scope).map_err(|message| {
            ConsentError::Refused {
                code: "scope_unverifiable".into(),
                message,
                detail: None,
            }
        })?;
        let route = conn
            .route_open_management_scoped_within(CONSENT_MODULE, identity, selector, retry)
            .await?;
        Ok(Self::on_route(conn, route, REPLY_TIMEOUT))
    }

    pub async fn request(&self, params: &Value) -> Result<FilingReply, ConsentError> {
        self.call("consent.request", params, self.reply_timeout)
            .await
    }

    pub async fn answers(&self, since: u64) -> Result<AnswersPage, ConsentError> {
        if since > i64::MAX as u64 {
            return Err(ConsentError::Protocol(
                "consent.answers cursor exceeds signed-64-bit range".into(),
            ));
        }
        let page: AnswersPage = self
            .call(
                "consent.answers",
                &json!({"since": since}),
                self.reply_timeout,
            )
            .await?;
        if page.cursor > i64::MAX as u64
            || page
                .continuation
                .is_some_and(|next| next != page.cursor || page.records.is_empty())
        {
            return Err(ConsentError::Protocol(
                "consent.answers returned an invalid continuation/cursor".into(),
            ));
        }
        Ok(page)
    }

    pub async fn r#await(&self, id: &str, wait_ms: u64) -> Result<ConsentRecord, ConsentError> {
        let wait_ms = wait_ms.min(120_000);
        self.record(
            "consent.await",
            &json!({"elicitation_id": id, "timeout_ms": wait_ms}),
            id,
            Duration::from_millis(wait_ms) + self.reply_timeout,
        )
        .await
    }

    pub async fn withdraw(&self, id: &str) -> WithdrawalReply {
        match self
            .record(
                "consent.withdraw",
                &json!({"elicitation_id": id}),
                id,
                self.reply_timeout,
            )
            .await
        {
            Ok(record) if record.state == ConsentState::Withdrawn => {
                WithdrawalReply::Acknowledged(record)
            }
            Ok(record) if record.state == ConsentState::Answered => WithdrawalReply::NoLongerOpen {
                code: "answered".into(),
            },
            Ok(record) if record.state == ConsentState::Expired => WithdrawalReply::NoLongerOpen {
                code: "expired".into(),
            },
            Ok(_) => WithdrawalReply::Refused(ConsentError::Protocol(
                "consent.withdraw returned a pending card".into(),
            )),
            Err(error)
                if matches!(
                    error.code(),
                    Some("elicitation_not_owner" | "elicitation_not_found")
                ) =>
            {
                WithdrawalReply::NoLongerOpen {
                    code: error.code().unwrap().to_owned(),
                }
            }
            Err(error) if error.is_retryable() => WithdrawalReply::Retryable(error),
            Err(error) => WithdrawalReply::Refused(error),
        }
    }

    async fn record(
        &self,
        method: &str,
        params: &Value,
        id: &str,
        deadline: Duration,
    ) -> Result<ConsentRecord, ConsentError> {
        let record: ConsentRecord = self.call(method, params, deadline).await?;
        if record.elicitation_id != id {
            return Err(ConsentError::Protocol(format!(
                "{method} returned card {} instead of requested card {id}",
                record.elicitation_id
            )));
        }
        Ok(record)
    }

    async fn call<R: DeserializeOwned>(
        &self,
        method: &str,
        params: &Value,
        deadline: Duration,
    ) -> Result<R, ConsentError> {
        let body = serde_json::to_vec(&json!({"method": method, "params": params}))
            .map_err(|error| ConsentError::Protocol(error.to_string()))?;
        let bytes = self
            .conn
            .route_unary_reply(&self.route, body, deadline)
            .await?
            .map_err(|error| ConsentError::Refused {
                code: error.code,
                message: error.message,
                detail: error.detail,
            })?;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Reply<R> {
            result: R,
        }
        serde_json::from_slice::<Reply<R>>(&bytes)
            .map(|reply| reply.result)
            .map_err(|error| {
                let message = format!("decode {method}: {error}");
                if method == "consent.request" {
                    ConsentError::AcceptedFilingReplyUnreadable(message)
                } else {
                    ConsentError::Protocol(message)
                }
            })
    }
}

impl Drop for ConsentClient {
    fn drop(&mut self) {
        // A scoped plane opens a separate route per operation. Closing that route
        // releases Cingulate's waiter when the await future is cancelled without a reply.
        let conn = Arc::clone(&self.conn);
        let route = self.route;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                let _ = conn.route_close(&route).await;
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::InProcessModuleEnd;

    fn bytes(name: &str) -> Vec<u8> {
        std::fs::read(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("tests/fixtures/consent")
                .join(name),
        )
        .unwrap()
    }
    fn fixture(name: &str) -> Value {
        serde_json::from_slice(&bytes(name)).unwrap()
    }
    fn wire(name: &str) -> Vec<u8> {
        let mut body = bytes(name);
        if body.last() == Some(&b'\n') {
            body.pop();
        }
        body
    }
    fn client() -> (ConsentClient, InProcessModuleEnd) {
        let (conn, route, module) = SubcConnection::in_process_for_test(5, 3);
        (
            ConsentClient::on_route(Arc::new(conn), route, Duration::from_millis(100)),
            module,
        )
    }
    async fn respond(module: &mut InProcessModuleEnd, request: &str, reply: &str) {
        let incoming = module.next_request().await;
        assert_eq!(
            incoming.body,
            wire(request),
            "actual sent bytes must equal the vendored body"
        );
        assert!(module.reply(&incoming, bytes(reply)));
    }
    #[tokio::test]
    async fn consent_withdrawn_reply_for_another_card_is_not_acknowledged() {
        let (client, mut module) = client();
        let (reply, ()) = tokio::join!(client.withdraw("requested-card"), async {
            let request = module.next_request().await;
            assert!(module.reply(
                &request,
                bytes("upstream/consent-requester-v1/withdraw-reply.json")
            ));
        });
        assert!(matches!(
            reply,
            WithdrawalReply::Refused(ConsentError::Protocol(_))
        ));
    }

    #[tokio::test]
    async fn consent_numeric_feed_keeps_continuation_and_refusals() {
        let (client, mut module) = client();
        let (page, ()) = tokio::join!(client.answers(7), async {
            let request = module.next_request().await;
            assert_eq!(
                request.body,
                br#"{"method":"consent.answers","params":{"since":7}}"#
            );
            let mut reply =
                fixture("upstream/consent-requester-v2/cerebellum-ref-answers-reply.json");
            reply["result"]["cursor"] = json!(8);
            reply["result"]["continuation"] = json!(8);
            assert!(module.reply(&request, serde_json::to_vec(&reply).unwrap()));
        });
        let page = page.unwrap();
        assert_eq!(page.continuation, Some(page.cursor));
        let (page, ()) = tokio::join!(client.answers(page.continuation.unwrap()), async {
            let request = module.next_request().await;
            assert_eq!(
                request.body,
                br#"{"method":"consent.answers","params":{"since":8}}"#
            );
            assert!(module.refuse(&request, "consent_unavailable"));
        });
        assert_eq!(page.unwrap_err().code(), Some("consent_unavailable"));
        assert!(client.answers(u64::MAX).await.is_err());
    }

    #[tokio::test]
    async fn consent_await_answers_preserve_upstream_bodies() {
        for prefix in ["cerebellum", "cerebellum-ref"] {
            let (client, mut module) = client();
            let root = format!("upstream/consent-requester-v2/{prefix}");
            let request = format!("{root}-await-request.json");
            let reply = format!("{root}-await-reply.json");
            let (record, ()) = tokio::join!(
                client.r#await("el_1", 0),
                respond(&mut module, &request, &reply)
            );
            // The reply is the pending card unchanged: a timeout grants nothing.
            let record = record.unwrap();
            assert_eq!(
                serde_json::to_vec(&json!({"result":record})).unwrap(),
                wire(&reply)
            );
            let request = format!("{root}-answers-request.json");
            let reply = format!("{root}-answers-reply.json");
            let (page, ()) =
                tokio::join!(client.answers(0), respond(&mut module, &request, &reply));
            assert_eq!(
                serde_json::to_vec(&json!({"result":page.unwrap()})).unwrap(),
                wire(&reply)
            );
        }
    }

    #[tokio::test]
    async fn consent_withdraw_matches_upstream_bodies() {
        let (client, mut module) = client();
        let (reply, ()) = tokio::join!(
            client.withdraw("el_2"),
            respond(
                &mut module,
                "upstream/consent-requester-v1/withdraw-request.json",
                "upstream/consent-requester-v1/withdraw-reply.json"
            )
        );
        let WithdrawalReply::Acknowledged(record) = reply else {
            panic!("{reply:?}")
        };
        assert_eq!(
            serde_json::to_vec(&json!({"result":record})).unwrap(),
            wire("upstream/consent-requester-v1/withdraw-reply.json")
        );
    }

    #[tokio::test]
    async fn consent_dropped_await_closes_route_within_one_second() {
        let (client, mut module) = client();
        let conn = client.conn.clone();
        let route = client.route;
        let task = tokio::spawn(async move { client.r#await("el_1", u64::MAX).await });
        let request = tokio::time::timeout(Duration::from_secs(1), module.next_request())
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&request.body).unwrap();
        assert_eq!(body["params"]["timeout_ms"], 120_000);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                if module
                    .take_written_for_test()
                    .iter()
                    .any(|frame| frame.header.ty == subc_protocol::FrameType::Goodbye)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("cancelled consent wait must close its route");
        assert!(!conn.route_is_open(&route));
        assert!(!module.reply(
            &request,
            bytes("upstream/consent-requester-v2/cerebellum-await-reply.json")
        ));
    }
}
