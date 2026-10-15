//! Cingulate's scoped consent requester transport. Core owns authorization,
//! durable intake and retries; this module translates the pinned requester wire.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use broca_core::tool_plane::{
    ApprovalCursor, ApprovalExecution, ApprovalFiling, ApprovalPage, ApprovalQuestion,
    ApprovalRouteError, ApprovalWithdrawal, BatchCtx, ElicitationPlane, WalRecord,
};
use broca_protocol::{BindIdentity, SessionScope};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use subc_protocol::scope::{ScopeSelector, CAP_SCOPES_V1};

use crate::connection::{rejection_code, ConnError, RouteHandle, RouteOpenRetry, SubcConnection};

pub const CONSENT_MODULE: &str = "cingulate";
// ApprovalCursor requires an incarnation string, but consent supplies only a
// numeric position. This fixed label identifies the encoding, not a daemon lifetime.
const CURSOR_ENCODING: &str = "cingulate-owner-feed-v1";
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

impl ConsentRecord {
    pub fn is_terminal(&self) -> bool {
        self.state != ConsentState::Pending
    }

    fn question(self) -> ApprovalQuestion {
        ApprovalQuestion {
            terminal: self.is_terminal(),
            record: serde_json::to_value(self).expect("requester record serializes"),
        }
    }
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
                code: rejection_code(&message).to_owned(),
                message,
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
        let owner =
            crate::subc_plane::owner_principal(scope).map_err(|message| ConsentError::Refused {
                code: "scope_unverifiable".into(),
                message,
                detail: None,
            })?;
        let route = conn
            .route_open_management_scoped_within(
                CONSENT_MODULE,
                identity,
                ScopeSelector {
                    owner,
                    scope_ref: scope.scope_ref.clone(),
                    scope_epoch: Some(scope.scope_epoch),
                },
                retry,
            )
            .await?;
        Ok(Self::on_route(conn, route, REPLY_TIMEOUT))
    }

    pub async fn close(&self) -> Result<(), ConsentError> {
        self.conn.route_close(&self.route).await.map_err(Into::into)
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

    pub async fn get(&self, id: &str) -> Result<ConsentRecord, ConsentError> {
        self.record(
            "consent.get",
            &json!({"elicitation_id": id}),
            id,
            self.reply_timeout,
        )
        .await
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

    pub async fn detach(&self, id: &str) -> Result<ConsentRecord, ConsentError> {
        self.record("consent.detach", &json!({"id": id}), id, self.reply_timeout)
            .await
    }

    pub async fn report_execution(
        &self,
        id: &str,
        execution: ApprovalExecution,
    ) -> Result<ConsentRecord, ConsentError> {
        let execution = match execution {
            ApprovalExecution::Executed => "executed",
            ApprovalExecution::ApprovedNotExecuted => "approved_not_executed",
            ApprovalExecution::OutcomeUnknown => "outcome_unknown",
        };
        self.record(
            "consent.report_execution",
            &json!({"elicitation_id": id, "execution": execution}),
            id,
            self.reply_timeout,
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

fn core_error(error: ConsentError) -> ApprovalRouteError {
    ApprovalRouteError {
        retryable: error.is_retryable(),
        code: error
            .code()
            .unwrap_or("consent_protocol_or_transport")
            .to_owned(),
    }
}

fn refusal(code: &str) -> ApprovalRouteError {
    ApprovalRouteError {
        code: code.into(),
        retryable: false,
    }
}

fn held_request(
    call_key: &str,
    prepared: &WalRecord,
    now: u64,
) -> Result<Value, ApprovalRouteError> {
    // A UUID lineage (36 bytes), colon and 1–20 decimal sequence digits always
    // fit the requester's 128-byte correlation limit (consent-requester-v2), including u64::MAX.
    debug_assert!(
        (38..=57).contains(&call_key.len()) && call_key.bytes().all(|b| b.is_ascii_graphic())
    );
    let WalRecord::ToolCallDeferred {
        module,
        tool,
        schema_pin,
        args_hash,
        ask,
        ..
    } = prepared
    else {
        return Err(refusal("consent_prepared_invalid"));
    };
    if schema_pin.is_some() {
        return Err(refusal("approval_schema_pin_unsupported"));
    }
    if ask
        .options
        .iter()
        .any(|option| !matches!(option.as_str(), "approve" | "deny"))
    {
        return Err(refusal("approval_options_unsupported"));
    }
    let expires_in_ms = ask.expires_at_ms.saturating_sub(now);
    if expires_in_ms == 0 {
        return Err(refusal("expired"));
    }
    Ok(json!({
        "kind": "consent", "title": format!("Run {tool}"), "prompt": ask.prompt,
        "options": [{"id": "approve", "label": "Approve", "effect": "grant"}, {"id": "deny", "label": "Deny", "effect": "decline"}],
        "default": "deny", "urgency": "normal", "on_expiry": "deny",
        "material_damage": ask.material_damage,
        "late_execution": match ask.late_execution { broca_core::tool_plane::LateExecution::Execute => "execute", broca_core::tool_plane::LateExecution::NotifyOnly => "notify_only" },
        "target": {"kind": "module", "label": module.as_deref().unwrap_or("broca")},
        "expires_in_ms": expires_in_ms, "requester_ref": call_key, "dedup_key": call_key,
        "binding": broca_core::consent::binding(prepared), "args_digest": args_hash,
    }))
}

/// File through the session's saved scope reference, epoch and owner. A separate
/// route per operation lets cancellation close only that operation's remote waiter.
pub struct ScopedConsentPlane {
    conn: Arc<SubcConnection>,
    retry: RouteOpenRetry,
}

impl ScopedConsentPlane {
    pub fn new(conn: Arc<SubcConnection>, retry: RouteOpenRetry) -> Self {
        Self { conn, retry }
    }

    async fn client(&self, ctx: &BatchCtx) -> Result<ConsentClient, ApprovalRouteError> {
        let scope = ctx
            .scope
            .as_ref()
            .ok_or_else(|| refusal("approve_requires_scope"))?;
        ConsentClient::open(Arc::clone(&self.conn), &ctx.bind, scope, self.retry)
            .await
            .map_err(core_error)
    }
}

#[async_trait::async_trait]
impl ElicitationPlane for ScopedConsentPlane {
    async fn request(
        &self,
        ctx: &BatchCtx,
        key: &str,
        prepared: &WalRecord,
        now: u64,
    ) -> Result<ApprovalFiling, ApprovalRouteError> {
        let params = held_request(key, prepared, now)?;
        let client = self.client(ctx).await?;
        let result = client
            .request(&params)
            .await
            .map(|reply| ApprovalFiling {
                elicitation_id: reply.elicitation_id,
                // Filing returns only an id, not an authorization hash. broca-core
                // compares the answer's argument hash, tool, module and schema pin
                // with the saved call before allowing execution.
                args_hash: params["args_digest"]
                    .as_str()
                    .expect("frozen args hash")
                    .into(),
            })
            .map_err(core_error);
        let _ = client.close().await;
        result
    }

    async fn answers(
        &self,
        ctx: &BatchCtx,
        since: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        if since.is_some_and(|cursor| cursor.provider_incarnation != CURSOR_ENCODING) {
            return Err(refusal("cursor_incarnation_changed"));
        }
        let client = self.client(ctx).await?;
        let result = client
            .answers(since.map_or(0, |cursor| cursor.seq))
            .await
            .map(|page| ApprovalPage {
                records: page.records,
                // Partial pages resume after the last returned row; final pages
                // resume at the store's latest position. broca-core keeps reading
                // until an empty page confirms that the scan reached the end.
                cursor: ApprovalCursor {
                    provider_incarnation: CURSOR_ENCODING.into(),
                    seq: page.continuation.unwrap_or(page.cursor),
                },
            })
            .map_err(core_error);
        let _ = client.close().await;
        result
    }

    async fn get(&self, ctx: &BatchCtx, id: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        let client = self.client(ctx).await?;
        let result = client
            .get(id)
            .await
            .map(ConsentRecord::question)
            .map_err(core_error);
        let _ = client.close().await;
        result
    }

    async fn r#await(
        &self,
        ctx: &BatchCtx,
        id: &str,
        wait_ms: u64,
    ) -> Result<ApprovalQuestion, ApprovalRouteError> {
        let client = self.client(ctx).await?;
        let result = client
            .r#await(id, wait_ms)
            .await
            .map(ConsentRecord::question)
            .map_err(core_error);
        let _ = client.close().await;
        result
    }

    async fn ack(
        &self,
        _ctx: &BatchCtx,
        _cursor: &ApprovalCursor,
    ) -> Result<(), ApprovalRouteError> {
        // Consent ack discards all answers through a cursor for the bound owner,
        // including other sessions' answers. No individual run may do that.
        Err(refusal("consent_owner_feed_ack_forbidden"))
    }

    async fn report_execution(
        &self,
        ctx: &BatchCtx,
        id: &str,
        execution: ApprovalExecution,
    ) -> Result<(), ApprovalRouteError> {
        let client = self.client(ctx).await?;
        let result = client
            .report_execution(id, execution)
            .await
            .map(|_| ())
            .map_err(core_error);
        let _ = client.close().await;
        result
    }

    async fn detach(
        &self,
        ctx: &BatchCtx,
        id: &str,
    ) -> Result<ApprovalQuestion, ApprovalRouteError> {
        let client = self.client(ctx).await?;
        let result = client
            .detach(id)
            .await
            .map(ConsentRecord::question)
            .map_err(core_error);
        let _ = client.close().await;
        result
    }

    async fn withdraw(&self, ctx: &BatchCtx, id: &str) -> ApprovalWithdrawal {
        let client = match self.client(ctx).await {
            Ok(client) => client,
            Err(error) if error.retryable => return ApprovalWithdrawal::Retry(error),
            Err(error) => return ApprovalWithdrawal::NotSent { reason: error.code },
        };
        let result = match client.withdraw(id).await {
            WithdrawalReply::Acknowledged(_) => ApprovalWithdrawal::Acknowledged,
            WithdrawalReply::NoLongerOpen { code } => ApprovalWithdrawal::NotSent { reason: code },
            WithdrawalReply::Retryable(error) => ApprovalWithdrawal::Retry(core_error(error)),
            WithdrawalReply::Refused(error) => ApprovalWithdrawal::NotSent {
                reason: core_error(error).code,
            },
        };
        let _ = client.close().await;
        result
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
    fn prepared() -> WalRecord {
        let input = fixture("broca/held-call-input.json");
        WalRecord::ToolCallDeferred {
            batch_id: broca_protocol::BatchId("b".into()),
            call_id: broca_protocol::ToolCallId("c".into()),
            executor: broca_wal::DeferredExecutor::Broca,
            module: Some("files".into()),
            tool: "files.write".into(),
            schema_pin: None,
            args_jcs: input["args_jcs"].as_str().unwrap().into(),
            args_hash: input["args_hash"].as_str().unwrap().into(),
            scope: serde_json::from_value(
                json!({"owner":{"kind":"reserved","module_id":"broca"},"ref":"s","scope_epoch":1}),
            )
            .unwrap(),
            ask: broca_wal::DeferredAsk {
                prompt: input["prompt"].as_str().unwrap().into(),
                options: vec![],
                expires_at_ms: 60_001,
                on_expiry: broca_wal::OnExpiry::Deny,
                material_damage: true,
                late_execution: broca_wal::LateExecution::Execute,
            },
        }
    }
    fn key() -> String {
        fixture("broca/held-call-input.json")["call_key"]
            .as_str()
            .unwrap()
            .into()
    }

    #[tokio::test]
    async fn consent_unreadable_filing_retries_same_key() {
        let params = held_request(&key(), &prepared(), 1).unwrap();
        let (client, mut module) = client();
        let (reply, ()) = tokio::join!(
            async {
                let error = client.request(&params).await.unwrap_err();
                assert!(
                    core_error(error).retryable,
                    "an accepted but unreadable filing must be recoverable"
                );
                client.request(&params).await.unwrap()
            },
            async {
                let mut first_body = None;
                for attempt in 0..2 {
                    let request = module.next_request().await;
                    assert_eq!(request.body, wire("broca/held-call-request.json"));
                    if let Some(first) = &first_body {
                        assert_eq!(first, &request.body);
                    }
                    first_body = Some(request.body.clone());
                    let reply = if attempt == 0 {
                        br#"{"result":{}}"#.to_vec()
                    } else {
                        bytes("upstream/consent-requester-v2/cerebellum-reply.json")
                    };
                    assert!(module.reply(&request, reply));
                }
            }
        );
        assert_eq!(reply.elicitation_id, "el_1");
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
    async fn consent_detach_and_non_success_reports_use_contract_fields() {
        let (client, mut module) = client();
        // The vendored Cingulate corpus lacks detach and non-success report vectors.
        // These fields follow consent-late-answer-v1's detach and
        // consent-requester-v1's report_execution sections instead.
        let (record, ()) = tokio::join!(client.detach("el_1"), async {
            let request = module.next_request().await;
            assert_eq!(
                request.body,
                br#"{"method":"consent.detach","params":{"id":"el_1"}}"#
            );
            assert!(module.reply(
                &request,
                bytes("upstream/consent-requester-v2/cerebellum-get-reply.json")
            ));
        });
        assert!(!record.unwrap().is_terminal());
        for (execution, value) in [
            (
                ApprovalExecution::ApprovedNotExecuted,
                "approved_not_executed",
            ),
            (ApprovalExecution::OutcomeUnknown, "outcome_unknown"),
        ] {
            let (record, ()) = tokio::join!(client.report_execution("el_1", execution), async {
                let request = module.next_request().await;
                assert_eq!(request.body, format!(r#"{{"method":"consent.report_execution","params":{{"elicitation_id":"el_1","execution":"{value}"}}}}"#).as_bytes());
                let mut reply =
                    fixture("upstream/consent-requester-v1/report-execution-reply.json");
                reply["result"]["execution"] = json!(value);
                assert!(module.reply(&request, serde_json::to_vec(&reply).unwrap()));
            });
            assert_eq!(record.unwrap().fields["execution"], value);
        }
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

    #[test]
    fn consent_call_key_bound_at_max_wal_seq() {
        use broca_wal::Wal;
        let path = broca_test_support::scratch_root().join("consent-key-bound.wal");
        let mut wal = broca_wal::FileWal::open(&path).unwrap();
        wal.append(
            1,
            &WalRecord::StepStarted {
                step_id: broca_protocol::StepId(1),
            },
            true,
        )
        .unwrap();
        let lineage = wal.lineage_id().expect("durable lineage");
        for (seq, length) in [(0, 38), (u64::MAX, 57)] {
            let key = broca_core::call_key(lineage, seq);
            assert_eq!(key.len(), length);
            assert!(key.bytes().all(|b| b.is_ascii_graphic()));
            let params = held_request(&key, &prepared(), 1).unwrap();
            assert_eq!(params["requester_ref"], key);
            assert_eq!(params["dedup_key"], key);
        }
    }

    #[tokio::test]
    async fn consent_request_body_has_no_identity_keys() {
        let params = held_request(&key(), &prepared(), 1).unwrap();
        let (client, mut module) = client();
        let (result, ()) = tokio::join!(
            client.request(&params),
            respond(
                &mut module,
                "broca/held-call-request.json",
                "upstream/consent-requester-v2/cerebellum-reply.json"
            )
        );
        assert_eq!(result.unwrap().elicitation_id, "el_1");
        for forbidden in [
            "identity",
            "subject",
            "owner",
            "via_worker",
            "approval_digest",
            "args_jcs",
            "call_key",
            "executor",
        ] {
            assert!(
                params.get(forbidden).is_none(),
                "request body contains {forbidden}"
            );
        }
        assert_eq!(params["binding"], broca_core::consent::binding(&prepared()));
    }

    #[test]
    fn consent_mapping_refuses_unsupported_inputs_and_expiry() {
        for (field, expected) in [
            ("schema", "approval_schema_pin_unsupported"),
            ("options", "approval_options_unsupported"),
            ("expiry", "expired"),
        ] {
            let mut record = prepared();
            let WalRecord::ToolCallDeferred {
                schema_pin, ask, ..
            } = &mut record
            else {
                unreachable!()
            };
            match field {
                "schema" => {
                    *schema_pin = Some(broca_wal::DispatchSchemaPin {
                        schema_digest: "s".into(),
                        semantics: 1,
                    })
                }
                "options" => ask.options.push("later".into()),
                _ => ask.expires_at_ms = 1,
            }
            let error = held_request(&key(), &record, 1).unwrap_err();
            assert_eq!(error.code, expected);
            assert!(!error.retryable);
        }
        let mut record = prepared();
        let WalRecord::ToolCallDeferred { module, .. } = &mut record else {
            unreachable!()
        };
        *module = None;
        let params = held_request(&key(), &record, 1).unwrap();
        assert!(params["binding"]["module"].is_null());
        assert!(params["binding"]["schema_pin"].is_null());
        assert_eq!(params["target"]["label"], "broca");
    }

    #[tokio::test]
    async fn consent_get_await_answers_preserve_upstream_bodies() {
        for prefix in ["cerebellum", "cerebellum-ref"] {
            let (client, mut module) = client();
            let root = format!("upstream/consent-requester-v2/{prefix}");
            let request = format!("{root}-get-request.json");
            let reply = format!("{root}-get-reply.json");
            let (record, ()) =
                tokio::join!(client.get("el_1"), respond(&mut module, &request, &reply));
            let record = record.unwrap();
            assert_eq!(
                serde_json::to_vec(&json!({"result":record})).unwrap(),
                wire(&reply)
            );
            let request = format!("{root}-await-request.json");
            let reply = format!("{root}-await-reply.json");
            let (record, ()) = tokio::join!(
                client.r#await("el_1", 0),
                respond(&mut module, &request, &reply)
            );
            let record = record.unwrap();
            assert!(
                !record.is_terminal(),
                "a timeout on a pending card grants nothing"
            );
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
    async fn consent_withdraw_and_execution_match_upstream_bodies() {
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
        let (reply, ()) = tokio::join!(
            client.report_execution("el_1", ApprovalExecution::Executed),
            respond(
                &mut module,
                "upstream/consent-requester-v1/report-execution-request.json",
                "upstream/consent-requester-v1/report-execution-reply.json"
            )
        );
        assert_eq!(
            serde_json::to_vec(&json!({"result":reply.unwrap()})).unwrap(),
            wire("upstream/consent-requester-v1/report-execution-reply.json")
        );
    }

    #[tokio::test]
    async fn consent_binding_mismatch_from_transport_is_unavailable() {
        let (client, mut module) = client();
        let mut reply = fixture("upstream/consent-requester-v2/cerebellum-ref-get-reply.json");
        reply["result"]["state"] = json!("answered");
        reply["result"]["answered_choice_id"] = json!("approve");
        reply["result"]["requester_ref"] = json!(key());
        reply["result"]["binding"] = broca_core::consent::binding(&prepared());
        reply["result"]["binding"]["args_hash"] = json!("different");
        let (record, ()) = tokio::join!(client.get("el_1"), async {
            let request = module.next_request().await;
            assert!(module.reply(&request, serde_json::to_vec(&reply).unwrap()));
        });
        let record = serde_json::to_value(record.unwrap()).unwrap();
        let error = broca_core::consent::answer(&record, &key(), &prepared()).unwrap_err();
        assert_eq!(error.code, "consent_binding_mismatch");
        assert!(!error.retryable);
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

    #[tokio::test]
    async fn consent_owner_feed_ack_is_refused_without_a_send() {
        let (client, mut module) = client();
        let plane =
            ScopedConsentPlane::new(client.conn.clone(), RouteOpenRetry::default_patience());
        let ctx = BatchCtx {
            batch_id: broca_protocol::BatchId("b".into()),
            run_id: broca_protocol::RunId("r".into()),
            bind: BindIdentity::new("/p", "broca", "s"),
            scope: None,
        };
        let error = plane
            .ack(
                &ctx,
                &ApprovalCursor {
                    provider_incarnation: CURSOR_ENCODING.into(),
                    seq: 1,
                },
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "consent_owner_feed_ack_forbidden");
        assert!(!error.retryable);
        assert!(module.take_written_for_test().is_empty());
    }

    #[tokio::test]
    async fn consent_errors_and_unreadable_filing_remain_fail_closed() {
        let (client, mut module) = client();
        let params = held_request(&key(), &prepared(), 1).unwrap();
        let (reply, ()) = tokio::join!(client.request(&params), async {
            let request = module.next_request().await;
            assert!(module.reply(&request, br#"{"accepted":true}"#.to_vec()));
        });
        assert!(core_error(reply.unwrap_err()).retryable);
        for code in [
            "consent_unavailable",
            "elicitation_store_error",
            "scope_ended",
            "elicitation_not_owner",
            "future_refusal",
        ] {
            let (reply, ()) = tokio::join!(client.withdraw("el_1"), async {
                let request = module.next_request().await;
                assert!(module.refuse(&request, code));
            });
            match code {
                "consent_unavailable" | "elicitation_store_error" => {
                    assert!(matches!(reply, WithdrawalReply::Retryable(_)))
                }
                "elicitation_not_owner" => {
                    assert!(matches!(reply, WithdrawalReply::NoLongerOpen { .. }))
                }
                _ => assert!(matches!(reply, WithdrawalReply::Refused(_))),
            }
        }
        let (reply, ()) = tokio::join!(client.withdraw("el_1"), async {
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
}
