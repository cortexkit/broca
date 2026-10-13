//! Requester operations on prefrontal-core's scoped management route.
//!
//! This adapter does not own approval policy, cursor persistence or retries. In particular, an
//! answers refusal is never an empty page, and a caller advances the returned cursor only after
//! durable intake. Filing and withdrawal may be retried by their owner with the same call key.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use broca_protocol::{BindIdentity, SessionScope};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use subc_protocol::scope::{ScopeSelector, CAP_SCOPES_V1};

use crate::connection::{rejection_code, ConnError, RouteHandle, RouteOpenRetry, SubcConnection};

pub const CURSOR_INCARNATION_CHANGED: &str = "cursor_incarnation_changed";
pub const ELICITATION_MODULE: &str = "prefrontal-core";
const REPLY_TIMEOUT: Duration = Duration::from_secs(10);

/// The held-call filing body pinned by C9. `args_jcs` is a string, not a parsed JSON value:
/// parsing and serializing it here would change the bytes bound by `args_hash`.
///
/// C9 calls broca's executor `runner` and takes a relative `expires_in_ms` on the wire. The
/// caller retains its absolute approval deadline and computes the remaining duration at filing.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HeldCallRequest {
    pub kind: String,
    pub title: String,
    pub prompt: String,
    pub options: Vec<ElicitationOption>,
    pub default: String,
    pub urgency: String,
    pub on_expiry: String,
    pub material_damage: bool,
    pub late_execution: String,
    pub target: ElicitationTarget,
    pub expires_in_ms: u64,
    pub call_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub module: Option<String>,
    pub tool: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_pin: Option<String>,
    pub executor: ElicitationExecutor,
    pub args_jcs: String,
    pub args_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ElicitationExecutor {
    Runner,
    Provider,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ElicitationOption {
    pub id: String,
    pub label: String,
    pub effect: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ElicitationTarget {
    pub kind: String,
    pub label: String,
}

/// A filing acknowledgement is not consent. The digest becomes usable only on an approved
/// answer, after the caller has durably recorded that answer.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FilingReply {
    pub elicitation_id: String,
    pub args_hash: String,
    pub approval_digest: String,
}

/// Only the incarnation-aware cursor is accepted; there is no integer compatibility path.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AnswersCursor {
    pub provider_incarnation: String,
    pub seq: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct AnswersPage {
    /// Includes retained records and the minimal expired tombstones; neither is rewritten.
    pub records: Vec<Value>,
    pub cursor: AnswersCursor,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ElicitationState {
    Pending,
    Answered,
    Expired,
    Withdrawn,
}

/// The provider's requester record, preserving all request and outcome fields. Its state, not
/// an empty answers page, tells the late-answer owner whether a question is terminal at core.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ElicitationRecord {
    pub elicitation_id: String,
    pub state: ElicitationState,
    #[serde(flatten)]
    pub fields: BTreeMap<String, Value>,
}

impl ElicitationRecord {
    pub fn is_terminal(&self) -> bool {
        self.state != ElicitationState::Pending
    }
}

#[derive(Debug)]
pub enum ElicitationError {
    Refused {
        code: String,
        message: String,
        detail: Option<Value>,
    },
    Transport(String),
    /// Core may have accepted the filing, but its successful acknowledgement was
    /// unreadable. Retrying the same key recovers that question without duplicating it.
    AcceptedFilingReplyUnreadable(String),
    Protocol(String),
}

impl ElicitationError {
    pub fn code(&self) -> Option<&str> {
        match self {
            Self::Refused { code, .. } => Some(code),
            Self::AcceptedFilingReplyUnreadable(_) => Some("elicitation_ack_unreadable"),
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
                            | "elicitation_unavailable"
                            | "elicitation_store_error"
                    )
            }
        }
    }
}

impl std::fmt::Display for ElicitationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused { code, message, .. } => write!(f, "{code} — {message}"),
            Self::Transport(message) => write!(f, "elicitation transport: {message}"),
            Self::Protocol(message) => write!(f, "elicitation protocol: {message}"),
            Self::AcceptedFilingReplyUnreadable(message) => {
                write!(f, "elicitation acknowledgement: {message}")
            }
        }
    }
}

impl std::error::Error for ElicitationError {}

impl From<ConnError> for ElicitationError {
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

/// Withdrawal's stop/retry classification. Core returns a record, not an invented `ok` flag
/// or `already_answered` error. Repeated withdrawals return the same withdrawn record, so an
/// already-withdrawn question is also acknowledged. Terminal scope/auth refusals remain named.
#[derive(Debug)]
pub enum WithdrawalReply {
    Acknowledged(ElicitationRecord),
    NoLongerOpen { code: String },
    Retryable(ElicitationError),
    Refused(ElicitationError),
}

pub struct ElicitationClient {
    conn: Arc<SubcConnection>,
    route: RouteHandle,
    reply_timeout: Duration,
}

impl ElicitationClient {
    /// Use a route whose owner has already bound it under the frozen session scope. Prefer
    /// `open` when opening a new production route so old daemons cannot silently drop its scope.
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

    /// Open prefrontal-core under exactly the frozen scope epoch. Scope authority is supplied
    /// by the daemon stamp, never by a client-controlled field in an elicitation payload.
    pub async fn open(
        conn: Arc<SubcConnection>,
        identity: &BindIdentity,
        scope: &SessionScope,
        retry: RouteOpenRetry,
    ) -> Result<Self, ElicitationError> {
        let capabilities = conn.server_capabilities().await?;
        if !capabilities.iter().any(|c| c == CAP_SCOPES_V1) {
            return Err(ElicitationError::Refused {
                code: "scope_unsupported".into(),
                message: "elicitation requires a daemon-attested scope".into(),
                detail: None,
            });
        }
        let owner = crate::subc_plane::owner_principal(scope).map_err(|message| {
            ElicitationError::Refused {
                code: "scope_unverifiable".into(),
                message,
                detail: None,
            }
        })?;
        let route = conn
            .route_open_management_scoped_within(
                ELICITATION_MODULE,
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

    pub async fn close(&self) -> Result<(), ElicitationError> {
        self.conn.route_close(&self.route).await.map_err(Into::into)
    }

    pub async fn request(
        &self,
        request: &HeldCallRequest,
    ) -> Result<FilingReply, ElicitationError> {
        self.call("elicitation.request", request).await
    }

    /// `None` sends an explicit null to start a fresh incarnation-aware feed. A refusal,
    /// including `cursor_incarnation_changed`, is returned unchanged for the owner to reset
    /// and re-read; the adapter never substitutes an empty page or advances a stored cursor.
    pub async fn answers(
        &self,
        since: Option<&AnswersCursor>,
    ) -> Result<AnswersPage, ElicitationError> {
        self.call("elicitation.answers", &json!({"since": since}))
            .await
    }

    pub async fn get(&self, elicitation_id: &str) -> Result<ElicitationRecord, ElicitationError> {
        self.call(
            "elicitation.get",
            &json!({"elicitation_id": elicitation_id}),
        )
        .await
    }

    pub async fn withdraw(&self, elicitation_id: &str) -> WithdrawalReply {
        let result = self
            .call::<_, ElicitationRecord>(
                "elicitation.withdraw",
                &json!({"elicitation_id": elicitation_id}),
            )
            .await;
        match result {
            Ok(record) if record.state == ElicitationState::Withdrawn => {
                WithdrawalReply::Acknowledged(record)
            }
            Ok(record) if record.state == ElicitationState::Answered => {
                WithdrawalReply::NoLongerOpen {
                    code: "answered".into(),
                }
            }
            Ok(record) if record.state == ElicitationState::Expired => {
                WithdrawalReply::NoLongerOpen {
                    code: "expired".into(),
                }
            }
            Ok(_) => WithdrawalReply::Refused(ElicitationError::Protocol(
                "withdraw returned a pending record".into(),
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

    async fn call<P: Serialize, R: DeserializeOwned>(
        &self,
        method: &str,
        params: &P,
    ) -> Result<R, ElicitationError> {
        // Serializing through Value sorts the envelope keys, matching the C9 restricted-domain
        // vector bytes. It never parses the JSON string inside args_jcs.
        let body = serde_json::to_vec(&json!({"method": method, "params": params}))
            .map_err(|error| ElicitationError::Protocol(error.to_string()))?;
        match self
            .conn
            .route_unary_reply(&self.route, body, self.reply_timeout)
            .await?
        {
            Ok(reply) => serde_json::from_slice(&reply).map_err(|error| {
                let message = format!("decode {method}: {error}");
                if method == "elicitation.request" {
                    ElicitationError::AcceptedFilingReplyUnreadable(message)
                } else {
                    ElicitationError::Protocol(message)
                }
            }),
            Err(error) => Err(ElicitationError::Refused {
                code: error.code,
                message: error.message,
                detail: error.detail,
            }),
        }
    }
}

/// Files human consent questions and reads their answers under the frozen session scope.
/// An approval hook only asks for consent; the daemon must install this requester
/// adapter as well before the run loop can file a question or execute its approved call.
pub struct ScopedElicitationPlane {
    conn: Arc<SubcConnection>,
    retry: RouteOpenRetry,
}
impl ScopedElicitationPlane {
    pub fn new(conn: Arc<SubcConnection>, retry: RouteOpenRetry) -> Self {
        Self { conn, retry }
    }
    async fn client(
        &self,
        ctx: &broca_core::tool_plane::BatchCtx,
    ) -> Result<ElicitationClient, broca_core::tool_plane::ApprovalRouteError> {
        let scope =
            ctx.scope
                .as_ref()
                .ok_or_else(|| broca_core::tool_plane::ApprovalRouteError {
                    code: "approve_requires_scope".into(),
                    retryable: false,
                })?;
        ElicitationClient::open(Arc::clone(&self.conn), &ctx.bind, scope, self.retry)
            .await
            .map_err(core_error)
    }
}
fn core_error(error: ElicitationError) -> broca_core::tool_plane::ApprovalRouteError {
    broca_core::tool_plane::ApprovalRouteError {
        retryable: error.is_retryable(),
        code: error.code().unwrap_or("elicitation_transport").to_owned(),
    }
}
fn held_request(
    call_key: &str,
    prepared: &broca_core::tool_plane::WalRecord,
    now: u64,
) -> Result<HeldCallRequest, broca_core::tool_plane::ApprovalRouteError> {
    let broca_core::tool_plane::WalRecord::ToolCallDeferred {
        module,
        tool,
        schema_pin,
        args_jcs,
        args_hash,
        ask,
        ..
    } = prepared
    else {
        return Err(broca_core::tool_plane::ApprovalRouteError {
            code: "elicitation_prepared_invalid".into(),
            retryable: false,
        });
    };
    if schema_pin.is_some() {
        return Err(broca_core::tool_plane::ApprovalRouteError {
            code: "approval_schema_pin_unsupported".into(),
            retryable: false,
        });
    }
    let expires_in_ms = ask.expires_at_ms.saturating_sub(now);
    if expires_in_ms == 0 {
        return Err(broca_core::tool_plane::ApprovalRouteError {
            code: "expired".into(),
            retryable: false,
        });
    }
    let mut options = vec![
        ElicitationOption {
            id: "approve".into(),
            label: "Approve".into(),
            effect: "grant".into(),
            detail: None,
        },
        ElicitationOption {
            id: "deny".into(),
            label: "Deny".into(),
            effect: "decline".into(),
            detail: None,
        },
    ];
    options.extend(
        ask.options
            .iter()
            .filter(|option| !matches!(option.as_str(), "approve" | "deny"))
            .map(|option| ElicitationOption {
                id: option.clone(),
                label: option.clone(),
                effect: "decline".into(),
                detail: None,
            }),
    );
    Ok(HeldCallRequest {
        kind: "consent".into(),
        title: format!("Run {tool}"),
        prompt: ask.prompt.clone(),
        options,
        default: "deny".into(),
        urgency: "normal".into(),
        on_expiry: "deny".into(),
        material_damage: ask.material_damage,
        late_execution: match ask.late_execution {
            broca_core::tool_plane::LateExecution::Execute => "execute",
            broca_core::tool_plane::LateExecution::NotifyOnly => "notify_only",
        }
        .into(),
        target: ElicitationTarget {
            kind: "module".into(),
            label: module.clone().unwrap_or_else(|| "broca".into()),
        },
        expires_in_ms,
        call_key: call_key.into(),
        module: module.clone(),
        tool: tool.clone(),
        schema_pin: None,
        executor: ElicitationExecutor::Runner,
        args_jcs: args_jcs.clone(),
        args_hash: args_hash.clone(),
    })
}
#[async_trait::async_trait]
impl broca_core::tool_plane::ElicitationPlane for ScopedElicitationPlane {
    async fn request(
        &self,
        ctx: &broca_core::tool_plane::BatchCtx,
        key: &str,
        prepared: &broca_core::tool_plane::WalRecord,
        now: u64,
    ) -> Result<broca_core::tool_plane::ApprovalFiling, broca_core::tool_plane::ApprovalRouteError>
    {
        let request = held_request(key, prepared, now)?;
        let client = self.client(ctx).await?;
        let result = client
            .request(&request)
            .await
            .map(|reply| broca_core::tool_plane::ApprovalFiling {
                elicitation_id: reply.elicitation_id,
                args_hash: reply.args_hash,
            })
            .map_err(core_error);
        let _ = client.close().await;
        result
    }
    async fn answers(
        &self,
        ctx: &broca_core::tool_plane::BatchCtx,
        since: Option<&broca_core::tool_plane::ApprovalCursor>,
    ) -> Result<broca_core::tool_plane::ApprovalPage, broca_core::tool_plane::ApprovalRouteError>
    {
        let client = self.client(ctx).await?;
        let cursor = since.map(|cursor| AnswersCursor {
            provider_incarnation: cursor.provider_incarnation.clone(),
            seq: cursor.seq,
        });
        let result = client
            .answers(cursor.as_ref())
            .await
            .map(|page| broca_core::tool_plane::ApprovalPage {
                records: page.records,
                cursor: broca_core::tool_plane::ApprovalCursor {
                    provider_incarnation: page.cursor.provider_incarnation,
                    seq: page.cursor.seq,
                },
            })
            .map_err(core_error);
        let _ = client.close().await;
        result
    }
    async fn get(
        &self,
        ctx: &broca_core::tool_plane::BatchCtx,
        id: &str,
    ) -> Result<broca_core::tool_plane::ApprovalQuestion, broca_core::tool_plane::ApprovalRouteError>
    {
        let client = self.client(ctx).await?;
        let result = client
            .get(id)
            .await
            .map(|record| broca_core::tool_plane::ApprovalQuestion {
                terminal: record.is_terminal(),
                record: serde_json::to_value(record).expect("requester record serializes"),
            })
            .map_err(core_error);
        let _ = client.close().await;
        result
    }
    async fn withdraw(
        &self,
        ctx: &broca_core::tool_plane::BatchCtx,
        id: &str,
    ) -> broca_core::tool_plane::ApprovalWithdrawal {
        use broca_core::tool_plane::ApprovalWithdrawal;
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

    fn fixture(name: &str) -> Value {
        let root =
            std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/elicitation");
        serde_json::from_slice(&std::fs::read(root.join(name)).unwrap()).unwrap()
    }

    fn client() -> (ElicitationClient, InProcessModuleEnd) {
        let (conn, route, module) = SubcConnection::in_process_for_test(5, 3);
        (
            ElicitationClient::on_route(Arc::new(conn), route, Duration::from_millis(30)),
            module,
        )
    }

    async fn respond(module: &mut InProcessModuleEnd, vector: &Value) {
        let request = module.next_request().await;
        assert_eq!(
            request.body,
            serde_json::to_vec(&json!({
                "method": vector["method"], "params": vector["request"],
            }))
            .unwrap(),
            "operation and parameter bytes must equal the frozen vector"
        );
        assert!(module.reply(&request, serde_json::to_vec(&vector["reply"]).unwrap()));
    }

    #[tokio::test]
    async fn accepted_filing_with_unreadable_ack_refiles_same_key_and_recovers_question() {
        let vector = fixture("c9/request.json");
        let request: HeldCallRequest = serde_json::from_value(vector["request"].clone()).unwrap();
        let (client, mut module) = client();
        let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
        let received = requests.clone();
        let (expected, ()) = tokio::join!(
            async {
                let error = client.request(&request).await.unwrap_err();
                assert!(
                    error.is_retryable(),
                    "accepted filing with unreadable acknowledgement must be retried"
                );
                assert!(
                    core_error(error).retryable,
                    "core must re-file instead of closing unavailable"
                );
                client.request(&request).await.unwrap()
            },
            async {
                let mut questions = std::collections::BTreeMap::new();
                for attempt in 0..2 {
                    let incoming = module.next_request().await;
                    let body: Value = serde_json::from_slice(&incoming.body).unwrap();
                    let key = body["params"]["call_key"].as_str().unwrap().to_owned();
                    received.lock().unwrap().push(key.clone());
                    questions
                        .entry(key.clone())
                        .or_insert_with(|| vector["reply"].clone());
                    let reply = if attempt == 0 {
                        json!({"accepted":true})
                    } else {
                        questions[&key].clone()
                    };
                    assert!(module.reply(&incoming, serde_json::to_vec(&reply).unwrap()));
                }
                assert_eq!(
                    questions.len(),
                    1,
                    "re-filing recovers the accepted question rather than creating another"
                );
            }
        );
        assert_eq!(
            expected.elicitation_id,
            vector["reply"]["elicitation_id"].as_str().unwrap()
        );
        assert_eq!(
            *requests.lock().unwrap(),
            vec![request.call_key.clone(), request.call_key]
        );
    }

    #[tokio::test]
    async fn held_call_filing_preserves_c9_bytes() {
        let vector = fixture("c9/request.json");
        let request: HeldCallRequest = serde_json::from_value(vector["request"].clone()).unwrap();
        let (client, mut module) = client();
        let (result, ()) = tokio::join!(client.request(&request), respond(&mut module, &vector));
        assert_eq!(
            serde_json::to_value(result.unwrap()).unwrap(),
            vector["reply"]
        );
    }

    #[tokio::test]
    async fn filing_never_reserializes_the_args_string() {
        let mut vector = fixture("c9/request.json");
        let probe = fixture("c9/args-noncanonical-key-order.json");
        vector["request"]["args_jcs"] = probe["args_jcs"].clone();
        vector["request"]["args_hash"] = probe["args_hash"].clone();
        vector["reply"]["args_hash"] = probe["args_hash"].clone();
        vector["reply"]["approval_digest"] = probe["approval_digest"].clone();
        let request = serde_json::from_value(vector["request"].clone()).unwrap();
        let (client, mut module) = client();
        let (result, ()) = tokio::join!(client.request(&request), respond(&mut module, &vector));
        assert_eq!(
            serde_json::to_value(result.unwrap()).unwrap(),
            vector["reply"]
        );
    }

    #[tokio::test]
    async fn answers_null_and_object_match_c9_and_never_accept_an_integer() {
        for name in ["c9/answers-incarnation.json", "c9/answers-object.json"] {
            let vector = fixture(name);
            let since: Option<AnswersCursor> =
                serde_json::from_value(vector["request"]["since"].clone()).unwrap();
            let (client, mut module) = client();
            let (result, ()) = tokio::join!(
                client.answers(since.as_ref()),
                respond(&mut module, &vector)
            );
            assert_eq!(
                serde_json::to_value(result.unwrap()).unwrap(),
                vector["reply"]
            );
        }
        assert!(serde_json::from_value::<AnswersCursor>(json!(7)).is_err());
        assert!(
            serde_json::from_value::<AnswersPage>(json!({"records": [], "cursor": 7})).is_err()
        );
    }

    #[tokio::test]
    async fn old_incarnation_is_named_and_a_fresh_read_recovers_the_answer() {
        let vector = fixture("c9/answers-old-incarnation.json");
        let since: AnswersCursor =
            serde_json::from_value(vector["request"]["since"].clone()).unwrap();
        let (client, mut module) = client();
        let (result, ()) = tokio::join!(client.answers(Some(&since)), async {
            let request = module.next_request().await;
            assert_eq!(
                request.body,
                serde_json::to_vec(
                    &json!({"method": vector["method"], "params": vector["request"]})
                )
                .unwrap()
            );
            assert!(module.refuse(&request, vector["error"]["code"].as_str().unwrap()));
        });
        let error = result.unwrap_err();
        assert_eq!(error.code(), Some(CURSOR_INCARNATION_CHANGED));
        assert!(
            !error.is_retryable(),
            "a retry must reset the cursor, not repeat the old one"
        );

        let mut fresh = fixture("c9/answers-incarnation.json");
        fresh["reply"]["records"] = json!([{
            "elicitation_id": "el_call", "state": "answered", "call_key": "br:episode:17",
            "answered_choice_id": "approve", "approval_digest": fixture("c9/request.json")["reply"]["approval_digest"],
        }]);
        let (result, ()) = tokio::join!(client.answers(None), respond(&mut module, &fresh));
        let page = result.unwrap();
        assert_eq!(page.records.len(), 1);
        assert_eq!(page.records[0]["answered_choice_id"], "approve");
        assert_eq!(serde_json::to_value(page).unwrap(), fresh["reply"]);
    }

    #[tokio::test]
    async fn get_preserves_the_requester_record_and_distinguishes_pending_from_terminal() {
        let vector = fixture("requester/get.json");
        let (client, mut module) = client();
        let (result, ()) = tokio::join!(
            client.get("@ELICITATION_ID@"),
            respond(&mut module, &vector)
        );
        let record = result.unwrap();
        assert!(!record.is_terminal());
        assert_eq!(serde_json::to_value(&record).unwrap(), vector["reply"]);
        for state in [
            ElicitationState::Answered,
            ElicitationState::Expired,
            ElicitationState::Withdrawn,
        ] {
            assert!(ElicitationRecord {
                state,
                ..record.clone()
            }
            .is_terminal());
        }
    }

    #[tokio::test]
    async fn withdrawals_classify_pinned_states_refusals_and_transport_loss() {
        let (client, mut module) = client();
        let vector = fixture("requester/withdraw.json");
        // Core does not distinguish a first withdrawal from an already-withdrawn repeat.
        for _ in 0..2 {
            let (reply, ()) = tokio::join!(
                client.withdraw("@ELICITATION_ID@"),
                respond(&mut module, &vector)
            );
            match reply {
                WithdrawalReply::Acknowledged(record) => {
                    assert_eq!(serde_json::to_value(record).unwrap(), vector["reply"])
                }
                other => panic!("withdrawn must be acknowledged: {other:?}"),
            }
        }
        for state in ["answered", "expired", "pending", "future_state"] {
            let mut vector = vector.clone();
            vector["reply"]["state"] = json!(state);
            let (reply, ()) = tokio::join!(
                client.withdraw("@ELICITATION_ID@"),
                respond(&mut module, &vector)
            );
            if matches!(state, "answered" | "expired") {
                assert!(matches!(reply, WithdrawalReply::NoLongerOpen { code } if code == state));
            } else {
                assert!(matches!(
                    reply,
                    WithdrawalReply::Refused(ElicitationError::Protocol(_))
                ));
            }
        }
        for code in [
            "elicitation_not_owner",
            "elicitation_not_found",
            "elicitation_store_error",
            "elicitation_unavailable",
            "module_reloading",
            "scope_not_synced",
            "scope_changed",
            "scope_ended",
            "elicitation_requester_refused",
            "future_refusal",
        ] {
            let (reply, ()) = tokio::join!(client.withdraw("el_call"), async {
                let request = module.next_request().await;
                assert!(module.refuse(&request, code));
            });
            match code {
                "elicitation_not_owner" | "elicitation_not_found" => assert!(
                    matches!(reply, WithdrawalReply::NoLongerOpen { code: actual } if actual == code)
                ),
                "elicitation_store_error"
                | "elicitation_unavailable"
                | "module_reloading"
                | "scope_not_synced"
                | "scope_changed" => assert!(
                    matches!(reply, WithdrawalReply::Retryable(error) if error.code() == Some(code))
                ),
                _ => assert!(
                    matches!(reply, WithdrawalReply::Refused(error) if error.code() == Some(code))
                ),
            }
        }
        let reply = client.withdraw("el_call").await;
        assert!(
            matches!(
                reply,
                WithdrawalReply::Retryable(ElicitationError::Transport(_))
            ),
            "timeout must retry, not acknowledge"
        );
    }
}
