//! The management-surface CLIENT — the consumer leg of the broca module surface.
//!
//! The inverse of the serve loop: where `serve.rs` is the MODULE (subc supervises it,
//! clients route to it), `SessionClient` is a CONSUMER driving a module's session lifecycle
//! over the wire — `route.open` the ManagementSurface, then `session.send` / `run.status` /
//! `run.cancel` / `session.retract` (request/response) and `session.subscribe` (a held-open
//! StreamData drain). It builds the SAME wire types the serve loop decodes
//! ([`crate::wire`] + [`broca_module::serve`]), so the client and server can't drift.
//!
//! It rides [`broca_subc::SubcConnection`]'s consumer demux (the same machinery the
//! `SubcToolPlane` tool-provider consumer uses), so one connection can carry a client's
//! management routes alongside (a different process's) tool routes.

use broca_module::serve::{
    CancelResponse, RetractResponse, RunStatusResponse, SessionImportResponse, SessionReadResponse,
    StatusResponse, SubscribeFrom,
};
use broca_protocol::request::Tool;
use broca_protocol::BindIdentity;
use broca_subc::connection::{
    ConnError, RouteHandle, RouteOpenRetry, SubcConnection, SubscribeStream,
    ROUTE_OPEN_RETRY_INTERVAL,
};

use crate::wire::{
    AuthParams, CacheParams, CancelParams, FromWire, ImportOriginKind, ImportOriginParams,
    ImportParams, ImportResult, ModelParams, ReadParams, ReadResult, RetractParams, SendParams,
    SendResult, StatusParams, SubscribeParams,
};

use broca_wire::RunResultParams;

pub use broca_wire::{decode_subscribe_event, SubscribeEvent};

/// A client for one session on a module. Short requests like send/status/cancel/retract use
/// the shared command channel because they complete quickly, and replies are matched by
/// channel plus correlation id. `subscribe` gets its own channel because it stays open for
/// the whole stream; otherwise it would monopolize the shared command channel and delay later
/// requests.
pub struct SessionClient {
    conn: SubcConnection,
    cmd_route: RouteHandle,
    module_id: String,
    session: BindIdentity,
}

/// Arguments for one `session.send` request.
#[derive(Debug)]
pub struct SendSpec<'a> {
    /// The user prompt for this turn.
    pub prompt: &'a str,
    /// Optional role-scoped system prompt for this send.
    pub system: Option<&'a str>,
    pub provider: &'a str,
    pub model: &'a str,
    /// The requested reasoning variant, sent inside the wire `model` object. `None` omits it.
    pub variant: Option<&'a str>,
    /// The requested service tier, sent top-level. `None` omits it (the provider's default).
    pub service_tier: Option<broca_protocol::request::ServiceTier>,
    /// `None` omits the wire field and inherits; `Some([])` requests a tool-less run.
    pub tools: Option<Vec<Tool>>,
    pub send_id: Option<&'a str>,
    pub cache: Option<CacheParams>,
    pub budget: Option<broca_protocol::BudgetConfig>,
    pub auth: Option<AuthParams>,
    /// `Some(true)` skips transcript materialization while keeping the WAL readable.
    pub one_shot: Option<bool>,
    /// `None` omits the wire field: inherit the session's value, or take the process default
    /// on its first episode.
    pub context_management: Option<broca_protocol::request::ContextManagement>,
}

/// What a `send` produced: an active run, an already-finished run, or a durably-queued submission.
#[derive(Debug, Clone, PartialEq)]
pub enum SendOutcome {
    Paused {
        run_id: String,
        pause_reason: broca_protocol::PauseReason,
    },
    Active {
        run_id: String,
    },
    Finished {
        run_id: String,
        reason: broca_protocol::RunFinishReason,
    },
    Pending {
        submission_id: String,
    },
}

#[derive(serde::Serialize)]
struct RequestRef<'a, P: serde::Serialize> {
    method: &'a str,
    params: &'a P,
}

#[derive(serde::Deserialize)]
struct Envelope<T> {
    result: T,
}

fn unwrap_result_envelope(mut envelope: serde_json::Value) -> Result<serde_json::Value, ConnError> {
    if let Some(result) = envelope
        .as_object_mut()
        .and_then(|object| object.remove("result"))
    {
        Ok(result)
    } else {
        Err(ConnError::Protocol(format!(
            "response missing 'result' field: {envelope}"
        )))
    }
}

fn decode_result_envelope<T: serde::de::DeserializeOwned>(
    body: &[u8],
    error_context: &str,
) -> Result<T, ConnError> {
    match serde_json::from_slice::<Envelope<T>>(body) {
        Ok(envelope) => Ok(envelope.result),
        Err(_) => {
            // Re-run only failed responses through the legacy Value path so malformed JSON,
            // missing-result envelopes, and result type errors keep their existing diagnostics.
            let envelope: serde_json::Value = serde_json::from_slice(body)
                .map_err(|e| ConnError::Protocol(format!("decode route response: {e}")))?;
            let result = unwrap_result_envelope(envelope)?;
            serde_json::from_value(result)
                .map_err(|e| ConnError::Protocol(format!("{error_context}: {e}")))
        }
    }
}

impl SessionClient {
    /// Open a management-surface route to `module_id` for `session` (the inverse of the
    /// serve loop's route.bind). Subsequent ops ride the returned route channel.
    pub async fn bind(
        conn: SubcConnection,
        module_id: &str,
        session: BindIdentity,
    ) -> Result<Self, ConnError> {
        Self::bind_with_timeout(conn, module_id, session, std::time::Duration::ZERO).await
    }

    /// Like [`bind`](Self::bind) but RETRIES the route.open until `timeout` elapses. A
    /// supervised module HELLO-registers ASYNCHRONOUSLY after subc binds, so a client that
    /// connects the instant the daemon is up sees `target_unavailable` (the module is
    /// "running" but not yet "live"). Retrying absorbs that startup window — the same
    /// discipline the tool-route `warm_bind` uses. `Duration::ZERO` ⇒ a single attempt.
    pub async fn bind_with_timeout(
        conn: SubcConnection,
        module_id: &str,
        session: BindIdentity,
        timeout: std::time::Duration,
    ) -> Result<Self, ConnError> {
        session
            .validate_keyable()
            .map_err(|message| ConnError::Rejected(message.into()))?;
        let deadline = std::time::Instant::now() + timeout;
        // The connection layer retries a temporary refusal within the caller's timeout (so
        // `Duration::ZERO` stays a single attempt); this loop additionally retries every other
        // error, which the startup window can also produce.
        let retry = RouteOpenRetry {
            deadline: tokio::time::Instant::from_std(deadline),
            max_interval: ROUTE_OPEN_RETRY_INTERVAL,
        };
        let cmd_route = loop {
            match conn
                .route_open_management_within(module_id, &session, retry)
                .await
            {
                Ok(ch) => break ch,
                Err(e) => {
                    if std::time::Instant::now() >= deadline {
                        return Err(e);
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
        };
        Ok(Self {
            conn,
            cmd_route,
            module_id: module_id.to_string(),
            session,
        })
    }

    /// `session.send`: start a run (idle lineage) or durably queue it (active). Arguments
    /// are bundled in `SendSpec` so the call stays tidy.
    pub async fn send(&self, spec: SendSpec<'_>) -> Result<SendOutcome, ConnError> {
        let params = SendParams {
            session_class: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            retention: None,
            conversation_key: None,
            service_tier: spec.service_tier,
            prompt: spec.prompt.to_string(),
            prompt_blocks: None,
            system: spec.system.map(str::to_owned),
            send_id: spec.send_id.map(str::to_owned),
            model: Some(ModelParams {
                provider: spec.provider.to_string(),
                model: spec.model.to_string(),
                variant: spec.variant.map(str::to_owned),
            }),
            tools: spec.tools,
            tool_choice: Default::default(),
            generation: Default::default(),
            stop_when: Vec::new(),
            cache: spec.cache,
            budget: spec.budget,
            structured_output: None,
            auth: spec.auth,
            context_limit: None,
            work_class: None,
            one_shot: spec.one_shot,
            delivery: None,
            dedupe_key: None,
            expires_at_ms: None,
            template: None,
            // Inert (back-compat wire field); the coordinator decides append-vs-no-op from
            // run_id identity. A fresh send over an idle lineage auto-appends a new episode.
            append_episode: false,
            context_management: spec.context_management,
        };
        let result: SendResult = self
            .request("session.send", &params, "decode send result")
            .await?;
        Ok(match result {
            SendResult::Finished { run_id, reason } => SendOutcome::Finished { run_id, reason },
            SendResult::Paused {
                run_id,
                pause_reason,
            } => SendOutcome::Paused {
                run_id,
                pause_reason,
            },
            SendResult::Active { run_id } => SendOutcome::Active { run_id },
            SendResult::Pending { submission_id } => SendOutcome::Pending { submission_id },
        })
    }

    /// `session.import`: seed canonical history as one completed provider-free episode.
    pub async fn import(
        &self,
        messages: Vec<broca_protocol::Message>,
        import_id: &str,
        source_ref: &str,
    ) -> Result<SessionImportResponse, ConnError> {
        let params = ImportParams {
            messages,
            import_id: import_id.to_string(),
            origin: ImportOriginParams {
                kind: ImportOriginKind::Import,
                source_ref: source_ref.to_string(),
            },
        };
        let result: ImportResult = self
            .request("session.import", &params, "decode session.import")
            .await?;
        Ok(result)
    }

    /// `vault.audit` lists credentials covered by the vault's grants and compares them with
    /// the credentials routed by the daemon's configuration. The audit is daemon-scoped;
    /// the bound session route is only the transport attachment.
    pub async fn vault_audit(&self) -> Result<crate::wire::VaultAuditResponse, ConnError> {
        self.request(
            crate::manifest::OP_VAULT_AUDIT,
            &crate::wire::VaultAuditParams::default(),
            "decode vault.audit",
        )
        .await
    }

    /// `run.status`: the lineage's current SESSION state (Idle|Active).
    pub async fn status(&self) -> Result<StatusResponse, ConnError> {
        self.request("run.status", &serde_json::json!({}), "decode status")
            .await
    }

    /// `run.status{run_id}`: the run-KEYED three-tier status of a specific run (the durable
    /// run index's wire surface — a finished/crashed run's outcome, reachable even when it's
    /// not in a live subscription).
    pub async fn run_status(&self, run_id: &str) -> Result<RunStatusResponse, ConnError> {
        let params = StatusParams {
            run_id: Some(run_id.to_string()),
        };
        self.request("run.status", &params, "decode run_status")
            .await
    }

    /// [`Self::run_status`] as the wire answers it, including broca's answer for the run id of
    /// a queued send that is not a run (see [`broca_wire::RunStatusReply`]).
    pub async fn run_status_reply(
        &self,
        run_id: &str,
    ) -> Result<broca_wire::RunStatusReply, ConnError> {
        let params = StatusParams {
            run_id: Some(run_id.to_string()),
        };
        self.request("run.status", &params, "decode run_status")
            .await
    }

    /// `run.status{run_id}` as the daemon returned it, before JSON decoding.
    pub async fn run_status_raw(&self, run_id: &str) -> Result<Vec<u8>, ConnError> {
        let params = StatusParams {
            run_id: Some(run_id.to_string()),
        };
        self.request_raw("run.status", &params).await
    }

    /// `run.result{run_id}`: a run's state and, once completed, its final message.
    pub async fn run_result(
        &self,
        run_id: &str,
    ) -> Result<broca_wire::RunResultResponse, ConnError> {
        let params = RunResultParams {
            run_id: run_id.to_string(),
        };
        self.request("run.result", &params, "decode run_result")
            .await
    }

    /// `run.result{run_id}` as the daemon returned it, before JSON decoding.
    pub async fn run_result_raw(&self, run_id: &str) -> Result<Vec<u8>, ConnError> {
        let params = RunResultParams {
            run_id: run_id.to_string(),
        };
        self.request_raw("run.result", &params).await
    }

    /// `session.read`: read a durable transcript page. An omitted `from_ordinal` returns the
    /// newest page; an explicit ordinal starts a forward range.
    pub async fn read(
        &self,
        from_ordinal: Option<u64>,
        limit: Option<u64>,
    ) -> Result<SessionReadResponse, ConnError> {
        self.read_with_tools(from_ordinal, limit, false).await
    }

    /// `session.read` that can also ask for the latest episode's frozen tool list.
    pub async fn read_with_tools(
        &self,
        from_ordinal: Option<u64>,
        limit: Option<u64>,
        include_tools: bool,
    ) -> Result<SessionReadResponse, ConnError> {
        let params = ReadParams {
            from_ordinal,
            limit,
            include_tools,
            ..ReadParams::default()
        };
        let result: ReadResult = self
            .request("session.read", &params, "decode session.read")
            .await?;
        Ok(result)
    }

    /// `run.cancel`: cooperatively stop a run by id.
    pub async fn cancel(&self, run_id: &str) -> Result<CancelResponse, ConnError> {
        let params = CancelParams {
            at: None,
            run_id: run_id.to_string(),
        };
        self.request("run.cancel", &params, "decode cancel").await
    }

    /// `session.retract`: retract a durably-queued submission by id.
    pub async fn retract(&self, submission_id: &str) -> Result<RetractResponse, ConnError> {
        let params = RetractParams {
            submission_id: submission_id.to_string(),
        };
        self.request("session.retract", &params, "decode retract")
            .await
    }

    /// `session.subscribe`: open the held-open control stream from `from` on a DEDICATED
    /// route (a subscribe holds the route's credit until the stream ends, so it gets its own
    /// channel, never the command channel). Returns the raw frame stream; decode each event
    /// with [`decode_subscribe_event`].
    pub async fn subscribe(&self, from: SubscribeFrom) -> Result<SubscribeStream, ConnError> {
        let params = SubscribeParams {
            from: Some(match from {
                SubscribeFrom::Start => FromWire::Named("start".into()),
                SubscribeFrom::Live => FromWire::Named("live".into()),
                SubscribeFrom::After { wal_seq, sub_index } => {
                    FromWire::Cursor { wal_seq, sub_index }
                }
            }),
        };
        let body = self.request_body("session.subscribe", &params)?;
        // A dedicated management route for the stream (same session, own channel).
        let sub_route = self
            .conn
            .route_open_management(&self.module_id, &self.session)
            .await?;
        self.conn.subscribe(&sub_route, body).await
    }

    /// Build a `{method, params}` body, send it, and decode the typed result directly from the
    /// server's `{"result": <payload>}` envelope.
    async fn request<P, T>(
        &self,
        method: &str,
        params: &P,
        error_context: &str,
    ) -> Result<T, ConnError>
    where
        P: serde::Serialize,
        T: serde::de::DeserializeOwned,
    {
        let response = self.request_raw(method, params).await?;
        decode_result_envelope(&response, error_context)
    }

    /// Return the response body unchanged so callers that need byte fidelity can read it before
    /// any JSON decoding takes place.
    async fn request_raw<P: serde::Serialize>(
        &self,
        method: &str,
        params: &P,
    ) -> Result<Vec<u8>, ConnError> {
        let body = self.request_body(method, params)?;
        self.conn.route_request_raw(&self.cmd_route, body).await
    }

    fn request_body<P: serde::Serialize>(
        &self,
        method: &str,
        params: &P,
    ) -> Result<Vec<u8>, ConnError> {
        let req = RequestRef { method, params };
        serde_json::to_vec(&req)
            .map_err(|error| ConnError::Transport(format!("encode {method} params: {error}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::{ContentBlock, Message, Role};

    fn assert_request_preserves_legacy_shape<P: serde::Serialize>(method: &str, params: &P) {
        let legacy = crate::wire::RouteRequest {
            method: method.to_string(),
            params: serde_json::to_value(params).unwrap(),
        };
        let legacy: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&legacy).unwrap()).unwrap();
        let direct: serde_json::Value =
            serde_json::from_slice(&serde_json::to_vec(&RequestRef { method, params }).unwrap())
                .unwrap();

        assert_eq!(direct, legacy);
        let envelope = direct.as_object().unwrap();
        assert_eq!(envelope.len(), 2);
        assert!(envelope.contains_key("method"));
        assert!(envelope.contains_key("params"));
    }

    #[test]
    fn unwrap_result_envelope_moves_payload_and_preserves_errors() {
        let envelope = serde_json::json!({
            "result": {
                "messages": [
                    {"ordinal": 1, "text": "hello"},
                    {"ordinal": 2, "text": "world"}
                ],
                "next": 3
            },
            "metadata": "ignored"
        });
        let legacy = envelope.get("result").cloned().unwrap();
        assert_eq!(unwrap_result_envelope(envelope).unwrap(), legacy);

        for malformed in [
            serde_json::json!({"error": "missing result"}),
            serde_json::json!(["bare", "array"]),
        ] {
            let expected = format!("response missing 'result' field: {malformed}");
            match unwrap_result_envelope(malformed).unwrap_err() {
                ConnError::Protocol(message) => assert_eq!(message, expected),
                other => panic!("unexpected envelope error: {other}"),
            }
        }
    }

    fn legacy_decode_result<T: serde::de::DeserializeOwned>(
        body: &[u8],
        error_context: &str,
    ) -> Result<T, ConnError> {
        let envelope: serde_json::Value = serde_json::from_slice(body)
            .map_err(|e| ConnError::Protocol(format!("decode route response: {e}")))?;
        let result = unwrap_result_envelope(envelope)?;
        serde_json::from_value(result)
            .map_err(|e| ConnError::Protocol(format!("{error_context}: {e}")))
    }

    fn response_body<T: serde::Serialize>(result: &T) -> Vec<u8> {
        serde_json::to_vec(&serde_json::json!({
            "result": result,
            "metadata": "ignored"
        }))
        .unwrap()
    }

    fn assert_typed_response_matches_legacy<T>(result: &T, error_context: &str)
    where
        T: serde::Serialize + serde::de::DeserializeOwned + std::fmt::Debug + PartialEq,
    {
        let body = response_body(result);
        let typed: T = decode_result_envelope(&body, error_context).unwrap();
        let legacy: T = legacy_decode_result(&body, error_context).unwrap();
        assert_eq!(typed, legacy);
        assert_eq!(&typed, result);
    }

    #[test]
    fn typed_read_matches_legacy_double_parse() {
        let read = ReadResult {
            next_late_after: None,
            deferred_calls: None,
            late_approvals: None,
            open_batch: None,
            // Absent: this fixture pins the DECODE shape of a legacy page, and a legacy
            // lineage genuinely has no id until its next write mints one.
            lineage_id: None,
            messages: vec![broca_wire::SessionReadMessage {
                delivered_as: None,
                tags: Vec::new(),
                run: None,
                ordinal: 7,
                mid: "m7".into(),
                message: Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "paged payload".into(),
                    }],
                ),
                tool_calls: Vec::new(),
            }],
            next_from_ordinal: Some(8),
            head: Some(broca_wire::CursorWire {
                wal_seq: 11,
                sub_index: 2,
            }),
            lineage_state: broca_wire::SessionReadLineageState {
                last_run_id: Some("run-1".into()),
                state: "idle".into(),
                reason: Some("completed".into()),
                error: None,
                usage: None,
            },
            // Absent: a legacy page predates the tools opt-in, so it never carries them.
            tools: None,
            tools_run_id: None,
        };
        assert_typed_response_matches_legacy(&read, "decode session.read");
    }

    #[test]
    fn generic_typed_decoder_supports_every_other_unary_result() {
        assert_typed_response_matches_legacy(
            &SendResult::Finished {
                run_id: "run-1".into(),
                reason: broca_protocol::RunFinishReason::Interrupted,
            },
            "decode finished send result",
        );
        assert_typed_response_matches_legacy(
            &SendResult::Active {
                run_id: "run-1".into(),
            },
            "decode send result",
        );
        assert_typed_response_matches_legacy(
            &ImportResult {
                run_id: "run-import".into(),
                synthesized_tool_results: 1,
                dropped_orphan_tool_results: 2,
                dropped_system_messages: Some(3),
                already_imported: false,
            },
            "decode session.import",
        );
        assert_typed_response_matches_legacy(
            &StatusResponse::Active {
                run_id: "run-1".into(),
                step: 4,
            },
            "decode status",
        );
        assert_typed_response_matches_legacy(&RunStatusResponse::Unknown, "decode run_status");
        assert_typed_response_matches_legacy(&CancelResponse::Ack, "decode cancel");
        assert_typed_response_matches_legacy(&RetractResponse::Retracted, "decode retract");
    }

    #[test]
    fn typed_decoder_preserves_legacy_envelope_errors() {
        for (body, expected) in [
            (
                br#"{"error":"missing result"}"#.as_slice(),
                "response missing 'result' field: {\"error\":\"missing result\"}",
            ),
            (
                br#"["bare","array"]"#.as_slice(),
                "response missing 'result' field: [\"bare\",\"array\"]",
            ),
            (
                br#"{"result":"#.as_slice(),
                "decode route response: EOF while parsing a value at line 1 column 10",
            ),
            (
                br#"{"result":false}"#.as_slice(),
                "decode session.read: invalid type: boolean `false`, expected struct SessionReadResponse",
            ),
        ] {
            let error = decode_result_envelope::<ReadResult>(body, "decode session.read")
                .expect_err("fixture must fail");
            match error {
                ConnError::Protocol(message) => assert_eq!(message, expected),
                other => panic!("unexpected envelope error: {other}"),
            }
        }
    }

    #[test]
    fn request_ref_preserves_import_and_send_json_shapes() {
        let import = ImportParams {
            messages: vec![
                Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "hello, 世界 🧪".into(),
                    }],
                ),
                Message::new(
                    Role::Assistant,
                    vec![ContentBlock::Text {
                        text: "réponse".into(),
                    }],
                ),
            ],
            import_id: "import-1".into(),
            origin: ImportOriginParams {
                kind: ImportOriginKind::Import,
                source_ref: "fixture://unicode".into(),
            },
        };
        assert_request_preserves_legacy_shape("session.import", &import);

        let send = SendParams {
            session_class: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            retention: None,
            conversation_key: None,
            service_tier: None,
            prompt: "ship it 🦀".into(),
            prompt_blocks: None,
            system: Some("be concise".into()),
            send_id: Some("send-1".into()),
            model: Some(ModelParams {
                provider: "provider".into(),
                model: "model".into(),
                variant: None,
            }),
            tools: None,
            tool_choice: Default::default(),
            generation: Default::default(),
            stop_when: Vec::new(),
            cache: None,
            budget: None,
            structured_output: None,
            auth: None,
            context_limit: None,
            work_class: None,
            one_shot: None,
            delivery: None,
            dedupe_key: None,
            expires_at_ms: None,
            template: None,
            append_episode: false,
            context_management: None,
        };
        assert_request_preserves_legacy_shape("session.send", &send);

        let result = RunResultParams {
            run_id: "run-1".into(),
        };
        let request: serde_json::Value = serde_json::from_slice(
            &serde_json::to_vec(&RequestRef {
                method: "run.result",
                params: &result,
            })
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            request,
            serde_json::json!({"method": "run.result", "params": {"run_id": "run-1"}})
        );
    }
}
