use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use broca_protocol::{BindPrincipal, SendScope};
use broca_store::grants::{GrantsStore, HeldCall, HeldState, HoldRequest};
use broca_store::system_one::{BatchScope, Outcome, StoredBatch};
use cortexkit_llm_tools::classify;
use cortexkit_role_classifier::errors::Refusal;
use serde_json::{json, value::RawValue, Value};
use subc_protocol::tool_call::ToolCallRequest;
use tokio::sync::watch;

use super::admission::{admit, hash, refusal, Admission};
use super::observed::Outcome as ConsentOutcome;
use super::private_content::PrivateContent;
use super::spend::Spend;

/// The private-content answer for a route with no cerebellum or Basal behind it, which is what
/// a test that drives this route directly gets: the scope counts as marked and its sources are
/// unknown, so only local models and providers the operator listed may be called.
pub struct NoPrivateContent;

#[async_trait::async_trait]
impl super::Asks for NoPrivateContent {
    async fn marked(
        &self,
        _scope_ref: &str,
        _parent_scope_ref: Option<&str>,
    ) -> Option<broca_subc::PrivateContent> {
        None
    }

    async fn flow_providers(
        &self,
        _flow_id: &str,
    ) -> Result<broca_subc::FlowPolicy, broca_subc::FlowProvidersError> {
        Err(broca_subc::FlowProvidersError::Unanswered(
            "no Basal behind this route".into(),
        ))
    }
}

type Answer = Result<Box<RawValue>, Vec<u8>>;

/// tool-provider 0.13's name for the opt-in reply format that 0.10 called `content/v1`. The
/// rename changed nothing else: the reply shapes, held replies and late results are identical,
/// so a consumer built against either version reads the same bodies.
pub const TOOL_RESULT_V1: &str = "tool-result/v1";

/// What a route's consumer declared at bind about reading replies.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConsumerFormat {
    /// The reply-format name the consumer declared, if any. The catalog echoes it, because a
    /// 0.13 consumer decodes replies only for a tool whose `result_format` it recognises.
    /// `None` keeps the legacy body: the bare tool result.
    pub result_format: Option<&'static str>,
    /// The consumer can hold a call for a later result (`held-reply/v1` with a result format).
    pub held_reply: bool,
}

impl ConsumerFormat {
    /// Read the bind's consumer capabilities, accepting either name of the reply format.
    pub fn from_capabilities(capabilities: Option<&[String]>) -> Self {
        let declares = |name: &str| capabilities.is_some_and(|caps| caps.iter().any(|c| c == name));
        let result_format = if declares(TOOL_RESULT_V1) {
            Some(TOOL_RESULT_V1)
        } else if declares(cortexkit_role_tool_provider::CONTENT_V1_CAPABILITY) {
            Some(cortexkit_role_tool_provider::CONTENT_V1_CAPABILITY)
        } else {
            None
        };
        Self {
            result_format,
            held_reply: result_format.is_some()
                && declares(cortexkit_role_tool_provider::HELD_REPLY_V1),
        }
    }
}

/// The classify tool's catalog name. Codemode programs call tools as `tools.<name>`, so a name
/// must be a plain identifier; the contract's tag `llm.classify/v1` goes in `capabilities`.
pub const CLASSIFY_TOOL_NAME: &str = "llm_classify";

#[derive(serde::Deserialize)]
struct RawEnvelope<'a> {
    #[serde(borrow)]
    arguments: &'a RawValue,
}

struct Flight {
    scope: BatchScope,
    digest: String,
    result: watch::Receiver<Option<Answer>>,
}

pub struct LlmTools {
    system: Arc<crate::system_one::SystemOne>,
    config: crate::config::LlmToolsConfig,
    private_content: PrivateContent,
    flights: Mutex<HashMap<String, Flight>>,
    held: Arc<super::held::HeldCalls>,
    consent: Option<Arc<super::consent::Consent>>,
    consent_workers: Mutex<std::collections::HashSet<String>>,
    consent_stop: tokio_util::sync::CancellationToken,
    /// Grants that have already released a call in this process. Only the first release by
    /// each grant is logged, so a run of calls under one grant writes one line.
    released_grants: Mutex<std::collections::HashSet<String>>,
}

impl LlmTools {
    pub fn new(
        system: Arc<crate::system_one::SystemOne>,
        config: crate::config::LlmToolsConfig,
        asks: Arc<dyn super::Asks>,
    ) -> Self {
        assert_eq!(
            system.store_role(),
            crate::system_one::LLM_CLASSIFY_STORE_ROLE
        );
        let private_content = PrivateContent::new(config.clone(), asks);
        Self {
            system,
            config,
            private_content,
            flights: Mutex::new(HashMap::new()),
            consent: None,
            consent_workers: Mutex::new(std::collections::HashSet::new()),
            consent_stop: tokio_util::sync::CancellationToken::new(),
            released_grants: Mutex::new(std::collections::HashSet::new()),
            held: Arc::new(super::held::HeldCalls::new(
                Arc::new(GrantsStore::open_in_memory().expect("in-memory grants store opens")),
                crate::serve::process_launch_id().into(),
            )),
        }
    }

    pub fn with_held(mut self, held: Arc<super::held::HeldCalls>) -> Self {
        self.held = held;
        self
    }

    /// The private-content counters `health.check` reports: each call's verdict and how the
    /// calls found marked ended at the consent step. Counts only; they start at zero with the
    /// process.
    pub fn consent_health(&self) -> Value {
        self.private_content.counters.health()
    }

    fn count(&self, outcome: ConsentOutcome) {
        self.private_content.counters.outcome(outcome);
    }

    /// `consent_error`, counted as the consent step failing.
    fn consent_unavailable(&self, error: impl std::fmt::Display) -> Vec<u8> {
        self.count(ConsentOutcome::ConsentUnavailable);
        consent_error(error)
    }

    /// Log the first call `grant` releases in this process. It names who holds the grant, for
    /// which provider, how long it lasts and which run used it, and nothing from the call.
    fn note_release(&self, grant: &broca_store::grants::Grant, scope_ref: &str) {
        let first = self
            .released_grants
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(grant.id.clone());
        if !first {
            return;
        }
        let kind = match grant.kind {
            broca_store::grants::GrantKind::Run => "run",
            broca_store::grants::GrantKind::Hours24 => "24_hours",
        };
        // A run grant has no clock expiry: it lasts while its run is live.
        let expires_at_ms = grant
            .expires_at_ms
            .map_or_else(|| "run_end".to_owned(), |expiry| expiry.to_string());
        tracing::info!(
            target: "llm_tools.consent",
            grant_id = %grant.id,
            agent_id = %grant.holder,
            provider = %grant.provider,
            kind,
            %expires_at_ms,
            scope_ref,
            "a standing grant released a private-content call without a consent card"
        );
    }

    pub(crate) fn permissions(&self) -> super::permissions::Permissions {
        super::permissions::Permissions::new(
            self.held.store.clone(),
            self.consent.as_ref().map(|consent| consent.io.clone()),
        )
    }

    pub fn with_consent(
        mut self,
        consent: Arc<super::consent::Consent>,
        stop: tokio_util::sync::CancellationToken,
    ) -> Self {
        assert!(
            Arc::ptr_eq(&consent.store, &self.held.store),
            "consent and replay must share their authority"
        );
        self.consent = Some(consent);
        self.consent_stop = stop;
        self
    }

    pub fn recover_consent(self: &Arc<Self>) -> Result<(), broca_store::grants::GrantsError> {
        if let Some(consent) = &self.consent {
            for card in consent.store.pending_cards()? {
                self.watch_consent(card.dedup_key);
            }
            for row in consent.store.recovering(self.system.tool_now())? {
                if matches!(row.state, HeldState::Filing | HeldState::Held) {
                    self.watch_consent(row.dedup_key);
                }
            }
        }
        Ok(())
    }

    fn watch_consent(self: &Arc<Self>, key: String) {
        if self.consent_stop.is_cancelled() {
            return;
        }
        if !self
            .consent_workers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key.clone())
        {
            return;
        }
        let service = self.clone();
        tokio::spawn(async move {
            let work = async {
                let Some(consent) = &service.consent else {
                    return;
                };
                loop {
                    let now = service.system.tool_now();
                    if let Err(error) = service.held.sweep_image_entries(consent, now).await {
                        tracing::warn!(%error, "held-call image cleanup will be retried");
                    }
                    if let Err(error) = consent.resume_granted(now).await {
                        tracing::warn!(%error, "held-call grant reconciliation failed");
                    }
                    let _ = service.recover_authorized();
                    let Ok(Some(card)) = consent.store.card(&key) else {
                        return;
                    };
                    let Ok(rows) = consent.store.card_calls(&key, now) else {
                        return;
                    };
                    if rows
                        .last()
                        .is_none_or(|row| now >= row.deadline_at_ms.saturating_add(86_400_000))
                    {
                        return;
                    }
                    if matches!(
                        card.state,
                        broca_store::grants::CardState::Allowed
                            | broca_store::grants::CardState::Denied
                    ) {
                        let _ = service.recover_authorized();
                        let waiting = consent.store.card_calls(&key, now).is_ok_and(|rows| {
                            rows.iter()
                                .any(|row| matches!(row.state, HeldState::Filing | HeldState::Held))
                        });
                        if !waiting {
                            return;
                        }
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        continue;
                    }
                    if card.state == broca_store::grants::CardState::Filing {
                        if let Err(error) = consent.recover(&key, now).await {
                            tracing::warn!(%error, "consent filing recovery failed");
                        }
                    } else {
                        match consent.wait_once(&key, now).await {
                            Ok(false) => {
                                let _ = service
                                    .held
                                    .sweep_image_entries(consent, service.system.tool_now())
                                    .await;
                                if let Err(error) =
                                    consent.resume_granted(service.system.tool_now()).await
                                {
                                    tracing::warn!(%error, "held-call grant reconciliation failed");
                                }
                                let _ = service.recover_authorized();
                                return;
                            }
                            Ok(true) => (),
                            Err(error) => {
                                tracing::warn!(%error, "consent await persistence failed")
                            }
                        }
                    }
                    let _ = service.recover_authorized();
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            };
            tokio::select! { _ = service.consent_stop.cancelled() => (), _ = work => () }
            service
                .consent_workers
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .remove(&key);
        });
    }

    /// Only previously authorized rows dispatch on startup. Filing and held rows remain in
    /// consent recovery, and their timers use the persisted deadlines rather than a new wait.
    pub fn recover_authorized(self: &Arc<Self>) -> Result<(), broca_store::grants::GrantsError> {
        for row in self.held.store.recovering(self.system.tool_now())? {
            if row.state != HeldState::Authorized {
                continue;
            }
            // An authorized row always keeps its validated request. A row without one can't be
            // dispatched, so it stays recorded and is skipped here rather than panicking the task.
            let Some(params) = row.request.clone() else {
                tracing::warn!(target: "llm_tools", batch = %row.batch_id,
                    "authorized held call has no stored request; not dispatched at startup");
                continue;
            };
            let service = self.clone();
            tokio::spawn(async move {
                let admission = Admission {
                    scope: row.batch_scope.clone(),
                    batch_id: row.batch_id.clone(),
                    args_digest: row.args_digest.clone(),
                    provider: row.provider.clone(),
                    params,
                };
                let _ = service
                    .call(admission, true, &SendScope::Internal, Some(row), None)
                    .await;
            });
        }
        Ok(())
    }

    /// In-process entrypoint for callers that already support held replies. Wire routes must
    /// use `answer_with_capabilities` with the daemon's bind declaration instead.
    pub async fn answer(
        self: &Arc<Self>,
        body: &[u8],
        principal: Option<&BindPrincipal>,
        scope: &SendScope,
    ) -> Answer {
        self.answer_with_capabilities(body, principal, scope, true)
            .await
    }

    pub async fn answer_with_capabilities(
        self: &Arc<Self>,
        body: &[u8],
        principal: Option<&BindPrincipal>,
        scope: &SendScope,
        reads_held_reply: bool,
    ) -> Answer {
        let consumer = ConsumerFormat {
            result_format: None,
            held_reply: reads_held_reply,
        };
        self.answer_for_consumer(body, principal, scope, consumer)
            .await
    }

    /// Answer one tool-provider request for a consumer whose bind declared `consumer`. A
    /// consumer that declared a reply format gets the catalog in that name and successes as
    /// tool results whose `structuredContent` is the tool's result object.
    pub async fn answer_for_consumer(
        self: &Arc<Self>,
        body: &[u8],
        principal: Option<&BindPrincipal>,
        scope: &SendScope,
        consumer: ConsumerFormat,
    ) -> Answer {
        let reads_held_reply = consumer.held_reply;
        let call: ToolCallRequest = serde_json::from_slice(body)
            .map_err(|e| unspent(Refusal::invalid_params("request", e.to_string())))?;
        match call.name.as_str() {
            // Refusals of model tool calls, an unknown tool name included, state spend; the
            // provider's own operations (`role.describe`, `tool.catalog`, late results) do not.
            "role.describe" => raw(
                None,
                json!({
                    "majors": [{"version":"tool-provider/v1", "ops":["role.describe", "tool.catalog", "late_results", "late_results.ack"], "stability":"alpha"}],
                    "implementation_version": env!("CARGO_PKG_VERSION"),
                }),
            ),
            "tool.catalog" => self.catalog(&call.arguments, consumer.result_format),
            "late_results" => self
                .held
                .read(&call.arguments, principal, self.system.tool_now()),
            "late_results.ack" => self.held.acknowledge(&call.arguments, principal),
            CLASSIFY_TOOL_NAME => {
                let envelope: RawEnvelope<'_> = serde_json::from_slice(body)
                    .map_err(|e| unspent(Refusal::invalid_params("arguments", e.to_string())))?;
                let admission = admit(
                    principal,
                    scope,
                    envelope.arguments,
                    call.call_key.as_deref(),
                    self.system.image_references(),
                )
                .map_err(unspent)?;
                let pin =
                    cortexkit_role_tool_provider::call::check_call(&call).map_err(|error| {
                        Spend::Nothing
                            .mark_body(serde_json::to_vec(&error).expect("call refusal encodes"))
                    })?;
                if let Some(pin) = pin {
                    let digest =
                        cortexkit_role_tool_provider::catalog::schema_digest(&self.input_schema())
                            .expect("contract schema has a digest");
                    if pin.schema_digest != digest {
                        return Err(serde_json::to_vec(&json!({"code":"tool_schema_changed", "message":"the pinned tool schema is no longer served", "detail":{"tool":call.name, "expected":pin.schema_digest, "current":digest, "no_spend":true}})).expect("refusal encodes"));
                    }
                    if pin.semantics != 1 {
                        return Err(serde_json::to_vec(&json!({"code":"tool_semantics_changed", "message":"the pinned tool semantics are no longer served", "detail":{"tool":call.name, "expected":pin.semantics, "current":1, "no_spend":true}})).expect("refusal encodes"));
                    }
                }
                let held = if let Some(call_key) = &call.call_key {
                    self.held.replay(
                        &admission,
                        principal,
                        scope,
                        call_key,
                        self.system.tool_now(),
                    )?
                } else {
                    None
                };
                let caller = principal.map(BindPrincipal::as_owner_string);
                let hold_caller = if reads_held_reply {
                    caller.as_deref().zip(call.call_key.as_deref())
                } else {
                    None
                };
                let answer = self
                    .call(admission, call.call_key.is_some(), scope, held, hold_caller)
                    .await;
                match consumer.result_format {
                    Some(_) => answer.and_then(as_tool_result),
                    None => answer,
                }
            }
            _ => Err(Spend::Nothing.mark_body(
                serde_json::to_vec(&subc_protocol::ErrorBody::new(
                    "unknown_method",
                    format!("tool-provider does not serve {}", call.name),
                ))
                .expect("error encodes"),
            )),
        }
    }

    fn catalog(&self, arguments: &Value, result_format: Option<&'static str>) -> Answer {
        let request: cortexkit_role_tool_provider::catalog::CatalogRequest =
            serde_json::from_value(arguments.clone())
                .map_err(|e| catalog_error("arguments", e.to_string()))?;
        if let Some(field) = request.params.keys().next() {
            return Err(catalog_error(
                &format!("params.{field}"),
                format!("model tools have no catalog parameter {field}"),
            ));
        }
        if request.preset.is_some() {
            return Err(catalog_error(
                "preset",
                "model tools have no catalog presets".into(),
            ));
        }
        if request.system_text.is_some() {
            return Err(catalog_error(
                "system_text",
                "model tools supply no system text".into(),
            ));
        }
        let schema = self.input_schema();
        let mut tool = json!({
            "name":CLASSIFY_TOOL_NAME, "schema_digest": cortexkit_role_tool_provider::catalog::schema_digest(&schema).expect("contract schema has a digest"),
            "semantics":1, "capabilities":[], "description":"Classify one item with a decision model.",
            "input_schema":schema,
            "modes":cortexkit_llm_tools::MODES,
            "result_format":result_format.unwrap_or(cortexkit_role_tool_provider::CONTENT_V1_CAPABILITY),
        });
        if self.config.advertise_tags {
            tool["capabilities"] = json!([classify::TAG]);
        }
        let generation = hash(&tool);
        let mut answer =
            json!({"generation":generation,"tools":[tool],"capabilities":{"late_results":true}});
        if let Some(composition) = request.composition {
            answer["composition_digest"] = json!(hash(
                &serde_json::to_value(composition).expect("composition encodes")
            ));
        }
        let digest = hash(&answer);
        if request.digest_only == Some(true) {
            return raw(
                None,
                json!({"generation":generation,"catalog_digest":digest}),
            );
        }
        answer["catalog_digest"] = json!(digest);
        raw(None, answer)
    }

    fn input_schema(&self) -> Value {
        if self.system.image_references() {
            classify::input_schema_with_image_references()
        } else {
            classify::input_schema()
        }
    }

    async fn call(
        self: &Arc<Self>,
        mut admission: Admission,
        keyed: bool,
        scope: &SendScope,
        held: Option<HeldCall>,
        hold_caller: Option<(&str, &str)>,
    ) -> Answer {
        if let Some(row) = &held {
            match row.state {
                HeldState::Final => return super::held::HeldCalls::final_reply(row),
                HeldState::Filing | HeldState::Held => {
                    if hold_caller.is_none() {
                        return Err(super::held::not_started("consent_required").into_bytes());
                    }
                    return super::held::HeldCalls::held_reply(row);
                }
                HeldState::Authorized => {
                    let Some(params) = &row.request else {
                        return Err(unspent(crate::system_one::internal_refusal(
                            "authorized held call has no validated request",
                        )));
                    };
                    admission.params.clone_from(params);
                }
            }
        }
        // Read replay before catalog, pricing or private-content policy. A changed catalog or
        // policy must never turn an already-paid answer into a new provider attempt.
        let stored = if keyed {
            self.system
                .stored_batch(&admission.batch_id)
                .await
                .map_err(|_| {
                    unspent(crate::system_one::internal_refusal(
                        "batch replay is unavailable",
                    ))
                })?
                .filter(|stored| {
                    self.system.tool_now().saturating_sub(stored.admitted_at_ms)
                        < broca_store::system_one::RETENTION_MS
                })
        } else {
            None
        };
        if let Some(stored) = &stored {
            // A refusal about the recorded batch reports this invocation's spend, which is
            // nothing: it sent no request. The recorded call's own cost reaches the caller
            // through its replayed answer, so it is never charged twice.
            check_replay(stored, &admission).map_err(unspent)?;
            if has_final_outcome(stored) {
                // Replaying sends nothing, so a failure to read the record spent nothing; the
                // replayed answer itself carries the spend its first answer carried.
                let answer = convert(
                    &self
                        .system
                        .replay_tool(&admission.params, stored)
                        .await
                        .map_err(unspent)?,
                );
                if held.is_some() {
                    self.persist_held_answer(&admission.batch_id, &answer, Spend::Nothing)?;
                }
                return answer;
            }
        }
        let running = {
            let flights = self.flights.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(flight) = flights.get(&admission.batch_id) {
                if flight.scope != admission.scope {
                    return Err(unspent(refusal(
                        "batch_scope_mismatch",
                        "batch_id is executing under another scope",
                    )));
                }
                if flight.digest != admission.args_digest {
                    return Err(unspent(Refusal::batch_id_reuse("arguments")));
                }
                Some(flight.result.clone())
            } else {
                None
            }
        };
        if let Some(receiver) = running {
            return await_flight(receiver).await;
        }
        let kept = match held.as_ref() {
            Some(row) => self
                .held
                .images
                .get(row, scope, &admission)
                .map_err(unspent)?,
            None => None,
        };
        let image_scope = match &kept {
            Some(kept) => SendScope::Stamped(kept.stamp.clone()),
            // Restarted holds have no attested parent metadata in memory. Never guess it
            // from the durable row or substitute the stamp on a later retry.
            None if held.is_some() => SendScope::Internal,
            None => scope.clone(),
        };
        let images = match kept.and_then(|kept| kept.images) {
            Some(images) => images,
            None => match self
                .system
                .prepare_images(&admission.params, &image_scope, stored.as_ref())
                .await
            {
                Ok(images) => Arc::new(images),
                Err(refusal) => {
                    let answer = Err(unspent(refusal));
                    if held.is_some() {
                        self.persist_held_answer(&admission.batch_id, &answer, Spend::Nothing)?;
                    }
                    return answer;
                }
            },
        };
        let pricing = if let Some(row) = &held {
            serde_json::from_value(json!({"frozen_row":row.frozen_row})).map_err(|_| {
                unspent(crate::system_one::internal_refusal(
                    "held pricing snapshot is unreadable",
                ))
            })?
        } else {
            self.system
                .admit_tool(&admission.params, stored.as_ref())
                .map_err(unspent)?
        };
        // Private-content admission runs on every call that is not replaying a stored answer:
        // a repeat must return the bytes it already paid for even after the operator's list
        // changed or cerebellum started marking the scope.
        if stored.is_none() && held.is_none() {
            if let Err(refusal) = self.private_content.admit(scope, &admission.provider).await {
                let reason: Value = serde_json::from_slice(&refusal).unwrap_or(Value::Null);
                let consent_reason = reason["detail"]["consent"].as_str().unwrap_or("");
                if !matches!(
                    consent_reason,
                    "consent_required" | "private_content_unknown" | "private_content_unreachable"
                ) {
                    // A marked flow asking for a provider outside its approved set. The other
                    // refusals here come before any verdict and are not a marked call's outcome.
                    if reason["code"] == "provider_not_allowed" {
                        self.count(ConsentOutcome::RefusedByFlowPolicy);
                    }
                    return Err(refusal);
                }
                let Some(consent) = &self.consent else {
                    self.count(ConsentOutcome::refused(consent_reason));
                    return Err(refusal);
                };
                let SendScope::Stamped(bind) = scope else {
                    self.count(ConsentOutcome::refused(consent_reason));
                    return Err(refusal);
                };
                let agent = bind.scope.agent_id.as_deref().unwrap_or("");
                let identity = super::consent::identity(&bind.scope);
                let now = self.system.tool_now();
                if consent
                    .store
                    .denied(agent, &identity, &admission.provider)
                    .map_err(|e| self.consent_unavailable(e))?
                {
                    self.count(ConsentOutcome::DeniedByStandingDeny);
                    return Err(super::held::not_started("denied").into_bytes());
                }
                let grant = consent
                    .releasing_grant(&bind.scope, &admission.provider, now)
                    .await
                    .map_err(|e| self.consent_unavailable(e))?;
                if let Some(grant) = &grant {
                    self.count(ConsentOutcome::ReleasedByGrant);
                    self.note_release(grant, &bind.scope.scope_ref);
                } else {
                    if consent_reason != "consent_required" {
                        self.count(ConsentOutcome::refused(consent_reason));
                        return Err(refusal);
                    }
                    let Some((principal, call_key)) = hold_caller else {
                        self.count(ConsentOutcome::RefusedConsentRequired);
                        return Err(refusal);
                    };
                    let params: Value = serde_json::from_str(&admission.params)
                        .map_err(|e| self.consent_unavailable(e))?;
                    let model = params["model"].as_str().unwrap_or("");
                    let display_row = self.system.tool_model_row(model);
                    let Some((provider_label, model_label)) = display_row
                        .as_ref()
                        .and_then(crate::system_one::consent_model_labels)
                    else {
                        self.count(ConsentOutcome::ConsentUnavailable);
                        return Err(super::held::not_started("consent_unavailable").into_bytes());
                    };
                    let sources = self.private_content.card_sources(scope).await;
                    let dedup_key = consent
                        .dedup_key(agent, &admission.provider, &identity)
                        .map_err(|e| self.consent_unavailable(e))?;
                    let body = broca_subc::consent_card::private_content_card(
                        provider_label,
                        model_label,
                        &sources,
                        &dedup_key,
                    )
                    .map_err(|e| self.consent_unavailable(e))?;
                    let frozen =
                        serde_json::to_value(&pricing).map_err(|e| self.consent_unavailable(e))?;
                    let row = consent
                        .file_with_admission(
                            HoldRequest {
                                batch_id: admission.batch_id.clone(),
                                scope: identity,
                                batch_scope: admission.scope.clone(),
                                principal: principal.into(),
                                call_key: call_key.into(),
                                agent_id: agent.into(),
                                provider: admission.provider.clone(),
                                model: model.into(),
                                request: admission.params.clone(),
                                args_digest: admission.args_digest.clone(),
                                frozen_row: frozen["frozen_row"].as_str().unwrap_or("").into(),
                                dedup_key: dedup_key.clone(),
                                admitted_at_ms: now,
                            },
                            body,
                            now,
                            |row| {
                                self.held.images.retain(
                                    self.held.store.clone(),
                                    row,
                                    bind,
                                    images.clone(),
                                    now,
                                )
                            },
                        )
                        .await
                        .map_err(|e| self.consent_unavailable(e))?;
                    self.count(ConsentOutcome::CardFiled);
                    self.watch_consent(dedup_key);
                    return match row.state {
                        HeldState::Final => super::held::HeldCalls::final_reply(&row),
                        _ => super::held::HeldCalls::held_reply(&row),
                    };
                }
            }
        }
        let receiver = {
            let mut flights = self.flights.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(flight) = flights.get(&admission.batch_id) {
                if flight.scope != admission.scope {
                    return Err(unspent(refusal(
                        "batch_scope_mismatch",
                        "batch_id is executing under another scope",
                    )));
                }
                if flight.digest != admission.args_digest {
                    return Err(unspent(Refusal::batch_id_reuse("arguments")));
                }
                flight.result.clone()
            } else {
                let (sender, receiver) = watch::channel(None);
                flights.insert(
                    admission.batch_id.clone(),
                    Flight {
                        scope: admission.scope.clone(),
                        digest: admission.args_digest.clone(),
                        result: receiver.clone(),
                    },
                );
                let service = self.clone();
                tokio::spawn(async move {
                    let batch_id = admission.batch_id.clone();
                    let system = service.system.clone();
                    let admitted_at = held.as_ref().map(|row| row.admitted_at_ms);
                    let execution = tokio::spawn(async move {
                        system
                            .execute_tool(
                                admission.params,
                                admission.scope,
                                admission.args_digest,
                                pricing,
                                admitted_at,
                                images,
                            )
                            .await
                    })
                    .await;
                    let answer = match execution {
                        Ok(Ok(reply)) => convert(&reply),
                        // `execute_tool` marks every refusal it returns with its spend.
                        Ok(Err(refusal)) => Err(error(refusal, None)),
                        // The task may have ended after its provider request was sent.
                        Err(_) => Err(error(
                            crate::system_one::internal_refusal(
                                "model tool execution failed; retry the call",
                            ),
                            Some(Spend::Unknown),
                        )),
                    };
                    let answer = if held.is_some() {
                        service
                            .persist_held_answer(&batch_id, &answer, Spend::Unknown)
                            .and(answer)
                    } else {
                        answer
                    };
                    sender.send_replace(Some(answer));
                    service
                        .flights
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .remove(&batch_id);
                });
                receiver
            }
        };
        await_flight(receiver).await
    }

    /// Record a held call's outcome. `lost` is the spend a failure to record states: unknown
    /// after an execution that may have reached the provider, nothing when no request was sent.
    fn persist_held_answer(
        &self,
        batch_id: &str,
        answer: &Answer,
        lost: Spend,
    ) -> Result<(), Vec<u8>> {
        let error = |refusal| error(refusal, Some(lost));
        // Transient execution refusals stay authorized so recovery can retry them. They must
        // not be converted into a not-started consent outcome after dispatch.
        if let Ok(body) = answer {
            self.held
                .store
                .complete_authorized(batch_id, body.get(), self.system.tool_now())
                .map_err(|_| {
                    error(crate::system_one::internal_refusal(
                        "held outcome persistence is unavailable",
                    ))
                })?;
        } else if let Err(body) = answer {
            let value: Value = serde_json::from_slice(body).map_err(|_| {
                error(crate::system_one::internal_refusal(
                    "held execution error is unreadable",
                ))
            })?;
            if value["detail"]["class"] == "permanent" {
                self.held
                    .store
                    .complete_authorized_error(
                        batch_id,
                        std::str::from_utf8(body).map_err(|_| {
                            error(crate::system_one::internal_refusal(
                                "held execution error is not UTF-8",
                            ))
                        })?,
                        self.system.tool_now(),
                    )
                    .map_err(|_| {
                        error(crate::system_one::internal_refusal(
                            "held error persistence is unavailable",
                        ))
                    })?;
            }
        }
        Ok(())
    }
}

fn consent_error(error: impl std::fmt::Display) -> Vec<u8> {
    tracing::warn!(%error, "model-tool consent is unavailable");
    super::held::not_started("consent_unavailable").into_bytes()
}

async fn await_flight(mut receiver: watch::Receiver<Option<Answer>>) -> Answer {
    loop {
        if let Some(answer) = receiver.borrow_and_update().clone() {
            return answer;
        }
        // The shared execution ended without an answer, possibly after its provider request.
        receiver.changed().await.map_err(|_| {
            error(
                crate::system_one::internal_refusal("model tool execution stopped"),
                Some(Spend::Unknown),
            )
        })?;
    }
}

fn check_replay(stored: &StoredBatch, admission: &Admission) -> Result<(), Refusal> {
    if stored.scope.as_ref() != Some(&admission.scope) {
        return Err(refusal(
            "batch_scope_mismatch",
            "batch_id belongs to a different stamped scope",
        ));
    }
    if stored.args_digest != admission.args_digest {
        return Err(Refusal::batch_id_reuse("arguments"));
    }
    Ok(())
}

fn has_final_outcome(stored: &StoredBatch) -> bool {
    stored
        .outcomes
        .first()
        .is_some_and(|outcome| match &outcome.outcome {
            Outcome::Answered(_) => true,
            Outcome::Failed(body) => {
                serde_json::from_str::<cortexkit_role_classifier::errors::ItemError>(body)
                    .map_or(true, |error| !error.is_retried_on_resend())
            }
        })
}

/// Encode a refusal with its spend mark. `None` is for a refusal an earlier step already
/// marked (a refusal `execute_tool` returns, or an item error that carries its stored mark)
/// and for refusals of the provider's own operations, which answer no model call; every model
/// tool call's other refusals state the spend.
fn error(refusal: Refusal, spend: Option<Spend>) -> Vec<u8> {
    let refusal = match spend {
        Some(spend) => spend.mark(refusal),
        None => refusal,
    };
    crate::system_one::refusal_body(&refusal)
}

/// A refusal returned before this invocation sent anything to a provider.
fn unspent(refusal: Refusal) -> Vec<u8> {
    error(refusal, Some(Spend::Nothing))
}

fn catalog_error(field: &str, message: String) -> Vec<u8> {
    let mut body = subc_protocol::ErrorBody::new("invalid_request", message);
    body.detail = Some(json!({"field":field}));
    serde_json::to_vec(&body).expect("catalog refusal encodes")
}

/// Wrap a bare tool result as a tool-result reply: the JSON text for display, and the same object
/// as `structuredContent`, which consumers hand to the caller. Held replies already carry
/// `content` and pass through unchanged.
fn as_tool_result(body: Box<RawValue>) -> Answer {
    // The body is an answer that may already have been paid for, so losing it is unknown spend.
    let value: Value = serde_json::from_str(body.get()).map_err(|_| {
        error(
            crate::system_one::internal_refusal("model tool reply cannot decode"),
            Some(Spend::Unknown),
        )
    })?;
    if !value.is_object() || value.get("content").is_some() {
        return Ok(body);
    }
    raw(
        Some(Spend::Unknown),
        json!({"content":[{"type":"text","text":body.get()}],"structuredContent":value}),
    )
}

/// Encode a reply. `lost` is the spend a failure to encode it states: `None` for the
/// provider's own operations (`role.describe`, `tool.catalog`), which answer no model call and
/// keep their refusal bodies free of spend members.
fn raw(lost: Option<Spend>, value: Value) -> Answer {
    serde_json::value::to_raw_value(&value).map_err(|_| {
        error(
            crate::system_one::internal_refusal("model tool reply cannot encode"),
            lost,
        )
    })
}

fn convert(reply: &RawValue) -> Answer {
    #[derive(serde::Deserialize)]
    struct BatchReply<'a> {
        #[serde(borrow)]
        items: Vec<ItemReply<'a>>,
        cost_usd: Option<serde_json::Number>,
    }
    #[derive(serde::Deserialize)]
    struct ItemReply<'a> {
        #[serde(borrow)]
        answers: Option<&'a RawValue>,
        error: Option<super::spend::MarkedItemError>,
        usage: Option<cortexkit_llm_tools::Usage>,
    }
    #[derive(serde::Serialize)]
    struct ToolResult<'a> {
        answers: &'a RawValue,
        usage: cortexkit_llm_tools::Usage,
        cost_usd: serde_json::Number,
    }
    // Before the reply's cost is read, a reply that cannot be read may hide a paid answer.
    let batch: BatchReply<'_> = serde_json::from_str(reply.get()).map_err(|_| {
        error(
            crate::system_one::internal_refusal("stored model reply cannot decode"),
            Some(Spend::Unknown),
        )
    })?;
    let item = batch.items.into_iter().next().ok_or_else(|| {
        error(
            crate::system_one::internal_refusal("stored model reply has no item"),
            Some(Spend::Unknown),
        )
    })?;
    if let Some(failure) = item.error {
        // The item error carries the spend mark it was stored with. One stored before model
        // tools marked refusals has none and replays unmarked, as its first answer was.
        return Err(error(failure.into_refusal(), None));
    }
    let Some(cost_usd) = batch.cost_usd else {
        // `cost_unmeasured` is its own spend mark.
        return Err(error(super::unmeasured_cost_refusal(), None));
    };
    // From here the call's billed cost is known, so every refusal charges exactly that.
    let measured = Some(cost_usd.as_f64().map_or(Spend::Unknown, Spend::Measured));
    let answers = item.answers.ok_or_else(|| {
        error(
            crate::system_one::internal_refusal("stored model reply has no answers"),
            measured,
        )
    })?;
    // Validate the shared result type without re-serializing provider answers. Unknown fields,
    // object order and JSON number spellings remain exactly as the provider returned them.
    let result = serde_json::value::to_raw_value(&ToolResult {
        answers,
        usage: item.usage.unwrap_or_default(),
        cost_usd,
    })
    .map_err(|_| {
        error(
            crate::system_one::internal_refusal("model tool reply cannot encode"),
            measured,
        )
    })?;
    let parsed: classify::Result = serde_json::from_str(result.get()).map_err(|_| {
        error(
            cortexkit_llm_tools::errors::refusal(
                "provider_error",
                "the provider returned a malformed classification result",
            )
            .expect("the contract classifies provider_error"),
            measured,
        )
    })?;
    classify::validate_result(&parsed).map_err(|_| {
        error(
            cortexkit_llm_tools::errors::refusal(
                "provider_error",
                "the provider's classification result failed validation",
            )
            .expect("the contract classifies provider_error"),
            measured,
        )
    })?;
    Ok(result)
}

#[cfg(test)]
#[path = "route_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "spend_route_tests.rs"]
mod spend_tests;
