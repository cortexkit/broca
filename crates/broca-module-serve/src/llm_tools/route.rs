use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

use broca_protocol::{BindPrincipal, BindScope, SendScope};
use broca_store::grants::{GrantsStore, HeldCall, HeldState, HoldRequest};
use broca_store::system_one::{BatchScope, StoredBatch};
use cortexkit_llm_tools::classify;
use cortexkit_role_classifier::errors::Refusal;
use serde_json::{json, value::RawValue, Value};
use subc_protocol::tool_call::ToolCallRequest;
use tokio::sync::watch;

use super::admission::{admit, hash, refusal, Admission};
use super::extract;
use super::observed::Outcome as ConsentOutcome;
use super::private_content::PrivateContent;
use super::spend::Spend;
use crate::system_one::{PreparedImages, ToolAdmission, ToolRoute};

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

/// How many grants the release log remembers; see `LlmTools::released_grants`.
const RELEASED_GRANTS_KEPT: usize = 256;

/// tool-provider 0.13's name for the opt-in reply format that 0.10 called `content/v1`. The
/// rename changed nothing else: the reply shapes, held replies and late results are identical,
/// so a consumer built against either version reads the same bodies.
pub const TOOL_RESULT_V1: &str = "tool-result/v1";

/// tool-provider 0.10's name for the same reply format. tool-provider 0.13 no longer exports it,
/// but broca still accepts it at bind, and its catalog names it for a consumer that declared
/// no format, as every released build has.
pub const CONTENT_V1: &str = "content/v1";

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
        } else if declares(CONTENT_V1) {
            Some(CONTENT_V1)
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

impl Flight {
    fn join(&self, admission: &Admission) -> Result<watch::Receiver<Option<Answer>>, Vec<u8>> {
        if self.scope != admission.scope {
            return Err(unspent(refusal(
                "batch_scope_mismatch",
                "batch_id is executing under another scope",
            )));
        }
        if self.digest != admission.args_digest {
            return Err(unspent(Refusal::batch_id_reuse("arguments")));
        }
        Ok(self.result.clone())
    }
}

enum Pricing {
    Pinned(ToolAdmission),
    Routed(ToolRoute),
}

pub struct LlmTools {
    system: Arc<crate::system_one::SystemOne>,
    config: crate::config::LlmToolsConfig,
    private_content: PrivateContent,
    flights: Mutex<HashMap<String, Flight>>,
    routing_calls: Mutex<HashMap<String, Weak<tokio::sync::Mutex<()>>>>,
    held: Arc<super::held::HeldCalls>,
    consent: Option<Arc<super::consent::Consent>>,
    consent_workers: Mutex<std::collections::HashSet<String>>,
    consent_stop: tokio_util::sync::CancellationToken,
    /// One reconciliation of every held call at a time, shared by all card watchers: see
    /// [`LlmTools::reconcile`].
    reconciling: tokio::sync::Mutex<()>,
    /// Reconciliations asked for, and how many of those requests the last finished pass
    /// covered.
    reconcile_requests: std::sync::atomic::AtomicU64,
    reconciled: std::sync::atomic::AtomicU64,
    /// Grants that have already released a call in this process, oldest first. Only the first
    /// release by each grant is logged, so a run of calls under one grant writes one line. Only
    /// the most recent [`RELEASED_GRANTS_KEPT`] are remembered: an older grant releasing again
    /// logs one more line, which is cheaper than remembering every grant ever used.
    released_grants: Mutex<std::collections::VecDeque<String>>,
    /// `llm.complete/v1`, when this broca serves it: listed in the catalog and answered only then.
    complete: Option<Arc<super::complete::Complete>>,
    /// The daemon stamp of each completion held for consent in this process, by held-call key,
    /// so its approved run is charged to the caller's run or flow. A stored row can't carry a
    /// stamp; a hold that outlived a restart runs without one.
    completion_stamps: Mutex<HashMap<String, (i64, BindScope)>>,
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
            routing_calls: Mutex::new(HashMap::new()),
            consent: None,
            consent_workers: Mutex::new(std::collections::HashSet::new()),
            consent_stop: tokio_util::sync::CancellationToken::new(),
            reconciling: tokio::sync::Mutex::new(()),
            reconcile_requests: std::sync::atomic::AtomicU64::new(0),
            reconciled: std::sync::atomic::AtomicU64::new(0),
            released_grants: Mutex::new(std::collections::VecDeque::new()),
            complete: None,
            completion_stamps: Mutex::new(HashMap::new()),
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

    /// Serve `llm.complete/v1` beside classify.
    pub fn with_complete(mut self, complete: Arc<super::complete::Complete>) -> Self {
        self.complete = Some(complete);
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
        {
            let mut released = self
                .released_grants
                .lock()
                .unwrap_or_else(|p| p.into_inner());
            if released.contains(&grant.id) {
                return;
            }
            if released.len() >= RELEASED_GRANTS_KEPT {
                released.pop_front();
            }
            released.push_back(grant.id.clone());
        }
        let kind = match grant.kind {
            broca_store::grants::GrantKind::Run => "run",
            broca_store::grants::GrantKind::Hours24 => "24_hours",
            broca_store::grants::GrantKind::Always => "always",
        };
        // Run grants end with their run; always grants have no automatic expiry.
        let expires_at_ms = grant.expires_at_ms.map_or_else(
            || if kind == "always" { "none" } else { "run_end" }.to_owned(),
            |expiry| expiry.to_string(),
        );
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
                    service.reconcile(consent).await;
                    let now = service.system.tool_now();
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
                                service.reconcile(consent).await;
                                return;
                            }
                            Ok(true) => (),
                            Err(error) => {
                                tracing::warn!(%error, "consent await persistence failed")
                            }
                        }
                    }
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

    /// Reconcile every held call with the durable consent state: drop finished calls' images,
    /// authorize waiting calls a grant now releases, and dispatch authorized calls that are due.
    /// One watcher runs per pending card and each asks for this on every wake, but a pass reads
    /// every held call whatever card it waits on. So a pass that started after a request covers
    /// it: watchers waking together share one pass instead of each repeating it.
    async fn reconcile(self: &Arc<Self>, consent: &super::consent::Consent) {
        use std::sync::atomic::Ordering;
        let requested = self.reconcile_requests.fetch_add(1, Ordering::SeqCst) + 1;
        let _turn = self.reconciling.lock().await;
        if self.reconciled.load(Ordering::SeqCst) >= requested {
            return;
        }
        let covers = self.reconcile_requests.load(Ordering::SeqCst);
        let now = self.system.tool_now();
        if let Err(error) = self.held.sweep_image_entries(consent, now).await {
            tracing::warn!(%error, "held-call image cleanup will be retried");
        }
        if let Err(error) = consent.resume_granted(now).await {
            tracing::warn!(%error, "held-call grant reconciliation failed");
        }
        if let Err(error) = self.dispatch_authorized() {
            tracing::warn!(%error, "authorized held calls will be dispatched on the next pass");
        }
        self.reconciled.store(covers, Ordering::SeqCst);
    }

    /// Dispatch every authorized call that is due, and hand later retries to the held-call
    /// timer, which calls back on each tick. Called at startup, when no call has a retry
    /// scheduled, so every authorized call is dispatched once. Filing and held rows remain in
    /// consent recovery, and their timers use the persisted deadlines rather than a new wait.
    pub fn recover_authorized(self: &Arc<Self>) -> Result<(), broca_store::grants::GrantsError> {
        let tools = Arc::downgrade(self);
        self.held.set_dispatch(Arc::new(move || {
            if let Some(tools) = tools.upgrade() {
                if let Err(error) = tools.dispatch_authorized() {
                    tracing::warn!(%error, "authorized held calls will be dispatched on the next tick");
                }
            }
        }));
        self.dispatch_authorized()
    }

    /// Start an attempt for each authorized call whose retry is due. A call already running
    /// or waiting out its backoff is left alone.
    fn dispatch_authorized(self: &Arc<Self>) -> Result<(), broca_store::grants::GrantsError> {
        let now = self.system.tool_now();
        for row in self.held.store.calls_in_state(HeldState::Authorized, now)? {
            if !self.held.start_attempt(&row, now) {
                continue;
            }
            if super::complete::held_key(&row.batch_id).is_some() {
                let service = self.clone();
                tokio::spawn(async move { service.run_approved_completion(row).await });
                continue;
            }
            // An authorized row always keeps its validated request. A row without one can't be
            // dispatched, so it stays recorded and is skipped here rather than panicking the task.
            let Some(params) = row.request.clone() else {
                tracing::warn!(target: "llm_tools", batch = %row.batch_id,
                    "authorized held call has no stored request; not dispatched");
                continue;
            };
            let service = self.clone();
            tokio::spawn(async move {
                let admission = Admission {
                    scope: row.batch_scope.clone(),
                    batch_id: row.batch_id.clone(),
                    args_digest: row.args_digest.clone(),
                    routing_consumer: None,
                    params,
                };
                let _ = service
                    .call(admission, true, &SendScope::Internal, None, Some(row), None)
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
        self.answer_for_consumer_in_project(body, principal, scope, consumer, None)
            .await
    }

    /// `stamped_project_id` comes only from the daemon scope stamp, never the bind identity
    /// or a workdir-derived project. Unstamped callers cannot use project-wide grants.
    pub async fn answer_for_consumer_in_project(
        self: &Arc<Self>,
        body: &[u8],
        principal: Option<&BindPrincipal>,
        scope: &SendScope,
        consumer: ConsumerFormat,
        stamped_project_id: Option<&str>,
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
            CLASSIFY_TOOL_NAME | extract::NAME
                if call.name == CLASSIFY_TOOL_NAME || self.system.tool_extractor().is_some() =>
            {
                let envelope: RawEnvelope<'_> = serde_json::from_slice(body)
                    .map_err(|e| unspent(Refusal::invalid_params("arguments", e.to_string())))?;
                let is_extract = call.name == extract::NAME;
                let admit = if is_extract { extract::admit } else { admit };
                let admission = admit(
                    principal,
                    scope,
                    envelope.arguments,
                    call.call_key.as_deref(),
                    self.system.image_references(),
                )
                .map_err(unspent)?;
                let schema = if is_extract {
                    extract::input_schema(self.system.image_references())
                } else {
                    self.input_schema()
                };
                self.check_pin(&call, &schema)?;
                // Serialize repeats before the held and batch lookups, so overlapping calls
                // cannot both select or skip a hold one of them is still filing.
                let _routing_call = if admission.routing_consumer.is_some()
                    && call.call_key.is_some()
                {
                    let slot = {
                        let mut calls =
                            self.routing_calls.lock().unwrap_or_else(|p| p.into_inner());
                        calls.retain(|_, lock| lock.strong_count() > 0);
                        if let Some(lock) = calls.get(&admission.batch_id).and_then(Weak::upgrade) {
                            lock
                        } else {
                            let lock = Arc::new(tokio::sync::Mutex::new(()));
                            calls.insert(admission.batch_id.clone(), Arc::downgrade(&lock));
                            lock
                        }
                    };
                    Some(slot.lock_owned().await)
                } else {
                    None
                };
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
                    .call(
                        admission,
                        call.call_key.is_some(),
                        scope,
                        stamped_project_id,
                        held,
                        hold_caller,
                    )
                    .await;
                match consumer.result_format {
                    Some(_) if is_extract => answer.and_then(as_extract_tool_result),
                    Some(_) => answer.and_then(as_tool_result),
                    None => answer,
                }
            }
            super::complete::COMPLETE_TOOL_NAME if self.complete.is_some() => {
                let complete = self.complete.as_ref().expect("checked above");
                let envelope: RawEnvelope<'_> = serde_json::from_slice(body)
                    .map_err(|e| unspent(Refusal::invalid_params("arguments", e.to_string())))?;
                self.check_pin(&call, &super::complete::Complete::input_schema())?;
                // As for classify: only a caller that reads held replies, and can name its call
                // again by its key, can wait for the person's consent.
                let caller = principal.map(BindPrincipal::as_owner_string);
                let holds = match (reads_held_reply, caller, call.call_key.clone()) {
                    (true, Some(caller), Some(call_key)) => Some(CompletionConsent {
                        tools: self,
                        stamped_project_id,
                        caller,
                        call_key,
                    }),
                    _ => None,
                };
                let answer = complete
                    .answer(
                        principal,
                        scope,
                        envelope.arguments,
                        call.call_key.as_deref(),
                        &self.private_content,
                        holds
                            .as_ref()
                            .map(|holds| holds as &dyn super::complete::Consent),
                    )
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

    /// Refuse a call whose pin names a schema or semantics this broca no longer serves.
    fn check_pin(&self, call: &ToolCallRequest, schema: &Value) -> Result<(), Vec<u8>> {
        let pin = cortexkit_role_tool_provider::call::check_call(call).map_err(|error| {
            Spend::Nothing.mark_body(serde_json::to_vec(&error).expect("call refusal encodes"))
        })?;
        if let Some(pin) = pin {
            let digest = cortexkit_role_tool_provider::catalog::schema_digest(schema)
                .expect("contract schema has a digest");
            if pin.schema_digest != digest {
                return Err(serde_json::to_vec(&json!({"code":"tool_schema_changed", "message":"the pinned tool schema is no longer served", "detail":{"tool":call.name, "expected":pin.schema_digest, "current":digest, "no_spend":true}})).expect("refusal encodes"));
            }
            if pin.semantics != 1 {
                return Err(serde_json::to_vec(&json!({"code":"tool_semantics_changed", "message":"the pinned tool semantics are no longer served", "detail":{"tool":call.name, "expected":pin.semantics, "current":1, "no_spend":true}})).expect("refusal encodes"));
            }
        }
        Ok(())
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
            "result_format":result_format.unwrap_or(CONTENT_V1),
        });
        if self.config.advertise_tags {
            tool["capabilities"] = json!([classify::TAG]);
        }
        // The generation of a catalog listing only classify stays what it was, so a consumer's
        // pinned generation doesn't change until completion is listed.
        let generation = hash(&tool);
        let mut tools = vec![tool];
        if self.complete.is_some() {
            let schema = super::complete::Complete::input_schema();
            let mut complete = json!({
                "name":super::complete::COMPLETE_TOOL_NAME, "schema_digest": cortexkit_role_tool_provider::catalog::schema_digest(&schema).expect("contract schema has a digest"),
                "semantics":1, "capabilities":[], "description":super::complete::COMPLETE_DESCRIPTION,
                "input_schema":schema,
                "modes":cortexkit_llm_tools::MODES,
                "result_format":result_format.unwrap_or(CONTENT_V1),
            });
            if self.config.advertise_tags {
                complete["capabilities"] = json!([cortexkit_llm_tools::complete::TAG]);
            }
            tools.push(complete);
        }
        if self.system.tool_extractor().is_some() {
            let schema = extract::input_schema(self.system.image_references());
            let mut tool = json!({
                "name":extract::NAME, "schema_digest": cortexkit_role_tool_provider::catalog::schema_digest(&schema).expect("contract schema has a digest"),
                "semantics":1, "capabilities":[], "description":extract::DESCRIPTION,
                "input_schema":schema,
                "modes":cortexkit_llm_tools::MODES,
                "result_format":result_format.unwrap_or(CONTENT_V1),
            });
            if self.config.advertise_tags {
                tool["capabilities"] = json!([cortexkit_llm_tools::extract::TAG]);
            }
            tools.push(tool);
        }
        let generation = if tools.len() > 1 {
            hash(&json!(tools))
        } else {
            generation
        };
        let mut answer =
            json!({"generation":generation,"tools":tools,"capabilities":{"late_results":true}});
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

    /// One attempt at a model-tool call. An authorized held call that is still authorized
    /// afterwards recorded no outcome, so its next attempt is scheduled; one that settled has
    /// its result stored and delivered as a late result.
    async fn call(
        self: &Arc<Self>,
        admission: Admission,
        keyed: bool,
        scope: &SendScope,
        stamped_project_id: Option<&str>,
        held: Option<HeldCall>,
        hold_caller: Option<(&str, &str)>,
    ) -> Answer {
        let authorized = held
            .as_ref()
            .filter(|row| row.state == HeldState::Authorized)
            .cloned();
        let answer = self
            .attempt(
                admission,
                keyed,
                scope,
                stamped_project_id,
                held,
                hold_caller,
            )
            .await;
        if let Some(row) = authorized {
            self.schedule_unsettled(&row, &answer);
        }
        answer
    }

    fn schedule_unsettled(&self, row: &HeldCall, answer: &Answer) {
        let now = self.system.tool_now();
        // A store that cannot be read counts as unsettled: a retry of a call that did settle
        // finds its stored result and sends nothing.
        let unsettled = self
            .held
            .store
            .lookup(&row.batch_id, now)
            .map_or(true, |current| {
                current.is_some_and(|current| {
                    current.state == HeldState::Authorized
                        && current.admitted_at_ms == row.admitted_at_ms
                })
            });
        if !unsettled {
            return;
        }
        let retry_after_ms = answer
            .as_ref()
            .err()
            .and_then(|body| serde_json::from_slice::<Value>(body).ok())
            .and_then(|body| body["detail"]["retry_after_ms"].as_i64());
        self.held.retry_later(row, retry_after_ms, now);
    }

    async fn attempt(
        self: &Arc<Self>,
        mut admission: Admission,
        keyed: bool,
        scope: &SendScope,
        stamped_project_id: Option<&str>,
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
        let pinned_model = admission.model();
        let extractor = if extract::is_key(&admission.batch_id) {
            Some(self.system.tool_extractor().ok_or_else(|| {
                unspent(Refusal::model_unavailable(
                    "cloudflare-workers-ai/@cf/cloudflare/clairic",
                ))
            })?)
        } else {
            None
        };
        let system = extractor.map_or(&self.system, |extractor| &extractor.service);
        // Read replay before catalog, pricing or private-content policy. A changed catalog or
        // policy must never turn an already-paid answer into a new provider attempt.
        let stored = if keyed {
            system
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
        let mut recorded_route = None;
        if let Some(stored) = &stored {
            // A refusal about the recorded batch reports this invocation's spend, which is
            // nothing: it sent no request. The recorded call's own cost reaches the caller
            // through its replayed answer, so it is never charged twice.
            check_replay(stored, &admission).map_err(unspent)?;
            if pinned_model.is_none() {
                recorded_route = system
                    .stored_tool_route(&admission.batch_id)
                    .await
                    .map_err(unspent)?;
                if let Some(ToolRoute::Refused(refused)) = recorded_route {
                    let answer = Err(refused);
                    if held.is_some() {
                        self.persist_held_answer(
                            &admission.batch_id,
                            &answer,
                            Spend::Nothing,
                            true,
                        )?;
                    }
                    return answer;
                }
            }
            if has_final_outcome(stored) {
                // Replaying sends nothing, so a failure to read the record spent nothing; the
                // replayed answer itself carries the spend its first answer carried.
                let answer = if let Some(extractor) = extractor {
                    extractor
                        .replay_tool(stored)
                        .await
                        .map_err(|refusal| error(Spend::Nothing.mark_undeclared(refusal), None))
                } else {
                    convert(
                        &self
                            .system
                            .replay_tool(&admission.params, stored)
                            .await
                            .map_err(unspent)?,
                    )
                };
                if held.is_some() {
                    self.persist_held_answer(&admission.batch_id, &answer, Spend::Nothing, true)?;
                }
                return answer;
            }
        }
        let running = self
            .flights
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(&admission.batch_id)
            .map(|flight| flight.join(&admission))
            .transpose()?;
        if let Some(receiver) = running {
            return await_flight(receiver).await;
        }
        let routed = if pinned_model.is_none() {
            let route = match (recorded_route, &held) {
                (_, Some(row)) => {
                    let choice = serde_json::from_str(&row.frozen_row).map_err(|_| {
                        unspent(crate::system_one::internal_refusal(
                            "the held classifier route is unreadable",
                        ))
                    })?;
                    // Execution compares the held decision with any recorded one before it
                    // dispatches. The person approved the provider on the held decision.
                    ToolRoute::Chosen(choice)
                }
                (Some(route), None) => route,
                (None, None) => {
                    let consumer = admission
                        .routing_consumer
                        .as_deref()
                        .expect("stamped consumer");
                    system
                        .select_tool_route(&admission.params, consumer)
                        .await
                        .map_err(unspent)?
                }
            };
            match &route {
                ToolRoute::Chosen(choice) if choice.refusal.is_none() => Some(route),
                _ => {
                    return system
                        .execute_routed_tool(
                            &admission.params,
                            &admission.scope,
                            &admission.args_digest,
                            route,
                            None,
                            &PreparedImages::default(),
                        )
                        .await
                        .and_then(|reply| convert(&reply));
                }
            }
        } else {
            None
        };
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
            None => match if let Some(extractor) = extractor {
                extractor
                    .prepare_tool_images(&admission.params, &image_scope)
                    .await
            } else if let Some(ToolRoute::Chosen(choice)) = &routed {
                self.system
                    .prepare_routed_tool_images(&admission.params, &image_scope, choice)
                    .await
            } else {
                self.system
                    .prepare_images(&admission.params, &image_scope, stored.as_ref())
                    .await
            } {
                Ok(images) => Arc::new(images),
                Err(refusal) => {
                    let answer = Err(unspent(refusal));
                    if held.is_some() {
                        self.persist_held_answer(
                            &admission.batch_id,
                            &answer,
                            Spend::Nothing,
                            false,
                        )?;
                    }
                    return answer;
                }
            },
        };
        let pricing = if let Some(route) = routed {
            Pricing::Routed(route)
        } else if let Some(row) = &held {
            Pricing::Pinned(
                serde_json::from_value(json!({"frozen_row":row.frozen_row})).map_err(|_| {
                    unspent(crate::system_one::internal_refusal(
                        "held pricing snapshot is unreadable",
                    ))
                })?,
            )
        } else if let Some(extractor) = extractor {
            Pricing::Pinned(
                extractor
                    .admit_tool(&admission.params, stored.as_ref())
                    .map_err(|refusal| {
                        crate::system_one::extraction_refusal_body(&Spend::Nothing.mark(refusal))
                    })?,
            )
        } else {
            Pricing::Pinned(
                self.system
                    .admit_tool(&admission.params, stored.as_ref())
                    .map_err(unspent)?,
            )
        };
        // Private-content admission runs on every call that may reach a provider. A recorded
        // outcome already returned above, so a stored batch here has none and this call would
        // dispatch its item again: it needs consent exactly as a fresh call would. A held call
        // was admitted by the person's decision instead.
        if held.is_none() {
            let (model, provider) = match &pricing {
                Pricing::Pinned(_) => {
                    let model = pinned_model.as_deref().expect("pinned tool model");
                    (model, model.split('/').next().unwrap_or_default())
                }
                Pricing::Routed(ToolRoute::Chosen(choice)) => {
                    (choice.model.as_str(), choice.provider())
                }
                Pricing::Routed(ToolRoute::Refused(_)) => {
                    unreachable!("a routing refusal already answered")
                }
            };
            if let Err(refusal) = self.private_content.admit(scope, provider).await {
                let frozen_row = match &pricing {
                    Pricing::Pinned(pricing) => pricing.frozen_row.clone(),
                    Pricing::Routed(ToolRoute::Chosen(choice)) => {
                        serde_json::to_string(choice).expect("a frozen classifier route encodes")
                    }
                    Pricing::Routed(ToolRoute::Refused(_)) => {
                        unreachable!("a routing refusal already answered")
                    }
                };
                let call = HoldCall {
                    batch_id: admission.batch_id.clone(),
                    batch_scope: admission.scope.clone(),
                    provider,
                    model,
                    request: admission.params.clone(),
                    args_digest: admission.args_digest.clone(),
                    frozen_row,
                };
                let retain = |row: &HeldCall, bind: &BindScope| {
                    self.held.images.retain(
                        self.held.store.clone(),
                        row,
                        bind,
                        images.clone(),
                        self.system.tool_now(),
                    )
                };
                match self
                    .consent_or_hold(
                        refusal,
                        scope,
                        stamped_project_id,
                        hold_caller,
                        call,
                        retain,
                    )
                    .await
                {
                    Held::Released => {}
                    Held::Answered(answer) => return answer,
                }
            }
        }
        let receiver = {
            let mut flights = self.flights.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(flight) = flights.get(&admission.batch_id) {
                flight.join(&admission)?
            } else {
                let (sender, receiver) = watch::channel(None);
                let batch_id = admission.batch_id.clone();
                flights.insert(
                    batch_id.clone(),
                    Flight {
                        scope: admission.scope.clone(),
                        digest: admission.args_digest.clone(),
                        result: receiver.clone(),
                    },
                );
                let service = self.clone();
                tokio::spawn(async move {
                    let worker = service.clone();
                    let answer = tokio::spawn(async move {
                        worker.dispatch(admission, pricing, held, images).await
                    })
                    .await
                    .unwrap_or_else(|_| {
                        Err(error(
                            crate::system_one::internal_refusal(
                                "model tool execution failed; retry the call",
                            ),
                            Some(Spend::Unknown),
                        ))
                    });
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

    async fn dispatch(
        self: &Arc<Self>,
        admission: Admission,
        pricing: Pricing,
        held: Option<HeldCall>,
        images: Arc<PreparedImages>,
    ) -> Answer {
        let admitted_at = held.as_ref().map(|row| row.admitted_at_ms);
        let batch_id = admission.batch_id;
        let routed = matches!(pricing, Pricing::Routed(_));
        let answer = match pricing {
            Pricing::Routed(route) => self
                .system
                .execute_routed_tool(
                    &admission.params,
                    &admission.scope,
                    &admission.args_digest,
                    route,
                    admitted_at,
                    &images,
                )
                .await
                .and_then(|reply| convert(&reply)),
            Pricing::Pinned(pricing) => {
                if extract::is_key(&batch_id) {
                    let extractor = self
                        .system
                        .tool_extractor()
                        .expect("extractor checked before dispatch");
                    extractor
                        .execute_tool(
                            &admission.params,
                            &admission.scope,
                            &admission.args_digest,
                            &pricing,
                            admitted_at,
                            &images,
                        )
                        .await
                        .map_err(|refusal| error(refusal, None))
                } else {
                    self.system
                        .clone()
                        .execute_tool(
                            admission.params,
                            admission.scope,
                            admission.args_digest,
                            pricing,
                            admitted_at,
                            images,
                        )
                        .await
                        .map_err(|refusal| error(refusal, None))
                        .and_then(|reply| convert(&reply))
                }
            }
        };
        if held.is_some() {
            let recorded = answer.is_err()
                && (self.recorded(&batch_id).await
                    || (routed
                        && matches!(
                            self.system.stored_tool_route(&batch_id).await,
                            Ok(Some(ToolRoute::Refused(_)))
                        )));
            self.persist_held_answer(&batch_id, &answer, Spend::Unknown, recorded)?;
        }
        answer
    }

    /// Whether the batch has a recorded outcome, which is final for its call key.
    async fn recorded(&self, batch_id: &str) -> bool {
        let system = if extract::is_key(batch_id) {
            let Some(extractor) = self.system.tool_extractor() else {
                return false;
            };
            &extractor.service
        } else {
            &self.system
        };
        system
            .stored_batch(batch_id)
            .await
            .ok()
            .flatten()
            .is_some_and(|stored| has_final_outcome(&stored))
    }

    /// Record a held call's outcome. `lost` is the spend a failure to record states: unknown
    /// after an execution that may have reached the provider, nothing when no request was sent.
    /// `recorded` says the refusal is the batch's recorded outcome.
    fn persist_held_answer(
        &self,
        batch_id: &str,
        answer: &Answer,
        lost: Spend,
        recorded: bool,
    ) -> Result<(), Vec<u8>> {
        let error = |refusal| error(refusal, Some(lost));
        // A recorded outcome is final for the call key whatever its class, so it is the held
        // call's result. A transient refusal that recorded nothing leaves the call authorized
        // for its retry schedule. Neither is ever converted into a not-started consent outcome
        // after dispatch.
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
            if recorded || value["detail"]["class"] == "permanent" {
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

/// Consent holds for one completion, on the consent store and held calls classify uses.
struct CompletionConsent<'a> {
    tools: &'a Arc<LlmTools>,
    stamped_project_id: Option<&'a str>,
    /// Who called, as the held row records it.
    caller: String,
    call_key: String,
}

#[async_trait::async_trait]
impl super::complete::Consent for CompletionConsent<'_> {
    fn held(&self, batch_id: &str) -> Option<Answer> {
        let now = self.tools.system.tool_now();
        let row = match self.tools.held.store.lookup(batch_id, now) {
            Ok(row) => row?,
            // Running a call that may be waiting for the person would run it before they
            // answered.
            Err(_) => {
                return Some(Err(unspent(crate::system_one::internal_refusal(
                    "held calls are unavailable",
                ))))
            }
        };
        Some(match row.state {
            HeldState::Final => super::held::HeldCalls::final_reply(&row),
            // Waiting for the person, or approved and running: its answer comes as a late result.
            _ => super::held::HeldCalls::held_reply(&row),
        })
    }

    async fn consent(
        &self,
        refused: Vec<u8>,
        scope: &SendScope,
        call: super::complete::ConsentCall<'_>,
    ) -> super::complete::Consented {
        let hold = HoldCall {
            batch_id: call.batch_id,
            batch_scope: call.batch_scope.clone(),
            provider: call.provider,
            model: &call.model,
            request: call.request.to_owned(),
            args_digest: call.args_digest.to_owned(),
            frozen_row: call.frozen_row,
        };
        let retain = |row: &HeldCall, bind: &BindScope| {
            self.tools
                .completion_stamps
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(row.batch_id.clone(), (row.admitted_at_ms, bind.clone()));
        };
        match self
            .tools
            .consent_or_hold(
                refused,
                scope,
                self.stamped_project_id,
                Some((&self.caller, &self.call_key)),
                hold,
                retain,
            )
            .await
        {
            Held::Released => super::complete::Consented::Released,
            Held::Answered(answer) => super::complete::Consented::Answered(answer),
        }
    }
}

impl LlmTools {
    /// Runs a completion the person approved, and stores its answer as the held call's result,
    /// which the caller collects as a late result. An answer the completion record didn't take
    /// as final (its run's end couldn't be read) leaves the call approved, for a later attempt.
    async fn run_approved_completion(self: Arc<Self>, row: HeldCall) {
        let Some(complete) = self.complete.clone() else {
            tracing::warn!(target: "llm_tools", batch = %row.batch_id,
                "an approved completion waits: this broca serves no completions");
            return;
        };
        let stamp = self
            .completion_stamps
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&row.batch_id)
            .filter(|(admitted_at, _)| *admitted_at == row.admitted_at_ms)
            .map(|(_, stamp)| stamp);
        let scope = stamp.map_or(SendScope::Internal, SendScope::Stamped);
        let principal = serde_json::from_value::<BindPrincipal>(json!(row.principal)).ok();
        let approved = super::complete::Approved {
            batch_id: row.batch_id.clone(),
            batch_scope: row.batch_scope.clone(),
            principal,
            request: row.request.clone().unwrap_or_default(),
            args_digest: row.args_digest.clone(),
            frozen_row: row.frozen_row.clone(),
        };
        let answer = complete.approved(approved, &scope).await;
        let recorded = match super::complete::held_key(&row.batch_id) {
            Some(key) => complete.recorded(key).await,
            None => false,
        };
        if let Err(error) =
            self.persist_held_answer(&row.batch_id, &answer, Spend::Unknown, recorded)
        {
            tracing::warn!(target: "llm_tools", batch = %row.batch_id,
                "an approved completion's answer was not stored: {}",
                String::from_utf8_lossy(&error));
        }
        self.schedule_unsettled(&row, &answer);
    }
}

/// A call the private-content gate refused, as a held row and its consent card record it.
struct HoldCall<'a> {
    batch_id: String,
    batch_scope: BatchScope,
    provider: &'a str,
    /// The catalog model the card names.
    model: &'a str,
    /// The arguments an approval runs the call with.
    request: String,
    args_digest: String,
    /// What the call froze at admission (route and prices), kept for its approved run.
    frozen_row: String,
}

/// What became of a call the private-content gate refused.
enum Held {
    /// A grant the person gave earlier releases it: the call runs now.
    Released,
    /// Refused, or held for the person's answer: the caller's answer.
    Answered(Answer),
}

impl LlmTools {
    /// What becomes of a call the private-content gate refused with `refusal` under `scope`,
    /// for classify and completion alike. A refusal the person can't lift by consenting (a
    /// flow asking for a provider outside its approved set, say) stands. Otherwise a standing
    /// deny refuses, a standing grant releases, and a caller that reads held replies
    /// (`hold_caller`: who it is and its `call_key`) has a consent card filed and the call held
    /// until the person answers. `retain` keeps beside the held row what only this process
    /// holds: the caller's stamp, and any images the call prepared.
    async fn consent_or_hold(
        self: &Arc<Self>,
        refusal: Vec<u8>,
        scope: &SendScope,
        stamped_project_id: Option<&str>,
        hold_caller: Option<(&str, &str)>,
        call: HoldCall<'_>,
        retain: impl FnOnce(&HeldCall, &BindScope) + Send,
    ) -> Held {
        let refused = |body: Vec<u8>| Held::Answered(Err(body));
        let reason: Value = serde_json::from_slice(&refusal).unwrap_or(Value::Null);
        let consent_reason = reason["detail"]["consent"].as_str().unwrap_or("");
        if !matches!(
            consent_reason,
            "consent_required" | "private_content_unknown" | "private_content_unreachable"
        ) {
            // A flow that read the person's private content asking for a provider outside the
            // set the person approved for it. The gate's other refusals come before it has
            // decided whether the call carries private content, so they aren't counted as a consent outcome.
            if reason["code"] == "provider_not_allowed" {
                self.count(ConsentOutcome::RefusedByFlowPolicy);
            }
            return refused(refusal);
        }
        let Some(consent) = &self.consent else {
            self.count(ConsentOutcome::refused(consent_reason));
            return refused(refusal);
        };
        let SendScope::Stamped(bind) = scope else {
            self.count(ConsentOutcome::refused(consent_reason));
            return refused(refusal);
        };
        let agent = bind.scope.agent_id.as_deref().unwrap_or("");
        let identity = super::consent::identity(&bind.scope);
        let now = self.system.tool_now();
        match consent.store.denied(agent, &identity, call.provider) {
            Ok(false) => {}
            Ok(true) => {
                self.count(ConsentOutcome::DeniedByStandingDeny);
                return refused(super::held::not_started("denied").into_bytes());
            }
            Err(error) => return refused(self.consent_unavailable(error)),
        }
        match consent
            .releasing_grant(&bind.scope, stamped_project_id, call.provider, now)
            .await
        {
            Ok(Some(grant)) => {
                self.count(ConsentOutcome::ReleasedByGrant);
                self.note_release(&grant, &bind.scope.scope_ref);
                return Held::Released;
            }
            Ok(None) => {}
            Err(error) => return refused(self.consent_unavailable(error)),
        }
        if consent_reason != "consent_required" {
            self.count(ConsentOutcome::refused(consent_reason));
            return refused(refusal);
        }
        let Some((principal, call_key)) = hold_caller else {
            self.count(ConsentOutcome::RefusedConsentRequired);
            return refused(refusal);
        };
        let display_row = self.system.tool_model_row(call.model);
        let Some((provider_label, model_label)) = display_row
            .as_ref()
            .and_then(crate::system_one::consent_model_labels)
        else {
            self.count(ConsentOutcome::ConsentUnavailable);
            return refused(super::held::not_started("consent_unavailable").into_bytes());
        };
        let sources = self.private_content.card_sources(scope).await;
        let dedup_key = match consent.dedup_key(agent, call.provider, &identity, now) {
            Ok(key) => key,
            Err(error) => return refused(self.consent_unavailable(error)),
        };
        let body = match broca_subc::consent_card::private_content_card_for_project(
            provider_label,
            model_label,
            &sources,
            &dedup_key,
            stamped_project_id,
        ) {
            Ok(body) => body,
            Err(error) => return refused(self.consent_unavailable(error)),
        };
        let filed = consent
            .file_with_admission(
                HoldRequest {
                    project_id: stamped_project_id.map(str::to_owned),
                    batch_id: call.batch_id,
                    scope: identity,
                    batch_scope: call.batch_scope,
                    principal: principal.into(),
                    call_key: call_key.into(),
                    agent_id: agent.into(),
                    provider: call.provider.into(),
                    model: call.model.into(),
                    request: call.request,
                    args_digest: call.args_digest,
                    frozen_row: call.frozen_row,
                    dedup_key: dedup_key.clone(),
                    admitted_at_ms: now,
                },
                body,
                now,
                |row| retain(row, bind),
            )
            .await;
        let row = match filed {
            Ok(row) => row,
            Err(error) => return refused(self.consent_unavailable(error)),
        };
        self.count(ConsentOutcome::CardFiled);
        self.watch_consent(dedup_key);
        Held::Answered(match row.state {
            HeldState::Final => super::held::HeldCalls::final_reply(&row),
            _ => super::held::HeldCalls::held_reply(&row),
        })
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

/// Every recorded outcome is final for its call key, a transient refusal included: a repeat
/// of the same key returns it with the spend members it was recorded with and sends nothing.
/// A caller that wants another attempt sends a new call key. Only a batch with no recorded
/// outcome (its process ended between the claim and the answer) may dispatch again.
fn has_final_outcome(stored: &StoredBatch) -> bool {
    !stored.outcomes.is_empty()
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

fn as_extract_tool_result(body: Box<RawValue>) -> Answer {
    // A consent hold is already a tool-result envelope.
    #[derive(serde::Deserialize)]
    struct Envelope {
        held: Option<serde::de::IgnoredAny>,
    }
    if serde_json::from_str::<Envelope>(body.get()).is_ok_and(|envelope| envelope.held.is_some()) {
        return Ok(body);
    }
    extract::tool_result(&body).map_err(|_| {
        error(
            crate::system_one::internal_refusal("extraction tool reply cannot encode"),
            Some(Spend::Unknown),
        )
    })
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
        model: String,
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
        model: &'a str,
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
        model: &batch.model,
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
mod contract03_tests {
    #[test]
    fn llm_classify_result_names_used_model_and_passes_contract03_parser() {
        let reply = serde_json::value::RawValue::from_string(r#"{"model":"typesafe/jev-1.13.0","items":[{"answers":{"q":{"type":"noul","noul":0.50}},"usage":{"input_tokens":10,"output_tokens":0}}],"cost_usd":0.00000042}"#.into()).unwrap();
        let result = super::convert(&reply).unwrap();
        let parsed: cortexkit_llm_tools::classify::Result =
            serde_json::from_str(result.get()).unwrap();
        cortexkit_llm_tools::classify::validate_result(&parsed).unwrap();
        assert_eq!(parsed.model, "typesafe/jev-1.13.0");
        assert!(
            result.get().contains(r#""noul":0.50"#),
            "provider numeric spelling remains unchanged"
        );
    }
}

#[cfg(test)]
#[path = "route_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "spend_route_tests.rs"]
mod spend_tests;
