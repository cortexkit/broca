//! Routed classifier batches keep selection separate from dispatch. The batch owns its
//! model/account choice; each dispatching call owns an admission and an immutable report.

use std::sync::Arc;

use async_trait::async_trait;
use broca_store::system_one::{
    BatchClaim, BatchScope, Claimed, Outcome, RoutingRecord, StoredBatch, StoredOutcome,
};
use cortexkit_model_batch::estimate::{cost_nanos, nanos_to_usd, usd_to_nanos, Estimate, Rounding};
use cortexkit_role_classifier::{
    errors::{self, ItemError, Refusal, RoutingRefusal},
    identity::BatchIdentity,
    request::{parse_request, ClassifyRequest},
};
use cortexkit_role_routing::{
    account::AccountForAnswer,
    describe::RoleDescribe,
    report::{Outcome as ReportOutcome, ReportRequest, Usage as ReportUsage},
    select::{Requirements, SelectAnswer, SelectRequest},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue, Value};

use super::{
    classify::{self, FrozenRow, Hold, RawParams, Slot},
    credential::{CredentialSource, VaultCredential},
    internal_refusal, ModelPin, SystemOne,
};
use crate::account_routing::{self, Candidates, RoutingCallError, RoutingClient};
use crate::llm_tools::spend::{ItemSpend, MarkedItemError, Spend};

/// The routing consumer every classifier caller's name hangs under. Routing matches a dotted
/// name to its nearest configured entry, so settings on this root cover every caller.
pub const CONSUMER_ROOT: &str = "broca.classifier";

// A positive IQ sets a minimum decision grade. The lowest positive value lets routing pick
// the cheapest eligible graded model without considering ungraded models.
const MIN_DECISION_GRADE: u8 = 1;

/// The routing consumer for a `classify.run` from `principal`: the module the daemon stamped on
/// the route's bind, or `direct` for a direct opener. Never anything the request body says, so a
/// caller can't ask for another caller's routing policy.
pub fn consumer_for(principal: Option<&broca_protocol::BindPrincipal>) -> String {
    match principal {
        Some(broca_protocol::BindPrincipal::Reserved { module_id }) => {
            format!("{CONSUMER_ROOT}.{module_id}")
        }
        _ => format!("{CONSUMER_ROOT}.direct"),
    }
}

#[derive(Clone)]
pub struct Routing {
    pub client: Arc<RoutingClient>,
    pub credentials: Arc<dyn RoutingCredentials>,
    pub module_id: String,
}

/// Metadata listing and public-id credential loading are shared with the LLM key-pool
/// admission path. No token or credential payload enters the batch database.
#[async_trait]
pub trait RoutingCredentials: Send + Sync {
    async fn candidates(&self, provider: &str) -> Result<Candidates, String>;
    fn source(&self, credential_id: &str) -> Arc<dyn CredentialSource>;
}

pub struct VaultRoutingCredentials(pub Arc<broca_subc::VaultClient>);

pub(super) fn classifier_candidates(
    listing: &broca_subc::vault::ScopedList,
    provider: &str,
) -> Candidates {
    let mut bound = listing.clone();
    bound
        .credentials
        .retain(|row| row.provider_ids.iter().any(|id| id == provider));
    // Every key left lists the provider, so the catalog never decides membership here.
    Candidates::api_keys(&bound, provider, &|_| false)
}

#[async_trait]
impl RoutingCredentials for VaultRoutingCredentials {
    async fn candidates(&self, provider: &str) -> Result<Candidates, String> {
        let listing = self
            .0
            .list_scoped()
            .await
            .map_err(|error| error.to_string())?;
        Ok(classifier_candidates(&listing, provider))
    }

    fn source(&self, credential_id: &str) -> Arc<dyn CredentialSource> {
        Arc::new(VaultCredential {
            client: self.0.clone(),
            credential_id: credential_id.into(),
            addressing: None,
        })
    }
}

pub(super) enum Error {
    Classifier(Refusal),
    Routing(Box<RoutingRefusal>),
    Runner(Box<Value>),
    ToolRefusal(Vec<u8>),
}
impl From<Refusal> for Error {
    fn from(error: Refusal) -> Self {
        Self::Classifier(error)
    }
}
impl Error {
    pub(super) fn bytes(self) -> Vec<u8> {
        match self {
            Self::Classifier(error) => super::refusal_body(&error),
            Self::Routing(error) => serde_json::to_vec(&error).expect("routing refusal encodes"),
            Self::Runner(error) => serde_json::to_vec(&error).expect("runner refusal encodes"),
            Self::ToolRefusal(body) => body,
        }
    }

    fn tool_bytes(self, spend: Spend) -> Vec<u8> {
        let body = match self {
            Self::ToolRefusal(body) => return body,
            Self::Routing(mut error) if error.code != errors::ROUTING_UNAVAILABLE => {
                error.code = errors::ROUTING_REFUSED.into();
                let mut body = serde_json::to_value(error).expect("routing refusal encodes");
                body["detail"]["class"] = json!("permanent");
                serde_json::to_vec(&body).expect("routing refusal encodes")
            }
            other => other.bytes(),
        };
        spend.mark_body(body)
    }
}

fn unavailable(message: impl Into<String>) -> Error {
    Error::Routing(Box::new(RoutingRefusal::unavailable(message)))
}
fn contract_error(message: impl Into<String>) -> Error {
    let mut refusal = Refusal::new("internal_error", message);
    refusal.detail.class = cortexkit_role_classifier::errors::ErrorClass::Permanent;
    refusal.into()
}

fn routing_error(error: RoutingCallError, pin: Option<&str>, now_ms: u64) -> Error {
    match error {
        RoutingCallError::Invalid(message) => contract_error(message),
        RoutingCallError::Unavailable(message) => unavailable(message),
        RoutingCallError::Refused {
            code,
            message,
            detail,
        } => {
            if matches!(code.as_str(), "invalid_request" | "admission_conflict") {
                tracing::warn!(routing_code = %code, routing_reason = %message, "routing refused a classifier request");
            }
            Error::Routing(Box::new(RoutingRefusal::refused_with_detail(
                code,
                Some(message),
                pin.map(str::to_owned),
                &detail,
                now_ms,
            )))
        }
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Choice {
    select: SelectAnswer,
    pub(crate) model: String,
    frozen: FrozenRow,
    pub(super) entry: Option<cortexkit_role_classifier::describe::ModelEntry>,
    describe: RoleDescribe,
    account: Option<AccountForAnswer>,
    pub(crate) refusal: Option<Value>,
}

impl Choice {
    pub(crate) fn provider(&self) -> &str {
        &self.select.runner.model.provider
    }
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
struct Journal {
    expired: bool,
    reselected: bool,
    selected: Option<Choice>,
    pending_admission: Option<PendingAdmission>,
    reselection: Option<Reselection>,
    admission_refusal: Option<RoutingRefusal>,
    admission_refusal_pending: bool,
    runs: Vec<Run>,
    outbox: Vec<Rpc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_refusal: Option<Vec<u8>>,
}

#[derive(Default, Serialize, Deserialize)]
struct Reselection {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    error: Option<Value>,
    choice: Option<Choice>,
    refusal: Option<Value>,
    finished: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct PendingAdmission {
    id: String,
    choice: Choice,
    params: Value,
}

#[derive(Serialize, Deserialize)]
struct Run {
    id: String,
    choice: Choice,
    finished: bool,
    dispatched: usize,
    input: Vec<Option<u64>>,
    output: Vec<Option<u64>>,
    costs: Vec<Option<u64>>,
    error: Option<ItemError>,
    report_queued: bool,
    renewals: u64,
    renewing: bool,
    renew_at_ms: u64,
    renew_failed: bool,
    reset_at_ms: Option<u64>,
    unmeasured_attempt: bool,
    #[serde(default)]
    cleanup_admissions: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    renewal_refusal: Option<Value>,
}

impl Run {
    fn note_error(&mut self, error: ItemError) {
        if self
            .error
            .as_ref()
            .is_none_or(|previous| error_significance(&error) > error_significance(previous))
        {
            self.error = Some(error);
        }
    }

    fn observation_admission(&self) -> &str {
        // Routing binds observations to the exact admission row. The latest renewal
        // names the row whose run_finished releases the currently held pin.
        &self
            .choice
            .account
            .as_ref()
            .expect("admitted run")
            .admission
            .id
    }
}

fn error_significance(error: &ItemError) -> u8 {
    // An account failure governs all unanswered work, even after a non-account error.
    if matches!(
        error.code.as_str(),
        errors::AUTH_FAILED | errors::ACCOUNT_NOT_ENTITLED
    ) {
        3
    } else if account_error(error) {
        2
    } else {
        u8::from(error.stops_the_call())
    }
}

#[derive(Serialize, Deserialize)]
struct Rpc {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cleanup_run: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    routing_refusal: Option<Value>,
    method: String,
    params: Value,
    /// Closed reports stay in the journal, so recovery never synthesizes a replacement body.
    closed: bool,
    conflict: bool,
    capabilities: Vec<String>,
}

impl Journal {
    fn pending(&self) -> bool {
        // A content-free row must remain discoverable after its final report is
        // acknowledged, until archive_expired permits deletion by the owner's release.
        self.expired
            || self.pending_admission.is_some()
            || self.admission_refusal_pending
            || pending_reselection(self)
            || self.runs.iter().any(|run| !run.finished)
            || self.outbox.iter().any(|rpc| !rpc.closed)
    }

    fn observe(&mut self, mut params: Value) {
        params
            .as_object_mut()
            .expect("observation object")
            .remove("caller");
        let capabilities = self
            .runs
            .last()
            .map_or_else(Vec::new, |run| run.choice.describe.capabilities.clone());
        self.outbox.push(Rpc {
            cleanup_run: None,
            routing_refusal: None,
            method: "route.account_observe".into(),
            params,
            closed: false,
            conflict: false,
            capabilities,
        });
    }

    fn report(&mut self, body: ReportRequest, capabilities: Vec<String>) {
        self.outbox.push(Rpc {
            cleanup_run: None,
            routing_refusal: None,
            method: "route.report".into(),
            params: serde_json::to_value(body).expect("typed report encodes"),
            closed: false,
            conflict: false,
            capabilities,
        });
    }
}

fn pending_reselection(journal: &Journal) -> bool {
    journal
        .reselection
        .as_ref()
        .is_some_and(|selection| selection.choice.is_some() && !selection.finished)
}

fn reselection_due(journal: &Journal, any_answer: bool) -> bool {
    !any_answer
        && !journal.reselected
        && journal.runs.last().is_some_and(|run| {
            !run.renew_failed
                && run.error.as_ref().is_some_and(|error| {
                    matches!(
                        error.code.as_str(),
                        errors::AUTH_FAILED | errors::ACCOUNT_NOT_ENTITLED
                    )
                })
        })
}

fn token() -> Result<String, Error> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes)
        .map_err(|_| internal_refusal("cannot mint a routing attempt id"))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub(super) fn validation_entry(
    service: &SystemOne,
) -> cortexkit_role_classifier::describe::ModelEntry {
    cortexkit_role_classifier::describe::ModelEntry::new(
        "routed",
        400,
        service.config.max_items,
        None,
        "",
    )
    .with_images(cortexkit_role_classifier::describe::ImageSupport::new(4))
}

pub(super) fn pinned_entry(
    service: &SystemOne,
    request: &ClassifyRequest,
) -> Result<Option<cortexkit_role_classifier::describe::ModelEntry>, Refusal> {
    let Some(model) = request.model.as_deref() else {
        return Ok(None);
    };
    let row = service.catalog.row(model);
    cortexkit_role_classifier::request::check_catalog_row(
        model,
        classify::catalog_row(row.as_ref()),
    )?;
    let row = row.expect("catalog admission checked presence");
    let pin = service
        .pin(model)
        .ok_or_else(|| Refusal::model_unavailable(model))?;
    if super::catalog_key_parts(model).is_none() {
        return Err(Refusal::model_unavailable(model));
    }
    Ok(Some(service.model_entry(pin, &row)))
}

impl SystemOne {
    pub fn with_routing(mut self, routing: Routing) -> Self {
        self.routing = Some(routing);
        self
    }

    pub(super) async fn routing_ready(&self, pin: Option<&str>) -> Result<RoleDescribe, Error> {
        let routing = self
            .routing
            .as_ref()
            .ok_or_else(|| unavailable("routing is not configured"))?;
        let describe = routing
            .client
            .classifier_describe()
            .await
            .map_err(|error| routing_error(error, pin, (self.clock)().max(0) as u64))?;
        if describe.consumer(CONSUMER_ROOT).is_none() {
            return Err(unavailable(format!(
                "routing does not serve consumer {CONSUMER_ROOT}"
            )));
        }
        Ok(describe)
    }
}

fn estimates(request: &ClassifyRequest) -> Result<Vec<Estimate>, Refusal> {
    (0..request.items.len())
        .map(|index| classify::estimate(request, index, 0))
        .collect()
}

fn reprice(estimates: &mut [Estimate], frozen: &FrozenRow) {
    for estimate in estimates {
        estimate.cost_nanos =
            cost_nanos(estimate.input_tokens, frozen.input_rate_nanos.unwrap_or(0))
                .unwrap_or(u64::MAX);
    }
}

fn requirements(
    request: &ClassifyRequest,
    estimates: &[Estimate],
    describe: &RoleDescribe,
) -> Requirements {
    let advertised = |name| {
        describe
            .requirement_fields
            .iter()
            .any(|field| field == name)
    };
    let mut requirements = Requirements::iq(if request.model.is_none() {
        MIN_DECISION_GRADE
    } else {
        0 // A pin is an explicit model choice, regardless of its grade.
    });
    if advertised("context_window") {
        requirements.context_window = estimates.iter().map(|estimate| estimate.input_tokens).max();
    }
    if advertised("modalities")
        && request.items.iter().any(|item| {
            item.images
                .as_ref()
                .is_some_and(|images| !images.is_empty())
        })
    {
        requirements.modalities = Some(vec![cortexkit_role_routing::select::Modality::Image]);
    }
    if advertised("questions") {
        requirements.questions = Some(request.questions.len() as u64);
    }
    if advertised("input_tokens") {
        requirements.input_tokens = Some(estimates.iter().fold(0u64, |total, estimate| {
            total.saturating_add(estimate.input_tokens)
        }));
    }
    if advertised("max_cost_usd") {
        requirements.max_cost_usd = request.max_cost_usd.clone();
    }
    requirements
}

async fn select(
    service: &SystemOne,
    request: &ClassifyRequest,
    estimates: &[Estimate],
    consumer: &str,
) -> Result<Choice, Error> {
    let describe = service.routing_ready(request.model.as_deref()).await?;
    let routing = service
        .routing
        .as_ref()
        .expect("readiness checked configuration");
    let mut requirements = requirements(request, estimates, &describe);
    if service.role == super::LLM_CLASSIFY_STORE_ROLE
        && requirements
            .max_cost_usd
            .as_ref()
            .and_then(serde_json::Number::as_f64)
            == Some(0.0)
    {
        // Routing only accepts positive caps. A zero tool ceiling still applies before dispatch.
        requirements.max_cost_usd = None;
    }
    let mut query =
        SelectRequest::new(consumer, requirements).with_runners(vec![routing.module_id.clone()]);
    if let Some(pin) = &request.model {
        query = query.with_pin(pin);
    }
    let answer = routing
        .client
        .classifier_select(&query)
        .await
        .map_err(|error| {
            routing_error(
                error,
                request.model.as_deref(),
                (service.clock)().max(0) as u64,
            )
        })?;
    let model = answer.runner.model.route_key();
    let mut refusal = if service.pin(&model).is_none()
        || request.model.as_ref().is_some_and(|pin| pin != &model)
    {
        let message =
            format!("routing selected {model}, which broca cannot serve for this request");
        let mut refusal = serde_json::to_value(Refusal::new(errors::MODEL_UNAVAILABLE, message))
            .expect("runner refusal encodes");
        refusal["detail"]["routing_answer"] =
            serde_json::to_value(&answer).expect("selection encodes");
        Some(refusal)
    } else {
        None
    };
    let row = service.catalog.row(&model);
    if refusal.is_none() {
        if let Err(error) = cortexkit_role_classifier::request::check_catalog_row(
            &model,
            classify::catalog_row(row.as_ref()),
        ) {
            let mut body = serde_json::to_value(error).expect("catalog refusal encodes");
            body["detail"]["routing_answer"] =
                serde_json::to_value(&answer).expect("selection encodes");
            refusal = Some(body);
        }
    }
    let entry = row
        .as_ref()
        .zip(service.pin(&model))
        .map(|(row, pin)| service.model_entry(pin, row));
    let frozen = row.map_or_else(FrozenRow::default, |row| FrozenRow {
        input_rate_nanos: row.input_rate_nanos,
        output_rate_nanos: row.output_rate_nanos,
        charge_basis: None,
    });
    Ok(Choice {
        select: answer,
        model,
        frozen,
        entry,
        describe,
        account: None,
        refusal,
    })
}

pub(crate) enum ToolRoute {
    Chosen(Box<Choice>),
    Refused(Vec<u8>),
}

impl SystemOne {
    pub(crate) async fn select_tool_route(
        &self,
        params: &str,
        consumer: &str,
    ) -> Result<ToolRoute, Refusal> {
        cortexkit_role_classifier::request::check_request_bytes(
            params.len(),
            &cortexkit_role_classifier::describe::Limits::new(
                self.config.max_items,
                self.config.max_request_bytes,
            ),
        )?;
        let request = parse_request(params)?;
        let mut choice = match select(self, &request, &estimates(&request)?, consumer).await {
            Ok(choice) => choice,
            Err(error) => return Ok(ToolRoute::Refused(error.tool_bytes(Spend::Nothing))),
        };
        use broca_core::{ChargeBasis, ChargeBasisResolver};
        let (basis, _) = self
            .charge_basis
            .resolve_charge_basis(choice.provider(), "apikey");
        let problem = if let Some(refusal) = &choice.refusal {
            refusal["message"].as_str()
        } else if basis != ChargeBasis::MeteredApi
            || choice.frozen.input_rate_nanos.is_none_or(|rate| rate < 0)
            || choice
                .frozen
                .output_rate_nanos
                .is_some_and(|rate| rate != 0)
            || choice.select.price.as_ref().is_some_and(|price| {
                price.basis != cortexkit_role_routing::price::PriceBasis::Metered
            })
        {
            Some("the model requires a per-token input price and no positive output price")
        } else {
            None
        };
        if let Some(problem) = problem {
            let mut refusal = serde_json::to_value(Refusal::new(
                errors::ROUTING_REFUSED,
                format!("routing selected {}: {problem}", choice.model),
            ))
            .expect("refusal encodes");
            refusal["detail"]["routing_answer"] = json!(choice.select);
            choice.refusal = Some(refusal);
        }
        choice.frozen.charge_basis = Some(basis);
        Ok(ToolRoute::Chosen(Box::new(choice)))
    }

    pub(crate) async fn stored_tool_route(
        &self,
        batch_id: &str,
    ) -> Result<Option<ToolRoute>, Refusal> {
        let (store, role, id) = (self.store.clone(), self.role.clone(), batch_id.to_owned());
        let Some(row) = classify::blocking(move || store.routing(&role, &id)).await? else {
            return Ok(None);
        };
        let journal: Journal = serde_json::from_str(&row.runs)
            .map_err(|_| internal_refusal("stored routing journal does not decode"))?;
        let choice: Option<Choice> = serde_json::from_str(&row.choice)
            .map_err(|_| internal_refusal("stored routing choice does not decode"))?;
        Ok(journal.tool_refusal.map(ToolRoute::Refused).or_else(|| {
            choice
                .or(journal.selected)
                .map(|choice| ToolRoute::Chosen(Box::new(choice)))
        }))
    }
}

async fn prepare_admission(
    service: &SystemOne,
    choice: &Choice,
    id: &str,
    batch_id: &str,
    initial_input_bytes: u64,
) -> Result<PendingAdmission, Error> {
    let routing = service
        .routing
        .as_ref()
        .ok_or_else(|| unavailable("routing is not configured"))?;
    let candidates = tokio::time::timeout(
        account_routing::ADMISSION_BUDGET,
        routing
            .credentials
            .candidates(&choice.select.runner.model.provider),
    )
    .await
    .map_err(|_| unavailable("credential listing timed out"))?
    .map_err(unavailable)?;
    let auth = choice.select.runner.auth.as_ref();
    let credential = choice
        .account
        .as_ref()
        .map(|account| account.account.credential_id.as_str())
        .or_else(|| auth.and_then(|auth| auth.credential_id.as_deref()));
    let mut params = account_routing::account_for_params(&account_routing::AccountForRequest {
        // The provider and model below already come from routing's own select decision, so
        // there is no caller-named route to override them with.
        account_route: None,
        decision_id: Some(&choice.select.decision_id),
        service_tier: None,
        pool: auth.and_then(|auth| auth.pool.as_deref()),
        credential_id: credential,
        provider: &choice.select.route.provider,
        model: &choice.select.route.model,
        conversation_key: batch_id,
        admission_id: id,
        initial_input_bytes,
        candidates: &candidates,
        hold_cap: Default::default(),
        head_turn: false,
        warm_credential: None,
    });
    params
        .as_object_mut()
        .expect("account params")
        .remove("caller");
    cortexkit_role_routing::account::parse_account_for_request(&params)
        .map_err(|error| contract_error(format!("invalid account admission: {error:?}")))?;
    Ok(PendingAdmission {
        id: id.into(),
        choice: choice.clone(),
        params,
    })
}

fn check_admission(pending: &PendingAdmission, answer: &AccountForAnswer) -> Result<(), Error> {
    let choice = &pending.choice;
    let offered = pending.params["candidates"]["accounts"]
        .as_array()
        .is_some_and(|accounts| {
            accounts.iter().any(|account| {
                account["account_id"] == answer.account.account_id
                    && account["credential_id"] == answer.account.credential_id
            })
        });
    let credential = pending.params["requested_account"]["credential_id"].as_str();
    if answer.admission.id != pending.id
        || answer.pin.id.is_empty()
        || answer.pin.mode != cortexkit_role_routing::account::AccountMode::Admit
        || answer.reason.decision_id != choice.select.decision_id
        || answer.account.provider != choice.select.route.provider
        || answer.account.method != "apikey"
        || !offered
        || credential.is_some_and(|id| id != answer.account.credential_id)
    {
        return Err(contract_error(
            "routing returned an account outside the frozen choice or admitted candidates",
        ));
    }
    Ok(())
}

fn queue_pending_release(
    journal: &mut Journal,
    pending: &PendingAdmission,
    answered: Option<&str>,
) {
    queue_release_ids(
        journal,
        &pending.id,
        &pending.id,
        answered,
        pending.choice.describe.capabilities.clone(),
    );
    journal.pending_admission = None;
}

fn queue_release_ids(
    journal: &mut Journal,
    run: &str,
    requested: &str,
    answered: Option<&str>,
    capabilities: Vec<String>,
) {
    let mut ids = vec![requested.to_owned()];
    if let Some(id) = answered.filter(|id| *id != requested) {
        ids.push(id.to_owned());
    }
    for (index, id) in ids.into_iter().enumerate() {
        let mut params = account_routing::run_finished_body(
            &format!("{run}:rejected:{index}"),
            &id,
            run,
            broca_protocol::RunFinishReason::Error,
            None,
        );
        params
            .as_object_mut()
            .expect("observation object")
            .remove("caller");
        journal.outbox.push(Rpc {
            cleanup_run: None,
            routing_refusal: None,
            method: "route.account_observe".into(),
            params,
            closed: false,
            conflict: false,
            capabilities: capabilities.clone(),
        });
    }
}

async fn release_pending(
    service: &Arc<SystemOne>,
    batch_id: &str,
    journal: &mut Journal,
    pending: &PendingAdmission,
    answered_id: Option<&str>,
) -> Result<(), Error> {
    queue_pending_release(journal, pending, answered_id);
    save(service, batch_id, journal).await?;
    flush(service, batch_id, journal).await
}

async fn save(service: &Arc<SystemOne>, batch_id: &str, journal: &Journal) -> Result<(), Error> {
    let (store, role, owner, batch_id) = (
        service.store.clone(),
        service.role.clone(),
        service.owner.clone(),
        batch_id.to_owned(),
    );
    let (json, pending) = (
        serde_json::to_string(journal).expect("journal encodes"),
        journal.pending(),
    );
    if !classify::blocking(move || {
        store.save_routing_runs(&role, &batch_id, &owner, &json, pending)
    })
    .await?
    {
        return Err(internal_refusal("the routed batch hold was lost").into());
    }
    #[cfg(test)]
    service
        .routing_journal_writes
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    Ok(())
}

async fn freeze(
    service: &Arc<SystemOne>,
    batch_id: &str,
    choice: &Choice,
    journal: &Journal,
) -> Result<(), Error> {
    let (store, role, owner, batch_id, model) = (
        service.store.clone(),
        service.role.clone(),
        service.owner.clone(),
        batch_id.to_owned(),
        choice.model.clone(),
    );
    let frozen = serde_json::to_string(&choice.frozen).expect("price encodes");
    let record = RoutingRecord {
        choice: serde_json::to_string(choice).expect("choice encodes"),
        runs: serde_json::to_string(journal).expect("journal encodes"),
        pending: journal.pending(),
    };
    if !classify::blocking(move || {
        store.freeze_routing(&role, &batch_id, &owner, &model, &frozen, &record)
    })
    .await?
    {
        return Err(internal_refusal(
            "a routed choice cannot change after an answer or a lost hold",
        )
        .into());
    }
    Ok(())
}

fn account_error(error: &ItemError) -> bool {
    matches!(
        error.code.as_str(),
        errors::AUTH_FAILED | errors::ACCOUNT_LIMIT_EXHAUSTED | errors::ACCOUNT_NOT_ENTITLED
    )
}

pub(super) fn retained(outcome: &StoredOutcome) -> bool {
    match &outcome.outcome {
        Outcome::Answered(_) => true,
        Outcome::Failed(body) => serde_json::from_str::<ItemError>(body).map_or(true, |error| {
            account_error(&error) || (error.is_stored() && !error.is_retried_on_resend())
        }),
    }
}

fn finished_report(
    run: &Run,
    batch: Option<&StoredBatch>,
    decision_measured: bool,
) -> ReportRequest {
    let mut usage = ReportUsage::default();
    let sum = |values: &[Option<u64>]| {
        (!values.is_empty())
            .then(|| {
                values
                    .iter()
                    .try_fold(0u64, |total, next| total.checked_add((*next)?))
            })
            .flatten()
    };
    if run.choice.describe.serves("report_usage_run_id") {
        usage = usage.with_run_id(&run.id);
        if !run.unmeasured_attempt && run.input.len() == run.dispatched {
            usage.input_tokens = sum(&run.input);
            usage.output_tokens = sum(&run.output);
            usage.cost_usd = sum(&run.costs).map(|nanos| nanos as f64 / 1_000_000_000.0);
        }
    } else if let Some(batch) = batch.filter(|_| decision_measured) {
        let answered = batch
            .outcomes
            .iter()
            .filter(|outcome| {
                matches!(outcome.outcome, Outcome::Answered(_))
                    || outcome.input_tokens.is_some()
                    || outcome.output_tokens.is_some()
            })
            .collect::<Vec<_>>();
        usage.input_tokens = sum(&answered
            .iter()
            .map(|outcome| outcome.input_tokens)
            .collect::<Vec<_>>());
        usage.output_tokens = sum(&answered
            .iter()
            .map(|outcome| outcome.output_tokens)
            .collect::<Vec<_>>());
        usage.cost_usd = (!answered.is_empty())
            .then(|| {
                classify::batch_cost(
                    &run.choice.frozen,
                    &answered
                        .iter()
                        .map(|outcome| (outcome.input_tokens, outcome.output_tokens))
                        .collect::<Vec<_>>(),
                )
            })
            .flatten()
            .map(|nanos| nanos as f64 / 1_000_000_000.0);
    }
    let batch_error = batch.and_then(|batch| {
        batch
            .outcomes
            .iter()
            .filter_map(|outcome| match &outcome.outcome {
                Outcome::Failed(body) => serde_json::from_str::<ItemError>(body).ok(),
                _ => None,
            })
            .max_by_key(error_significance)
    });
    let all_answered = batch.is_some_and(|batch| {
        serde_json::from_str::<BatchIdentity>(&batch.identity).is_ok_and(|identity| {
            identity.items.len() == batch.outcomes.len()
                && batch
                    .outcomes
                    .iter()
                    .all(|outcome| matches!(outcome.outcome, Outcome::Answered(_)))
        })
    });
    let error = if run.choice.describe.serves("report_usage_run_id") {
        run.error
            .as_ref()
            .filter(|_| !run.renew_failed || !all_answered)
    } else {
        batch_error.as_ref()
    };
    let outcome = if error.is_some() {
        ReportOutcome::Failed
    } else {
        ReportOutcome::Completed
    };
    let mut report = ReportRequest::new(&run.choice.select.decision_id, outcome).with_usage(usage);
    if let Some(pool) = run
        .choice
        .select
        .runner
        .auth
        .as_ref()
        .and_then(|auth| auth.pool.as_ref())
    {
        report = report.with_pool(pool);
    }
    if let Some(account) = &run.choice.account {
        report = report.with_credential_id(&account.account.credential_id);
    }
    if let Some(error) = error {
        report = report.with_runner_code(&error.code);
        if run.choice.describe.serves("report_outcomes")
            && !run.renew_failed
            && matches!(
                error.code.as_str(),
                errors::AUTH_FAILED | errors::ACCOUNT_NOT_ENTITLED
            )
        {
            report = report.with_refusal_class(
                cortexkit_role_routing::report::RefusalClass::CredentialRefused,
            );
        }
    }
    if let Some(reset) = run.reset_at_ms {
        report = report.with_reset_at_ms(reset);
    }
    report
}

async fn flush(
    service: &Arc<SystemOne>,
    batch_id: &str,
    journal: &mut Journal,
) -> Result<(), Error> {
    let Some(routing) = &service.routing else {
        return Ok(());
    };
    let mut index = 0;
    while index < journal.outbox.len() {
        if journal.outbox[index].conflict {
            journal.outbox[index].closed = true;
            journal.outbox[index].conflict = false;
            save(service, batch_id, journal).await?;
        }
        let rpc = &journal.outbox[index];
        if rpc.closed {
            index += 1;
            continue;
        }
        if rpc.method == "route.account_for" {
            let run = rpc
                .cleanup_run
                .clone()
                .ok_or_else(|| internal_refusal("expired admission cleanup identity is missing"))?;
            let requested = rpc.params["admission_id"]
                .as_str()
                .ok_or_else(|| internal_refusal("expired admission request identity is missing"))?
                .to_owned();
            let capabilities = rpc.capabilities.clone();
            let answered = match routing
                .client
                .classifier_rpc::<AccountForAnswer>(&rpc.method, &rpc.params)
                .await
            {
                Ok(answer) => Some(answer.admission.id),
                Err(RoutingCallError::Refused { .. }) => None,
                Err(error) => {
                    tracing::warn!(target: "system_one", ?error, "expired admission outcome is still unknown; keeping its exact cleanup request");
                    break;
                }
            };
            journal.outbox[index].closed = true;
            queue_release_ids(journal, &run, &requested, answered.as_deref(), capabilities);
            save(service, batch_id, journal).await?;
            index += 1;
            continue;
        }
        let result: Result<(), RoutingCallError> = async {
            if rpc.method == "route.report" {
                let request: ReportRequest =
                    serde_json::from_value(rpc.params.clone()).map_err(|_| {
                        RoutingCallError::Invalid("a stored routing report does not decode".into())
                    })?;
                routing
                    .client
                    .classifier_report(&request, &rpc.capabilities)
                    .await
                    .map(|_| ())
            } else {
                let request =
                    cortexkit_role_routing::observe::parse_account_observe_request_serving(
                        &rpc.params,
                        &rpc.capabilities,
                    )
                    .map_err(|error| {
                        RoutingCallError::Invalid(format!(
                            "stored account observation is invalid: {error:?}"
                        ))
                    })?;
                routing
                    .client
                    .classifier_rpc::<cortexkit_role_routing::observe::AccountObserveAnswer>(
                        &rpc.method,
                        &request,
                    )
                    .await
                    .map(|_| ())
            }
        }
        .await;
        match result {
            Ok(()) => journal.outbox[index].closed = true,
            Err(RoutingCallError::Refused { code, message, .. }) if code == "unknown_decision" => {
                tracing::warn!(target: "system_one", %code, %message, "closing routing report for an unknown decision");
                journal.outbox[index].closed = true;
            }
            Err(RoutingCallError::Refused { code, message, .. }) if code == "report_conflict" => {
                tracing::error!(target: "system_one", %code, %message, "routing report conflict closed; immutable body cannot be retried");
                journal.outbox[index].closed = true;
            }
            Err(RoutingCallError::Invalid(message)) => {
                tracing::error!(target: "system_one", %message, "routing RPC was not confirmed; keeping its immutable debt");
                break;
            }
            Err(RoutingCallError::Refused { code, message, .. })
                if matches!(
                    code.as_str(),
                    "invalid_request" | "account_not_authorized" | "admission_conflict"
                ) =>
            {
                tracing::error!(target: "system_one", %code, %message, "terminal routing RPC refusal closed");
                journal.outbox[index].closed = true;
            }
            Err(error) => {
                tracing::warn!(target: "system_one", ?error, "routing outbox delivery will be retried");
                break;
            }
        }
        save(service, batch_id, journal).await?;
        index += 1;
    }
    Ok(())
}

fn queue_finished_run(
    journal: &mut Journal,
    index: usize,
    final_batch: bool,
    batch: Option<&StoredBatch>,
) {
    if !journal.runs[index].finished {
        journal.runs[index].finished = true;
        let run = &journal.runs[index];
        if run.choice.account.is_some() {
            journal.observe(account_routing::run_finished_body(
                &format!("{}:finished", run.id),
                run.observation_admission(),
                &run.id,
                if run.error.is_some() {
                    broca_protocol::RunFinishReason::Error
                } else {
                    broca_protocol::RunFinishReason::Completed
                },
                None,
            ));
        }
        for admission in journal.runs[index].cleanup_admissions.clone() {
            let run = &journal.runs[index];
            journal.observe(account_routing::run_finished_body(
                &format!("{}:rejected:{admission}", run.id),
                &admission,
                &run.id,
                broca_protocol::RunFinishReason::Error,
                None,
            ));
        }
    }
    let run = &journal.runs[index];
    if !run.report_queued && (run.choice.describe.serves("report_usage_run_id") || final_batch) {
        let already_reported = !run.choice.describe.serves("report_usage_run_id")
            && journal.outbox.iter().any(|rpc| {
                rpc.method == "route.report"
                    && rpc.params["decision_id"] == run.choice.select.decision_id
            });
        if !already_reported {
            let decision_measured = run.choice.describe.serves("report_usage_run_id")
                || journal
                    .runs
                    .iter()
                    .filter(|other| {
                        other.choice.select.decision_id == run.choice.select.decision_id
                    })
                    .all(|other| {
                        !other.unmeasured_attempt && other.input.len() == other.dispatched
                    });
            let report = finished_report(run, batch, decision_measured);
            let capabilities = run.choice.describe.capabilities.clone();
            let refusal = run.renewal_refusal.clone();
            journal.report(report, capabilities);
            journal
                .outbox
                .last_mut()
                .expect("report queued")
                .routing_refusal = refusal;
        }
        journal.runs[index].report_queued = true;
    }
}

async fn finish_run(
    service: &Arc<SystemOne>,
    batch_id: &str,
    journal: &mut Journal,
    index: usize,
    final_batch: bool,
) -> Result<(), Error> {
    let batch = service
        .stored_batch_for_routing_cleanup(batch_id)
        .await
        .map_err(|_| internal_refusal("routing usage recovery is unavailable"))?;
    queue_finished_run(journal, index, final_batch, batch.as_ref());
    save(service, batch_id, journal).await?;
    flush(service, batch_id, journal).await
}

async fn record(
    service: &Arc<SystemOne>,
    batch_id: &str,
    outcome: &StoredOutcome,
    cost: u64,
    fact: Option<broca_store::system_one::FactContext>,
    journal: &Journal,
) -> Result<(), Error> {
    classify::record_paid_routed(
        service,
        batch_id,
        outcome,
        cost,
        fact,
        Some(serde_json::to_string(journal).expect("journal encodes")),
    )
    .await?;
    Ok(())
}

/// Rebuild report bodies only for runs that never reached their durable terminal. Bodies
/// already queued are immutable, including across a send/acknowledgement crash window.
async fn recover_claimed(
    service: &Arc<SystemOne>,
    batch_id: &str,
    journal: &mut Journal,
) -> Result<(), Error> {
    if let Some(pending) = journal.pending_admission.clone() {
        let client = &service
            .routing
            .as_ref()
            .ok_or_else(|| unavailable("routing is not configured"))?
            .client;
        match client
            .classifier_rpc::<AccountForAnswer>("route.account_for", &pending.params)
            .await
        {
            Ok(answer) => {
                let checked = check_admission(&pending, &answer);
                release_pending(
                    service,
                    batch_id,
                    journal,
                    &pending,
                    Some(&answer.admission.id),
                )
                .await?;
                checked?;
            }
            Err(RoutingCallError::Unavailable(message)) => return Err(unavailable(message)),
            Err(RoutingCallError::Invalid(message)) => {
                release_pending(service, batch_id, journal, &pending, None).await?;
                return Err(routing_error(
                    RoutingCallError::Invalid(message),
                    None,
                    (service.clock)().max(0) as u64,
                ));
            }
            Err(error @ RoutingCallError::Refused { .. }) => {
                // A refusal of a replay is not proof that an earlier unknown grant never
                // happened. Only a definitive grant or explicit cleanup clears the intent.
                return Err(routing_error(error, None, (service.clock)().max(0) as u64));
            }
        }
    }
    let batch = service
        .stored_batch_for_routing_cleanup(batch_id)
        .await
        .map_err(|_| internal_refusal("routing recovery is unavailable"))?;
    let final_batch = batch.as_ref().is_some_and(|batch| {
        let identity: BatchIdentity = match serde_json::from_str(&batch.identity) {
            Ok(identity) => identity,
            Err(_) => return false,
        };
        batch.outcomes.len() == identity.items.len() && batch.outcomes.iter().all(retained)
    });
    let mut changed = journal.runs.is_empty() && journal.outbox.is_empty();
    for index in 0..journal.runs.len() {
        if journal.runs[index].renewing {
            let _ = renew(service, batch_id, journal, index).await?;
        }
        let run = &journal.runs[index];
        if run.finished
            && (run.report_queued
                || (!final_batch && !run.choice.describe.serves("report_usage_run_id")))
        {
            continue;
        }
        if !journal.runs[index].finished
            && (journal.runs[index].dispatched == 0
                || journal.runs[index].input.len() != journal.runs[index].dispatched)
        {
            // A killed call has no more provider attempts. Its measured outcomes committed
            // atomically with this run's progress; missing outcomes remain unrecorded usage.
            journal.runs[index].error.get_or_insert_with(|| {
                ItemError::for_code(
                    errors::PROVIDER_ERROR,
                    "the dispatching call ended before its terminal was recorded",
                )
            });
        }
        queue_finished_run(journal, index, final_batch, batch.as_ref());
        changed = true;
    }
    // Commit every rebuilt body before any is sent, without rewriting the same journal
    // once per historical run. The outbox still delivers and acknowledges in order.
    if changed {
        save(service, batch_id, journal).await?;
    }
    flush(service, batch_id, journal).await
}

impl SystemOne {
    /// Drain routing work even after its batch content has expired.
    pub async fn recover_routing(self: &Arc<Self>) -> Result<(), String> {
        let service = self.clone();
        let now = (self.clock)();
        let ids =
            classify::blocking(move || service.store.routing_recovery_batches(&service.role, now))
                .await
                .map_err(|error| error.message)?;
        for id in ids {
            if let Err(error) = recover_batch(self, &id).await {
                tracing::warn!(target: "system_one", batch_id=%id, error=%String::from_utf8_lossy(&error.bytes()), "routing batch recovery failed; continuing with other batches");
            }
        }
        Ok(())
    }

    /// Run once at boot, then periodically, so owed reports can reach routing after a restart.
    pub fn spawn_routing_recovery(
        self: &Arc<Self>,
        stop: tokio_util::sync::CancellationToken,
    ) -> tokio::task::JoinHandle<()> {
        let service = self.clone();
        let tools = (self.role == super::STORE_ROLE && self.routing.is_some())
            .then(|| Arc::new(self.fork_for_tools()));
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                tokio::select! { biased; _ = stop.cancelled() => return, _ = tick.tick() => {} }
                if let Err(error) = service.recover_routing().await {
                    tracing::warn!(target: "system_one", %error, "routing recovery scan failed");
                }
                if let Some(tools) = &tools {
                    if let Err(error) = tools.recover_routing().await {
                        tracing::warn!(target: "system_one", %error, "model-tool routing recovery scan failed");
                    }
                }
            }
        })
    }
}

async fn recover_batch(service: &Arc<SystemOne>, batch_id: &str) -> Result<(), Error> {
    let (store, role, id, owner, now) = (
        service.store.clone(),
        service.role.clone(),
        batch_id.to_owned(),
        service.owner.clone(),
        (service.clock)(),
    );
    if !classify::blocking(move || store.claim_routing_recovery(&role, &id, &owner, now)).await? {
        return Ok(());
    }
    let hold = Hold {
        service,
        batch_id: batch_id.into(),
        stored: true,
        local: false,
    };
    let result = async {
        let (store, role, id) = (service.store.clone(), service.role.clone(), batch_id.to_owned());
        let Some(row) = classify::blocking(move || store.routing(&role, &id)).await? else { return Ok(()); };
        let batch = service.stored_batch_for_routing_cleanup(batch_id).await
            .map_err(|_| internal_refusal("routing retention lookup failed"))?;
        let expired = batch.as_ref().is_some_and(|batch| now.saturating_sub(batch.admitted_at_ms) >= broca_store::system_one::RETENTION_MS);
        let mut journal: Journal = match serde_json::from_str(&row.runs) {
            Ok(journal) => journal,
            Err(_) if expired => {
                tracing::error!(target: "system_one", %batch_id, "expired routing journal is undecodable; scrubbing unrecoverable content");
                let (store, role, id, owner) = (service.store.clone(), service.role.clone(), batch_id.to_owned(), service.owner.clone());
                classify::blocking(move || store.expire_routed(&role, &id, &owner, None)).await?;
                return Ok(());
            }
            Err(_) => return Err(internal_refusal("routing journal does not decode").into()),
        };
        if !journal.expired && expired {
            if let Some(pending) = journal.pending_admission.clone() {
                // A finish sent before routing creates the admission cannot cancel a
                // later grant. Resolve the saved idempotent admission request first.
                journal.outbox.push(Rpc {
                    cleanup_run: Some(pending.id), routing_refusal: None,
                    method: "route.account_for".into(), params: pending.params,
                    closed: false, conflict: false,
                    capabilities: pending.choice.describe.capabilities,
                });
                journal.pending_admission = None;
            }
            for index in 0..journal.runs.len() {
                let run = &mut journal.runs[index];
                if run.renewing {
                    run.cleanup_admissions.push(format!("{}:renew:{}", run.id, run.renewals + 1));
                    run.renewing = false;
                }
                if !run.finished && run.input.len() != run.dispatched {
                    run.note_error(ItemError::for_code(errors::PROVIDER_ERROR, "dispatch ended before its terminal was committed"));
                }
                // Expiry ends this decision even when the last item was retryable.
                queue_finished_run(&mut journal, index, true, batch.as_ref());
            }
            let unrun = journal.selected.iter().chain(journal.reselection.iter().filter_map(|selection| selection.choice.as_ref())).cloned().collect::<Vec<_>>();
            for choice in unrun {
                queue_abandoned_selection(&mut journal, &choice);
            }
            if let Some(selection) = &mut journal.reselection { selection.finished = true; }
            journal.outbox.retain(|rpc| !rpc.closed);
            journal = Journal { expired: true, outbox: journal.outbox, ..Default::default() };
            archive_expired(service, batch_id, &journal).await?;
        } else if !journal.expired {
            recover_claimed(service, batch_id, &mut journal).await?;
        }
        flush(service, batch_id, &mut journal).await?;
        // Content-free rows track pending routing reports. Once acknowledged, cleanup
        // must finish even when ordinary content retention is disabled.
        if journal.expired {
            archive_expired(service, batch_id, &journal).await?;
        }
        Ok(())
    }.await;
    hold.release().await;
    result
}

fn queue_abandoned_selection(journal: &mut Journal, choice: &Choice) {
    let decision = &choice.select.decision_id;
    if journal
        .runs
        .iter()
        .any(|run| run.choice.select.decision_id == *decision)
        || journal
            .outbox
            .iter()
            .any(|rpc| rpc.method == "route.report" && rpc.params["decision_id"] == *decision)
    {
        return;
    }
    let outcome = if choice.describe.serves("report_outcomes") {
        ReportOutcome::LaunchFailed
    } else {
        ReportOutcome::Cancelled
    };
    let mut report = ReportRequest::new(decision, outcome).with_runner_code("provider_error");
    if let Some(pool) = choice
        .select
        .runner
        .auth
        .as_ref()
        .and_then(|auth| auth.pool.as_ref())
    {
        report = report.with_pool(pool);
    }
    journal.report(report, choice.describe.capabilities.clone());
}

async fn archive_expired(
    service: &Arc<SystemOne>,
    batch_id: &str,
    journal: &Journal,
) -> Result<(), Error> {
    let owed = journal
        .outbox
        .iter()
        .filter(|rpc| !rpc.closed)
        .collect::<Vec<_>>();
    let archive = (!owed.is_empty()).then(|| {
        serde_json::to_string(&json!({"expired":true,"outbox":owed}))
            .expect("outbox archive encodes")
    });
    let (store, role, id, owner) = (
        service.store.clone(),
        service.role.clone(),
        batch_id.to_owned(),
        service.owner.clone(),
    );
    classify::blocking(move || store.expire_routed(&role, &id, &owner, archive.as_deref())).await?;
    Ok(())
}

pub(super) async fn run(
    service: &Arc<SystemOne>,
    params: &str,
    scope: &BatchScope,
    admitted_at_ms: Option<i64>,
    prepared: &super::images::PreparedImages,
    consumer: &str,
) -> Result<Option<Box<RawValue>>, Error> {
    run_on(
        service,
        params,
        scope,
        admitted_at_ms,
        prepared,
        consumer,
        None,
    )
    .await
}

pub(super) struct ToolRun<'a> {
    pub args_digest: &'a str,
    pub route: ToolRoute,
}

pub(super) async fn run_tool(
    service: &Arc<SystemOne>,
    params: &str,
    scope: &BatchScope,
    admitted_at_ms: Option<i64>,
    prepared: &super::images::PreparedImages,
    tool: ToolRun<'_>,
) -> Result<Box<RawValue>, Vec<u8>> {
    run_on(
        service,
        params,
        scope,
        admitted_at_ms,
        prepared,
        "",
        Some(tool),
    )
    .await
    .and_then(|reply| {
        reply.ok_or_else(|| internal_refusal("routed tool reservation is missing").into())
    })
    .map_err(|error| error.tool_bytes(Spend::Nothing))
}

async fn run_on(
    service: &Arc<SystemOne>,
    params: &str,
    scope: &BatchScope,
    admitted_at_ms: Option<i64>,
    prepared: &super::images::PreparedImages,
    consumer: &str,
    tool: Option<ToolRun<'_>>,
) -> Result<Option<Box<RawValue>>, Error> {
    cortexkit_role_classifier::request::check_request_bytes(
        params.len(),
        &cortexkit_role_classifier::describe::Limits::new(
            service.config.max_items,
            service.config.max_request_bytes,
        ),
    )?;
    let request = parse_request(params)?;
    let (store, role, id) = (
        service.store.clone(),
        service.role.clone(),
        request.batch_id.clone(),
    );
    let routing = classify::blocking(move || store.routing(&role, &id)).await?;
    if routing
        .as_ref()
        .and_then(|row| serde_json::from_str::<Value>(&row.runs).ok())
        .is_some_and(|journal| journal["expired"] == true)
    {
        return Err(unavailable(format!(
            "batch_id {} has an owed routing report; retry after report delivery",
            request.batch_id
        )));
    }
    let snapshot = if routing.is_none() {
        // Legacy claims compare stored scope bytes and do not need to decode them first.
        service
            .stored_batch_for_routing_cleanup(&request.batch_id)
            .await
    } else {
        service.stored_batch(&request.batch_id).await
    }
    .map_err(|_| internal_refusal("batch replay is unavailable"))?;
    let snapshot = snapshot.filter(|batch| {
        (service.clock)().saturating_sub(batch.admitted_at_ms)
            < broca_store::system_one::RETENTION_MS
    });
    if snapshot.is_some() && routing.is_none() {
        // Batches admitted before routing have no decision or account to resume.
        return Ok(None);
    }
    // A caller-pinned model has known question, item and image limits before routing.
    // Retries validate against their frozen entry rather than live catalog rows.
    let entry = if snapshot.is_none() {
        pinned_entry(service, &request)?.unwrap_or_else(|| validation_entry(service))
    } else {
        let routing = routing.as_ref().expect("routed snapshot checked presence");
        let choice: Option<Choice> = serde_json::from_str(&routing.choice)
            .map_err(|_| internal_refusal("stored routing choice does not decode"))?;
        let journal: Journal = serde_json::from_str(&routing.runs)
            .map_err(|_| internal_refusal("stored routing journal does not decode"))?;
        choice
            .or(journal.selected)
            .and_then(|choice| choice.entry)
            .unwrap_or_else(|| validation_entry(service))
    };
    if tool.is_none() {
        check_model_request(service, &request, &entry, prepared)?;
    }
    estimates(&request)?;
    let identity =
        BatchIdentity::of(&request).map_err(|error| Refusal::invalid_params("items", error.0))?;
    let identity_json = serde_json::to_string(&identity).expect("identity encodes");
    let digest = tool.as_ref().map_or_else(
        || classify::sha256_hex(&identity_json),
        |tool| tool.args_digest.to_owned(),
    );
    flush_facts(service).await?;
    {
        let mut claims = service
            .claims
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(recorded) = claims.get(&request.batch_id) {
            return Err(if recorded != scope {
                Refusal::new(
                    "batch_scope_mismatch",
                    "batch_id is running under a different scope",
                )
            } else {
                Refusal::batch_in_progress(super::BATCH_IN_PROGRESS_RETRY_AFTER_MS)
            }
            .into());
        }
        claims.insert(request.batch_id.clone(), scope.clone());
    }
    let mut hold = Hold {
        service,
        batch_id: request.batch_id.clone(),
        stored: false,
        local: true,
    };
    let answer = async {
        let (store, role, owner, id, model, scope) = (
            service.store.clone(),
            service.role.clone(),
            service.owner.clone(),
            request.batch_id.clone(),
            request.model.clone().unwrap_or_default(),
            scope.clone(),
        );
        let (stored_identity, stored_digest) = (identity_json.clone(), digest.clone());
        let now = (service.clock)();
        let ceiling = request
            .max_cost_usd
            .as_ref()
            .map(|number| usd_to_nanos(number, Rounding::Down).unwrap_or(u64::MAX));
        let claimed = classify::blocking(move || {
            store.claim_routed(
                &BatchClaim {
                    role: &role,
                    batch_id: &id,
                    model: &model,
                    identity: &stored_identity,
                    body_digest: &stored_digest,
                    frozen_row: "{}",
                    scope: &scope,
                    ceiling_nanos: ceiling,
                    owner: &owner,
                    now_ms: now,
                },
                admitted_at_ms.unwrap_or(now),
            )
        })
        .await?;
        let (stored, fresh) =
            match claimed {
                Claimed::Fresh => (None, true),
                Claimed::Expired => return Err(unavailable("batch content has expired; routing cleanup must finish before this id can be reused")),
                Claimed::Resumed(record) => (Some(record), false),
                Claimed::ScopeMismatch => return Err(Refusal::new(
                    "batch_scope_mismatch",
                    "batch_id belongs to a different scope; a retry cannot change its attribution",
                )
                .into()),
                Claimed::InProgress => {
                    return Err(
                        Refusal::batch_in_progress(super::BATCH_IN_PROGRESS_RETRY_AFTER_MS).into(),
                    )
                }
                Claimed::Reuse { recorded_identity } => {
                    let recorded: BatchIdentity = serde_json::from_str(&recorded_identity)
                        .map_err(|_| internal_refusal("stored identity does not decode"))?;
                    return Err(Refusal::batch_id_reuse(
                        recorded
                            .first_difference(&identity)
                            .unwrap_or_else(|| "items".into()),
                    )
                    .into());
                }
            };
        hold.stored = true;
        let (store, role, id) = (
            service.store.clone(),
            service.role.clone(),
            request.batch_id.clone(),
        );
        let routing = classify::blocking(move || store.routing(&role, &id))
            .await?
            .ok_or_else(|| internal_refusal("routed reservation is missing"))?;
        let choice: Option<Choice> = serde_json::from_str(&routing.choice)
            .map_err(|_| internal_refusal("stored routing choice does not decode"))?;
        let mut journal: Journal = serde_json::from_str(&routing.runs)
            .map_err(|_| internal_refusal("stored routing journal does not decode"))?;
        let is_tool = tool.is_some();
        if let Some(tool) = tool {
            if let Some(refused) = &journal.tool_refusal {
                return Err(Error::ToolRefusal(refused.clone()));
            }
            match tool.route {
                ToolRoute::Refused(refused) => {
                    // A concurrently admitted selection wins over a later refusal to select.
                    if choice.is_some() || journal.selected.is_some() {
                        return Err(Refusal::batch_in_progress(super::BATCH_IN_PROGRESS_RETRY_AFTER_MS).into());
                    }
                    journal.tool_refusal = Some(refused.clone());
                    save(service, &request.batch_id, &journal).await?;
                    return Err(Error::ToolRefusal(refused));
                }
                ToolRoute::Chosen(selected) => {
                    if let Some(recorded) = choice.as_ref().or(journal.selected.as_ref()) {
                        // The consent check (for a run that read private content) admitted this
                        // selection's provider, not another process's choice. A retry reads the
                        // recorded route before checking consent again.
                        if recorded.select.decision_id != selected.select.decision_id {
                            return Err(Refusal::batch_in_progress(super::BATCH_IN_PROGRESS_RETRY_AFTER_MS).into());
                        }
                    } else {
                        journal.selected = Some(*selected);
                        save(service, &request.batch_id, &journal).await?;
                    }
                }
            }
        }
        if let Some(error) = journal.reselection.as_ref().and_then(|selection| selection.error.clone()) {
            return Err(Error::Runner(Box::new(error)));
        }
        let stored_outcomes = stored
            .as_ref()
            .map_or_else(Vec::new, |record| record.outcomes.clone());
        if let Some(choice) = &choice {
            if let Some(refusal) = &choice.refusal {
                return Err(Error::Runner(Box::new(refusal.clone())));
            }
            if stored_outcomes.len() == request.items.len()
                && (is_tool || stored_outcomes.iter().all(retained))
                && !pending_reselection(&journal)
                && !journal.admission_refusal_pending
                && (is_tool || !reselection_due(
                    &journal,
                    stored_outcomes
                        .iter()
                        .any(|outcome| matches!(outcome.outcome, Outcome::Answered(_))),
                ))
            {
                let mut answered = request.clone();
                answered.model = Some(choice.model.clone());
                return classify::reply_decision(
                    &answered,
                    &choice.frozen,
                    stored_outcomes
                        .into_iter()
                        .map(|outcome| Some(Slot::Stored(outcome)))
                        .collect(),
                    is_tool,
                    Some(&choice.select.decision_id),
                )
                .map(Some)
                .map_err(Into::into);
            }
        }
        if is_tool && stored_outcomes.is_empty() && journal.runs.iter().any(|run| run.dispatched > 0) {
            // Without a recorded outcome we cannot tell whether the interrupted send was billed.
            // Finish its routing work, but never send the same call key to the provider again.
            let refused = Error::from(internal_refusal("model tool execution failed; retry the call"))
                .tool_bytes(Spend::Unknown);
            journal.tool_refusal = Some(refused.clone());
            save(service, &request.batch_id, &journal).await?;
            recover_claimed(service, &request.batch_id, &mut journal).await?;
            return Err(Error::ToolRefusal(refused));
        }
        let unadmitted = choice.as_ref().is_none_or(|choice| choice.account.is_none());
        let answer = dispatch(
            service,
            &request,
            params,
            choice,
            &mut journal,
            stored_outcomes,
            stored
                .as_ref()
                .and_then(|record| record.ceiling_nanos)
                .or(ceiling),
            stored.as_ref().map_or(0, |record| record.spent_nanos),
            prepared,
            consumer,
        )
        .await;
        if is_tool {
            return match answer {
                Ok(reply) => Ok(Some(reply)),
                Err(error) => {
                    let spend = if journal.runs.iter().any(|run| run.dispatched > 0) {
                        Spend::Unknown
                    } else {
                        Spend::Nothing
                    };
                    let refused = error.tool_bytes(spend);
                    journal.tool_refusal = Some(refused.clone());
                    save(service, &request.batch_id, &journal).await?;
                    Err(Error::ToolRefusal(refused))
                }
            };
        }
        if answer.is_err() && (fresh || unadmitted) && journal.runs.is_empty() && journal.outbox.iter().all(|rpc| rpc.closed)
            && journal.pending_admission.is_none()
            && journal.selected.is_none() {
            let (store, role, owner, id) = (
                service.store.clone(),
                service.role.clone(),
                service.owner.clone(),
                request.batch_id.clone(),
            );
            classify::blocking(move || store.discard_fresh(&role, &id, &owner)).await?;
            hold.stored = false;
        }
        answer.map(Some)
    }
    .await;
    hold.release().await;
    answer
}

async fn flush_facts(service: &Arc<SystemOne>) -> Result<(), Error> {
    let service = service.clone();
    tokio::task::spawn_blocking(move || service.flush_facts())
        .await
        .map_err(|_| internal_refusal("measurement export task did not finish"))?
        .map_err(|_| internal_refusal("measurement export is unavailable"))?;
    Ok(())
}

pub(super) fn unavailable_refusal(message: impl Into<String>) -> Refusal {
    let mut refusal = Refusal::new(errors::ROUTING_UNAVAILABLE, message);
    refusal.detail.class = cortexkit_role_classifier::errors::ErrorClass::Transient;
    refusal
}

#[allow(clippy::too_many_arguments)]
fn validate_choice(
    service: &SystemOne,
    request: &ClassifyRequest,
    choice: &Choice,
    prepared: &super::images::PreparedImages,
    estimates: &mut [Estimate],
    slots: &[Option<Slot>],
    ceiling: Option<u64>,
    replacement: bool,
) -> Result<(), Refusal> {
    let entry = choice
        .entry
        .as_ref()
        .ok_or_else(|| Refusal::model_unavailable(&choice.model))?;
    check_model_request(service, request, entry, prepared)?;
    reprice(estimates, &choice.frozen);
    if let Some(ceiling) = ceiling {
        let wanted = slots
            .iter()
            .zip(estimates)
            .filter(|(slot, _)| replacement || slot.is_none())
            .fold(0u64, |total, (_, estimate)| {
                total.saturating_add(estimate.cost_nanos)
            });
        if wanted > ceiling {
            return Err(Refusal::cost_exceeded(
                nanos_to_usd(wanted),
                nanos_to_usd(ceiling),
            ));
        }
    }
    Ok(())
}

fn check_model_request(
    service: &SystemOne,
    request: &ClassifyRequest,
    entry: &cortexkit_role_classifier::describe::ModelEntry,
    prepared: &super::images::PreparedImages,
) -> Result<(), Refusal> {
    service.check_images_request(request, entry)?;
    for (index, images) in prepared.0.iter().enumerate() {
        if let Some(images) = images {
            let images: Vec<cortexkit_model_batch::image::Image> = serde_json::from_str(images)
                .map_err(|_| internal_refusal("resolved images did not decode"))?;
            let decoded = images
                .iter()
                .map(|image| {
                    image
                        .decode()
                        .and_then(Result::ok)
                        .ok_or_else(|| internal_refusal("resolved image is invalid"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            cortexkit_role_classifier::request::check_resolved_item_images(entry, index, &decoded)?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn reject_choice(
    service: &Arc<SystemOne>,
    request: &ClassifyRequest,
    choice: &Choice,
    original: Option<&Choice>,
    journal: &mut Journal,
    slots: Vec<Option<Slot>>,
    refusal: Value,
) -> Result<Box<RawValue>, Error> {
    if let Some(selection) = &mut journal.reselection {
        selection.finished = true;
        selection.refusal = Some(refusal.clone());
    }
    let outcome = if choice.describe.serves("report_outcomes") {
        ReportOutcome::LaunchFailed
    } else {
        ReportOutcome::Cancelled
    };
    journal.report(
        ReportRequest::new(&choice.select.decision_id, outcome)
            .with_runner_code(refusal["code"].as_str().unwrap_or("internal_error")),
        choice.describe.capabilities.clone(),
    );
    // A fresh choice refusal never admitted an immutable body (model-batch RULES.md
    // §§2, 7-8), so free its id. If replacement selection fails after an account
    // failure, keep the original admitted batch and replay its account errors rather
    // than routing them again (classifier CONTRACT.md §6.3).
    if service.role != super::LLM_CLASSIFY_STORE_ROLE
        && original.is_none()
        && journal.runs.is_empty()
        && choice.account.is_none()
        && journal.pending_admission.is_none()
    {
        let outbox = std::mem::take(&mut journal.outbox);
        *journal = Journal {
            expired: true,
            outbox,
            ..Default::default()
        };
        archive_expired(service, &request.batch_id, journal).await?;
        flush(service, &request.batch_id, journal).await?;
        archive_expired(service, &request.batch_id, journal).await?;
        return Err(Error::Runner(Box::new(refusal)));
    }
    let mut rejected = choice.clone();
    rejected.refusal = Some(refusal.clone());
    freeze(
        service,
        &request.batch_id,
        original.unwrap_or(&rejected),
        journal,
    )
    .await?;
    flush(service, &request.batch_id, journal).await?;
    if let Some(original) = original {
        let mut answered = request.clone();
        answered.model = Some(original.model.clone());
        return classify::reply_decision(
            &answered,
            &original.frozen,
            slots,
            false,
            Some(&original.select.decision_id),
        )
        .map_err(Into::into);
    }
    Err(Error::Runner(Box::new(refusal)))
}

async fn reject_replacement_admission(
    service: &Arc<SystemOne>,
    request: &ClassifyRequest,
    choice: &Choice,
    original: &Choice,
    journal: &mut Journal,
    error: &Error,
) -> Result<(), Error> {
    let refusal = match error {
        Error::Classifier(body) => serde_json::to_value(body).expect("refusal encodes"),
        Error::Routing(body) => serde_json::to_value(body).expect("refusal encodes"),
        Error::Runner(body) => body.as_ref().clone(),
        Error::ToolRefusal(body) => serde_json::from_slice(body).expect("tool refusal encodes"),
    };
    let selection = journal
        .reselection
        .as_mut()
        .expect("replacement selection exists");
    selection.finished = true;
    selection.error = Some(refusal.clone());
    selection.refusal = Some(refusal.clone());
    let outcome = if choice.describe.serves("report_outcomes") {
        ReportOutcome::LaunchFailed
    } else {
        ReportOutcome::Cancelled
    };
    journal.report(
        ReportRequest::new(&choice.select.decision_id, outcome)
            .with_runner_code(refusal["code"].as_str().unwrap_or("internal_error")),
        choice.describe.capabilities.clone(),
    );
    journal
        .outbox
        .last_mut()
        .expect("report queued")
        .routing_refusal = Some(refusal);
    freeze(service, &request.batch_id, original, journal).await?;
    flush(service, &request.batch_id, journal).await
}

#[allow(clippy::too_many_arguments)]
async fn dispatch(
    service: &Arc<SystemOne>,
    request: &ClassifyRequest,
    params: &str,
    choice: Option<Choice>,
    journal: &mut Journal,
    stored: Vec<StoredOutcome>,
    ceiling: Option<u64>,
    mut spent: u64,
    prepared: &super::images::PreparedImages,
    consumer: &str,
) -> Result<Box<RawValue>, Error> {
    let is_tool = service.role == super::LLM_CLASSIFY_STORE_ROLE;
    let raw: RawParams<'_> = serde_json::from_str(params)
        .map_err(|_| Refusal::invalid_params("params", "request did not decode"))?;
    let mut estimates = estimates(request)?;
    let mut slots: Vec<Option<Slot>> = (0..request.items.len()).map(|_| None).collect();
    for outcome in stored {
        if is_tool || retained(&outcome) {
            let index = outcome.index as usize;
            if index < slots.len() {
                slots[index] = Some(Slot::Stored(outcome));
            }
        }
    }
    let mut choice = match choice {
        Some(choice) => choice,
        None => match journal.selected.clone() {
            Some(choice) => choice,
            None => {
                if is_tool {
                    return Err(internal_refusal("the admitted tool has no frozen route").into());
                }
                let choice = select(service, request, &estimates, consumer).await?;
                journal.selected = Some(choice.clone());
                // Persist a newly selected model's validation refusal and its report
                // together, never a replayable body waiting to be refused after a crash.
                let refusal = choice.refusal.clone().or_else(|| {
                    validate_choice(
                        service,
                        request,
                        &choice,
                        prepared,
                        &mut estimates,
                        &slots,
                        ceiling,
                        false,
                    )
                    .err()
                    .map(|refusal| serde_json::to_value(refusal).expect("refusal encodes"))
                });
                if let Some(refusal) = refusal {
                    return reject_choice(service, request, &choice, None, journal, slots, refusal)
                        .await;
                }
                save(service, &request.batch_id, journal).await?;
                choice
            }
        },
    };
    recover_claimed(service, &request.batch_id, journal).await?;
    if let Some(refusal) = journal.admission_refusal.clone() {
        return refuse_admission(service, request, &choice, journal, slots, refusal).await;
    }
    let any_answer = slots.iter().any(|slot| {
        matches!(
            slot,
            Some(Slot::Stored(StoredOutcome {
                outcome: Outcome::Answered(_),
                ..
            }))
        )
    });
    let mut previous_choice = None;
    let replacement = if is_tool {
        None
    } else if pending_reselection(journal) {
        journal
            .reselection
            .as_ref()
            .and_then(|selection| selection.choice.clone())
    } else if reselection_due(journal, any_answer) {
        Some(reselect(service, request, &estimates, journal, consumer).await?)
    } else {
        None
    };
    if let Some(replacement) = replacement {
        previous_choice = Some(choice);
        choice = replacement;
    } else if let Some(error) = journal
        .runs
        .last()
        .filter(|run| run.choice.select.decision_id == choice.select.decision_id)
        .and_then(|run| run.error.as_ref())
        .filter(|error| !is_tool && account_error(error))
        .cloned()
    {
        // A process can stop between committing the account failure and recording the
        // remaining failures. That gap does not permit another admission or provider send.
        cascade_account_error(service, &request.batch_id, journal, &mut slots, &error).await?;
        finish_run(
            service,
            &request.batch_id,
            journal,
            journal.runs.len() - 1,
            true,
        )
        .await?;
        let mut answered = request.clone();
        answered.model = Some(choice.model.clone());
        return classify::reply_decision(
            &answered,
            &choice.frozen,
            slots,
            false,
            Some(&choice.select.decision_id),
        )
        .map_err(Into::into);
    }
    loop {
        if let Some(refusal) = &choice.refusal {
            return reject_choice(
                service,
                request,
                &choice,
                previous_choice.as_ref(),
                journal,
                slots,
                refusal.clone(),
            )
            .await;
        }
        if let Err(refusal) = validate_choice(
            service,
            request,
            &choice,
            prepared,
            &mut estimates,
            &slots,
            ceiling,
            previous_choice.is_some(),
        ) {
            return reject_choice(
                service,
                request,
                &choice,
                previous_choice.as_ref(),
                journal,
                slots,
                serde_json::to_value(refusal).expect("refusal encodes"),
            )
            .await;
        }
        if previous_choice.is_some() {
            slots.iter_mut().for_each(|slot| *slot = None);
        }
        if let Some(ceiling) = ceiling {
            if !slots.iter().zip(&estimates).any(|(slot, estimate)| {
                slot.is_none()
                    && !cortexkit_model_batch::estimate::crosses_ceiling(spent, estimate, ceiling)
            }) {
                for (index, slot) in slots
                    .iter_mut()
                    .enumerate()
                    .filter(|(_, slot)| slot.is_none())
                {
                    let error = ItemError::for_code(
                        errors::COST_EXCEEDED,
                        "answering this item could cross the batch ceiling",
                    );
                    let outcome = StoredOutcome {
                        index: index as u64,
                        outcome: Outcome::Failed(classify::stored_error_json(
                            &error,
                            is_tool.then_some(ItemSpend::Nothing),
                        )),
                        input_tokens: None,
                        output_tokens: None,
                    };
                    record(service, &request.batch_id, &outcome, 0, None, journal).await?;
                    *slot = Some(Slot::Stored(outcome));
                }
                recover_claimed(service, &request.batch_id, journal).await?;
                let mut answered = request.clone();
                answered.model = Some(choice.model.clone());
                return classify::reply_decision(
                    &answered,
                    &choice.frozen,
                    slots,
                    is_tool,
                    Some(&choice.select.decision_id),
                )
                .map_err(Into::into);
            }
        }
        if choice.account.is_some() {
            service.routing_ready(request.model.as_deref()).await?;
        }
        let id = token()?;
        let pending = prepare_admission(
            service,
            &choice,
            &id,
            &request.batch_id,
            params.len() as u64,
        )
        .await?;
        journal.pending_admission = Some(pending.clone());
        save(service, &request.batch_id, journal).await?;
        let client = &service
            .routing
            .as_ref()
            .expect("routing is configured")
            .client;
        let admission = client
            .classifier_rpc::<AccountForAnswer>("route.account_for", &pending.params)
            .await;
        if matches!(admission, Err(RoutingCallError::Invalid(_))) {
            release_pending(service, &request.batch_id, journal, &pending, None).await?;
        }
        let refused = matches!(&admission, Err(RoutingCallError::Refused { .. }));
        let account = match admission.map_err(|error| {
            routing_error(
                error,
                request.model.as_deref(),
                (service.clock)().max(0) as u64,
            )
        }) {
            Ok(account) => {
                if let Err(error) = check_admission(&pending, &account) {
                    release_pending(
                        service,
                        &request.batch_id,
                        journal,
                        &pending,
                        Some(&account.admission.id),
                    )
                    .await?;
                    if let Some(original) = &previous_choice {
                        reject_replacement_admission(
                            service, request, &choice, original, journal, &error,
                        )
                        .await?;
                    }
                    return Err(error);
                }
                account
            }
            Err(error) => {
                if refused {
                    journal.pending_admission = None;
                    save(service, &request.batch_id, journal).await?;
                }
                if !matches!(&error, Error::Routing(refusal) if refusal.code == errors::ROUTING_UNAVAILABLE)
                {
                    if let Some(original) = &previous_choice {
                        reject_replacement_admission(
                            service, request, &choice, original, journal, &error,
                        )
                        .await?;
                    }
                }
                return Err(error);
            }
        };
        let old_account = choice.account.is_some();
        choice.account = Some(account.clone());
        if let Some(selection) = &mut journal.reselection {
            selection.finished = true;
        }
        let routing = service
            .routing
            .as_ref()
            .ok_or_else(|| unavailable("routing is not configured"))?;
        let mut pin = service
            .pin(&choice.model)
            .ok_or_else(|| internal_refusal("frozen model has no adapter"))?
            .clone();
        pin.provider = pin
            .provider
            .with_credential(routing.credentials.source(&account.account.credential_id))
            .ok_or_else(|| internal_refusal("the adapter cannot bind a routed credential"))?;
        journal.pending_admission = None;
        let run_index = journal.runs.len();
        journal.runs.push(Run {
            id: id.clone(),
            choice: choice.clone(),
            finished: false,
            dispatched: 0,
            input: Vec::new(),
            output: Vec::new(),
            costs: Vec::new(),
            error: None,
            report_queued: false,
            renewals: 0,
            renewing: false,
            renew_at_ms: renew_at(
                (service.clock)().max(0) as u64,
                account.pin.lease_valid_until_ms,
            ),
            renew_failed: false,
            reset_at_ms: None,
            unmeasured_attempt: false,
            cleanup_admissions: Vec::new(),
            renewal_refusal: None,
        });
        journal.observe(account_routing::admission_started_body(
            &format!("{id}:started"),
            &id,
            &id,
        ));
        if old_account {
            save(service, &request.batch_id, journal).await?;
        } else {
            freeze(service, &request.batch_id, &choice, journal).await?;
        }
        flush(service, &request.batch_id, journal).await?;
        let result = dispatch_items(
            service, request, &raw, &pin, &choice, &estimates, &mut slots, ceiling, &mut spent,
            prepared, journal, run_index,
        )
        .await;
        if let Err(error) = result {
            // Store/export failures still release this call's account, then return the error.
            journal.runs[run_index].error.get_or_insert_with(|| {
                ItemError::for_code(
                    errors::PROVIDER_ERROR,
                    "the dispatching call could not finish",
                )
            });
            finish_run(service, &request.batch_id, journal, run_index, false).await?;
            return Err(error);
        }
        let failure = journal.runs[run_index].error.clone();
        if let Some(error) = failure
            .as_ref()
            .filter(|error| !is_tool && account_error(error))
        {
            cascade_account_error(service, &request.batch_id, journal, &mut slots, error).await?;
        }
        let any_answer = slots.iter().any(|slot| {
            matches!(
                slot,
                Some(Slot::Stored(StoredOutcome {
                    outcome: Outcome::Answered(_),
                    ..
                }))
            )
        });
        let should_reselect = !is_tool
            && failure.as_ref().is_some_and(|error| {
                matches!(
                    error.code.as_str(),
                    errors::AUTH_FAILED | errors::ACCOUNT_NOT_ENTITLED
                )
            })
            && !any_answer
            && !journal.reselected
            && !journal.runs[run_index].renew_failed;
        let final_batch = slots.iter().all(|slot| match slot {
            Some(Slot::Stored(outcome)) => is_tool || retained(outcome),
            _ => false,
        });
        finish_run(
            service,
            &request.batch_id,
            journal,
            run_index,
            final_batch || should_reselect,
        )
        .await?;
        if should_reselect {
            let selected = reselect(service, request, &estimates, journal, consumer).await?;
            previous_choice = Some(choice);
            choice = selected;
            continue;
        }
        let mut answered = request.clone();
        answered.model = Some(choice.model.clone());
        return classify::reply_decision(
            &answered,
            &choice.frozen,
            slots,
            is_tool,
            Some(&choice.select.decision_id),
        )
        .map_err(Into::into);
    }
}

async fn cascade_account_error(
    service: &Arc<SystemOne>,
    batch_id: &str,
    journal: &Journal,
    slots: &mut [Option<Slot>],
    error: &ItemError,
) -> Result<(), Error> {
    let body = classify::item_error_json(error);
    for (index, slot) in slots.iter_mut().enumerate() {
        if matches!(
            slot,
            Some(Slot::Stored(StoredOutcome {
                outcome: Outcome::Answered(_),
                ..
            }))
        ) || matches!(slot, Some(Slot::Stored(StoredOutcome { outcome: Outcome::Failed(stored), .. })) if *stored == body)
        {
            continue;
        }
        let outcome = StoredOutcome {
            index: index as u64,
            outcome: Outcome::Failed(body.clone()),
            input_tokens: None,
            output_tokens: None,
        };
        record(service, batch_id, &outcome, 0, None, journal).await?;
        *slot = Some(Slot::Stored(outcome));
    }
    Ok(())
}

async fn reselect(
    service: &Arc<SystemOne>,
    request: &ClassifyRequest,
    estimates: &[Estimate],
    journal: &mut Journal,
    consumer: &str,
) -> Result<Choice, Error> {
    journal.reselection = Some(Reselection::default());
    save(service, &request.batch_id, journal).await?;
    match select(service, request, estimates, consumer).await {
        Ok(choice) => {
            journal.reselected = true;
            journal
                .reselection
                .as_mut()
                .expect("reselection exists")
                .choice = Some(choice.clone());
            save(service, &request.batch_id, journal).await?;
            Ok(choice)
        }
        Err(error) => {
            if matches!(&error, Error::Routing(refusal) if refusal.code == errors::ROUTING_UNAVAILABLE)
            {
                // An unanswered attempt has not chosen a replacement. A resend can finish
                // the same single replacement without discarding the original decision.
                journal.reselected = false;
                journal.reselection = None;
            } else {
                journal.reselected = true;
                let selection = journal.reselection.as_mut().expect("reselection exists");
                selection.error = Some(match &error {
                    Error::Classifier(body) => serde_json::to_value(body).expect("refusal encodes"),
                    Error::Routing(body) => serde_json::to_value(body).expect("refusal encodes"),
                    Error::Runner(body) => body.as_ref().clone(),
                    Error::ToolRefusal(body) => {
                        serde_json::from_slice(body).expect("tool refusal encodes")
                    }
                });
                selection.finished = true;
            }
            save(service, &request.batch_id, journal).await?;
            Err(error)
        }
    }
}

async fn refuse_admission(
    service: &Arc<SystemOne>,
    request: &ClassifyRequest,
    choice: &Choice,
    journal: &mut Journal,
    mut slots: Vec<Option<Slot>>,
    refusal: RoutingRefusal,
) -> Result<Box<RawValue>, Error> {
    let routing_code = refusal
        .detail
        .routing_code
        .as_deref()
        .unwrap_or("unknown_refusal");
    if !matches!(routing_code, "auth_required" | "account_not_authorized") {
        return Err(Error::Routing(Box::new(refusal)));
    }
    journal.admission_refusal = Some(refusal.clone());
    journal.admission_refusal_pending = true;
    if let Some(selection) = journal
        .reselection
        .as_mut()
        .filter(|selection| !selection.finished)
    {
        selection.refusal = Some(serde_json::to_value(&refusal).expect("refusal encodes"));
    }
    save(service, &request.batch_id, journal).await?;
    let routing_code = refusal
        .detail
        .routing_code
        .as_deref()
        .unwrap_or("unknown_refusal");
    let code = match routing_code {
        "auth_required" | "account_not_authorized" => errors::AUTH_FAILED,
        "account_hold"
        | "account_pool_exhausted"
        | "account_quota_unknown"
        | "account_pool_reserve_limited" => errors::ACCOUNT_LIMIT_EXHAUSTED,
        _ => errors::ACCOUNT_NOT_ENTITLED,
    };
    let error = ItemError::for_code(
        code,
        format!(
            "routing re-admission refused: {routing_code}: {}",
            refusal
                .detail
                .routing_reason
                .as_deref()
                .unwrap_or(&refusal.message)
        ),
    );
    for (index, slot) in slots.iter_mut().enumerate() {
        if matches!(
            slot,
            Some(Slot::Stored(StoredOutcome {
                outcome: Outcome::Answered(_),
                ..
            }))
        ) {
            continue;
        }
        let outcome = StoredOutcome {
            index: index as u64,
            outcome: Outcome::Failed(classify::item_error_json(&error)),
            input_tokens: None,
            output_tokens: None,
        };
        record(service, &request.batch_id, &outcome, 0, None, journal).await?;
        *slot = Some(Slot::Stored(outcome));
    }
    journal.admission_refusal_pending = false;
    if let Some(selection) = &mut journal.reselection {
        selection.finished = true;
    }
    save(service, &request.batch_id, journal).await?;
    let mut answered = request.clone();
    answered.model = Some(choice.model.clone());
    classify::reply_decision(
        &answered,
        &choice.frozen,
        slots,
        false,
        Some(&choice.select.decision_id),
    )
    .map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
async fn dispatch_items(
    service: &Arc<SystemOne>,
    request: &ClassifyRequest,
    raw: &RawParams<'_>,
    pin: &ModelPin,
    choice: &Choice,
    estimates: &[Estimate],
    slots: &mut [Option<Slot>],
    ceiling: Option<u64>,
    spent: &mut u64,
    prepared: &super::images::PreparedImages,
    journal: &mut Journal,
    run_index: usize,
) -> Result<(), Error> {
    let is_tool = service.role == super::LLM_CLASSIFY_STORE_ROLE;
    let mut stop: Option<ItemError> = None;
    for index in 0..request.items.len() {
        if slots[index].is_some() {
            continue;
        }
        if let Some(error) = &stop {
            if !account_error(error) && !error.is_stored() {
                slots[index] = Some(Slot::Unsent(MarkedItemError::new(error.clone(), None)));
                continue;
            }
            let outcome = StoredOutcome {
                index: index as u64,
                outcome: Outcome::Failed(classify::item_error_json(error)),
                input_tokens: None,
                output_tokens: None,
            };
            record(service, &request.batch_id, &outcome, 0, None, journal).await?;
            slots[index] = Some(Slot::Stored(outcome));
            continue;
        }
        if ceiling.is_some_and(|ceiling| {
            cortexkit_model_batch::estimate::crosses_ceiling(*spent, &estimates[index], ceiling)
        }) {
            let error = ItemError::for_code(
                errors::COST_EXCEEDED,
                "answering this item could cross the batch ceiling",
            );
            let outcome = StoredOutcome {
                index: index as u64,
                outcome: Outcome::Failed(classify::stored_error_json(
                    &error,
                    is_tool.then_some(ItemSpend::Nothing),
                )),
                input_tokens: None,
                output_tokens: None,
            };
            record(service, &request.batch_id, &outcome, 0, None, journal).await?;
            slots[index] = Some(Slot::Stored(outcome));
            continue;
        }
        let images = prepared
            .0
            .get(index)
            .and_then(Option::as_deref)
            .or_else(|| {
                raw.items[index]
                    .images
                    .map(RawValue::get)
                    .filter(|images| images.trim() != "null")
            });
        let images = images.map_or_else(String::new, |images| format!(",\"images\":{images}"));
        let body = format!(
            "{{\"model\":{},\"state\":{},\"questions\":{}{images}}}",
            serde_json::to_string(&pin.provider_model).expect("model encodes"),
            raw.items[index].state.get(),
            raw.questions.get()
        );
        let first_provider_call = journal.runs[run_index].dispatched == 0;
        journal.runs[run_index].dispatched += 1;
        save(service, &request.batch_id, journal).await?;
        let answer =
            provider_call(service, &request.batch_id, pin, body, journal, run_index).await?;
        let mut unstored = None;
        let (outcome, cost, fact) = match answer {
            Ok(classify::PaidAnswer {
                answers,
                usage,
                auth_selection,
                account_ref,
            }) => {
                let cost = classify::spend_of(&choice.frozen, usage.as_ref(), &estimates[index]);
                let input_tokens = usage.and_then(|usage| usage.input_tokens);
                let output_tokens = usage.and_then(|usage| usage.output_tokens);
                let run = &mut journal.runs[run_index];
                run.input.push(input_tokens);
                run.output.push(output_tokens);
                let measured =
                    classify::batch_cost(&choice.frozen, &[(input_tokens, output_tokens)]);
                run.costs.push(measured);
                let debit = measured.unwrap_or_else(|| super::unknown_spend_debit(ceiling));
                let fact = classify::fact_context(
                    service,
                    &choice.model,
                    auth_selection,
                    account_ref,
                    cost,
                );
                let outcome = if is_tool && measured.is_none() {
                    let refusal = crate::llm_tools::unmeasured_cost_refusal();
                    Outcome::Failed(classify::item_error_json(&ItemError::new(
                        refusal.code,
                        refusal.detail.class,
                        refusal.message,
                    )))
                } else {
                    Outcome::Answered(answers)
                };
                (
                    StoredOutcome {
                        index: index as u64,
                        outcome,
                        input_tokens,
                        output_tokens,
                    },
                    debit,
                    fact,
                )
            }
            Err(classify::FailedAnswer {
                error,
                spend,
                reported,
            }) => {
                let usage = reported.as_ref().map(|r| r.usage);
                journal.runs[run_index].note_error(error.clone());
                if let ItemSpend::Measured(nanos) = spend {
                    let run = &mut journal.runs[run_index];
                    run.input.push(usage.and_then(|usage| usage.input_tokens));
                    run.output.push(usage.and_then(|usage| usage.output_tokens));
                    run.costs.push(Some(nanos));
                }
                if !is_tool && first_provider_call && error.code == errors::MODEL_UNAVAILABLE {
                    // Account failures freeze as item errors, but a missing model still
                    // refuses admission. dispatch releases and reports the run before
                    // returning the refusal.
                    return Err(Refusal::model_unavailable(&choice.model).into());
                }
                if account_error(&error) || error.stops_the_call() {
                    stop = Some(error.clone());
                }
                if !is_tool && !account_error(&error) && !error.is_stored() {
                    unstored = Some(error.clone());
                }
                let fact = reported
                    .filter(|_| is_tool || !error.is_retried_on_resend())
                    .and_then(|r| {
                        classify::fact_context(
                            service,
                            &choice.model,
                            r.auth_selection,
                            r.account_ref,
                            classify::spend_of(&choice.frozen, Some(&r.usage), &estimates[index]),
                        )
                    });
                (
                    StoredOutcome {
                        index: index as u64,
                        outcome: Outcome::Failed(classify::stored_error_json(
                            &error,
                            is_tool.then_some(spend),
                        )),
                        input_tokens: usage.and_then(|usage| usage.input_tokens),
                        output_tokens: usage.and_then(|usage| usage.output_tokens),
                    },
                    classify::budget_debit(spend, ceiling),
                    fact,
                )
            }
        };
        if let Some(error) = unstored {
            // Keep run progress durable without making a missing model a permanent item
            // result: the same frozen choice must be tried again after an operator's fix.
            save(service, &request.batch_id, journal).await?;
            slots[index] = Some(Slot::Unsent(MarkedItemError::new(error, None)));
        } else {
            // Account failures remain frozen so a resend cannot silently change accounts.
            record(service, &request.batch_id, &outcome, cost, fact, journal).await?;
            *spent = spent.saturating_add(cost);
            slots[index] = Some(Slot::Stored(outcome));
        }
        flush(service, &request.batch_id, journal).await?;
        if journal.runs[run_index].renew_failed {
            stop = journal.runs[run_index].error.clone();
        }
    }
    Ok(())
}

async fn provider_call(
    service: &Arc<SystemOne>,
    batch_id: &str,
    pin: &ModelPin,
    body: String,
    journal: &mut Journal,
    run_index: usize,
) -> Result<Result<classify::PaidAnswer, classify::FailedAnswer>, Error> {
    let unsent = |error| classify::FailedAnswer {
        error,
        spend: ItemSpend::Nothing,
        reported: None,
    };
    let slot = service.model_slot(&pin.catalog_id);
    for attempt in 1..=service.config.max_attempts.max(2) {
        let started = std::sync::atomic::AtomicBool::new(false);
        let provider = async {
            let _permit = slot.acquire().await;
            started.store(true, std::sync::atomic::Ordering::SeqCst);
            pin.provider.call(body.clone()).await
        };
        tokio::pin!(provider);
        let reply = loop {
            if journal.runs[run_index].renew_failed {
                if started.load(std::sync::atomic::Ordering::SeqCst) {
                    break provider.await;
                }
                return Ok(Err(unsent(
                    journal.runs[run_index]
                        .error
                        .clone()
                        .expect("failed renewal has an error"),
                )));
            }
            let now = (service.clock)().max(0) as u64;
            let delay = journal.runs[run_index].renew_at_ms.saturating_sub(now);
            if delay == 0 {
                if let Some(reply) = renew_while_polling(
                    service,
                    batch_id,
                    journal,
                    run_index,
                    provider.as_mut(),
                    started.load(std::sync::atomic::Ordering::SeqCst),
                )
                .await
                {
                    break reply;
                }
                continue;
            }
            tokio::select! {
                biased;
                reply = &mut provider => break reply,
                _ = tokio::time::sleep(std::time::Duration::from_millis(delay)) => {
                    if let Some(reply) = renew_while_polling(service, batch_id, journal, run_index, provider.as_mut(), started.load(std::sync::atomic::Ordering::SeqCst)).await {
                        break reply;
                    }
                }
            }
        };
        // The observation and its reset deadline must use the same refusal receipt time.
        let received_at_ms = (service.clock)();
        let mapped = if let super::ProviderReply::Credential(message)
        | super::ProviderReply::CredentialUnavailable(message) = &reply
        {
            Err(ItemError::for_code(
                errors::PROVIDER_ERROR,
                format!("credential read unavailable: {message}"),
            ))
        } else {
            classify::paid_answer(reply.clone(), pin)
        };
        let (spend, reported) =
            classify::error_spend(&reply, &journal.runs[run_index].choice.frozen);
        if mapped.is_err() && !matches!(spend, ItemSpend::Measured(_)) {
            journal.runs[run_index].unmeasured_attempt = true;
        }
        let retry_after = match &reply {
            super::ProviderReply::Status { retry_after_ms, .. }
            | super::ProviderReply::WorkersAiError { retry_after_ms, .. } => *retry_after_ms,
            super::ProviderReply::ClassifiedError { error, .. } => error.retry_after_ms,
            _ => None,
        };
        if mapped.as_ref().is_err_and(account_error) {
            journal.runs[run_index].reset_at_ms =
                retry_after.map(|delay| (received_at_ms.max(0) as u64).saturating_add(delay));
        }
        let run = &journal.runs[run_index];
        let event_id = format!("{}:{}:{attempt}", run.id, run.dispatched);
        let mut observation = json!({"event_id":event_id,"kind":"attempt","admission_id":run.observation_admission(),"run_id":run.id,"observed_at_ms":received_at_ms,"upstream_origin":"upstream"});
        if let Err(error) = &mapped {
            if error.code == errors::ACCOUNT_LIMIT_EXHAUSTED {
                observation["kind"] = json!("account_limit_exhausted");
                if let Some(reset) = run.reset_at_ms {
                    observation["resets_at_ms"] = json!(reset);
                    observation["resets_at_source"] = json!("retry_after");
                }
            }
            if (error.code == errors::ACCOUNT_NOT_ENTITLED
                || (error.code == errors::AUTH_FAILED && attempt > 1))
                && !matches!(
                    reply,
                    super::ProviderReply::Credential(_)
                        | super::ProviderReply::CredentialUnavailable(_)
                )
                && run.choice.describe.serves("observe_credential_refused")
            {
                observation["kind"] = json!("credential_refused");
                observation["credential_id"] = json!(
                    run.choice
                        .account
                        .as_ref()
                        .expect("admitted")
                        .account
                        .credential_id
                );
            }
            if matches!(
                reply,
                super::ProviderReply::Transport(_) | super::ProviderReply::Unconnected(_)
            ) {
                observation["terminal_reason"] = json!("transport_failure");
            }
        }
        if !matches!(
            reply,
            super::ProviderReply::Credential(_) | super::ProviderReply::CredentialUnavailable(_)
        ) {
            journal.observe(observation);
        }
        // Terminal answers and observations are committed together by the caller.
        if journal.runs[run_index].renew_failed
            || matches!(
                reply,
                super::ProviderReply::Credential(_)
                    | super::ProviderReply::CredentialUnavailable(_)
            )
        {
            return Ok(mapped.map_err(|error| classify::FailedAnswer {
                error,
                spend,
                reported,
            }));
        }
        if mapped
            .as_ref()
            .is_err_and(|error| error.code == errors::AUTH_FAILED)
            && attempt == 1
            && spend == ItemSpend::Nothing
        {
            save(service, batch_id, journal).await?;
            flush(service, batch_id, journal).await?;
            continue;
        }
        if let Some(wait) =
            classify::retry_wait(&reply, service).filter(|_| attempt < service.config.max_attempts)
        {
            save(service, batch_id, journal).await?;
            flush(service, batch_id, journal).await?;
            let deadline = tokio::time::Instant::now() + wait;
            loop {
                let now = (service.clock)().max(0) as u64;
                let delay = journal.runs[run_index].renew_at_ms.saturating_sub(now);
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => break,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(delay)) => {
                        if let Some(error) = renew(service, batch_id, journal, run_index).await? { return Ok(Err(unsent(error))); }
                    }
                }
            }
        } else {
            return Ok(mapped.map_err(|error| classify::FailedAnswer {
                error,
                spend,
                reported,
            }));
        }
    }
    unreachable!("the final provider attempt returns")
}

fn renew_at(now_ms: u64, expiry_ms: u64) -> u64 {
    let remaining = expiry_ms.saturating_sub(now_ms);
    now_ms.saturating_add(remaining.saturating_sub(30_000.min(remaining / 3)))
}

#[cfg(test)]
mod refusal_tests {
    use super::*;

    #[test]
    fn expired_cleanup_remains_pending_after_report_acknowledgement() {
        let mut journal = Journal {
            expired: true,
            ..Default::default()
        };
        journal.report(
            ReportRequest::new("decision", ReportOutcome::LaunchFailed)
                .with_runner_code("invalid_params"),
            vec![],
        );
        journal.outbox[0].closed = true;
        assert!(
            journal.pending(),
            "recovery must still find the row until archive_expired permits its deletion"
        );
    }

    #[test]
    fn answered_request_contract_refusals_preserve_routing_code() {
        for code in ["invalid_request", "admission_conflict"] {
            let bytes = routing_error(
                RoutingCallError::Refused {
                    code: code.into(),
                    message: "the runner violated the request contract".into(),
                    detail: json!({}),
                },
                Some("typesafe/jev-1.13.0"),
                0,
            )
            .bytes();
            let refusal: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(refusal["code"], "routing_refused", "{code}");
            assert_eq!(refusal["detail"]["class"], "permanent", "{code}");
            assert_eq!(refusal["detail"]["routing_code"], code);
        }
    }
}

/// A renewal error must not cancel a provider future that may already have spent.
async fn renew_during_provider(
    service: &Arc<SystemOne>,
    batch_id: &str,
    journal: &mut Journal,
    index: usize,
) {
    if renew(service, batch_id, journal, index).await.is_err() {
        let run = &mut journal.runs[index];
        run.renewing = false;
        run.renew_failed = true;
        run.error.get_or_insert_with(|| {
            ItemError::for_code(
                errors::PROVIDER_ERROR,
                "account lease renewal could not finish locally",
            )
        });
    }
}

async fn renew_while_polling<F: std::future::Future<Output = super::ProviderReply>>(
    service: &Arc<SystemOne>,
    batch_id: &str,
    journal: &mut Journal,
    index: usize,
    mut provider: std::pin::Pin<&mut F>,
    started: bool,
) -> Option<super::ProviderReply> {
    let renewal = renew_during_provider(service, batch_id, journal, index);
    tokio::pin!(renewal);
    // A ready response must be consumed before an adapter's deadline can win. Do not
    // start a request still waiting for its model permit until the renewal is known.
    tokio::select! {
        biased;
        reply = &mut provider, if started => {
            renewal.await;
            Some(reply)
        }
        () = &mut renewal => None,
    }
}

async fn renew(
    service: &Arc<SystemOne>,
    batch_id: &str,
    journal: &mut Journal,
    index: usize,
) -> Result<Option<ItemError>, Error> {
    use cortexkit_role_routing::account::{
        AccountForRequest, AccountMode, ExpectedPin, RequestedAccount,
    };
    let run = &journal.runs[index];
    let account = run
        .choice
        .account
        .as_ref()
        .expect("only admitted runs renew");
    let mut request = AccountForRequest::new(
        &run.choice.select.route.provider,
        &run.choice.select.route.model,
        batch_id,
        format!("{}:renew:{}", run.id, run.renewals + 1),
    )
    .with_mode(AccountMode::Renew)
    .with_expected_pin(ExpectedPin {
        id: account.pin.id.clone(),
        generation: account.pin.generation,
    })
    .with_decision_id(&run.choice.select.decision_id)
    .with_requested_account(RequestedAccount {
        credential_id: Some(account.account.credential_id.clone()),
        account_id: Some(account.account.account_id.clone()),
    });
    if let Some(pool) = run
        .choice
        .select
        .runner
        .auth
        .as_ref()
        .and_then(|auth| auth.pool.as_ref())
    {
        request = request.with_pool(pool);
    }
    journal.runs[index].renewing = true;
    save(service, batch_id, journal).await?;
    let client = &service
        .routing
        .as_ref()
        .ok_or_else(|| unavailable("routing is not configured"))?
        .client;
    let mut answer = client.renew_account(&request).await;
    if matches!(answer, Err(RoutingCallError::Unavailable(_))) {
        answer = client.renew_account(&request).await;
    }
    let run = &mut journal.runs[index];
    let previous = run.choice.account.as_ref().expect("admitted");
    let now = (service.clock)().max(0) as u64;
    let answered_admission = answer
        .as_ref()
        .ok()
        .map(|answer| answer.admission.id.clone());
    let answer = answer.and_then(|answer| {
        if answer.admission.id != request.admission_id
            || answer.reason.decision_id != run.choice.select.decision_id
            || answer.account != previous.account
            || answer.pin.id != previous.pin.id
            || answer.pin.generation < previous.pin.generation
            || answer.pin.mode != AccountMode::Renew
        {
            Err(RoutingCallError::Invalid(
                "routing renewal changed the frozen account or returned an expired pin".into(),
            ))
        } else if answer.pin.lease_valid_until_ms <= now {
            Err(RoutingCallError::Unavailable(
                "renewed lease expired before it could be used".into(),
            ))
        } else {
            Ok(answer)
        }
    });
    run.renewing = false;
    let failed = match answer {
        Ok(answer) => {
            run.renew_at_ms = renew_at(now, answer.pin.lease_valid_until_ms);
            run.choice.account = Some(answer);
            run.renewals += 1;
            None
        }
        Err(error) => {
            run.renew_failed = true;
            let message = match &error {
                RoutingCallError::Refused { .. } => "routing refused the batch".into(),
                RoutingCallError::Unavailable(message) => {
                    format!("routing renewal unavailable: {message}")
                }
                RoutingCallError::Invalid(message) => {
                    format!("routing renewal contract error: {message}")
                }
            };
            let pin = matches!(
                &run.choice.select.pin_outcome,
                Some(cortexkit_role_routing::select::PinOutcome::Pinned)
            )
            .then_some(run.choice.model.as_str());
            let refusal = routing_error(error, pin, now);
            let item_error = match &refusal {
                Error::Routing(refusal) => refusal.as_ref().clone().into_item_error(),
                _ => None,
            };
            run.renewal_refusal =
                Some(serde_json::from_slice(&refusal.bytes()).expect("refusal JSON"));
            // The request may have committed even when its answer was lost or invalid.
            // Finish the original admission first, then this possibly granted renewal.
            run.cleanup_admissions.push(request.admission_id.clone());
            if let Some(id) = answered_admission.filter(|id| *id != request.admission_id) {
                run.cleanup_admissions.push(id);
            }
            let error =
                item_error.unwrap_or_else(|| ItemError::for_code(errors::PROVIDER_ERROR, message));
            run.note_error(error.clone());
            Some(error)
        }
    };
    save(service, batch_id, journal).await?;
    Ok(failed)
}

#[cfg(test)]
mod renewal_schedule_tests {
    #[test]
    fn routed_renewal_schedule_short_and_long_leases() {
        assert_eq!(super::renew_at(1_000_000, 1_012_000), 1_008_000);
        assert_eq!(super::renew_at(1_000_000, 1_600_000), 1_570_000);
    }
}
