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

#[async_trait]
impl RoutingCredentials for VaultRoutingCredentials {
    async fn candidates(&self, provider: &str) -> Result<Candidates, String> {
        let listing = self
            .0
            .list_scoped()
            .await
            .map_err(|error| error.to_string())?;
        Ok(Candidates::api_keys(&listing, provider))
    }

    fn source(&self, credential_id: &str) -> Arc<dyn CredentialSource> {
        Arc::new(VaultCredential {
            client: self.0.clone(),
            credential_id: credential_id.into(),
            vault_id: credential_id.into(),
        })
    }
}

pub(super) enum Error {
    Classifier(Refusal),
    Routing(Box<RoutingRefusal>),
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
        }
    }
}

fn unavailable(message: impl Into<String>) -> Error {
    Error::Routing(Box::new(RoutingRefusal::unavailable(message)))
}
fn routing_error(error: RoutingCallError, pin: Option<&str>) -> Error {
    match error {
        RoutingCallError::Unavailable(message) => unavailable(message),
        RoutingCallError::Refused { code, message } => Error::Routing(Box::new(
            RoutingRefusal::refused(code, Some(message), pin.map(str::to_owned)),
        )),
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Choice {
    select: SelectAnswer,
    model: String,
    frozen: FrozenRow,
    entry: Option<cortexkit_role_classifier::describe::ModelEntry>,
    describe: RoleDescribe,
    account: Option<AccountForAnswer>,
    refusal: Option<RoutingRefusal>,
}

#[derive(Default, Serialize, Deserialize)]
#[serde(default)]
struct Journal {
    reselected: bool,
    reselection: Option<Reselection>,
    admission_refusal: Option<RoutingRefusal>,
    admission_refusal_pending: bool,
    runs: Vec<Run>,
    outbox: Vec<Rpc>,
}

#[derive(Default, Serialize, Deserialize)]
struct Reselection {
    choice: Option<Choice>,
    refusal: Option<Value>,
    finished: bool,
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
}

#[derive(Serialize, Deserialize)]
struct Rpc {
    method: String,
    params: Value,
    /// Closed reports stay in the journal, so recovery never synthesizes a replacement body.
    closed: bool,
    conflict: bool,
    capabilities: Vec<String>,
}

impl Journal {
    fn pending(&self) -> bool {
        self.admission_refusal_pending
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
            method: "route.account_observe".into(),
            params,
            closed: false,
            conflict: false,
            capabilities,
        });
    }

    fn report(&mut self, body: ReportRequest, capabilities: Vec<String>) {
        self.outbox.push(Rpc {
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

impl SystemOne {
    pub fn with_routing(mut self, routing: Routing) -> Self {
        self.routing = Some(routing);
        self
    }

    pub(super) async fn routing_ready(&self) -> Result<RoleDescribe, Error> {
        let routing = self
            .routing
            .as_ref()
            .ok_or_else(|| unavailable("routing is not configured"))?;
        let describe = routing
            .client
            .classifier_describe()
            .await
            .map_err(|error| match error {
                RoutingCallError::Unavailable(message) => unavailable(message),
                RoutingCallError::Refused { .. } => unavailable("routing is not ready"),
            })?;
        if describe.consumer("classifier").is_none() {
            return Err(unavailable("routing does not serve consumer classifier"));
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
    let mut requirements = Requirements::iq(0);
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
) -> Result<Choice, Error> {
    let describe = service.routing_ready().await?;
    let routing = service
        .routing
        .as_ref()
        .expect("readiness checked configuration");
    let mut query = SelectRequest::new("classifier", requirements(request, estimates, &describe))
        .with_runners(vec![routing.module_id.clone()]);
    if let Some(pin) = &request.model {
        query = query.with_pin(pin);
    }
    let answer = routing
        .client
        .classifier_select(&query)
        .await
        .map_err(|error| routing_error(error, request.model.as_deref()))?;
    let model = answer.runner.model.route_key();
    let refusal = if service.pin(&model).is_none()
        || request.model.as_ref().is_some_and(|pin| pin != &model)
    {
        let message =
            format!("routing selected {model}, which broca cannot serve for this request");
        let mut refusal = RoutingRefusal::refused(
            "model_unavailable",
            Some(message.clone()),
            request.model.clone(),
        );
        refusal.message = message;
        Some(refusal)
    } else {
        None
    };
    let row = service.catalog.row(&model);
    if refusal.is_none() {
        cortexkit_role_classifier::request::check_catalog_row(
            &model,
            classify::catalog_row(row.as_ref()),
        )?;
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

async fn admit(
    service: &SystemOne,
    choice: &Choice,
    id: &str,
    batch_id: &str,
    initial_input_bytes: u64,
    pin: Option<&str>,
) -> Result<AccountForAnswer, Error> {
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
    });
    // New operations use the attested route principal, never the legacy caller hint.
    params
        .as_object_mut()
        .expect("account params")
        .remove("caller");
    let request = cortexkit_role_routing::account::parse_account_for_request(&params)
        .map_err(|error| internal_refusal(&format!("invalid account admission: {error:?}")))?;
    let answer: AccountForAnswer = routing
        .client
        .classifier_rpc("route.account_for", &request)
        .await
        .map_err(|error| routing_error(error, pin))?;
    if answer.admission.id != id
        || answer.reason.decision_id != choice.select.decision_id
        || answer.account.provider != choice.select.route.provider
        || answer.account.method != "apikey"
        || !candidates.offers(&answer.account.account_id, &answer.account.credential_id)
        || credential.is_some_and(|credential| credential != answer.account.credential_id)
    {
        return Err(unavailable(
            "routing returned an account outside the frozen choice or admitted candidates",
        ));
    }
    Ok(answer)
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
            account_error(&error) || !error.is_retried_on_resend()
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
            .filter(|outcome| matches!(outcome.outcome, Outcome::Answered(_)))
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
            .find_map(|outcome| match &outcome.outcome {
                Outcome::Failed(body) => serde_json::from_str::<ItemError>(body).ok(),
                _ => None,
            })
    });
    let error = if run.choice.describe.serves("report_usage_run_id") {
        run.error.as_ref()
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
    for index in 0..journal.outbox.len() {
        let rpc = &journal.outbox[index];
        if rpc.closed || rpc.conflict {
            continue;
        }
        let result = if rpc.method == "route.report" {
            let request: ReportRequest = serde_json::from_value(rpc.params.clone())
                .map_err(|_| internal_refusal("a stored routing report does not decode"))?;
            routing
                .client
                .classifier_report(&request, &rpc.capabilities)
                .await
                .map(|_| ())
        } else {
            let request = cortexkit_role_routing::observe::parse_account_observe_request_serving(
                &rpc.params,
                &rpc.capabilities,
            )
            .map_err(|error| {
                internal_refusal(&format!("stored account observation is invalid: {error:?}"))
            })?;
            routing
                .client
                .classifier_rpc::<cortexkit_role_routing::observe::AccountObserveAnswer>(
                    &rpc.method,
                    &request,
                )
                .await
                .map(|_| ())
        };
        match result {
            Ok(()) => journal.outbox[index].closed = true,
            Err(RoutingCallError::Refused { code, message }) if code == "unknown_decision" => {
                tracing::warn!(target: "system_one", %code, %message, "closing routing report for an unknown decision");
                journal.outbox[index].closed = true;
            }
            Err(RoutingCallError::Refused { code, message }) if code == "report_conflict" => {
                tracing::error!(target: "system_one", %code, %message, "conflicting routing report retained for investigation; it will not be retried");
                journal.outbox[index].conflict = true;
            }
            Err(error) => {
                tracing::warn!(target: "system_one", ?error, "routing outbox delivery will be retried")
            }
        }
        save(service, batch_id, journal).await?;
    }
    Ok(())
}

async fn finish_run(
    service: &Arc<SystemOne>,
    batch_id: &str,
    journal: &mut Journal,
    index: usize,
    final_batch: bool,
) -> Result<(), Error> {
    if !journal.runs[index].finished {
        journal.runs[index].finished = true;
        let run = &journal.runs[index];
        if let Some(account) = &run.choice.account {
            journal.observe(account_routing::run_finished_body(
                &format!("{}:finished", run.id),
                &account.admission.id,
                &run.id,
                if run.error.is_some() {
                    broca_protocol::RunFinishReason::Error
                } else {
                    broca_protocol::RunFinishReason::Completed
                },
                None,
            ));
        }
    }
    let run = &journal.runs[index];
    if !run.report_queued
        && journal.admission_refusal.is_none()
        && (run.choice.describe.serves("report_usage_run_id") || final_batch)
    {
        let already_reported = !run.choice.describe.serves("report_usage_run_id")
            && journal.outbox.iter().any(|rpc| {
                rpc.method == "route.report"
                    && rpc.params["decision_id"] == run.choice.select.decision_id
            });
        let batch = service
            .stored_batch(batch_id)
            .await
            .map_err(|_| internal_refusal("routing usage recovery is unavailable"))?;
        let decision_measured = journal
            .runs
            .iter()
            .filter(|other| other.choice.select.decision_id == run.choice.select.decision_id)
            .all(|other| !other.unmeasured_attempt && other.input.len() == other.dispatched);
        let report = finished_report(run, batch.as_ref(), decision_measured);
        journal.runs[index].report_queued = true;
        let capabilities = journal.runs[index].choice.describe.capabilities.clone();
        if !already_reported {
            journal.report(report, capabilities);
        }
    }
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
    let batch = service
        .stored_batch(batch_id)
        .await
        .map_err(|_| internal_refusal("routing recovery is unavailable"))?;
    let final_batch = batch.as_ref().is_some_and(|batch| {
        let identity: BatchIdentity = match serde_json::from_str(&batch.identity) {
            Ok(identity) => identity,
            Err(_) => return false,
        };
        batch.outcomes.len() == identity.items.len() && batch.outcomes.iter().all(retained)
    });
    for index in 0..journal.runs.len() {
        if journal.runs[index].renewing {
            let _ = renew(service, batch_id, journal, index).await?;
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
        finish_run(service, batch_id, journal, index, final_batch).await?;
    }
    if journal.runs.is_empty() && journal.outbox.is_empty() {
        save(service, batch_id, journal).await?;
    }
    flush(service, batch_id, journal).await
}

impl SystemOne {
    /// Retry durable reports outside request replay. A fully stored reply performs no
    /// routing lookup, even if a previous acknowledgement was lost.
    pub async fn recover_routing(self: &Arc<Self>) -> Result<(), String> {
        if self.routing.is_none() {
            return Ok(());
        }
        let service = self.clone();
        let ids = classify::blocking(move || service.store.pending_routing_batches(&service.role))
            .await
            .map_err(|error| error.message)?;
        for batch_id in ids {
            let Some(batch) = self.stored_batch(&batch_id).await? else {
                continue;
            };
            let Some(scope) = batch.scope.clone() else {
                continue;
            };
            let service = self.clone();
            let id = batch_id.clone();
            let claimed = classify::blocking(move || {
                service.store.claim_routed(
                    &BatchClaim {
                        role: &service.role,
                        batch_id: &id,
                        model: &batch.model,
                        identity: &batch.identity,
                        body_digest: &batch.args_digest,
                        frozen_row: &batch.frozen_row,
                        scope: &scope,
                        ceiling_nanos: None,
                        owner: &service.owner,
                        now_ms: (service.clock)(),
                    },
                    batch.admitted_at_ms,
                )
            })
            .await
            .map_err(|error| error.message)?;
            if !matches!(claimed, Claimed::Resumed(_)) {
                continue;
            }
            let hold = Hold {
                service: self,
                batch_id: batch_id.clone(),
                stored: true,
            };
            let result = async {
                let service = self.clone();
                let id = batch_id.clone();
                if let Some(record) =
                    classify::blocking(move || service.store.routing(&service.role, &id)).await?
                {
                    let mut journal: Journal = serde_json::from_str(&record.runs)
                        .map_err(|_| internal_refusal("routing journal does not decode"))?;
                    recover_claimed(self, &batch_id, &mut journal).await?;
                }
                Ok::<_, Error>(())
            }
            .await;
            hold.release().await;
            result.map_err(|error| String::from_utf8_lossy(&error.bytes()).into_owned())?;
        }
        Ok(())
    }
}

pub(super) async fn run(
    service: &Arc<SystemOne>,
    params: &str,
    scope: &BatchScope,
    admitted_at_ms: Option<i64>,
    prepared: &super::images::PreparedImages,
) -> Result<Option<Box<RawValue>>, Error> {
    cortexkit_role_classifier::request::check_request_bytes(
        params.len(),
        &cortexkit_role_classifier::describe::Limits::new(
            service.config.max_items,
            service.config.max_request_bytes,
        ),
    )?;
    let request = parse_request(params)?;
    flush_facts(service).await?;
    let snapshot = service
        .stored_batch(&request.batch_id)
        .await
        .map_err(|_| internal_refusal("batch replay is unavailable"))?;
    let snapshot = snapshot.filter(|batch| {
        (service.clock)().saturating_sub(batch.admitted_at_ms)
            < broca_store::system_one::RETENTION_MS
    });
    if snapshot.is_some() {
        let (store, role, id) = (
            service.store.clone(),
            service.role.clone(),
            request.batch_id.clone(),
        );
        if classify::blocking(move || store.routing(&role, &id))
            .await?
            .is_none()
        {
            return Ok(None);
        }
    }
    let identity =
        BatchIdentity::of(&request).map_err(|error| Refusal::invalid_params("items", error.0))?;
    let identity_json = serde_json::to_string(&identity).expect("identity encodes");
    let digest = classify::sha256_hex(&identity_json);
    // Shape validation is independent of routing. Model-specific bounds are checked against
    // the selected entry before any provider request.
    service.check_images_request(&request, &validation_entry(service))?;
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
        let stored_outcomes = stored
            .as_ref()
            .map_or_else(Vec::new, |record| record.outcomes.clone());
        if let Some(choice) = &choice {
            if let Some(refusal) = &choice.refusal {
                return Err(Error::Routing(Box::new(refusal.clone())));
            }
            if stored_outcomes.len() == request.items.len()
                && stored_outcomes.iter().all(retained)
                && !pending_reselection(&journal)
                && !journal.admission_refusal_pending
                && !reselection_due(
                    &journal,
                    stored_outcomes
                        .iter()
                        .any(|outcome| matches!(outcome.outcome, Outcome::Answered(_))),
                )
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
                    false,
                    Some(&choice.select.decision_id),
                )
                .map(Some)
                .map_err(Into::into);
            }
        }
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
        )
        .await;
        if answer.is_err() && fresh && journal.runs.is_empty() && journal.outbox.is_empty() {
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

/// Disabling new routed admission cannot turn an existing decision into a fixed-key call.
/// The choice and outcomes never change here; an unfinished refusal performs no writes.
pub(super) async fn replay_when_disabled(
    service: &Arc<SystemOne>,
    params: &str,
    scope: &BatchScope,
) -> Result<Option<Box<RawValue>>, Error> {
    #[derive(Deserialize)]
    struct Probe {
        batch_id: String,
    }
    let Ok(probe) = serde_json::from_str::<Probe>(params) else {
        return Ok(None);
    };
    let Some(batch) = service
        .stored_batch(&probe.batch_id)
        .await
        .map_err(|_| internal_refusal("batch replay is unavailable"))?
    else {
        return Ok(None);
    };
    let (store, role, id) = (
        service.store.clone(),
        service.role.clone(),
        probe.batch_id.to_owned(),
    );
    let Some(record) = classify::blocking(move || store.routing(&role, &id)).await? else {
        return Ok(None);
    };
    cortexkit_role_classifier::request::check_request_bytes(
        params.len(),
        &cortexkit_role_classifier::describe::Limits::new(
            service.config.max_items,
            service.config.max_request_bytes,
        ),
    )?;
    let request = parse_request(params)?;
    if batch.scope.as_ref() != Some(scope) {
        return Err(Refusal::new(
            "batch_scope_mismatch",
            "batch_id belongs to a different scope; a retry cannot change its attribution",
        )
        .into());
    }
    let identity =
        BatchIdentity::of(&request).map_err(|error| Refusal::invalid_params("items", error.0))?;
    let recorded: BatchIdentity = serde_json::from_str(&batch.identity)
        .map_err(|_| internal_refusal("stored identity does not decode"))?;
    if let Some(field) = recorded.first_difference(&identity) {
        return Err(Refusal::batch_id_reuse(field).into());
    }
    let (store, role, id) = (
        service.store.clone(),
        service.role.clone(),
        probe.batch_id.to_owned(),
    );
    if classify::blocking(move || store.routing_batch_running(&role, &id)).await? {
        return Err(Refusal::batch_in_progress(super::BATCH_IN_PROGRESS_RETRY_AFTER_MS).into());
    }
    let choice: Option<Choice> = serde_json::from_str(&record.choice)
        .map_err(|_| internal_refusal("stored routing choice does not decode"))?;
    let journal: Journal = serde_json::from_str(&record.runs)
        .map_err(|_| internal_refusal("stored routing journal does not decode"))?;
    if let Some(choice) = choice.filter(|_| {
        batch.outcomes.len() == request.items.len()
            && batch.outcomes.iter().all(retained)
            && !pending_reselection(&journal)
            && !journal.admission_refusal_pending
            && !reselection_due(
                &journal,
                batch
                    .outcomes
                    .iter()
                    .any(|outcome| matches!(outcome.outcome, Outcome::Answered(_))),
            )
    }) {
        flush_facts(service).await?;
        let mut answered = request;
        answered.model = Some(choice.model);
        return classify::reply_decision(
            &answered,
            &choice.frozen,
            batch
                .outcomes
                .into_iter()
                .map(|outcome| Some(Slot::Stored(outcome)))
                .collect(),
            false,
            Some(&choice.select.decision_id),
        )
        .map(Some)
        .map_err(Into::into);
    }
    Err(unavailable("routed models are switched off on this runner"))
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
) -> Result<Box<RawValue>, Error> {
    let raw: RawParams<'_> = serde_json::from_str(params)
        .map_err(|_| Refusal::invalid_params("params", "request did not decode"))?;
    let mut estimates = estimates(request)?;
    let mut choice = match choice {
        Some(choice) => choice,
        None => select(service, request, &estimates).await?,
    };
    let mut slots: Vec<Option<Slot>> = (0..request.items.len()).map(|_| None).collect();
    for outcome in stored {
        if retained(&outcome) {
            let index = outcome.index as usize;
            if index < slots.len() {
                slots[index] = Some(Slot::Stored(outcome));
            }
        }
    }
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
    let replacement = if pending_reselection(journal) {
        journal
            .reselection
            .as_ref()
            .and_then(|selection| selection.choice.clone())
    } else if reselection_due(journal, any_answer) {
        let replacement = reselect(service, request, &estimates, journal).await?;
        if replacement.is_none() {
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
        replacement
    } else {
        None
    };
    if let Some(replacement) = replacement {
        previous_choice = Some(choice);
        choice = replacement;
        slots.iter_mut().for_each(|slot| *slot = None);
    } else if let Some(error) = journal
        .runs
        .last()
        .filter(|run| run.choice.select.decision_id == choice.select.decision_id)
        .and_then(|run| run.error.as_ref())
        .filter(|error| account_error(error))
        .cloned()
    {
        // A process can stop between committing the account failure and recording the
        // remaining failures. That gap does not permit another admission or provider send.
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
            if let Some(selection) = &mut journal.reselection {
                selection.finished = true;
            }
            let outcome = if choice.describe.serves("report_outcomes") {
                ReportOutcome::LaunchFailed
            } else {
                ReportOutcome::Cancelled
            };
            journal.report(
                ReportRequest::new(&choice.select.decision_id, outcome)
                    .with_runner_code("model_unavailable"),
                choice.describe.capabilities.clone(),
            );
            freeze(service, &request.batch_id, &choice, journal).await?;
            flush(service, &request.batch_id, journal).await?;
            return Err(Error::Routing(Box::new(refusal.clone())));
        }
        let entry = choice
            .entry
            .as_ref()
            .ok_or_else(|| internal_refusal("selected model entry is missing"))?;
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
                cortexkit_role_classifier::request::check_resolved_item_images(
                    entry, index, &decoded,
                )?;
            }
        }
        reprice(&mut estimates, &choice.frozen);
        if let Some(ceiling) = ceiling {
            let wanted = slots
                .iter()
                .zip(&estimates)
                .filter(|(slot, _)| slot.is_none())
                .fold(0u64, |total, (_, estimate)| {
                    total.saturating_add(estimate.cost_nanos)
                });
            if wanted > ceiling {
                return Err(
                    Refusal::cost_exceeded(nanos_to_usd(wanted), nanos_to_usd(ceiling)).into(),
                );
            }
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
                        outcome: Outcome::Failed(classify::item_error_json(&error)),
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
                    false,
                    Some(&choice.select.decision_id),
                )
                .map_err(Into::into);
            }
        }
        if choice.account.is_some() {
            choice.describe = service.routing_ready().await?;
        }
        let id = token()?;
        let account = match admit(
            service,
            &choice,
            &id,
            &request.batch_id,
            params.len() as u64,
            request.model.as_deref(),
        )
        .await
        {
            Ok(account) => account,
            Err(Error::Routing(refusal))
                if refusal.code == errors::ROUTING_REFUSED
                    && (choice.account.is_some() || previous_choice.is_some()) =>
            {
                return refuse_admission(
                    service,
                    request,
                    previous_choice.as_ref().unwrap_or(&choice),
                    journal,
                    slots,
                    *refusal,
                )
                .await;
            }
            Err(error) => return Err(error),
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
        let any_answer = slots.iter().any(|slot| {
            matches!(
                slot,
                Some(Slot::Stored(StoredOutcome {
                    outcome: Outcome::Answered(_),
                    ..
                }))
            )
        });
        let should_reselect = failure.as_ref().is_some_and(|error| {
            matches!(
                error.code.as_str(),
                errors::AUTH_FAILED | errors::ACCOUNT_NOT_ENTITLED
            )
        }) && !any_answer
            && !journal.reselected
            && !journal.runs[run_index].renew_failed;
        let final_batch = slots.iter().all(|slot| match slot {
            Some(Slot::Stored(outcome)) => retained(outcome),
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
            if let Some(selected) = reselect(service, request, &estimates, journal).await? {
                previous_choice = Some(choice);
                choice = selected;
                slots.iter_mut().for_each(|slot| *slot = None);
                continue;
            }
        }
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
}

async fn reselect(
    service: &Arc<SystemOne>,
    request: &ClassifyRequest,
    estimates: &[Estimate],
    journal: &mut Journal,
) -> Result<Option<Choice>, Error> {
    // Spending the allowance precedes the RPC. A lost selection answer never authorizes
    // another select, while a durable answer can finish account admission after restart.
    journal.reselected = true;
    journal.reselection = Some(Reselection::default());
    save(service, &request.batch_id, journal).await?;
    let choice = match select(service, request, estimates).await {
        Ok(choice) => {
            journal
                .reselection
                .as_mut()
                .expect("reselection exists")
                .choice = Some(choice.clone());
            Some(choice)
        }
        Err(error) => {
            let selection = journal.reselection.as_mut().expect("reselection exists");
            selection.refusal = Some(serde_json::from_slice(&error.bytes()).expect("refusal JSON"));
            selection.finished = true;
            None
        }
    };
    save(service, &request.batch_id, journal).await?;
    Ok(choice)
}

async fn refuse_admission(
    service: &Arc<SystemOne>,
    request: &ClassifyRequest,
    choice: &Choice,
    journal: &mut Journal,
    mut slots: Vec<Option<Slot>>,
    refusal: RoutingRefusal,
) -> Result<Box<RawValue>, Error> {
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
    let mut stop: Option<ItemError> = None;
    for index in 0..request.items.len() {
        if slots[index].is_some() {
            continue;
        }
        if let Some(error) = &stop {
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
                outcome: Outcome::Failed(classify::item_error_json(&error)),
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
        journal.runs[run_index].dispatched += 1;
        save(service, &request.batch_id, journal).await?;
        let answer =
            provider_call(service, &request.batch_id, pin, body, journal, run_index).await?;
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
                run.costs.push(classify::batch_cost(
                    &choice.frozen,
                    &[(input_tokens, output_tokens)],
                ));
                let fact = service
                    .export
                    .as_ref()
                    .zip(super::catalog_key_parts(&choice.model))
                    .map(|(export, (provider, model))| {
                        use broca_core::ChargeBasisResolver;
                        let method = auth_selection
                            .as_deref()
                            .and_then(|id| id.split(':').next())
                            .unwrap_or("apikey");
                        let (charge_basis, charge_basis_mapping_version) =
                            service.charge_basis.resolve_charge_basis(provider, method);
                        broca_store::system_one::FactContext {
                            source_instance_id: export.source_instance_id().into(),
                            provider: provider.into(),
                            model: model.into(),
                            auth_selection,
                            account_ref,
                            charge_basis,
                            charge_basis_mapping_version,
                            charge_basis_overrides: service.charge_basis.charge_basis_overrides(),
                        }
                    });
                *spent = spent.saturating_add(cost);
                (
                    StoredOutcome {
                        index: index as u64,
                        outcome: Outcome::Answered(answers),
                        input_tokens,
                        output_tokens,
                    },
                    cost,
                    fact,
                )
            }
            Err(error) => {
                journal.runs[run_index]
                    .error
                    .get_or_insert_with(|| error.clone());
                if account_error(&error) || error.stops_the_call() {
                    stop = Some(error.clone());
                }
                (
                    StoredOutcome {
                        index: index as u64,
                        outcome: Outcome::Failed(classify::item_error_json(&error)),
                        input_tokens: None,
                        output_tokens: None,
                    },
                    0,
                    None,
                )
            }
        };
        // Account failures are stored too. Unlike a legacy batch, a routed batch cannot
        // discard the account that failed and silently choose another one on a resend.
        record(service, &request.batch_id, &outcome, cost, fact, journal).await?;
        slots[index] = Some(Slot::Stored(outcome));
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
) -> Result<Result<classify::PaidAnswer, ItemError>, Error> {
    let slot = service.model_slot(&pin.catalog_id);
    for attempt in 1..=service.config.max_attempts.max(2) {
        let provider = async {
            let _permit = slot.acquire().await;
            pin.provider.call(body.clone()).await
        };
        tokio::pin!(provider);
        let reply = loop {
            let now = (service.clock)().max(0) as u64;
            let delay = journal.runs[run_index].renew_at_ms.saturating_sub(now);
            if delay == 0 {
                if let Some(error) = renew(service, batch_id, journal, run_index).await? {
                    return Ok(Err(error));
                }
                continue;
            }
            tokio::select! {
                biased;
                reply = &mut provider => break reply,
                _ = tokio::time::sleep(std::time::Duration::from_millis(delay)) => {
                    if let Some(error) = renew(service, batch_id, journal, run_index).await? { return Ok(Err(error)); }
                }
            }
        };
        let mapped = classify::map_reply(reply.clone(), pin);
        if mapped.is_err() {
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
                retry_after.map(|delay| ((service.clock)().max(0) as u64).saturating_add(delay));
        }
        let run = &journal.runs[run_index];
        let event_id = format!("{}:{}:{attempt}", run.id, run.dispatched);
        let mut observation = json!({"event_id":event_id,"kind":"attempt","admission_id":run.choice.account.as_ref().expect("admitted").admission.id,"run_id":run.id,"observed_at_ms":(service.clock)(),"upstream_origin":"upstream"});
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
                && !matches!(reply, super::ProviderReply::Credential(_))
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
        journal.observe(observation);
        save(service, batch_id, journal).await?;
        flush(service, batch_id, journal).await?;
        if mapped
            .as_ref()
            .is_err_and(|error| error.code == errors::AUTH_FAILED)
            && attempt == 1
        {
            continue;
        }
        if let Some(wait) =
            classify::retry_wait(&reply, service).filter(|_| attempt < service.config.max_attempts)
        {
            let deadline = tokio::time::Instant::now() + wait;
            loop {
                let now = (service.clock)().max(0) as u64;
                let delay = journal.runs[run_index].renew_at_ms.saturating_sub(now);
                tokio::select! {
                    _ = tokio::time::sleep_until(deadline) => break,
                    _ = tokio::time::sleep(std::time::Duration::from_millis(delay)) => {
                        if let Some(error) = renew(service, batch_id, journal, run_index).await? { return Ok(Err(error)); }
                    }
                }
            }
        } else {
            return Ok(classify::paid_answer(reply, pin));
        }
    }
    unreachable!("the final provider attempt returns")
}

fn renew_at(now_ms: u64, expiry_ms: u64) -> u64 {
    let remaining = expiry_ms.saturating_sub(now_ms);
    now_ms.saturating_add(remaining.saturating_sub(30_000.min(remaining / 3)))
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
    let answer = answer.and_then(|answer| {
        if answer.admission.id != request.admission_id
            || answer.reason.decision_id != run.choice.select.decision_id
            || answer.account != previous.account
            || answer.pin.id != previous.pin.id
            || answer.pin.generation < previous.pin.generation
            || answer.pin.mode != AccountMode::Renew
            || answer.pin.lease_valid_until_ms <= now
        {
            Err(RoutingCallError::Unavailable(
                "routing renewal changed the frozen account or returned an expired pin".into(),
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
            let message = match error {
                RoutingCallError::Unavailable(message) => message,
                RoutingCallError::Refused { code, message } => format!("{code}: {message}"),
            };
            let error = ItemError::for_code(
                errors::AUTH_FAILED,
                format!("account lease renewal failed: {message}"),
            );
            run.error = Some(error.clone());
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
