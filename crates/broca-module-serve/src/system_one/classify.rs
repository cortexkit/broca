//! One `classify.run` call: validation, the batch hold, each item's provider call, and the
//! reply. The rules are `classifier/v1`'s `CONTRACT.md` and `cortexkit-model-batch`'s
//! `RULES.md`; the section numbers below are RULES'.

// An item's error is the role's own `ItemError`, returned once per provider call and stored or
// answered as it is; boxing it would buy nothing on a path that makes a network call each time.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use broca_catalog::ModelBatchRow;
use broca_store::system_one::{BatchClaim, Claimed, Outcome, StoredOutcome};
use broca_store::SystemOneStore;
use cortexkit_model_batch::{
    absent::{add_reported, input_cost_nanos},
    estimate::{
        cost_nanos, crosses_ceiling, estimate_item, nanos_to_usd, usd_to_nanos, Estimate,
        ItemCostInput, Rounding, Tokens,
    },
};
use cortexkit_role_classifier::{
    errors::{self, ItemError, Refusal},
    identity::BatchIdentity,
    provider::{item_code_for_status, workers_ai_item_code},
    reply::Usage,
    request::{
        check_catalog_row, check_request, check_request_bytes, parse_request, CatalogRow,
        ClassifyRequest, CLASSIFIER_KIND,
    },
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use super::{internal_refusal, ModelPin, ProviderReply, SystemOne, ToolAdmission};

/// The catalog kind fusiform gives a decision model. `classifier/v1` calls the same kind
/// `classifier`; a row stating either is a classifier.
pub(crate) const DECISION_KIND: &str = "decision";

/// A bound on the tokens TypeSafe adds to each request around the state and questions (its
/// prompt template). TypeSafe documents none; the bound only has to be at least the real
/// figure, since it serves the spend ceiling, never the reported cost.
const OVERHEAD_TOKENS: u64 = 512;

/// No pinned model takes images yet; a bound is still required by the estimate.
const IMAGE_TOKENS_EACH: u64 = 4_096;

pub(crate) fn is_classifier_row(row: &ModelBatchRow) -> bool {
    matches!(row.kind.as_deref(), Some(DECISION_KIND | CLASSIFIER_KIND))
}

/// The catalog row as the role's check reads it: fusiform's `decision` is the role's
/// `classifier`; any other stated kind is passed on as stated, so a refusal names it.
fn catalog_row(row: Option<&ModelBatchRow>) -> CatalogRow<'_> {
    match row {
        None => CatalogRow::Absent,
        Some(row) => match row.kind.as_deref() {
            None => CatalogRow::Kindless,
            Some(_) if is_classifier_row(row) => CatalogRow::Kind(CLASSIFIER_KIND),
            Some(kind) => CatalogRow::Kind(kind),
        },
    }
}

/// The catalog facts a batch is priced by, frozen at its first admission so a re-send after a
/// catalog change prices the batch as it was admitted.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize)]
struct FrozenRow {
    #[serde(default)]
    input_rate_nanos: Option<i64>,
    #[serde(default)]
    output_rate_nanos: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    charge_basis: Option<broca_core::ChargeBasis>,
}

impl FrozenRow {
    fn priced(&self) -> bool {
        self.input_rate_nanos.is_some() || self.output_rate_nanos.is_some()
    }
}

/// The parts of the request forwarded to the provider as the caller wrote them: the questions
/// and each item's state, as their exact text. Decoding through `Value` would respell numbers.
#[derive(Deserialize)]
struct RawParams<'a> {
    #[serde(borrow)]
    questions: &'a RawValue,
    #[serde(borrow)]
    items: Vec<RawItem<'a>>,
}

#[derive(Deserialize)]
struct RawItem<'a> {
    #[serde(borrow)]
    state: &'a RawValue,
    /// Forwarded as written to a provider that takes images; validation already bounded them
    /// by the model's `images.max`, which is absent for a text-only model.
    #[serde(borrow, default)]
    images: Option<&'a RawValue>,
}

/// The provider's reply, with its answers kept as the exact text it sent (RULES §12).
#[derive(Deserialize)]
struct ProviderAnswer<'a> {
    #[serde(borrow)]
    answers: &'a RawValue,
    #[serde(default)]
    usage: Option<Usage>,
    /// The version that answered, which the provider may name even when it differs from the
    /// one sent.
    #[serde(default)]
    model: Option<String>,
}

/// The model a reply names when it differs from the one broca sent. Such a reply is still
/// answered and stored: the row stays pinned, and the difference is only logged.
pub(super) fn answered_by_another_model<'a>(
    sent: &str,
    answered: Option<&'a str>,
) -> Option<&'a str> {
    answered.filter(|answered| *answered != sent)
}

/// One item of the reply.
#[derive(Serialize)]
struct ReplyItem<'a> {
    index: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    answers: Option<&'a RawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<&'a ItemError>,
    #[serde(skip_serializing_if = "Option::is_none")]
    usage: Option<Usage>,
}

#[derive(Serialize)]
struct Reply<'a> {
    batch_id: &'a str,
    model: &'a str,
    items: Vec<ReplyItem<'a>>,
    usage: Usage,
    #[serde(skip_serializing_if = "Option::is_none")]
    cost_usd: Option<serde_json::Number>,
}

/// Where an item stands as the call goes.
enum Slot {
    /// A stored outcome: from an earlier call, or recorded by this one.
    Stored(StoredOutcome),
    /// Not answered and not stored: a code that stopped the call, or one never stored.
    Unsent(ItemError),
}

/// This call's hold on a batch: the in-process claim, and the store's `running` row once it
/// is written. A call that ends normally (a reply or a refusal) gives it up through
/// [`Hold::release`], which waits for the store write. Dropping it is the fallback for a call
/// that panics or whose future is dropped: it removes the in-process claim and starts the
/// store release in the background, since nothing is left to wait for it. Either way a
/// failed store release is logged and the row stays held until the next start releases it.
struct Hold<'a> {
    service: &'a Arc<SystemOne>,
    batch_id: String,
    stored: bool,
}

/// End this process's store hold on a batch. A failure is only logged: the call is answered
/// anyway, and the next start releases every hold a previous process left.
fn release_stored(store: &SystemOneStore, role: &str, batch_id: &str, owner: &str) {
    if let Err(error) = store.release(role, batch_id, owner) {
        tracing::warn!(target: "system_one", %error, batch_id,
            "could not release a batch hold; the next start releases it");
    }
}

impl Hold<'_> {
    /// Release the store's row and wait for it, then the in-process claim (by dropping the
    /// hold). The reply leaves only after this, so once the release succeeds a caller that
    /// re-sends the same batch the moment it is answered finds the batch free, not
    /// `batch_in_progress`.
    async fn release(mut self) {
        if self.stored {
            // Cleared first so the drop below does not start a second release. If this future
            // is dropped mid-wait, the blocking task still runs to completion.
            self.stored = false;
            let store = Arc::clone(&self.service.store);
            let (role, batch_id, owner) = (
                self.service.role.clone(),
                self.batch_id.clone(),
                self.service.owner.clone(),
            );
            let released = tokio::task::spawn_blocking(move || {
                release_stored(&store, &role, &batch_id, &owner)
            })
            .await;
            if let Err(error) = released {
                tracing::warn!(target: "system_one", %error, batch_id = %self.batch_id,
                    "could not release a batch hold; the next start releases it");
            }
        }
    }
}

impl Drop for Hold<'_> {
    fn drop(&mut self) {
        if self.stored {
            let store = Arc::clone(&self.service.store);
            let (role, batch_id, owner) = (
                self.service.role.clone(),
                self.batch_id.clone(),
                self.service.owner.clone(),
            );
            #[cfg(test)]
            let gate = self
                .service
                .fallback_release_gate
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .take();
            let release = move || {
                #[cfg(test)]
                if let Some(gate) = gate {
                    // Returns once the test drops the sending side.
                    let _ = gate.recv();
                }
                release_stored(&store, &role, &batch_id, &owner);
            };
            match tokio::runtime::Handle::try_current() {
                Ok(runtime) => drop(runtime.spawn_blocking(release)),
                Err(_) => release(),
            }
        }
        self.service
            .claims
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&self.batch_id);
    }
}

async fn blocking<T: Send + 'static>(
    work: impl FnOnce() -> Result<T, broca_store::system_one::SystemOneStoreError> + Send + 'static,
) -> Result<T, Refusal> {
    match tokio::task::spawn_blocking(work).await {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => {
            tracing::error!(target: "system_one", %error, "the batch store refused a write");
            Err(internal_refusal(
                "the batch store is unavailable; re-send the batch",
            ))
        }
        Err(error) => {
            tracing::error!(target: "system_one", %error, "a batch store task failed");
            Err(internal_refusal(
                "the batch store is unavailable; re-send the batch",
            ))
        }
    }
}

fn sha256_hex(text: &str) -> String {
    Sha256::digest(text.as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn item_error_json(error: &ItemError) -> String {
    serde_json::to_string(error).expect("an item error encodes")
}

/// One item's estimate, at `rate` (zero for an unpriced input).
fn estimate(request: &ClassifyRequest, index: usize, rate: i64) -> Result<Estimate, Refusal> {
    let item = &request.items[index];
    let state = Tokens::bound_of_json(&item.state)
        .map_err(|e| Refusal::invalid_params(format!("items[{index}].state"), e.0))?;
    let schema = Tokens::bound_of_json(&request.questions)
        .map_err(|e| Refusal::invalid_params("questions", e.0))?;
    let images = item.images.as_ref().map_or(0, |images| images.len() as u64);
    let input =
        ItemCostInput::new(state, schema, IMAGE_TOKENS_EACH, OVERHEAD_TOKENS).with_images(images);
    // An overflow is an estimate past any ceiling, never a wrapped small one.
    Ok(estimate_item(&input, rate).unwrap_or(Estimate::new(1, u64::MAX, u64::MAX)))
}

/// What answering an item cost, for the batch's recorded spend: the reported input at the
/// frozen rate, or the item's estimate when the provider did not report it, plus reported
/// output where output is priced.
fn spend_of(frozen: &FrozenRow, usage: Option<&Usage>, estimate: &Estimate) -> u64 {
    let input = match (frozen.input_rate_nanos, usage.and_then(|u| u.input_tokens)) {
        (Some(rate), Some(tokens)) => cost_nanos(tokens, rate).unwrap_or(u64::MAX),
        (Some(_), None) => estimate.cost_nanos,
        (None, _) => 0,
    };
    let output = match (
        frozen.output_rate_nanos,
        usage.and_then(|u| u.output_tokens),
    ) {
        (Some(rate), Some(tokens)) if rate > 0 => cost_nanos(tokens, rate).unwrap_or(u64::MAX),
        _ => 0,
    };
    input.saturating_add(output)
}

/// The batch's cumulative cost over every answered item: absent when the model is unpriced,
/// or when an answered item did not report the tokens a priced direction bills (RULES §10).
fn batch_cost(frozen: &FrozenRow, answered: &[(Option<u64>, Option<u64>)]) -> Option<u64> {
    if !frozen.priced() {
        return None;
    }
    let mut total = 0u64;
    if let Some(rate) = frozen.input_rate_nanos {
        total = total.checked_add(
            input_cost_nanos(answered.iter().map(|(input, _)| *input), Some(rate)).ok()??,
        )?;
    }
    if let Some(rate) = frozen.output_rate_nanos.filter(|rate| *rate > 0) {
        total = total.checked_add(
            input_cost_nanos(answered.iter().map(|(_, output)| *output), Some(rate)).ok()??,
        )?;
    }
    Some(total)
}

/// The refusal a batch-level code met on the call's first provider call becomes (RULES §14).
fn first_call_refusal(error: &ItemError, model: &str, pin: &ModelPin) -> Refusal {
    match error.code.as_str() {
        errors::AUTH_FAILED => {
            let mut refusal = Refusal::auth_failed();
            refusal.message = error.message.clone();
            if let Some(credential) = pin.provider.credential_id() {
                refusal.detail.credential = Some(credential);
            }
            refusal
        }
        errors::MODEL_UNAVAILABLE => Refusal::model_unavailable(model),
        errors::ACCOUNT_LIMIT_EXHAUSTED => Refusal::account_limit_exhausted(),
        _ => Refusal::account_not_entitled(),
    }
}

/// Map one attempt's reply to the item's answer text and usage, or its error (RULES §15,
/// TypeSafe's errors by status).
fn map_reply(reply: ProviderReply, pin: &ModelPin) -> Result<(String, Option<Usage>), ItemError> {
    match reply {
        ProviderReply::AttributedAnswer { body, .. } => {
            map_reply(ProviderReply::Answered(body), pin)
        }
        ProviderReply::Answered(body) => {
            let decoded: ProviderAnswer<'_> = serde_json::from_slice(&body).map_err(|e| {
                ItemError::for_code(
                    errors::PROVIDER_ERROR,
                    format!("the provider's reply did not decode: {e}"),
                )
            })?;
            if let Some(answered) =
                answered_by_another_model(&pin.provider_model, decoded.model.as_deref())
            {
                tracing::warn!(target: "system_one", catalog_id = %pin.catalog_id,
                    sent = %pin.provider_model, answered = %answered,
                    "the provider answered with a different model than the one sent");
            }
            if !decoded.answers.get().trim_start().starts_with('{') {
                return Err(ItemError::for_code(
                    errors::PROVIDER_ERROR,
                    "the provider's reply carried no answers object",
                ));
            }
            let usage = decoded.usage.filter(|usage| !usage.is_empty());
            Ok((decoded.answers.get().to_owned(), usage))
        }
        ProviderReply::Status {
            status,
            retry_after_ms,
        } => {
            let code = item_code_for_status(status).unwrap_or(errors::PROVIDER_ERROR);
            let mut error =
                ItemError::for_code(code, format!("the provider answered HTTP {status}"));
            if let (errors::RATE_LIMITED, Some(wait)) = (code, retry_after_ms) {
                error = error.with_retry_after_ms(wait);
            }
            Err(error)
        }
        ProviderReply::WorkersAiError {
            status,
            code,
            retry_after_ms,
        } => {
            let item_code = workers_ai_item_code(status, code).unwrap_or(errors::PROVIDER_ERROR);
            // The code goes into the message whether or not the table lists it, so a caller can
            // read a code the table does not map.
            let message = match code {
                Some(code) => format!("Workers AI answered HTTP {status} with error code {code}"),
                None => format!("Workers AI answered HTTP {status}"),
            };
            let mut error = ItemError::for_code(item_code, message);
            if let (errors::RATE_LIMITED, Some(wait)) = (item_code, retry_after_ms) {
                error = error.with_retry_after_ms(wait);
            }
            Err(error)
        }
        ProviderReply::Transport(message) => Err(ItemError::for_code(
            errors::PROVIDER_ERROR,
            format!("the provider could not be reached: {message}"),
        )),
        ProviderReply::Credential(message) => Err(ItemError::for_code(
            errors::AUTH_FAILED,
            format!("the provider credential could not be read: {message}"),
        )),
    }
}

/// Whether an attempt's refusal is one the provider marked retryable: an overload or rate
/// status that names a wait short enough to sit out inside the call. A Workers AI refusal is
/// retried only when its code maps to a rate limit or a provider error: a 429 for the daily
/// allocation (3036) names the account, and waiting cannot clear it.
fn retry_wait(reply: &ProviderReply, service: &SystemOne) -> Option<std::time::Duration> {
    let wait = match reply {
        ProviderReply::Status {
            status: 429 | 503 | 529,
            retry_after_ms: Some(wait),
        } => *wait,
        ProviderReply::WorkersAiError {
            status: status @ (429 | 503),
            code,
            retry_after_ms: Some(wait),
        } if matches!(
            workers_ai_item_code(*status, *code),
            Some(errors::RATE_LIMITED | errors::PROVIDER_ERROR)
        ) =>
        {
            *wait
        }
        _ => return None,
    };
    Some(std::time::Duration::from_millis(wait))
        .filter(|wait| *wait <= service.config.max_retry_wait)
}

/// One item's provider call, retried only where the provider marked its refusal retryable.
/// The item's spend check ran once before this; a refused attempt bills nothing, so retries
/// stay inside it.
async fn call_provider(
    service: &SystemOne,
    pin: &ModelPin,
    body: String,
) -> Result<PaidAnswer, ItemError> {
    let slot = service.model_slot(&pin.catalog_id);
    let mut attempt = 0;
    loop {
        attempt += 1;
        let reply = {
            let _permit = slot.acquire().await;
            pin.provider.call(body.clone()).await
        };
        let code = match &reply {
            ProviderReply::Answered(_) | ProviderReply::AttributedAnswer { .. } => None,
            ProviderReply::Status { status, .. } => Some(status.to_string()),
            ProviderReply::WorkersAiError { status, code, .. } => {
                Some(code.map_or_else(|| status.to_string(), |code| code.to_string()))
            }
            ProviderReply::Transport(_) => Some("transport_error".into()),
            ProviderReply::Credential(_) => Some("auth_failed".into()),
        };
        if let Some(code) = code {
            service
                .metrics
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .last_provider_error
                .insert(
                    pin.catalog_id.split('/').next().unwrap_or_default().into(),
                    code,
                );
        }
        match retry_wait(&reply, service) {
            Some(wait) if attempt < service.config.max_attempts => {
                tokio::time::sleep(wait).await;
            }
            _ => {
                let (auth_selection, account_ref) = match &reply {
                    ProviderReply::AttributedAnswer {
                        auth_selection,
                        account_ref,
                        ..
                    } => (auth_selection.clone(), account_ref.clone()),
                    _ => (pin.provider.credential_id(), None),
                };
                return map_reply(reply, pin).map(|(answers, usage)| PaidAnswer {
                    answers,
                    usage,
                    auth_selection,
                    account_ref,
                });
            }
        }
    }
}

struct PaidAnswer {
    answers: String,
    usage: Option<Usage>,
    auth_selection: Option<String>,
    account_ref: Option<String>,
}

/// Serve one `classify.run`. `admitted_at_ms`, when given, is when the call was first admitted:
/// a new batch's retention runs from it instead of from now.
pub(crate) async fn run(
    service: &Arc<SystemOne>,
    params: &str,
    scope: &broca_store::system_one::BatchScope,
    admitted_at_ms: Option<i64>,
) -> Result<Box<RawValue>, Refusal> {
    run_inner(service, params, scope, admitted_at_ms, None).await
}

pub(super) fn admit_tool(
    service: &SystemOne,
    params: &str,
    stored: Option<&broca_store::system_one::StoredBatch>,
) -> Result<ToolAdmission, Refusal> {
    let request = parse_request(params)?;
    let frozen = if let Some(stored) = stored {
        serde_json::from_str::<FrozenRow>(&stored.frozen_row)
            .map_err(|_| internal_refusal("the stored tool price does not decode"))?
    } else {
        check_request_bytes(
            params.len(),
            &cortexkit_role_classifier::describe::Limits::new(
                service.config.max_items,
                service.config.max_request_bytes,
            ),
        )?;
        let row = service.catalog.row(&request.model);
        check_catalog_row(&request.model, catalog_row(row.as_ref()))?;
        let row = row.expect("catalog admission checked presence");
        let pin = service
            .pin(&request.model)
            .ok_or_else(|| Refusal::model_unavailable(&request.model))?;
        check_request(&request, &service.model_entry(pin, &row))?;
        use broca_core::ChargeBasisResolver;
        FrozenRow {
            input_rate_nanos: row.input_rate_nanos,
            output_rate_nanos: row.output_rate_nanos,
            charge_basis: Some(
                service
                    .charge_basis
                    .resolve_charge_basis(
                        request.model.split('/').next().unwrap_or_default(),
                        "apikey",
                    )
                    .0,
            ),
        }
    };
    if frozen.charge_basis != Some(broca_core::ChargeBasis::MeteredApi)
        || frozen.input_rate_nanos.is_none_or(|rate| rate < 0)
        || frozen.output_rate_nanos.is_some_and(|rate| rate > 0)
    {
        return Err(Refusal::new(
            "cost_unpriceable",
            "the model requires a per-token input price and no positive output price",
        ));
    }
    Ok(ToolAdmission {
        frozen_row: serde_json::to_string(&frozen).expect("admitted pricing encodes"),
    })
}

pub(super) fn replay_tool(
    params: &str,
    stored: &broca_store::system_one::StoredBatch,
) -> Result<Box<RawValue>, Refusal> {
    let request = parse_request(params)?;
    let frozen: FrozenRow = serde_json::from_str(&stored.frozen_row)
        .map_err(|_| internal_refusal("the stored tool price does not decode"))?;
    reply(
        &request,
        &frozen,
        stored
            .outcomes
            .iter()
            .cloned()
            .map(|outcome| Some(Slot::Stored(outcome)))
            .collect(),
    )
}

/// The tool supplies its argument digest and a read-only replay snapshot. Classifier callers
/// retain their identity digest and live-catalog admission, including their original bytes.
pub(super) async fn run_inner(
    service: &Arc<SystemOne>,
    params: &str,
    scope: &broca_store::system_one::BatchScope,
    admitted_at_ms: Option<i64>,
    tool: Option<(&str, &ToolAdmission)>,
) -> Result<Box<RawValue>, Refusal> {
    let flush = Arc::clone(service);
    tokio::task::spawn_blocking(move || flush.flush_facts())
        .await
        .map_err(|_| internal_refusal("usage export recovery task failed"))?
        .map_err(|_| internal_refusal("usage export is unavailable; re-send the batch"))?;
    let limits = cortexkit_role_classifier::describe::Limits::new(
        service.config.max_items,
        service.config.max_request_bytes,
    );
    // §8: everything is checked before anything is written or sent.
    check_request_bytes(params.len(), &limits)?;
    let request = parse_request(params)?;
    let raw: RawParams<'_> = serde_json::from_str(params)
        .map_err(|e| Refusal::invalid_params("params", e.to_string()))?;
    let row = if let Some((_, admission)) = tool {
        let frozen: FrozenRow = serde_json::from_str(&admission.frozen_row)
            .map_err(|_| internal_refusal("the stored tool price does not decode"))?;
        Some(broca_catalog::ModelBatchRow {
            kind: Some(DECISION_KIND.into()),
            context_tokens: None,
            attachment: false,
            input_rate_nanos: frozen.input_rate_nanos,
            output_rate_nanos: frozen.output_rate_nanos,
        })
    } else {
        service.catalog.row(&request.model)
    };
    check_catalog_row(&request.model, catalog_row(row.as_ref()))?;
    let row = row.expect("check_catalog_row refuses an absent row");
    // A classifier row broca has no route to: the catalog knows it, broca cannot serve it.
    let pin = service
        .pin(&request.model)
        .ok_or_else(|| Refusal::model_unavailable(&request.model))?
        .clone();
    let entry = service.model_entry(&pin, &row);
    if tool.is_none() {
        check_request(&request, &entry)?;
    }

    let identity =
        BatchIdentity::of(&request).map_err(|e| Refusal::invalid_params("items", e.0))?;
    let identity_json = serde_json::to_string(&identity).expect("an identity encodes");
    let digest = tool.map_or_else(
        || sha256_hex(&identity_json),
        |(digest, _)| digest.to_owned(),
    );
    // A ceiling too large for the arithmetic is no ceiling at all in practice.
    let sent_ceiling = request
        .max_cost_usd
        .as_ref()
        .map(|usd| usd_to_nanos(usd, Rounding::Down).unwrap_or(u64::MAX));

    // §3, in process: a second call for a batch this process holds is refused at once.
    {
        let mut claims = service.claims.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(recorded) = claims.get(&request.batch_id) {
            if recorded != scope {
                return Err(Refusal::new(
                    "batch_scope_mismatch",
                    "batch_id is running under a different scope",
                ));
            }
            return Err(Refusal::batch_in_progress(
                super::BATCH_IN_PROGRESS_RETRY_AFTER_MS,
            ));
        }
        claims.insert(request.batch_id.clone(), scope.clone());
    }
    let mut hold = Hold {
        service,
        batch_id: request.batch_id.clone(),
        stored: false,
    };
    // Everything after the in-process claim runs in `serve_claimed`, so that every way the
    // call can end normally once the store row may exist (a reply, a scope or reuse refusal,
    // a failed outcome or usage-export write, a failed outbox flush) passes through the
    // awaited release below before `run` returns.
    let answer = serve_claimed(
        service,
        scope,
        &mut hold,
        &request,
        &raw,
        &row,
        &pin,
        &identity,
        &identity_json,
        &digest,
        sent_ceiling,
        admitted_at_ms,
        tool.map(|(_, admission)| admission.frozen_row.as_str()),
    )
    .await;
    hold.release().await;
    answer
}

/// The part of one `classify.run` that runs while this call holds the batch: the store claim,
/// each item's provider call and outcome write, and the reply. `run` releases `hold` after
/// it, whatever it returns.
// The arguments are the request as `run` already checked it; a struct for one call site
// would only rename them.
#[allow(clippy::too_many_arguments)]
async fn serve_claimed(
    service: &Arc<SystemOne>,
    scope: &broca_store::system_one::BatchScope,
    hold: &mut Hold<'_>,
    request: &ClassifyRequest,
    raw: &RawParams<'_>,
    row: &ModelBatchRow,
    pin: &ModelPin,
    identity: &BatchIdentity,
    identity_json: &str,
    digest: &str,
    sent_ceiling: Option<u64>,
    admitted_at_ms: Option<i64>,
    tool_frozen: Option<&str>,
) -> Result<Box<RawValue>, Refusal> {
    let is_tool = tool_frozen.is_some();
    let live_frozen = if let Some(frozen) = tool_frozen {
        serde_json::from_str(frozen)
            .map_err(|_| internal_refusal("the admitted tool price does not decode"))?
    } else {
        FrozenRow {
            input_rate_nanos: row.input_rate_nanos,
            output_rate_nanos: row.output_rate_nanos,
            charge_basis: None,
        }
    };
    let now = (service.clock)();
    let claimed = {
        let store = Arc::clone(&service.store);
        let (role, batch_id, model, owner) = (
            service.role.clone(),
            request.batch_id.clone(),
            request.model.clone(),
            service.owner.clone(),
        );
        let (identity_json, digest) = (identity_json.to_owned(), digest.to_owned());
        let frozen = serde_json::to_string(&live_frozen).expect("frozen facts encode");
        let scope = scope.clone();
        blocking(move || {
            store.claim_admitted_at(
                &BatchClaim {
                    role: &role,
                    batch_id: &batch_id,
                    model: &model,
                    identity: &identity_json,
                    body_digest: &digest,
                    frozen_row: &frozen,
                    scope: &scope,
                    ceiling_nanos: sent_ceiling,
                    owner: &owner,
                    now_ms: now,
                },
                admitted_at_ms.unwrap_or(now),
            )
        })
        .await?
    };
    let (fresh, frozen, ceiling, mut spent, stored) = match claimed {
        Claimed::ScopeMismatch => {
            return Err(Refusal::new(
                "batch_scope_mismatch",
                "batch_id belongs to a different scope; a retry cannot change its attribution",
            ))
        }
        // §2: a different body under a recorded id names the first part that differs.
        Claimed::Reuse { recorded_identity } => {
            let recorded: BatchIdentity = serde_json::from_str(&recorded_identity)
                .map_err(|_| internal_refusal("a recorded batch identity does not decode"))?;
            let field = recorded
                .first_difference(identity)
                .unwrap_or_else(|| "items".to_owned());
            return Err(Refusal::batch_id_reuse(field));
        }
        Claimed::InProgress => {
            return Err(Refusal::batch_in_progress(
                super::BATCH_IN_PROGRESS_RETRY_AFTER_MS,
            ))
        }
        Claimed::Fresh => (true, live_frozen, sent_ceiling, 0u64, Vec::new()),
        Claimed::Resumed(record) => (
            false,
            serde_json::from_str(&record.frozen_row).unwrap_or_default(),
            record.ceiling_nanos,
            record.spent_nanos,
            record.outcomes,
        ),
    };
    hold.stored = true;
    let rate = frozen.input_rate_nanos.unwrap_or(0);

    // §4: what each item needs. A stored answer and a stored permanent error are served as
    // stored; a stored transient error and an item with nothing stored get a provider call.
    let mut slots: Vec<Option<Slot>> = (0..request.items.len()).map(|_| None).collect();
    for outcome in stored {
        let index = outcome.index as usize;
        if index >= slots.len() {
            continue;
        }
        let keep = match &outcome.outcome {
            Outcome::Answered(_) => true,
            Outcome::Failed(text) => serde_json::from_str::<ItemError>(text)
                .map_or(true, |error| !error.is_retried_on_resend()),
        };
        if keep {
            slots[index] = Some(Slot::Stored(outcome));
        }
    }
    let estimates = (0..request.items.len())
        .map(|index| estimate(request, index, rate))
        .collect::<Result<Vec<_>, _>>()?;

    // CONTRACT §6.9: an estimate of what this call would send above the ceiling is refused
    // before any dispatch. Spend already recorded is checked item by item below, so a re-send
    // whose earlier spend crossed the ceiling still gets each item's own `cost_exceeded`.
    if let Some(ceiling) = ceiling {
        let wanted = slots
            .iter()
            .zip(&estimates)
            .filter(|(slot, _)| slot.is_none())
            .fold(0u64, |total, (_, estimate)| {
                total.saturating_add(estimate.cost_nanos)
            });
        if wanted > ceiling {
            if fresh {
                discard_fresh(service, &request.batch_id).await;
                hold.stored = false;
            }
            return Err(Refusal::cost_exceeded(
                nanos_to_usd(wanted),
                nanos_to_usd(ceiling),
            ));
        }
    }

    let questions = raw.questions.get();
    let mut first_provider_call = true;
    // Outcomes found before the call's first provider call wait for it: if that call is
    // refused at admission, the refusal writes nothing (§14).
    let mut pending: Vec<StoredOutcome> = Vec::new();
    let mut stop: Option<ItemError> = None;
    for index in 0..request.items.len() {
        if slots[index].is_some() {
            continue;
        }
        if let Some(stopped) = &stop {
            let mut error = ItemError::for_code(
                &stopped.code,
                format!(
                    "not sent: an earlier item of this call ended {}",
                    stopped.code
                ),
            );
            error.retry_after_ms = stopped.retry_after_ms;
            slots[index] = Some(Slot::Unsent(error));
            continue;
        }
        // §5: recorded spend plus this item's estimate, before its provider call.
        if let Some(limit) = ceiling {
            if crosses_ceiling(spent, &estimates[index], limit) {
                let error = ItemError::for_code(
                    errors::COST_EXCEEDED,
                    format!(
                        "answering this item could cross the batch ceiling of {} USD",
                        nanos_to_usd(limit)
                    ),
                );
                let outcome = StoredOutcome {
                    index: index as u64,
                    outcome: Outcome::Failed(item_error_json(&error)),
                    input_tokens: None,
                    output_tokens: None,
                };
                if first_provider_call {
                    pending.push(outcome.clone());
                } else {
                    record(service, &request.batch_id, &outcome, 0).await?;
                }
                slots[index] = Some(Slot::Stored(outcome));
                continue;
            }
        }
        let images = raw.items[index]
            .images
            .filter(|images| images.get().trim() != "null")
            .map_or(String::new(), |images| {
                format!(",\"images\":{}", images.get())
            });
        let body = format!(
            "{{\"model\":{},\"state\":{},\"questions\":{questions}{images}}}",
            serde_json::to_string(&pin.provider_model).expect("a string encodes"),
            raw.items[index].state.get(),
        );
        let answer = call_provider(service, pin, body).await;
        if first_provider_call {
            first_provider_call = false;
            if let Err(error) = &answer {
                if errors::is_batch_level(&error.code) {
                    // §14: a batch-level code on the call's first provider call is an
                    // admission refusal. A batch this call created is removed; a recorded one
                    // keeps what it had, its tightened ceiling included.
                    if fresh {
                        discard_fresh(service, &request.batch_id).await;
                        hold.stored = false;
                    }
                    return Err(first_call_refusal(error, &request.model, pin));
                }
            }
            for outcome in std::mem::take(&mut pending) {
                record(service, &request.batch_id, &outcome, 0).await?;
            }
        }
        match answer {
            Ok(PaidAnswer {
                answers,
                usage,
                auth_selection,
                account_ref,
            }) => {
                if is_tool {
                    tool_answer_cut().await;
                }
                let cost = spend_of(&frozen, usage.as_ref(), &estimates[index]);
                let measured = usage
                    .as_ref()
                    .and_then(|usage| usage.input_tokens)
                    .is_some();
                let result = if is_tool && !measured {
                    let refusal = crate::llm_tools::unmeasured_cost_refusal();
                    Outcome::Failed(item_error_json(&ItemError::new(
                        refusal.code,
                        refusal.detail.class,
                        refusal.message,
                    )))
                } else {
                    Outcome::Answered(answers)
                };
                let outcome = StoredOutcome {
                    index: index as u64,
                    outcome: result,
                    input_tokens: usage.and_then(|u| u.input_tokens),
                    output_tokens: usage.and_then(|u| u.output_tokens),
                };
                let fact = service.export.as_ref().map(|export| {
                    use broca_core::ChargeBasisResolver;
                    let provider = request
                        .model
                        .split('/')
                        .next()
                        .unwrap_or_default()
                        .to_owned();
                    let method = auth_selection
                        .as_deref()
                        .and_then(|id| id.split(':').next())
                        .unwrap_or("apikey");
                    let (charge_basis, charge_basis_mapping_version) =
                        service.charge_basis.resolve_charge_basis(&provider, method);
                    broca_store::system_one::FactContext {
                        source_instance_id: export.source_instance_id().into(),
                        provider,
                        model: request.model.clone(),
                        auth_selection,
                        account_ref,
                        charge_basis,
                        charge_basis_mapping_version,
                        charge_basis_overrides: service.charge_basis.charge_basis_overrides(),
                    }
                });
                record_paid(service, &request.batch_id, &outcome, cost, fact).await?;
                spent = spent.saturating_add(cost);
                slots[index] = Some(Slot::Stored(outcome));
            }
            Err(error) => {
                if error.is_stored() {
                    let outcome = StoredOutcome {
                        index: index as u64,
                        outcome: Outcome::Failed(item_error_json(&error)),
                        input_tokens: None,
                        output_tokens: None,
                    };
                    record(service, &request.batch_id, &outcome, 0).await?;
                    slots[index] = Some(Slot::Stored(outcome));
                } else {
                    slots[index] = Some(Slot::Unsent(error.clone()));
                }
                if error.stops_the_call() {
                    stop = Some(error);
                }
            }
        }
    }
    for outcome in pending {
        record(service, &request.batch_id, &outcome, 0).await?;
    }
    reply(request, &frozen, slots)
}

/// The debug test hook can stop after a provider answer, before the atomic outcome/fact write.
/// The create-once marker prevents the restarted process from stopping at the same cut.
async fn tool_answer_cut() {
    #[cfg(debug_assertions)]
    if std::env::var_os("BROCA_PROVIDER_BASE_URL").is_some() {
        if let Some(path) = std::env::var_os("BROCA_TEST_SYSTEM_ONE_ANSWER_GATE") {
            use std::io::Write;
            let path = std::path::PathBuf::from(path);
            if let Ok(mut marker) = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
            {
                let _ = writeln!(marker, "{}", std::process::id());
                while !path.with_extension("release").exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            }
        }
    }
}

async fn record(
    service: &Arc<SystemOne>,
    batch_id: &str,
    outcome: &StoredOutcome,
    spend_nanos: u64,
) -> Result<(), Refusal> {
    record_paid(service, batch_id, outcome, spend_nanos, None).await
}

async fn record_paid(
    service: &Arc<SystemOne>,
    batch_id: &str,
    outcome: &StoredOutcome,
    spend_nanos: u64,
    fact: Option<broca_store::system_one::FactContext>,
) -> Result<(), Refusal> {
    let store = Arc::clone(&service.store);
    let (role, batch_id, owner) = (
        service.role.clone(),
        batch_id.to_owned(),
        service.owner.clone(),
    );
    let outcome_for_metrics = outcome.clone();
    let outcome = outcome.clone();
    let now = (service.clock)();
    let held = blocking(move || {
        store.record_paid(
            &role,
            &batch_id,
            &owner,
            &outcome,
            &broca_store::system_one::PaidRecord {
                spend_nanos,
                now_ms: now,
                fact: fact.as_ref(),
            },
        )
    })
    .await?;
    if held {
        service.publish_facts().await?;
        let mut metrics = service.metrics.lock().unwrap_or_else(|p| p.into_inner());
        match &outcome_for_metrics.outcome {
            Outcome::Answered(_) => metrics.items_answered += 1,
            Outcome::Failed(_) => metrics.items_failed += 1,
        }
        Ok(())
    } else {
        Err(internal_refusal(
            "this call lost its hold on the batch; re-send the batch",
        ))
    }
}

async fn discard_fresh(service: &Arc<SystemOne>, batch_id: &str) {
    let store = Arc::clone(&service.store);
    let (role, batch_id, owner) = (
        service.role.clone(),
        batch_id.to_owned(),
        service.owner.clone(),
    );
    if let Err(refusal) = blocking(move || store.discard_fresh(&role, &batch_id, &owner)).await {
        tracing::warn!(target: "system_one", message = %refusal.message,
            "could not remove a refused batch's record");
    }
}

fn reply(
    request: &ClassifyRequest,
    frozen: &FrozenRow,
    slots: Vec<Option<Slot>>,
) -> Result<Box<RawValue>, Refusal> {
    let mut answers_raw: Vec<Option<Box<RawValue>>> = Vec::with_capacity(slots.len());
    let mut errors_parsed: Vec<Option<ItemError>> = Vec::with_capacity(slots.len());
    let mut usages: Vec<Option<Usage>> = Vec::with_capacity(slots.len());
    let mut answered_tokens = Vec::new();
    let mut total = Usage::default();
    for slot in &slots {
        let (answer, error, usage) = match slot {
            Some(Slot::Stored(outcome)) => {
                let mut usage = Usage::new();
                usage.input_tokens = outcome.input_tokens;
                usage.output_tokens = outcome.output_tokens;
                let usage = (!usage.is_empty()).then_some(usage);
                match &outcome.outcome {
                    Outcome::Answered(text) => {
                        let raw = RawValue::from_string(text.clone()).map_err(|_| {
                            internal_refusal("a stored answer is not JSON; re-send the batch")
                        })?;
                        answered_tokens.push((outcome.input_tokens, outcome.output_tokens));
                        if let Some(usage) = usage {
                            total.input_tokens =
                                add_reported(total.input_tokens, usage.input_tokens);
                            total.output_tokens =
                                add_reported(total.output_tokens, usage.output_tokens);
                        }
                        (Some(raw), None, usage)
                    }
                    Outcome::Failed(text) => {
                        let error: ItemError = serde_json::from_str(text)
                            .map_err(|_| internal_refusal("a stored item error does not decode"))?;
                        (None, Some(error), None)
                    }
                }
            }
            Some(Slot::Unsent(error)) => (None, Some(error.clone()), None),
            None => (
                None,
                Some(ItemError::for_code(
                    errors::PROVIDER_ERROR,
                    "the item was not reached",
                )),
                None,
            ),
        };
        answers_raw.push(answer);
        errors_parsed.push(error);
        usages.push(usage);
    }
    let items = (0..slots.len())
        .map(|index| ReplyItem {
            index: index as u64,
            answers: answers_raw[index].as_deref(),
            error: errors_parsed[index].as_ref(),
            usage: usages[index],
        })
        .collect();
    let reply = Reply {
        batch_id: &request.batch_id,
        model: &request.model,
        items,
        usage: total,
        cost_usd: batch_cost(frozen, &answered_tokens).map(nanos_to_usd),
    };
    serde_json::value::to_raw_value(&reply).map_err(|e| internal_refusal(&e.to_string()))
}
