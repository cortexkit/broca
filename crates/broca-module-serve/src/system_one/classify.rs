//! One `classify.run` call: validation, the batch hold, each item's provider call, and the
//! reply. The rules are `classifier/v1`'s `CONTRACT.md` and `cortexkit-model-batch`'s
//! `RULES.md`; the section numbers below are RULES'.

// An item's error is the role's own `ItemError`, returned once per provider call and stored or
// answered as it is; boxing it would buy nothing on a path that makes a network call each time.
#![allow(clippy::result_large_err)]

use std::sync::Arc;

use broca_catalog::ModelBatchRow;
use broca_store::system_one::{BatchClaim, Claimed, Outcome, StoredOutcome};
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

use super::{internal_refusal, ModelPin, ProviderReply, SystemOne, STORE_ROLE};

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
/// is written. Dropping it releases both, so a call that ends any way (a reply, a refusal, a
/// panic, a dropped future) never leaves its batch held.
struct Hold<'a> {
    service: &'a Arc<SystemOne>,
    batch_id: String,
    stored: bool,
}

impl Drop for Hold<'_> {
    fn drop(&mut self) {
        if self.stored {
            let store = Arc::clone(&self.service.store);
            let (batch_id, owner) = (self.batch_id.clone(), self.service.owner.clone());
            let release = move || {
                if let Err(error) = store.release(STORE_ROLE, &batch_id, &owner) {
                    tracing::warn!(target: "system_one", %error, batch_id,
                        "could not release a batch hold; the next start releases it");
                }
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
) -> Result<(String, Option<Usage>), ItemError> {
    let slot = service.model_slot(&pin.catalog_id);
    let mut attempt = 0;
    loop {
        attempt += 1;
        let reply = {
            let _permit = slot.acquire().await;
            pin.provider.call(body.clone()).await
        };
        let code = match &reply {
            ProviderReply::Answered(_) => None,
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
            _ => return map_reply(reply, pin),
        }
    }
}

/// Serve one `classify.run`.
pub(crate) async fn run(
    service: &Arc<SystemOne>,
    params: &str,
    scope: &broca_store::system_one::BatchScope,
) -> Result<Box<RawValue>, Refusal> {
    let limits = cortexkit_role_classifier::describe::Limits::new(
        service.config.max_items,
        service.config.max_request_bytes,
    );
    // §8: everything is checked before anything is written or sent.
    check_request_bytes(params.len(), &limits)?;
    let request = parse_request(params)?;
    let raw: RawParams<'_> = serde_json::from_str(params)
        .map_err(|e| Refusal::invalid_params("params", e.to_string()))?;
    let row = service.catalog.row(&request.model);
    check_catalog_row(&request.model, catalog_row(row.as_ref()))?;
    let row = row.expect("check_catalog_row refuses an absent row");
    // A classifier row broca has no route to: the catalog knows it, broca cannot serve it.
    let pin = service
        .pin(&request.model)
        .ok_or_else(|| Refusal::model_unavailable(&request.model))?
        .clone();
    let entry = service.model_entry(&pin, &row);
    check_request(&request, &entry)?;

    let identity =
        BatchIdentity::of(&request).map_err(|e| Refusal::invalid_params("items", e.0))?;
    let identity_json = serde_json::to_string(&identity).expect("an identity encodes");
    let digest = sha256_hex(&identity_json);
    // A ceiling too large for the arithmetic is no ceiling at all in practice.
    let sent_ceiling = request
        .max_cost_usd
        .as_ref()
        .map(|usd| usd_to_nanos(usd, Rounding::Down).unwrap_or(u64::MAX));

    // §3, in process: a second call for a batch this process holds is refused at once.
    {
        let mut claims = service.claims.lock().unwrap_or_else(|p| p.into_inner());
        if !claims.insert(request.batch_id.clone()) {
            return Err(Refusal::batch_in_progress(
                super::BATCH_IN_PROGRESS_RETRY_AFTER_MS,
            ));
        }
    }
    let mut hold = Hold {
        service,
        batch_id: request.batch_id.clone(),
        stored: false,
    };

    let live_frozen = FrozenRow {
        input_rate_nanos: row.input_rate_nanos,
        output_rate_nanos: row.output_rate_nanos,
    };
    let now = (service.clock)();
    let claimed = {
        let store = Arc::clone(&service.store);
        let (batch_id, model, owner) = (
            request.batch_id.clone(),
            request.model.clone(),
            service.owner.clone(),
        );
        let (identity_json, digest) = (identity_json.clone(), digest.clone());
        let frozen = serde_json::to_string(&live_frozen).expect("frozen facts encode");
        let scope = scope.clone();
        blocking(move || {
            store.claim(&BatchClaim {
                role: STORE_ROLE,
                batch_id: &batch_id,
                model: &model,
                identity: &identity_json,
                body_digest: &digest,
                frozen_row: &frozen,
                scope: &scope,
                ceiling_nanos: sent_ceiling,
                owner: &owner,
                now_ms: now,
            })
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
                .first_difference(&identity)
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
        .map(|index| estimate(&request, index, rate))
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
        let answer = call_provider(service, &pin, body).await;
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
                    return Err(first_call_refusal(error, &request.model, &pin));
                }
            }
            for outcome in std::mem::take(&mut pending) {
                record(service, &request.batch_id, &outcome, 0).await?;
            }
        }
        match answer {
            Ok((answers, usage)) => {
                let cost = spend_of(&frozen, usage.as_ref(), &estimates[index]);
                let outcome = StoredOutcome {
                    index: index as u64,
                    outcome: Outcome::Answered(answers),
                    input_tokens: usage.and_then(|u| u.input_tokens),
                    output_tokens: usage.and_then(|u| u.output_tokens),
                };
                record(service, &request.batch_id, &outcome, cost).await?;
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
    drop(hold);
    reply(&request, &frozen, slots)
}

async fn record(
    service: &Arc<SystemOne>,
    batch_id: &str,
    outcome: &StoredOutcome,
    spend_nanos: u64,
) -> Result<(), Refusal> {
    let store = Arc::clone(&service.store);
    let (batch_id, owner, outcome) = (batch_id.to_owned(), service.owner.clone(), outcome.clone());
    let outcome_for_metrics = outcome.clone();
    let now = (service.clock)();
    let held =
        blocking(move || store.record(STORE_ROLE, &batch_id, &owner, &outcome, spend_nanos, now))
            .await?;
    if held {
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
    let (batch_id, owner) = (batch_id.to_owned(), service.owner.clone());
    if let Err(refusal) = blocking(move || store.discard_fresh(STORE_ROLE, &batch_id, &owner)).await
    {
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
