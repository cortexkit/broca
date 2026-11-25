//! Admission, durable replay and per-item dispatch for the sessionless extractor role.

use std::sync::Arc;

use broca_catalog::ModelBatchRow;
use broca_protocol::{BindPrincipal, SendScope};
use broca_store::system_one::{
    BatchClaim, BatchScope, Claimed, Outcome, StoredBatch, StoredOutcome,
};
use cortexkit_model_batch::{
    absent::input_cost_nanos,
    estimate::{cost_nanos, nanos_to_usd, usd_to_nanos, Rounding},
};
use cortexkit_role_extractor::{
    describe::{ImageLimits, ImageReferenceSupport, Limits, ModelEntry, Price, RoleDescribe},
    errors::{self, ItemError, Refusal},
    field::{types, FieldType},
    identity::BatchIdentity,
    provider::{self, catalog_ids::CLAIRIC, clairic_limits, ProviderRequest},
    reply::{ExtractReply, ItemResult, Usage},
    request::{self, CatalogRow, ExtractRequest},
    spend::{self, ItemSpendInput, ModuleBounds},
};
use serde_json::value::{to_raw_value, RawValue};
use zeroize::Zeroizing;

use crate::llm_tools::spend::Spend;

use super::{
    caller_scope_for, clairic,
    classify::{self, blocking, Hold},
    error_body, images, internal_refusal, refusal_body,
    workers_ai::WorkersAi,
    ClassifierProvider, SystemOne, BATCH_IN_PROGRESS_RETRY_AFTER_MS, INTERNAL_ERROR,
    OP_EXTRACT_RUN, OP_ROLE_DESCRIBE,
};

mod tool;
use tool::ToolCall;

const STORE_ROLE: &str = "extractor";
const MAX_FIELDS: u64 = 64;
const MAX_INSTRUCTION_BYTES: u64 = 60_000;
const MAX_ANSWER_BYTES: u64 = 8_192;
/// The extraction models broca serves. A `model_unknown` refusal lists them, so a caller can
/// correct its call without guessing.
const ACCEPTED_MODELS: &[&str] = &[CLAIRIC];

/// Encodes an extraction refusal. A `model_unknown` refusal also carries
/// `detail.accepted_models`, added after encoding because the shared refusal detail type has
/// no member for it.
pub(crate) fn extraction_refusal_body(refusal: &Refusal) -> Vec<u8> {
    if refusal.code != errors::MODEL_UNKNOWN {
        return refusal_body(refusal);
    }
    let mut body = serde_json::to_value(refusal).expect("refusal encodes");
    body["detail"]["accepted_models"] = serde_json::json!(ACCEPTED_MODELS);
    serde_json::to_vec(&body).expect("refusal encodes")
}
const ITEM_METADATA_BYTES: usize = 1024;

const BOUNDS: ModuleBounds = ModuleBounds {
    // Measured image usage was about one token per 1024 pixels, with a minimum of 64
    // (1,024 tokens at 1024x1024). This bound covers an image of up to 16 megapixels. Admission
    // limits images by bytes, not pixels, and no larger image has been measured, so a bigger one
    // could cost more than the estimate.
    image_tokens_each: 16_384,
    // The most added schema text observed was 285 tokens for one field, and the provider's
    // prompt grew between measurements. Leave headroom for that text and image descriptions.
    schema_tokens_each: 512,
    // Observed charges fit the schema once and the state and images once per call, with no
    // fixed overhead. We only have byte bounds for caller text, not Clairic token counts.
    overhead_tokens: 0,
};

#[derive(Clone)]
pub(crate) struct Extractor {
    pub(crate) service: Arc<SystemOne>,
    clairic: Arc<WorkersAi>,
}

impl SystemOne {
    /// Serve Clairic beside the classifiers, sharing the store, process cap and usage outbox.
    pub fn with_clairic(mut self, clairic: Arc<WorkersAi>) -> Self {
        self.extractor = Some(self.extractor_for(clairic, STORE_ROLE));
        self
    }

    fn extractor_for(&self, clairic: Arc<WorkersAi>, store_role: &str) -> Extractor {
        let mut service = Self::new(
            self.store.clone(),
            self.owner.clone(),
            self.catalog.clone(),
            Vec::new(),
            self.config.clone(),
            self.in_flight.clone(),
        )
        .with_store_role(store_role)
        .with_clock(self.clock.clone());
        service.call_slots = self.call_slots.clone();
        service.metrics = self.metrics.clone();
        service.export = self.export.clone();
        service.fact_flush = self.fact_flush.clone();
        service.charge_basis = self.charge_basis.clone();
        service.image_resolver = self.image_resolver.clone();
        Extractor {
            service: Arc::new(service),
            clairic,
        }
    }

    pub(crate) fn serves_extractor(&self) -> bool {
        self.extractor.is_some()
    }

    pub(crate) async fn answer_extractor(
        &self,
        body: &[u8],
        principal: Option<&BindPrincipal>,
        send: &SendScope,
    ) -> Result<Box<RawValue>, Vec<u8>> {
        let extractor = self.extractor.as_ref().ok_or_else(|| {
            error_body("unknown_method", "this build serves no extractor/v1 role")
        })?;
        #[derive(serde::Deserialize)]
        struct Envelope<'a> {
            method: String,
            #[serde(borrow, default)]
            params: Option<&'a RawValue>,
        }
        let envelope: Envelope<'_> = serde_json::from_slice(body).map_err(|e| {
            error_body(
                "invalid_request",
                &format!("request body not decodable: {e}"),
            )
        })?;
        let params = envelope.params.map_or("null", RawValue::get);
        match envelope.method.as_str() {
            OP_ROLE_DESCRIBE => {
                if !serde_json::from_str::<serde_json::Value>(params).is_ok_and(|value| {
                    value.is_null() || value.as_object().is_some_and(|map| map.is_empty())
                }) {
                    return Err(refusal_body(&Refusal::invalid_params(
                        "params",
                        "role.describe takes no params",
                    )));
                }
                to_raw_value(&extractor.describe())
                    .map_err(|e| error_body(INTERNAL_ERROR, &e.to_string()))
            }
            OP_EXTRACT_RUN => {
                let scope = caller_scope_for(principal, send, STORE_ROLE).map_err(|e| {
                    self.metrics
                        .lock()
                        .unwrap_or_else(|p| p.into_inner())
                        .batches_failed += 1;
                    refusal_body(&e)
                })?;
                let (params, send, extractor) =
                    (params.to_owned(), send.clone(), extractor.clone());
                extractor
                    .service
                    .clone()
                    .spawn_batch_task(move |_| async move {
                        extractor
                            .run(&params, &scope, &send)
                            .await
                            .map_err(|e| extraction_refusal_body(&e))
                    })
                    .await
            }
            other => Err(error_body(
                "unknown_method",
                &format!("unknown method '{other}' on an extractor/v1 route"),
            )),
        }
    }
}

impl Extractor {
    fn limits(&self) -> Limits {
        Limits::new(
            self.service.config.max_items,
            self.service.config.max_request_bytes,
            MAX_INSTRUCTION_BYTES,
        )
    }

    fn model_entry(&self, row: Option<&ModelBatchRow>) -> ModelEntry {
        let entry = ModelEntry::new(
            CLAIRIC,
            self.service.config.max_items,
            MAX_FIELDS,
            types::ALL.iter().copied().map(FieldType::parse).collect(),
            clairic_limits::PASS_TOKENS,
            MAX_ANSWER_BYTES,
            self.clairic.egress_host(),
        )
        .with_images(ImageLimits::workers_ai(2));
        let rate = |nanos: Option<i64>| nanos.and_then(|n| u64::try_from(n).ok()).map(nanos_to_usd);
        let Some(row) = row else { return entry };
        let (input, output) = (rate(row.input_rate_nanos), rate(row.output_rate_nanos));
        if input.is_none() && output.is_none() {
            return entry;
        }
        let mut price = Price::new();
        price.input_per_mtok_usd = input;
        price.output_per_mtok_usd = output;
        entry.with_price(price)
    }

    fn describe(&self) -> RoleDescribe {
        let models = self
            .service
            .catalog
            .row(CLAIRIC)
            .filter(|row| row.kind.as_deref() == Some(request::EXTRACTION_KIND))
            .map(|row| self.model_entry(Some(&row)))
            .into_iter()
            .collect();
        let describe = RoleDescribe::new(self.limits(), models);
        if self.service.image_references() {
            describe.with_image_reference(ImageReferenceSupport::new())
        } else {
            describe
        }
    }

    async fn run(
        &self,
        params: &str,
        scope: &BatchScope,
        send: &SendScope,
    ) -> Result<Box<RawValue>, Refusal> {
        self.run_inner(params, scope, send, None).await
    }

    async fn run_inner(
        &self,
        params: &str,
        scope: &BatchScope,
        send: &SendScope,
        tool: Option<&mut ToolCall<'_>>,
    ) -> Result<Box<RawValue>, Refusal> {
        let service = &self.service;
        let limits = self.limits();
        request::check_request_bytes(params.len(), &limits)?;
        let request = request::parse_request(params)?;
        {
            let mut claims = service.claims.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(recorded) = claims.get(&request.batch_id) {
                return Err(if recorded == scope {
                    in_progress()
                } else {
                    scope_mismatch()
                });
            }
            claims.insert(request.batch_id.clone(), scope.clone());
        }
        let mut hold = Hold {
            service,
            batch_id: request.batch_id.clone(),
            stored: false,
            local: true,
        };
        let reply = self
            .run_held(&request, &limits, scope, send, &mut hold, tool)
            .await;
        hold.release().await;
        reply
    }

    async fn snapshot(&self, batch_id: &str) -> Result<Option<StoredBatch>, Refusal> {
        let service = &self.service;
        let read = || async {
            service
                .stored_batch(batch_id)
                .await
                .map_err(|_| internal_refusal("batch replay is unavailable"))
        };
        let expired = |batch: &StoredBatch| {
            (service.clock)().saturating_sub(batch.admitted_at_ms)
                >= broca_store::system_one::RETENTION_MS
        };
        let snapshot = read().await?;
        if !snapshot.as_ref().is_some_and(expired) {
            return Ok(snapshot);
        }
        // An idle expired row may be reused even if the periodic sweep has not run yet.
        // A row held by another process stays out of replay until that process releases it.
        service
            .sweep()
            .await
            .map_err(|_| internal_refusal("batch retention is unavailable"))?;
        let snapshot = read().await?;
        if snapshot.as_ref().is_some_and(expired) {
            Err(in_progress())
        } else {
            Ok(snapshot)
        }
    }

    async fn run_held(
        &self,
        request: &ExtractRequest,
        limits: &Limits,
        scope: &BatchScope,
        send: &SendScope,
        hold: &mut Hold<'_>,
        tool: Option<&mut ToolCall<'_>>,
    ) -> Result<Box<RawValue>, Refusal> {
        let service = &self.service;
        let snapshot = self.snapshot(&request.batch_id).await?;
        let row = if let Some(stored) = &snapshot {
            if stored.scope.as_ref() != Some(scope) {
                return Err(scope_mismatch());
            }
            if stored.running {
                return Err(in_progress());
            }
            if let Some(tool) = &tool {
                tool.check_digest(&stored.args_digest)?;
                if !stored.outcomes.is_empty() {
                    return self.replay_tool(stored).await;
                }
            }
            None
        } else if tool.is_some() {
            // Tool admission already checked the catalog and froze the applicable price.
            None
        } else {
            Some(self.live_row(&request.model)?)
        };
        let model = self.model_entry(row.as_ref());
        check_request_limits(request, &model, limits, service.image_references())?;
        let identity =
            BatchIdentity::of(request).map_err(|e| Refusal::invalid_params("items", e.0))?;
        if let Some(stored) = &snapshot {
            check_identity(&identity, &stored.identity)?;
        }
        let bodies = self
            .prepare(
                request,
                &model,
                send,
                snapshot.as_ref(),
                tool.as_ref().map(|tool| tool.images),
            )
            .await?;
        let estimates = (0..request.items.len())
            .map(|index| {
                spend::item_cost_input(request, index, &model, BOUNDS, None, None)
                    .map_err(|e| Refusal::invalid_params("fields", e.0))
            })
            .collect::<Result<Vec<_>, _>>()?;

        service.publish_facts().await?;
        let admission_rate = if let Some(tool) = &tool {
            serde_json::from_str(&tool.admission.frozen_row)
                .map_err(|_| internal_refusal("the admitted Clairic price does not decode"))?
        } else {
            row.and_then(|r| r.input_rate_nanos)
                .filter(|rate| *rate >= 0)
        };
        let sent_ceiling = request.max_cost_usd.as_ref().map(|usd| {
            usd_to_nanos(usd, Rounding::Down)
                .unwrap_or(u64::MAX)
                .min(i64::MAX as u64)
        });
        let claimed = {
            let (store, role, owner, id, model, scope) = (
                service.store.clone(),
                service.role.clone(),
                service.owner.clone(),
                request.batch_id.clone(),
                request.model.clone(),
                scope.clone(),
            );
            let identity = serde_json::to_string(&identity).expect("identity encodes");
            let digest = tool.as_ref().map_or_else(
                || classify::sha256_hex(&identity),
                |tool| tool.args_digest.to_owned(),
            );
            let frozen = serde_json::to_string(&admission_rate).expect("the input rate encodes");
            let now_ms = (service.clock)();
            let admitted_at = tool.as_ref().and_then(|tool| tool.admitted_at);
            blocking(move || {
                let claim = BatchClaim {
                    role: &role,
                    batch_id: &id,
                    model: &model,
                    identity: &identity,
                    body_digest: &digest,
                    frozen_row: &frozen,
                    scope: &scope,
                    ceiling_nanos: sent_ceiling,
                    owner: &owner,
                    now_ms,
                };
                match admitted_at {
                    Some(at) => store.claim_admitted_at(&claim, at),
                    None => store.claim(&claim),
                }
            })
            .await?
        };
        let (fresh, rate, ceiling, spent, outcomes) = match claimed {
            Claimed::Expired | Claimed::InProgress => return Err(in_progress()),
            Claimed::ScopeMismatch => return Err(scope_mismatch()),
            Claimed::Reuse { recorded_identity } => {
                if tool.is_some() {
                    return Err(Refusal::batch_id_reuse("arguments"));
                }
                check_identity(&identity, &recorded_identity)?;
                return Err(internal_refusal(
                    "stored batch digest does not match its identity",
                ));
            }
            Claimed::Fresh => {
                hold.stored = true;
                if snapshot.is_some() {
                    // Retention removed the snapshot while this call was preparing. Retry as a
                    // fresh admission rather than sending on a stale model or unresolved images.
                    self.discard_fresh(hold).await?;
                    return Err(internal_refusal(
                        "batch expired during replay; re-send the batch",
                    ));
                }
                (true, admission_rate, sent_ceiling, 0, Vec::new())
            }
            Claimed::Resumed(record) => {
                hold.stored = true;
                if snapshot
                    .as_ref()
                    .is_some_and(|snapshot| snapshot.admitted_at_ms != record.admitted_at_ms)
                {
                    return Err(internal_refusal(
                        "batch expired during replay; re-send the batch",
                    ));
                }
                if tool.is_some() && !record.outcomes.is_empty() {
                    service.publish_facts().await?;
                    return tool::replay(&record.outcomes);
                }
                let rate = serde_json::from_str(&record.frozen_row)
                    .map_err(|_| internal_refusal("the recorded Clairic price does not decode"))?;
                (
                    false,
                    rate,
                    record.ceiling_nanos,
                    record.spent_nanos,
                    record.outcomes,
                )
            }
        };

        let mut batch = Dispatch {
            request,
            fresh,
            rate,
            ceiling,
            spent,
            tool: tool.is_some(),
            tool_result: None,
            spend: Spend::Nothing,
        };
        let items = self
            .dispatch(&mut batch, hold, outcomes, bodies, estimates)
            .await;
        if let Some(tool) = tool {
            tool.spend = batch.spend;
            items?;
            return batch
                .tool_result
                .ok_or_else(|| internal_refusal("the extraction has no tool outcome"))?;
        }
        let items = items?;
        let answered = || items.iter().filter(|item| item.answers.is_some());
        let cost = input_cost_nanos(
            answered().map(|item| item.usage.as_ref().and_then(|u| u.input_tokens)),
            rate,
        )
        .ok()
        .flatten();
        let usage = cortexkit_role_extractor::reply::sum_usage(answered());
        let mut reply = ExtractReply::new(&request.batch_id, &request.model, items);
        reply.usage = usage;
        reply.cost_usd = cost.map(nanos_to_usd);
        to_raw_value(&reply)
            .map_err(|_| internal_refusal("the extraction reply could not be encoded"))
    }

    async fn prepare(
        &self,
        request: &ExtractRequest,
        model: &ModelEntry,
        send: &SendScope,
        snapshot: Option<&StoredBatch>,
        prepared: Option<&super::images::PreparedImages>,
    ) -> Result<Vec<Option<Zeroizing<String>>>, Refusal> {
        let mut bodies = Vec::with_capacity(request.items.len());
        for (index, item) in request.items.iter().enumerate() {
            if snapshot.is_some_and(|batch| {
                batch
                    .outcomes
                    .iter()
                    .any(|outcome| outcome.index == index as u64 && retained(outcome))
            }) {
                bodies.push(None);
                continue;
            }
            if let Some(body) = prepared
                .and_then(|images| images.0.get(index))
                .and_then(Option::as_ref)
            {
                bodies.push(Some(Zeroizing::new(body.clone())));
            } else if prepared.is_some()
                && item
                    .images
                    .iter()
                    .flatten()
                    .any(|image| image.reference().is_some())
            {
                return Err(cortexkit_model_batch::image::ResolveFailure::Unavailable
                    .refusal(format!("items[{index}].images")));
            } else {
                bodies.push(Some(self.prepare_body(request, model, send, index).await?));
            }
        }
        Ok(bodies)
    }

    async fn prepare_body(
        &self,
        request: &ExtractRequest,
        model: &ModelEntry,
        send: &SendScope,
        index: usize,
    ) -> Result<Zeroizing<String>, Refusal> {
        let mut body = ProviderRequest::for_item(request, index, super::CLAIRIC_MODEL_PATH)
            .expect("the item exists");
        if let Some(images) = &request.items[index].images {
            let resolved = self.service.resolve_images(images, send, index).await?;
            request::check_resolved_item_images(model, index, &resolved)?;
            images::validate_dimensions(index, &resolved)?;
            if images.iter().any(|image| image.reference().is_some()) {
                body = body.with_resolved_images(&resolved);
                provider::check_provider_body(index, &body, clairic_limits::MAX_BODY_BYTES)?;
            }
        }
        Ok(Zeroizing::new(
            serde_json::to_string(&body).expect("the request encodes"),
        ))
    }

    fn live_row(&self, model: &str) -> Result<ModelBatchRow, Refusal> {
        let row = self.service.catalog.row(model);
        let kind = match &row {
            None => CatalogRow::Absent,
            Some(row) => row
                .kind
                .as_deref()
                .map_or(CatalogRow::Kindless, CatalogRow::Kind),
        };
        if let Err(mut refusal) = request::check_catalog_row(model, kind) {
            if refusal.code == errors::MODEL_UNKNOWN {
                refusal.message.push_str(&format!(
                    "; accepted extraction models: {}",
                    ACCEPTED_MODELS.join(", ")
                ));
            }
            return Err(refusal);
        }
        if !ACCEPTED_MODELS.contains(&model) {
            let mut refusal = Refusal::model_unknown(model);
            refusal.message = format!(
                "{model} is not a supported extraction model; accepted extraction models: {}",
                ACCEPTED_MODELS.join(", ")
            );
            return Err(refusal);
        }
        Ok(row.expect("catalog admission checked the row"))
    }

    async fn discard_fresh(&self, hold: &mut Hold<'_>) -> Result<(), Refusal> {
        let id = hold.batch_id.clone();
        let (store, role, owner) = (
            self.service.store.clone(),
            self.service.role.clone(),
            self.service.owner.clone(),
        );
        blocking(move || store.discard_fresh(&role, &id, &owner)).await?;
        hold.stored = false;
        Ok(())
    }

    async fn dispatch(
        &self,
        batch: &mut Dispatch<'_>,
        hold: &mut Hold<'_>,
        stored: Vec<StoredOutcome>,
        bodies: Vec<Option<Zeroizing<String>>>,
        estimates: Vec<ItemSpendInput>,
    ) -> Result<Vec<ItemResult>, Refusal> {
        let request = batch.request;
        let mut kept = vec![None; request.items.len()];
        for outcome in stored.into_iter().filter(retained) {
            let slot = kept.get_mut(outcome.index as usize).ok_or_else(|| {
                internal_refusal("a stored extraction outcome has no request item")
            })?;
            let (Outcome::Answered(body) | Outcome::Failed(body)) = outcome.outcome;
            *slot =
                Some(serde_json::from_str::<ItemResult>(&body).map_err(|_| {
                    internal_refusal("a stored extraction outcome does not decode")
                })?);
        }
        // Usually a missing prepared body means the item has a retained outcome. If retention
        // replaced the batch while preparing a replay, don't send a request with missing images.
        if kept
            .iter()
            .zip(&bodies)
            .any(|(item, body)| item.is_none() && body.is_none())
        {
            return Err(internal_refusal(
                "batch changed during replay; re-send the batch",
            ));
        }

        let mut items = Vec::with_capacity(request.items.len());
        let mut pending = Vec::new();
        let mut first_call = true;
        let mut stopped: Option<ItemError> = None;
        for (index, ((retained, body), estimate)) in
            kept.into_iter().zip(bodies).zip(estimates).enumerate()
        {
            let i = index as u64;
            if let Some(item) = retained {
                items.push(item);
                continue;
            }
            if let Some(error) = &stopped {
                let mut unsent = ItemError::for_code(
                    &error.code,
                    format!(
                        "not sent: an earlier item of this call ended {}",
                        error.code
                    ),
                );
                unsent.retry_after_ms = error.retry_after_ms;
                items.push(ItemResult::failed(i, unsent));
                continue;
            }
            if let Err(error) =
                spend::check_before_call(&estimate, batch.rate, batch.spent, batch.ceiling)
            {
                let item = ItemResult::failed(i, error);
                if batch.tool {
                    self.record_tool(batch, &item, Payment::default(), Some(Spend::Nothing))
                        .await?;
                } else if first_call {
                    pending.push(item.clone());
                } else {
                    self.record(&request.batch_id, &item, Payment::default())
                        .await?;
                }
                items.push(item);
                continue;
            }

            let body = body.expect("unanswered items have a prepared provider request");
            batch.spend = Spend::Unknown;
            let answer = clairic::call(&self.service, &self.clairic, CLAIRIC, &body).await;
            if first_call {
                first_call = false;
                if let Err(failed) = &answer {
                    if errors::is_batch_level(&failed.error.code) {
                        let refusal = classify::first_call_refusal(
                            &bounded_error(failed.error.clone()),
                            CLAIRIC,
                            self.clairic.as_ref(),
                        );
                        if batch.tool {
                            batch.spend = if failed.may_have_spent {
                                Spend::Unknown
                            } else {
                                Spend::Nothing
                            };
                            let debit = if failed.may_have_spent {
                                super::unknown_spend_debit(batch.ceiling)
                            } else {
                                0
                            };
                            let refusal = batch.spend.mark(refusal);
                            self.store_tool_outcome(
                                batch,
                                Err(refusal),
                                Payment {
                                    debit,
                                    ..Payment::default()
                                },
                            )
                            .await?;
                            return Ok(items);
                        }
                        if batch.fresh {
                            self.discard_fresh(hold).await?
                        }
                        return Err(refusal);
                    }
                }
                // A refusal on the first provider call writes no outcomes, even if earlier
                // items crossed the ceiling without dispatch. We can now commit those errors.
                for item in pending.drain(..) {
                    self.record(&request.batch_id, &item, Payment::default())
                        .await?;
                }
            }
            match answer {
                Ok(answer) => {
                    let usage = answer.result.usage.clone();
                    let mut item = answer.result.into_item(i);
                    item.state_text = request
                        .effective_options()
                        .state_text
                        .then(|| request.items[index].state.clone());
                    let item = fit_answer(item, request, MAX_ANSWER_BYTES);
                    // A malformed result can be retried. Unlike a valid but oversized answer,
                    // it does not have a final measurement to publish under this item's id.
                    let final_outcome = batch.tool
                        || item
                            .error
                            .as_ref()
                            .is_none_or(|error| !error.is_retried_on_resend());
                    let usage = usage.as_ref().filter(|_| final_outcome);
                    let measured = batch
                        .rate
                        .zip(usage.and_then(|u| u.input_tokens))
                        .and_then(|(rate, tokens)| cost_nanos(tokens, rate).ok());
                    let debit =
                        measured.unwrap_or_else(|| super::unknown_spend_debit(batch.ceiling));
                    let fact = final_outcome
                        .then(|| {
                            classify::fact_context(
                                &self.service,
                                CLAIRIC,
                                answer.auth_selection,
                                answer.account_ref,
                                measured.unwrap_or(0),
                            )
                        })
                        .flatten();
                    let paid = Payment { debit, usage, fact };
                    if batch.tool {
                        batch.spend = measured.map_or(Spend::Unknown, |nanos| {
                            crate::llm_tools::spend::ItemSpend::Measured(nanos).into()
                        });
                        self.record_tool(batch, &item, paid, measured.map(|_| batch.spend))
                            .await?;
                    } else {
                        self.record(&request.batch_id, &item, paid).await?;
                    }
                    batch.spent = batch.spent.saturating_add(debit);
                    items.push(item);
                }
                Err(failed) => {
                    let error = bounded_error(failed.error);
                    if error.stops_the_call() {
                        stopped = Some(error.clone())
                    }
                    let stored = error.is_stored();
                    let item = ItemResult::failed(i, error);
                    if stored || batch.tool {
                        let debit = if failed.may_have_spent {
                            super::unknown_spend_debit(batch.ceiling)
                        } else {
                            0
                        };
                        let paid = Payment {
                            debit,
                            ..Payment::default()
                        };
                        if batch.tool {
                            batch.spend = if failed.may_have_spent {
                                Spend::Unknown
                            } else {
                                Spend::Nothing
                            };
                            self.record_tool(batch, &item, paid, Some(batch.spend))
                                .await?;
                        } else {
                            self.record(&request.batch_id, &item, paid).await?;
                        }
                        batch.spent = batch.spent.saturating_add(debit);
                    }
                    items.push(item);
                }
            }
        }
        for item in pending {
            self.record(&request.batch_id, &item, Payment::default())
                .await?;
        }
        Ok(items)
    }

    async fn record(&self, id: &str, item: &ItemResult, paid: Payment<'_>) -> Result<(), Refusal> {
        let body = serde_json::to_string(item).expect("item outcomes encode");
        classify::record_paid(
            &self.service,
            id,
            &StoredOutcome {
                index: item.index,
                outcome: if item.error.is_none() {
                    Outcome::Answered(body)
                } else {
                    Outcome::Failed(body)
                },
                input_tokens: paid.usage.and_then(|u| u.input_tokens),
                output_tokens: paid.usage.and_then(|u| u.output_tokens),
            },
            paid.debit,
            paid.fact,
        )
        .await
    }
}

#[derive(Default)]
struct Payment<'a> {
    debit: u64,
    usage: Option<&'a Usage>,
    fact: Option<broca_store::system_one::FactContext>,
}

struct Dispatch<'a> {
    request: &'a ExtractRequest,
    fresh: bool,
    rate: Option<i64>,
    ceiling: Option<u64>,
    spent: u64,
    tool: bool,
    tool_result: Option<Result<Box<RawValue>, Refusal>>,
    spend: Spend,
}

fn in_progress() -> Refusal {
    Refusal::batch_in_progress(BATCH_IN_PROGRESS_RETRY_AFTER_MS)
}

fn scope_mismatch() -> Refusal {
    Refusal::new(
        "batch_scope_mismatch",
        "batch_id belongs to a different scope; a retry cannot change its attribution",
    )
}

fn check_request_limits(
    request: &ExtractRequest,
    model: &ModelEntry,
    limits: &Limits,
    references: bool,
) -> Result<(), Refusal> {
    if references {
        request::check_request_with_references(request, model, limits)?;
    } else {
        request::check_request(request, model, limits)?;
    }
    provider::check_provider_bodies(request, clairic_limits::MAX_BODY_BYTES)?;
    request::check_reply_size(request, model)
}

fn check_identity(identity: &BatchIdentity, recorded: &str) -> Result<(), Refusal> {
    let recorded: BatchIdentity = serde_json::from_str(recorded)
        .map_err(|_| internal_refusal("a recorded batch identity does not decode"))?;
    match recorded.first_difference(identity) {
        Some(field) => Err(Refusal::batch_id_reuse(field)),
        None => Ok(()),
    }
}

fn retained(outcome: &StoredOutcome) -> bool {
    match &outcome.outcome {
        Outcome::Answered(_) => true,
        Outcome::Failed(body) => serde_json::from_str::<ItemResult>(body)
            .ok()
            .and_then(|item| item.error)
            .is_none_or(|error| !error.is_retried_on_resend()),
    }
}

fn fit_answer(mut item: ItemResult, request: &ExtractRequest, limit: u64) -> ItemResult {
    let answers = item.answers.take();
    let state_text = item.state_text.take();
    for (field, answer) in answers.iter().flatten() {
        if !request.fields.contains_key(field) {
            return ItemResult::failed(
                item.index,
                ItemError::for_code(
                    errors::PROVIDER_ERROR,
                    "Clairic answered a field that was not requested",
                ),
            );
        }
        let size = serde_json::to_vec(answer).expect("an answer encodes").len() as u64;
        if size > limit {
            let mut failed = ItemResult::failed(
                item.index,
                ItemError::answer_too_large(format!("fields.{field}"), limit, size),
            );
            // A paid but undeliverable answer still has usage. Very large extension members
            // can't fit the reply allowance, but all measured counts stay in the ledger.
            failed.usage = item.usage;
            if serde_json::to_vec(&failed)
                .expect("a failed item encodes")
                .len()
                > ITEM_METADATA_BYTES
            {
                failed.usage = None;
            }
            return failed;
        }
    }
    // Answer keys and state text were budgeted at admission. The shared allowance also has
    // to hold the item's usage (which can have provider-defined extension members).
    if serde_json::to_vec(&item)
        .expect("item metadata encodes")
        .len()
        > ITEM_METADATA_BYTES - 128
    {
        return ItemResult::failed(
            item.index,
            ItemError::for_code(
                errors::PROVIDER_ERROR,
                "Clairic reported more usage metadata than the reply can carry",
            ),
        );
    }
    item.answers = answers;
    item.state_text = state_text;
    item
}

fn bounded_error(mut error: ItemError) -> ItemError {
    let size = |error: &ItemError| {
        serde_json::to_vec(error)
            .expect("an item error encodes")
            .len()
    };
    if size(&error) > ITEM_METADATA_BYTES - 32 {
        // The error starts with the status and provider code. Keep that diagnosis even if the
        // provider's free text would otherwise exceed the frame allowance for this item.
        error.message = error.message.chars().take(120).chain(['…']).collect();
    }
    error
}

#[cfg(test)]
mod tests;
