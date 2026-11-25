//! Tool-only admission and outcomes, sharing the extractor's provider and spend guard.

use cortexkit_llm_tools::{errors::COST_UNPRICEABLE, extract as contract, Usage as ToolUsage};
use cortexkit_role_extractor::errors::RefusalDetail;
use serde_json::{json, Value};

use super::*;
use crate::system_one::{images::PreparedImages, GaugeGuard, ToolAdmission};

const TOOL_STORE_ROLE: &str = "llm.extract";

pub(super) struct ToolCall<'a> {
    pub(super) args_digest: &'a str,
    pub(super) admission: &'a ToolAdmission,
    pub(super) admitted_at: Option<i64>,
    pub(super) images: &'a PreparedImages,
    pub(super) spend: Spend,
}

impl ToolCall<'_> {
    pub(super) fn check_digest(&self, stored: &str) -> Result<(), Refusal> {
        if self.args_digest == stored {
            Ok(())
        } else {
            Err(Refusal::batch_id_reuse("arguments"))
        }
    }
}

impl SystemOne {
    pub(in crate::system_one) fn fork_tool_extractor(&self, tools: &mut Self) {
        if let Some(extractor) = &self.extractor {
            tools.extractor = Some(tools.extractor_for(extractor.clairic.clone(), TOOL_STORE_ROLE));
        }
    }

    pub(crate) fn tool_extractor(&self) -> Option<&Extractor> {
        self.extractor
            .as_ref()
            .filter(|extractor| extractor.service.role == TOOL_STORE_ROLE)
    }
}

impl Extractor {
    pub(crate) fn admit_tool(
        &self,
        params: &str,
        stored: Option<&StoredBatch>,
    ) -> Result<ToolAdmission, Refusal> {
        let request = self.tool_request(params)?;
        let frozen_row = if let Some(stored) = stored {
            stored.frozen_row.clone()
        } else {
            let row = self.live_row(&request.model)?;
            use broca_core::{ChargeBasis, ChargeBasisResolver};
            let (basis, _) = self
                .service
                .charge_basis
                .resolve_charge_basis(super::super::workers_ai::PROVIDER_ID, "apikey");
            if basis != ChargeBasis::MeteredApi
                || row.input_rate_nanos.is_none_or(|rate| rate < 0)
                || row.output_rate_nanos.is_some_and(|rate| rate > 0)
            {
                return Err(cortexkit_llm_tools::errors::refusal(
                    COST_UNPRICEABLE,
                    "Clairic requires a per-token input price and no positive output price",
                )
                .expect("the contract classifies cost_unpriceable"));
            }
            serde_json::to_string(&row.input_rate_nanos).expect("the input rate encodes")
        };
        Ok(ToolAdmission { frozen_row })
    }

    fn tool_request(&self, params: &str) -> Result<ExtractRequest, Refusal> {
        let limits = self.limits();
        request::check_request_bytes(params.len(), &limits)?;
        let request = request::parse_request(params)?;
        if request.items.len() != 1 || request.max_cost_usd.is_none() {
            return Err(internal_refusal(
                "an extraction tool call needs one item and a ceiling",
            ));
        }
        check_request_limits(
            &request,
            &self.model_entry(None),
            &limits,
            self.service.image_references(),
        )
        .map_err(item_field)?;
        Ok(request)
    }

    /// Retain only bodies with image bytes resolved using the caller's daemon-stamped scope.
    /// Inline images can be validated again from the durable request without a resolver.
    pub(crate) async fn prepare_tool_images(
        &self,
        params: &str,
        send: &SendScope,
    ) -> Result<PreparedImages, Refusal> {
        let request = self.tool_request(params)?;
        let references = request.items[0]
            .images
            .iter()
            .flatten()
            .any(|image| image.reference().is_some());
        let body = if references {
            Some(
                self.prepare_body(&request, &self.model_entry(None), send, 0)
                    .await
                    .map_err(item_field)?
                    .to_string(),
            )
        } else {
            None
        };
        Ok(PreparedImages(vec![body]))
    }

    pub(crate) async fn replay_tool(&self, stored: &StoredBatch) -> Result<Box<RawValue>, Refusal> {
        self.service.publish_facts().await?;
        replay(&stored.outcomes)
    }

    pub(crate) async fn execute_tool(
        &self,
        params: &str,
        scope: &BatchScope,
        args_digest: &str,
        admission: &ToolAdmission,
        admitted_at: Option<i64>,
        images: &PreparedImages,
    ) -> Result<Box<RawValue>, Refusal> {
        let mut tool = ToolCall {
            args_digest,
            admission,
            admitted_at,
            images,
            spend: Spend::Nothing,
        };
        let gauge = self.service.in_flight.clone();
        gauge.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let _lowered = GaugeGuard(gauge);
        let _slot = self.service.call_slots.acquire().await;
        self.run_inner(params, scope, &SendScope::Internal, Some(&mut tool))
            .await
            .map_err(|refusal| tool.spend.mark_undeclared(item_field(refusal)))
    }

    pub(super) async fn record_tool(
        &self,
        batch: &mut Dispatch<'_>,
        item: &ItemResult,
        paid: Payment<'_>,
        spend: Option<Spend>,
    ) -> Result<(), Refusal> {
        let reply = tool_outcome(batch.request, item, paid.debit, spend);
        self.store_tool_outcome(batch, reply, paid).await
    }

    pub(super) async fn store_tool_outcome(
        &self,
        batch: &mut Dispatch<'_>,
        reply: Result<Box<RawValue>, Refusal>,
        paid: Payment<'_>,
    ) -> Result<(), Refusal> {
        let outcome = match &reply {
            Ok(body) => Outcome::Answered(body.get().to_owned()),
            Err(refusal) => {
                Outcome::Failed(serde_json::to_string(refusal).expect("the refusal encodes"))
            }
        };
        classify::record_paid(
            &self.service,
            &batch.request.batch_id,
            &StoredOutcome {
                index: 0,
                outcome,
                input_tokens: paid.usage.and_then(|usage| usage.input_tokens),
                output_tokens: paid.usage.and_then(|usage| usage.output_tokens),
            },
            paid.debit,
            paid.fact,
        )
        .await?;
        batch.tool_result = Some(reply);
        Ok(())
    }
}

pub(super) fn replay(outcomes: &[StoredOutcome]) -> Result<Box<RawValue>, Refusal> {
    let unreadable = || {
        Spend::Unknown.mark(internal_refusal(
            "the stored extraction tool outcome does not decode",
        ))
    };
    match outcomes {
        [StoredOutcome {
            index: 0,
            outcome: Outcome::Answered(body),
            ..
        }] => RawValue::from_string(body.clone()).map_err(|_| unreadable()),
        [StoredOutcome {
            index: 0,
            outcome: Outcome::Failed(body),
            ..
        }] => Err(serde_json::from_str(body).map_err(|_| unreadable())?),
        _ => Err(unreadable()),
    }
}

fn tool_outcome(
    request: &ExtractRequest,
    item: &ItemResult,
    debit: u64,
    spend: Option<Spend>,
) -> Result<Box<RawValue>, Refusal> {
    if let Some(error) = &item.error {
        return Err(match spend {
            Some(spend) => spend.mark(item_refusal(error)),
            None => crate::llm_tools::unmeasured_cost_refusal(),
        });
    }
    let Some(spend) = spend else {
        return Err(crate::llm_tools::unmeasured_cost_refusal());
    };
    let answers = item
        .answers
        .as_ref()
        .filter(|answers| {
            answers.len() == request.fields.len()
                && request.fields.iter().all(|(id, field)| {
                    answers
                        .get(id)
                        .is_some_and(|answer| answer.kind == field.kind)
                })
        })
        .ok_or_else(|| {
            spend.mark(
                cortexkit_llm_tools::errors::refusal(
                    errors::PROVIDER_ERROR,
                    "Clairic did not answer each requested field with its requested type",
                )
                .expect("the contract classifies provider_error"),
            )
        })?;
    let result = contract::Result {
        answers: answers.clone(),
        truncated: item.truncated,
        state_text: item.state_text.clone(),
        usage: ToolUsage {
            input_tokens: item.usage.as_ref().and_then(|usage| usage.input_tokens),
            output_tokens: item.usage.as_ref().and_then(|usage| usage.output_tokens),
            ..ToolUsage::default()
        },
        cost_usd: nanos_to_usd(debit),
    };
    to_raw_value(&result).map_err(|_| {
        spend.mark(internal_refusal(
            "the extraction tool result could not be encoded",
        ))
    })
}

fn item_refusal(error: &ItemError) -> Refusal {
    let mut detail = serde_json::to_value(&error.detail).expect("item error details encode");
    if !detail.is_object() {
        detail = Value::Object(Default::default());
    }
    detail["class"] = json!(error.class);
    let mut detail: RefusalDetail =
        serde_json::from_value(detail).unwrap_or_else(|_| RefusalDetail::new(error.class.clone()));
    detail.retry_after_ms = error.retry_after_ms;
    Refusal::new(&error.code, &error.message).with_detail(detail)
}

fn item_field(mut refusal: Refusal) -> Refusal {
    if let Some(field) = refusal.detail.field.as_mut() {
        if let Some(suffix) = field.strip_prefix("items[0].") {
            *field = format!("item.{suffix}");
        }
    }
    refusal
}
