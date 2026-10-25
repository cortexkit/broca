use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use broca_protocol::{BindPrincipal, SendScope};
use broca_store::system_one::{BatchScope, Outcome, StoredBatch};
use cortexkit_llm_tools::classify;
use cortexkit_role_classifier::errors::Refusal;
use serde_json::{json, value::RawValue, Value};
use subc_protocol::tool_call::ToolCallRequest;
use tokio::sync::watch;

use super::admission::{admit, hash, refusal, Admission};

type Answer = Result<Box<RawValue>, Vec<u8>>;

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
    flights: Mutex<HashMap<String, Flight>>,
}

impl LlmTools {
    pub fn new(
        system: Arc<crate::system_one::SystemOne>,
        config: crate::config::LlmToolsConfig,
    ) -> Self {
        assert_eq!(
            system.store_role(),
            crate::system_one::LLM_CLASSIFY_STORE_ROLE
        );
        Self {
            system,
            config,
            flights: Mutex::new(HashMap::new()),
        }
    }

    pub async fn answer(
        self: &Arc<Self>,
        body: &[u8],
        principal: Option<&BindPrincipal>,
        scope: &SendScope,
    ) -> Answer {
        let call: ToolCallRequest = serde_json::from_slice(body)
            .map_err(|e| error(Refusal::invalid_params("request", e.to_string())))?;
        match call.name.as_str() {
            "role.describe" => raw(json!({
                "majors": [{"version":"tool-provider/v1", "ops":["role.describe", "tool.catalog", "late_results", "late_results.ack"], "stability":"alpha"}],
                "implementation_version": env!("CARGO_PKG_VERSION"),
            })),
            "tool.catalog" => self.catalog(&call.arguments),
            "late_results" => {
                let since = &call.arguments["since"];
                if !since.is_null()
                    && (since["provider_incarnation"] != crate::serve::process_launch_id()
                        || since["seq"] != 0)
                {
                    return Err(error(refusal(
                        "cursor_incarnation_changed",
                        "late-results cursor is not from this provider incarnation",
                    )));
                }
                raw(
                    json!({"entries":[], "cursor":{"provider_incarnation":crate::serve::process_launch_id(),"seq":0}, "more":false}),
                )
            }
            "late_results.ack" => {
                let through = &call.arguments["through"];
                if through["provider_incarnation"] != crate::serve::process_launch_id()
                    || through["seq"] != 0
                {
                    return Err(error(refusal(
                        "cursor_incarnation_changed",
                        "late-results acknowledgement is not from this provider incarnation",
                    )));
                }
                raw(json!({}))
            }
            "llm.classify/v1" => {
                let envelope: RawEnvelope<'_> = serde_json::from_slice(body)
                    .map_err(|e| error(Refusal::invalid_params("arguments", e.to_string())))?;
                let admission = admit(
                    principal,
                    scope,
                    envelope.arguments,
                    call.call_key.as_deref(),
                )
                .map_err(error)?;
                let pin = cortexkit_role_tool_provider::call::check_call(&call)
                    .map_err(|error| serde_json::to_vec(&error).expect("call refusal encodes"))?;
                if let Some(pin) = pin {
                    let digest = cortexkit_role_tool_provider::catalog::schema_digest(
                        &classify::input_schema(),
                    )
                    .expect("contract schema has a digest");
                    if pin.schema_digest != digest {
                        return Err(serde_json::to_vec(&json!({"code":"tool_schema_changed", "message":"the pinned tool schema is no longer served", "detail":{"tool":call.name, "expected":pin.schema_digest, "current":digest}})).expect("refusal encodes"));
                    }
                    if pin.semantics != 1 {
                        return Err(serde_json::to_vec(&json!({"code":"tool_semantics_changed", "message":"the pinned tool semantics are no longer served", "detail":{"tool":call.name, "expected":pin.semantics, "current":1}})).expect("refusal encodes"));
                    }
                }
                self.call(admission, call.call_key.is_some()).await
            }
            _ => Err(serde_json::to_vec(&subc_protocol::ErrorBody::new(
                "unknown_method",
                format!("tool-provider does not serve {}", call.name),
            ))
            .expect("error encodes")),
        }
    }

    fn catalog(&self, arguments: &Value) -> Answer {
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
        let schema = classify::input_schema();
        let mut tool = json!({
            "name":"llm.classify/v1", "schema_digest": cortexkit_role_tool_provider::catalog::schema_digest(&schema).expect("contract schema has a digest"),
            "semantics":1, "capabilities":[], "description":"Classify one item with a decision model.",
            "input_schema":schema,
            "modes":cortexkit_llm_tools::MODES,
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
            return raw(json!({"generation":generation,"catalog_digest":digest}));
        }
        answer["catalog_digest"] = json!(digest);
        raw(answer)
    }

    async fn call(self: &Arc<Self>, admission: Admission, keyed: bool) -> Answer {
        // Read replay before catalog, pricing or private-content policy. A changed catalog or
        // policy must never turn an already-paid answer into a new provider attempt.
        let stored = if keyed {
            self.system
                .stored_batch(&admission.batch_id)
                .await
                .map_err(|_| {
                    error(crate::system_one::internal_refusal(
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
            check_replay(stored, &admission).map_err(error)?;
            if has_final_outcome(stored) {
                return convert(
                    &self
                        .system
                        .replay_tool(&admission.params, stored)
                        .map_err(error)?,
                );
            }
        }
        let running = {
            let flights = self.flights.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(flight) = flights.get(&admission.batch_id) {
                if flight.scope != admission.scope {
                    return Err(error(refusal(
                        "batch_scope_mismatch",
                        "batch_id is executing under another scope",
                    )));
                }
                if flight.digest != admission.args_digest {
                    return Err(error(Refusal::batch_id_reuse("arguments")));
                }
                Some(flight.result.clone())
            } else {
                None
            }
        };
        if let Some(receiver) = running {
            return await_flight(receiver).await;
        }
        let pricing = self
            .system
            .admit_tool(&admission.params, stored.as_ref())
            .map_err(error)?;
        // Until this route can determine whether a call contains private content, require
        // every provider to be operator-listed. This fallback creates no consent request,
        // approval card, or persisted held-call record.
        if stored.is_none()
            && !self
                .config
                .private_content_providers
                .contains(&admission.provider)
        {
            let mut body = serde_json::to_value(refusal(
                "not_started",
                "private-content status is unknown for this provider",
            ))
            .expect("refusal encodes");
            body["detail"]["consent"] = json!("private_content_unknown");
            return Err(serde_json::to_vec(&body).expect("refusal encodes"));
        }
        let receiver = {
            let mut flights = self.flights.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(flight) = flights.get(&admission.batch_id) {
                if flight.scope != admission.scope {
                    return Err(error(refusal(
                        "batch_scope_mismatch",
                        "batch_id is executing under another scope",
                    )));
                }
                if flight.digest != admission.args_digest {
                    return Err(error(Refusal::batch_id_reuse("arguments")));
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
                    let execution = tokio::spawn(async move {
                        system
                            .execute_tool(
                                admission.params,
                                admission.scope,
                                admission.args_digest,
                                pricing,
                                None,
                            )
                            .await
                    })
                    .await;
                    let answer = match execution {
                        Ok(Ok(reply)) => convert(&reply),
                        Ok(Err(refusal)) => Err(error(refusal)),
                        Err(_) => Err(error(crate::system_one::internal_refusal(
                            "model tool execution failed; retry the call",
                        ))),
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
}

async fn await_flight(mut receiver: watch::Receiver<Option<Answer>>) -> Answer {
    loop {
        if let Some(answer) = receiver.borrow_and_update().clone() {
            return answer;
        }
        receiver.changed().await.map_err(|_| {
            error(crate::system_one::internal_refusal(
                "model tool execution stopped",
            ))
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

fn error(refusal: Refusal) -> Vec<u8> {
    crate::system_one::refusal_body(&refusal)
}

fn catalog_error(field: &str, message: String) -> Vec<u8> {
    let mut body = subc_protocol::ErrorBody::new("invalid_request", message);
    body.detail = Some(json!({"field":field}));
    serde_json::to_vec(&body).expect("catalog refusal encodes")
}

fn raw(value: Value) -> Answer {
    serde_json::value::to_raw_value(&value).map_err(|_| {
        error(crate::system_one::internal_refusal(
            "model tool reply cannot encode",
        ))
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
        error: Option<cortexkit_role_classifier::errors::ItemError>,
        usage: Option<cortexkit_llm_tools::Usage>,
    }
    #[derive(serde::Serialize)]
    struct ToolResult<'a> {
        answers: &'a RawValue,
        usage: cortexkit_llm_tools::Usage,
        cost_usd: serde_json::Number,
    }
    let batch: BatchReply<'_> = serde_json::from_str(reply.get()).map_err(|_| {
        error(crate::system_one::internal_refusal(
            "stored model reply cannot decode",
        ))
    })?;
    let item = batch.items.into_iter().next().ok_or_else(|| {
        error(crate::system_one::internal_refusal(
            "stored model reply has no item",
        ))
    })?;
    if let Some(failure) = item.error {
        let mut refusal = Refusal::new(failure.code, failure.message);
        refusal.detail.class = failure.class;
        refusal.detail.retry_after_ms = failure.retry_after_ms;
        return Err(error(refusal));
    }
    let Some(cost_usd) = batch.cost_usd else {
        return Err(error(super::unmeasured_cost_refusal()));
    };
    let answers = item.answers.ok_or_else(|| {
        error(crate::system_one::internal_refusal(
            "stored model reply has no answers",
        ))
    })?;
    // Validate the shared result type without re-serializing provider answers. Unknown fields,
    // object order and JSON number spellings remain exactly as the provider returned them.
    let result = serde_json::value::to_raw_value(&ToolResult {
        answers,
        usage: item.usage.unwrap_or_default(),
        cost_usd,
    })
    .map_err(|_| {
        error(crate::system_one::internal_refusal(
            "model tool reply cannot encode",
        ))
    })?;
    let parsed: classify::Result = serde_json::from_str(result.get()).map_err(|_| {
        error(
            cortexkit_llm_tools::errors::refusal(
                "provider_error",
                "the provider returned a malformed classification result",
            )
            .expect("the contract classifies provider_error"),
        )
    })?;
    classify::validate_result(&parsed).map_err(|_| {
        error(
            cortexkit_llm_tools::errors::refusal(
                "provider_error",
                "the provider's classification result failed validation",
            )
            .expect("the contract classifies provider_error"),
        )
    })?;
    Ok(result)
}
