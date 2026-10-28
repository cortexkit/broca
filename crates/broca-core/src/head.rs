//! Small, replayable head facts, committed with the projection's exact WAL watermark.

use broca_protocol::RunId;
use broca_wal::WalRecord;
use cortexkit_role_llm_runner::{read::HeadModel, usage::SessionUsage};
use serde::{Deserialize, Serialize};

/// A content-free retained summary with a watermark separate from transcript projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetainedHeadCheckpoint {
    pub lineage_id: String,
    pub at_seq: u64,
    pub facts: HeadProjection,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HeadProjection {
    pub model: Option<HeadModel>,
    pub usage: Option<SessionUsage>,
    run_id: Option<RunId>,
    ownership: crate::SessionOwnershipV1,
    senders: Vec<cortexkit_role_llm_runner::delivery_order::PlanSender>,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    queued_times: std::collections::BTreeMap<String, u64>,
}

impl Default for HeadProjection {
    fn default() -> Self {
        Self {
            model: None,
            usage: None,
            run_id: None,
            ownership: crate::SessionOwnershipV1::default().for_owner_access(),
            senders: vec![],
            queued_times: Default::default(),
        }
    }
}

impl HeadProjection {
    pub fn queued_times(&self) -> &std::collections::BTreeMap<String, u64> {
        &self.queued_times
    }

    pub fn observe(&mut self, record: &WalRecord) {
        match record {
            WalRecord::RunStarted {
                run_id,
                config,
                principal,
                scope,
                manifest,
                submission_id,
                ..
            } => {
                if let Some(id) = submission_id {
                    self.queued_times.remove(&id.0);
                }
                if self.ownership.episodes == 0 {
                    self.senders = manifest
                        .as_ref()
                        .and_then(|manifest| {
                            serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text())
                                .ok()
                        })
                        .and_then(|plan| {
                            plan.as_object().and_then(|plan| {
                                cortexkit_role_llm_runner::delivery_order::plan_senders(plan).ok()
                            })
                        })
                        .unwrap_or_default();
                    for sender in &mut self.senders {
                        sender.templates.clear();
                    }
                }
                self.ownership
                    .fold_run_started(principal.as_ref(), scope.as_ref());
                self.run_id = Some(run_id.clone());
                let mut model =
                    HeadModel::new(&config.model.provider_module_id, &config.model.model_id);
                model.variant = config.variant.clone().filter(|value| !value.is_empty());
                self.model = Some(model);
            }
            // A refused call was billed, so it counts as a call of the model that refused; a
            // step another model answered counts under that model.
            WalRecord::ModelStepFinished { .. } | WalRecord::ModelStepRefused { .. } => {
                if let (Some(run_id), Some(model)) = (&self.run_id, &self.model) {
                    let episode = broca_protocol::request::ModelHandle::new(
                        model.provider.as_str(),
                        model.model.as_str(),
                    );
                    for (provider, model, variant, usage) in crate::refusal_fallback::usage_by_model(
                        &episode,
                        model.variant.as_deref(),
                        record,
                    ) {
                        self.usage
                            .get_or_insert_with(|| SessionUsage::new(&run_id.0))
                            .record_step(
                                provider,
                                model,
                                variant,
                                crate::replay::reported_step_usage(&usage),
                            );
                    }
                }
            }
            WalRecord::Queued {
                submission_id,
                written_at_ms,
                ts_ms,
                ..
            } => {
                if let Some(at) = written_at_ms.or(*ts_ms).filter(|at| *at != 0) {
                    self.queued_times.insert(submission_id.0.clone(), at);
                }
            }
            WalRecord::Retracted { submission_id, .. }
            | WalRecord::SendEnded { submission_id, .. }
            | WalRecord::SendDelivered { submission_id, .. } => {
                self.queued_times.remove(&submission_id.0);
            }
            _ => {}
        }
    }

    pub fn sender(
        &self,
        principal: Option<&broca_protocol::BindPrincipal>,
        scope: &broca_protocol::SendScope,
        modes: &[cortexkit_role_llm_runner::delivery_order::Mode],
    ) -> Option<cortexkit_role_llm_runner::read::HeadSender> {
        use cortexkit_role_llm_runner::{delivery_order, read::HeadSender};
        if self.ownership.episodes == 0 {
            return None;
        }
        if crate::scope_admission::decide_owner_access(&self.ownership, principal, scope).is_ok() {
            return Some(HeadSender::for_owner(
                modes.iter().map(|mode| mode.as_str().into()).collect(),
            ));
        }
        let broca_protocol::BindPrincipal::Reserved { module_id } = principal? else {
            return None;
        };
        let owner = self
            .ownership
            .scope
            .as_ref()
            .map(|scope| &scope.owner)
            .or(self.ownership.first_principal.as_ref())?
            .as_owner_string();
        let granted = modes
            .iter()
            .filter(|mode| {
                delivery_order::admit(module_id, Some(&owner), &self.senders, &[**mode]).is_ok()
            })
            .map(|mode| mode.as_str().to_owned())
            .collect::<Vec<_>>();
        self.senders
            .iter()
            .find(|sender| &sender.module == module_id)?;
        let interrupt = delivery_order::admit_delivery(
            module_id,
            Some(&owner),
            &self.senders,
            cortexkit_role_llm_runner::send::Delivery::Interrupt,
        )
        .is_ok();
        Some(HeadSender::new(granted, interrupt))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn start(run: &str, variant: Option<&str>) -> WalRecord {
        let mut value: serde_json::Value = serde_json::from_str(
            include_str!("../../broca-wal/tests/fixtures/before_message_times.jsonl")
                .lines()
                .next()
                .unwrap(),
        )
        .unwrap();
        value["run_id"] = json!(run);
        if let Some(variant) = variant {
            value["config"]["variant"] = json!(variant);
        }
        serde_json::from_value(value).unwrap()
    }

    fn step(usage: serde_json::Value) -> WalRecord {
        serde_json::from_value(json!({"type":"model_step_finished", "step_id":1,
            "assistant_message":{"message_id":"m","content":[]},"usage":usage,"retries_used":0}))
        .unwrap()
    }

    /// A refused call counts under the model that refused it, and a step routed to the
    /// refusal-fallback model, or served by a server-side fallback, under the model that
    /// answered it, without the caller's variant.
    #[test]
    fn session_spend_counts_refused_and_fallback_calls_under_the_model_that_answered() {
        let mut head = HeadProjection::default();
        head.observe(&start("run", Some("deep")));
        head.observe(&step(json!({"input_tokens":2,"output_tokens":1})));
        head.observe(
            &serde_json::from_value(json!({"type":"model_step_refused","step_id":2,
                "usage":{"input_tokens":4,"output_tokens":1},"discarded_blocks":0,
                "schedule":{"source":{"provider_module_id":"p","model_id":"m"},
                "fallback":{"provider_module_id":"p","model_id":"m-fallback"},
                "phase":{"kind":"fallback","remaining":10},"re_refusals":0}}))
            .unwrap(),
        );
        let mut routed =
            serde_json::to_value(step(json!({"input_tokens":8,"output_tokens":2}))).unwrap();
        routed["route"] =
            json!({"model":{"provider_module_id":"p","model_id":"m-fallback"},"reason":"refusal"});
        head.observe(&serde_json::from_value(routed).unwrap());
        let mut served =
            serde_json::to_value(step(json!({"input_tokens":16,"output_tokens":4}))).unwrap();
        served["served_model"] = json!("m-served");
        head.observe(&serde_json::from_value(served).unwrap());
        let usage = serde_json::to_value(head.usage.unwrap()).unwrap();
        assert_eq!(usage["steps"], 4);
        assert_eq!(usage["input_tokens"], 30);
        assert_eq!(
            usage["by_model"],
            json!([
                {"provider":"p","model":"m","variant":"deep","steps":2,"unreported_steps":0,"input_tokens":6,"output_tokens":2},
                {"provider":"p","model":"m-fallback","steps":1,"unreported_steps":0,"input_tokens":8,"output_tokens":2},
                {"provider":"p","model":"m-served","steps":1,"unreported_steps":0,"input_tokens":16,"output_tokens":4}
            ])
        );
    }

    #[test]
    fn session_spend_keeps_model_keys_partial_counts_and_first_counted_run() {
        let mut head = HeadProjection::default();
        head.observe(&start("empty-run", None));
        head.observe(&start("first-counted", Some("deep")));
        head.observe(&step(json!({"input_tokens":7,"output_tokens":3})));
        let persisted = serde_json::to_string(&head).unwrap();
        let mut head: HeadProjection = serde_json::from_str(&persisted).unwrap();
        head.observe(&start("second-counted", None));
        head.observe(&step(json!({})));
        head.observe(&step(
            json!({"input_tokens":0,"output_tokens":5,"reasoning_tokens":2}),
        ));
        assert_eq!(
            serde_json::to_value(&head.model).unwrap(),
            json!({"provider":"p","model":"m"})
        );
        let usage = head.usage.unwrap();
        assert!(usage.consistent());
        assert_eq!(
            serde_json::to_value(usage).unwrap(),
            json!({
                "since":"first-counted", "steps":3,"unreported_steps":1,"input_tokens":7,"output_tokens":8,"reasoning_tokens":2,
                "by_model":[
                    {"provider":"p","model":"m","variant":"deep","steps":1,"unreported_steps":0,"input_tokens":7,"output_tokens":3},
                    {"provider":"p","model":"m","steps":2,"unreported_steps":1,"input_tokens":0,"output_tokens":5,"reasoning_tokens":2}
                ]
            })
        );
    }
}
