//! Bounded display facts from records the session already folds; no read fetches evidence.

use std::collections::BTreeMap;

use broca_protocol::ModelHandle;
use serde::Deserialize;

use crate::fold::operations::Operations;
use crate::held::HeldBytes;
use crate::record::{Record, StepStart};

use super::Session;
use crate::backend::SessionBackend;

/// A committed refusal-fallback reply's exact model ids, in display-row form.
/// This is transient read metadata, never a record or checkpoint field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplyModelFallback {
    pub requested: String,
    pub answered: String,
    seq: u64,
}

impl HeldBytes for ReplyModelFallback {
    fn heap_bytes(&self) -> usize {
        self.requested.heap_bytes() + self.answered.heap_bytes()
    }
}

/// A small cache may omit older facts rather than reading ledger evidence on demand.
const MAX_BYTES: usize = 256 * 1024;

#[derive(Default)]
pub(super) struct ReplyFallbacks {
    by_mid: BTreeMap<String, ReplyModelFallback>,
    run: Option<(String, ModelHandle)>,
    step: Option<StepStart>,
}

#[derive(Deserialize)]
struct Fallback {
    route: Option<Route>,
    served_model: Option<String>,
}

#[derive(Deserialize)]
struct Route {
    model: ModelHandle,
}

impl ReplyFallbacks {
    pub(super) fn from_operations(operations: &Operations) -> Self {
        Self {
            run: operations
                .run
                .as_ref()
                .filter(|run| run.finished.is_none())
                .and_then(|run| {
                    run.start
                        .as_ref()
                        .map(|start| (start.run_id.clone(), start.model.clone()))
                }),
            ..Default::default()
        }
    }

    pub(super) fn note(&mut self, seq: u64, record: &Record) {
        match record {
            Record::RunStart(start) => {
                self.run = Some((start.run_id.clone(), start.model.clone()));
                self.step = None;
            }
            Record::StepStart(start) => self.step = Some(start.clone()),
            Record::RunEnd(_) => {
                self.run = None;
                self.step = None;
            }
            Record::StepEnd(end) => {
                let (Some((run_id, requested)), Some(step)) = (&self.run, &self.step) else {
                    return;
                };
                if end.run_id != *run_id || step.run_id != *run_id || step.step_id != end.step_id {
                    return;
                }
                let Some(mid) = &step.mid else { return };
                let Some(fallback) = end.fallback.as_ref().and_then(|value| {
                    serde_json::from_value::<Fallback>(value.clone().parse()).ok()
                }) else {
                    return;
                };
                // A schedule or usage alone cannot prove the step was replaced.
                let Some(route) = fallback.route else { return };
                let provider = &route.model.provider_module_id;
                let answered = fallback
                    .served_model
                    .as_deref()
                    .unwrap_or(&route.model.model_id);
                if provider.is_empty()
                    || requested.provider_module_id != *provider
                    || requested.model_id.is_empty()
                    || route.model.model_id.is_empty()
                    || answered.is_empty()
                    || requested.model_id == answered
                {
                    return;
                }
                self.by_mid.insert(
                    mid.clone(),
                    ReplyModelFallback {
                        requested: format!("{provider}/{}", requested.model_id),
                        answered: format!("{provider}/{answered}"),
                        seq,
                    },
                );
                while self.by_mid.heap_bytes() > MAX_BYTES {
                    let oldest = self
                        .by_mid
                        .iter()
                        .min_by_key(|(_, fact)| fact.seq)
                        .map(|(mid, _)| mid.clone());
                    if let Some(mid) = oldest {
                        self.by_mid.remove(&mid);
                    } else {
                        break;
                    }
                }
            }
            _ => {}
        }
    }
}

impl HeldBytes for ReplyFallbacks {
    fn heap_bytes(&self) -> usize {
        let run = self.run.as_ref().map_or(0, |(id, model)| {
            id.heap_bytes() + model.provider_module_id.heap_bytes() + model.model_id.heap_bytes()
        });
        self.by_mid.heap_bytes()
            + run
            + self.step.as_ref().map_or(0, |step| {
                step.run_id.heap_bytes() + step.mid.as_ref().map_or(0, HeldBytes::heap_bytes)
            })
    }
}

impl<B: SessionBackend> Session<B> {
    /// Whether the open log identified replies written by a fallback model. A sealed message
    /// records which model answered, but not which model the runner originally requested.
    pub fn has_reply_model_fallbacks(&self) -> bool {
        !self.reply_fallbacks.by_mid.is_empty()
    }

    /// A reply fact already proved by folded records, with no storage lookup. A checkpoint or
    /// sealed transcript alone contains no route evidence, so unavailable facts remain absent.
    pub fn reply_model_fallback(&self, mid: &str) -> Option<&ReplyModelFallback> {
        self.reply_fallbacks.by_mid.get(mid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::StepEnd;
    use serde_json::json;

    fn facts() -> ReplyFallbacks {
        ReplyFallbacks {
            run: Some(("run".into(), ModelHandle::new("provider", "source"))),
            step: Some(StepStart {
                run_id: "run".into(),
                step_id: 1,
                mid: Some("reply".into()),
            }),
            ..Default::default()
        }
    }

    fn end(fallback: serde_json::Value) -> Record {
        Record::StepEnd(StepEnd {
            run_id: "run".into(),
            step_id: 1,
            usage: None,
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
            call_count: 0,
            fallback: Some(fallback.into()),
            hooks: None,
            budget_finalize: None,
            total_usage: None,
            stop_met: None,
            limiting_stop_met: None,
            by_model: vec![],
        })
    }

    #[test]
    fn only_a_recorded_replacement_of_this_step_creates_a_fact() {
        let route = json!({"model": {"provider_module_id": "provider", "model_id": "fallback"}});
        for fallback in [
            json!({"served_model": "fallback"}),
            json!({"refusal_fallback": {"source": "source", "fallback": "fallback"}}),
            json!({"route": route, "served_model": "source"}),
            json!({"route": {"model": {"provider_module_id": "provider", "model_id": ""}}}),
        ] {
            let mut facts = facts();
            facts.note(10, &end(fallback));
            assert!(facts.by_mid.is_empty());
        }
        let mut facts = facts();
        facts.step.as_mut().unwrap().step_id = 2;
        facts.note(10, &end(json!({"route": route})));
        assert!(facts.by_mid.is_empty());
    }

    #[test]
    fn provider_handoff_overrides_the_recorded_route_model() {
        let mut facts = facts();
        facts.note(
            10,
            &end(json!({
                "route": {"model": {"provider_module_id": "provider", "model_id": "fallback"}},
                "served_model": "served"
            })),
        );
        let reply = &facts.by_mid["reply"];
        assert_eq!(reply.requested, "provider/source");
        assert_eq!(reply.answered, "provider/served");
        assert!(
            facts.heap_bytes() > 0,
            "reply facts count in session memory"
        );
    }

    #[test]
    fn retained_reply_facts_are_bounded_without_fetching_older_evidence() {
        let mut facts = facts();
        let end = end(json!({
            "route": {"model": {"provider_module_id": "provider", "model_id": "x".repeat(8192)}}
        }));
        for seq in 0..64 {
            facts.step.as_mut().unwrap().mid = Some(format!("reply-{seq}"));
            facts.note(seq, &end);
            assert!(facts.by_mid.heap_bytes() <= MAX_BYTES);
        }
        assert!(!facts.by_mid.contains_key("reply-0"));
        assert!(facts.by_mid.contains_key("reply-63"));
    }
}
