//! A small metadata projection of new-storage writes. No replay or transcript copy is needed.

use super::*;
use broca_protocol::BindIdentity;
use broca_session_store::{
    record::{Record, RunStart},
    session::Appended,
};
use std::collections::{BTreeMap, BTreeSet};

struct Run {
    model: broca_protocol::ModelHandle,
    work_class: Option<String>,
    context_limit: Option<u64>,
    route_decision_id: Option<String>,
    frozen_render_config: serde_json::Value,
    keep_warm: Option<broca_protocol::request::KeepWarm>,
}

fn apply_step_end(event: &mut StepFinished, end: &broca_session_store::record::StepEnd) {
    if let [serving] = end.by_model.as_slice() {
        event.provider.clone_from(&serving.provider);
        event.model.clone_from(&serving.model);
    } else if end.by_model.is_empty() {
        if let Some(model) = end
            .fallback
            .as_ref()
            .and_then(|v| v.get("served_model"))
            .and_then(|v| v.as_str())
        {
            event.model = model.to_owned();
        }
    }
}

fn step_envelope(end: &broca_session_store::record::StepEnd) -> broca_wal::record::AttemptEnvelope {
    let usage = if end.by_model.is_empty() {
        end.total_usage.or(end.usage)
    } else {
        Some(
            end.by_model
                .iter()
                .fold(broca_protocol::Usage::default(), |sum, model| {
                    sum + model.usage
                }),
        )
    };
    let mut envelope = broca_wal::record::AttemptEnvelope {
        usage,
        finish_reason: end.finish_reason,
        // Usage is canonical: recorded reasoning is a subset of output, not extra output.
        reasoning_included_in_output: usage.and_then(|usage| usage.reasoning_tokens.map(|_| true)),
        ..Default::default()
    };
    if let Some(iterations) = end
        .fallback
        .as_ref()
        .and_then(|v| v.get("usage_iterations"))
        .and_then(|v| v.as_array())
    {
        for iteration in iterations {
            // As in the provider's envelope, only model-tagged measurements have attribution.
            if iteration
                .get("model")
                .and_then(|v| v.as_str())
                .is_none_or(str::is_empty)
            {
                continue;
            }
            let split = |name| {
                iteration
                    .get("cache_creation")
                    .and_then(|v| v.get(name))
                    .and_then(|v| v.as_u64())
            };
            envelope.cache_write_5m_tokens = broca_protocol::usage::sum_reported(
                envelope.cache_write_5m_tokens,
                split("ephemeral_5m_input_tokens"),
            );
            envelope.cache_write_1h_tokens = broca_protocol::usage::sum_reported(
                envelope.cache_write_1h_tokens,
                split("ephemeral_1h_input_tokens"),
            );
        }
    }
    envelope
}

pub(crate) struct Observer {
    bind: BindIdentity,
    incarnation: String,
    first_live_seq: u64,
    runs: BTreeMap<String, Run>,
    completed: BTreeMap<(String, u64), Vec<StepFinished>>,
    unsynced: Vec<StepFinished>,
    emitted: BTreeSet<String>,
    tx: tokio::sync::mpsc::Sender<StepFinished>,
    dropped: Arc<AtomicU64>,
    parts: broca_session_loop::parts::SharedParts,
    warm_stops: super::WarmStops,
}

impl Observer {
    pub(super) fn new(
        bind: BindIdentity,
        incarnation: [u8; 16],
        first_live_seq: u64,
        tx: tokio::sync::mpsc::Sender<StepFinished>,
        dropped: Arc<AtomicU64>,
        parts: broca_session_loop::parts::SharedParts,
        warm_stops: super::WarmStops,
    ) -> Self {
        Self {
            bind,
            incarnation: incarnation.iter().map(|b| format!("{b:02x}")).collect(),
            first_live_seq,
            runs: BTreeMap::new(),
            completed: BTreeMap::new(),
            unsynced: Vec::new(),
            emitted: BTreeSet::new(),
            tx,
            dropped,
            parts,
            warm_stops,
        }
    }

    pub(crate) fn run_start(&mut self, start: &RunStart) {
        self.runs.insert(
            start.run_id.clone(),
            Run {
                model: start.model.clone(),
                context_limit: start.context_limit,
                route_decision_id: start.route_decision_id.clone(),
                frozen_render_config: broca_session_loop::run::run_config_of_start(
                    start,
                    &self.parts,
                )
                .map(|config| config.frozen_render_config)
                .unwrap_or_default(),
                keep_warm: match &start.keep_warm {
                    Some(setting) => serde_json::from_value(setting.clone()).ok(),
                    None => Some(Default::default()),
                },
                work_class: start
                    .run_choices
                    .as_ref()
                    .and_then(|v| v.get("config"))
                    .and_then(|v| v.get("work_class"))
                    .and_then(|v| v.as_str())
                    .map(str::to_owned),
            },
        );
    }

    pub(crate) fn seed_run(&mut self, start: &RunStart, refrozen: Option<&serde_json::Value>) {
        self.run_start(start);
        if let Some(config) = refrozen.and_then(|refrozen| refrozen.get("render_config")) {
            self.refrozen(&start.run_id, config);
        }
    }

    fn refrozen(&mut self, run_id: &str, config: &serde_json::Value) {
        if let Some(run) = self.runs.get_mut(run_id) {
            run.frozen_render_config.clone_from(config);
        }
    }

    fn cache(&self, event: &StepFinished) -> Option<cortexkit_role_llm_runner::cache::Report> {
        let run = self.runs.get(&event.run_id)?;
        let stops = self.warm_stops.lock().ok();
        // A fallback on another provider did not necessarily use this run's frozen route.
        let frozen = if event.provider == run.model.provider_module_id {
            &run.frozen_render_config
        } else {
            &serde_json::Value::Null
        };
        super::cache_report(
            frozen,
            &event.provider,
            stops.as_ref().and(run.keep_warm),
            stops
                .as_ref()
                .and_then(|stops| stops.get(&event.run_id))
                .map(String::as_str),
        )
    }

    pub(crate) fn appended(&mut self, appended: Appended<'_>) {
        match appended {
            Appended::Records {
                first,
                records,
                durable,
            } => {
                for (offset, record) in records.iter().enumerate() {
                    let seq = first + offset as u64;
                    // A seal may copy old records into its new log. They are not new attempts.
                    if seq >= self.first_live_seq {
                        self.record(seq, record);
                    }
                }
                if durable {
                    self.synced();
                }
            }
            Appended::Synced => self.synced(),
            Appended::Replaced => {
                self.unsynced.clear();
                self.completed.clear();
            }
        }
    }

    fn record(&mut self, seq: u64, record: &Record) {
        match record {
            Record::RunStart(start) => self.run_start(start),
            Record::ConfigRefrozen(refrozen) => {
                self.refrozen(&refrozen.run_id, &refrozen.render_config);
            }
            Record::Attempt(attempt) => {
                let Some(id) = &attempt.attempt_id else {
                    return;
                };
                if self.emitted.contains(id) {
                    return;
                }
                let Some(run) = self.runs.get(&attempt.run_id) else {
                    return;
                };
                let Some(started_at_ms) = attempt.sent_at_ms else {
                    // The event schema requires a start time; a completion time is not one.
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                    return;
                };
                let status = match attempt.outcome.as_str() {
                    "complete" => "completed",
                    "cancelled" => "cancelled",
                    "unknown" | "sealed_in_flight" => "interrupted",
                    _ => "failed",
                };
                let position = StepRef::new(
                    "broca",
                    broca_core::agent_session_ref(&self.bind),
                    &self.incarnation,
                    seq,
                );
                let Some(mut event) = (Facts {
                    position,
                    run_id: &attempt.run_id,
                    step_id: attempt.step_id,
                    attempt: attempt.attempt,
                    status,
                    harness: &self.bind.harness,
                    session: &self.bind.session,
                    provider: &run.model.provider_module_id,
                    model: &run.model.model_id,
                    started_at_ms,
                    finished_at_ms: attempt.ts_ms,
                    attempt_id: id,
                })
                .build() else {
                    return;
                };
                // request_body_len is canonical, not the physical (possibly compressed) body
                // length that today's reservation records for this event field.
                event.work_class.clone_from(&run.work_class);
                event.context_limit = run.context_limit;
                event.route_decision_id.clone_from(&run.route_decision_id);
                event.cache = self.cache(&event);
                (
                    event.sent_to_last_content_ms,
                    event.first_to_last_content_ms,
                ) = broca_core::replay::content_timing_intervals(
                    attempt.sent_at_ms,
                    attempt.first_content_at_ms,
                    attempt.last_content_at_ms,
                );
                if status == "completed" {
                    // The attempt is not a usage measurement. Its completed step supplies usage.
                    self.completed
                        .entry((attempt.run_id.clone(), attempt.step_id))
                        .or_default()
                        .push(event);
                } else {
                    self.unsynced.push(event);
                }
            }
            Record::StepEnd(end) => {
                if let Some(events) = self.completed.remove(&(end.run_id.clone(), end.step_id)) {
                    for mut event in events {
                        apply_step_end(&mut event, end);
                        event.cache = self.cache(&event);
                        self.unsynced
                            .push(finish(event, &step_envelope(end), false));
                    }
                }
            }
            Record::Warmed(warmed) => {
                let Some(id) = &warmed.attempt_id else {
                    return;
                };
                let Some(facts) = &warmed.billing else {
                    return;
                };
                let Some(finished_at_ms) = facts.get("finished_at_ms").and_then(|v| v.as_u64())
                else {
                    return;
                };
                let Some(run) = self.runs.get(&warmed.run_id) else {
                    return;
                };
                let position = StepRef::new(
                    "broca",
                    broca_core::agent_session_ref(&self.bind),
                    &self.incarnation,
                    seq,
                );
                let Some(mut event) = (Facts {
                    position,
                    run_id: &warmed.run_id,
                    step_id: warmed.step_id,
                    attempt: 0,
                    status: "completed",
                    harness: &self.bind.harness,
                    session: &self.bind.session,
                    provider: &run.model.provider_module_id,
                    model: &run.model.model_id,
                    started_at_ms: warmed.sent_at_ms,
                    finished_at_ms,
                    attempt_id: id,
                })
                .build() else {
                    return;
                };
                event.request_body_bytes = facts.get("request_body_bytes").and_then(|v| v.as_u64());
                event.work_class.clone_from(&run.work_class);
                event.context_limit = run.context_limit;
                event.route_decision_id.clone_from(&run.route_decision_id);
                event.cache = self.cache(&event);
                let envelope = broca_wal::record::AttemptEnvelope {
                    usage: Some(warmed.usage),
                    reasoning_included_in_output: warmed.usage.reasoning_tokens.map(|_| true),
                    ..Default::default()
                };
                self.unsynced.push(finish(event, &envelope, false));
            }
            Record::RunEnd(end) => {
                let keys: Vec<_> = self
                    .completed
                    .keys()
                    .filter(|(run, _)| run == &end.run_id)
                    .cloned()
                    .collect();
                for key in keys {
                    for mut event in self.completed.remove(&key).unwrap_or_default() {
                        // No step end survived the cancellation, so no usage can be attributed.
                        event.terminal_status = "interrupted".into();
                        self.unsynced.push(event);
                    }
                }
            }
            _ => {}
        }
    }

    fn synced(&mut self) {
        for event in self.unsynced.drain(..) {
            let Some(id) = event.attempt_id.as_ref() else {
                continue;
            };
            if !self.emitted.insert(id.clone()) {
                continue;
            }
            if event.check().is_err() || self.tx.try_send(event).is_err() {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_session_store::record::{ModelStepUsage, StepEnd};
    use serde_json::json;

    #[test]
    fn recovered_run_cache_uses_the_folded_refreeze_not_the_original_setup() {
        use broca_provider::{
            spec::WireFamilyId,
            wire::{CacheClass, CachePolicy, CacheTtl, MarkerSyntax},
        };
        use broca_session_loop::parts::{MemoryParts, Parts, SharedParts};
        use broca_session_store::{
            fold::operations::Operations,
            record::{ConfigRefrozen, PartKind},
        };
        let original = json!({"family_id": WireFamilyId::AnthropicMessages, "cache": CachePolicy {
            version: 1,
            class: CacheClass::Markers {
                syntax: MarkerSyntax::AnthropicCacheControl, ttl: CacheTtl::FiveMinutes,
                max_breakpoints: 4, lookback_blocks: 20, tail: None, omit_ttl: false,
            },
        }});
        let mut parts = MemoryParts::default();
        let setup = vec![parts.put(PartKind::RenderConfig, &original).unwrap()];
        let start = RunStart {
            run_id: "run".into(),
            model: broca_protocol::ModelHandle::new("anthropic", "synthetic"),
            setup,
            renderer_version: 1,
            submission_id: None,
            send_id: None,
            request_identity_digest: None,
            ts_ms: None,
            written_at_ms: None,
            session_class: None,
            retention_secs: None,
            title: None,
            context_limit: None,
            service_tier: None,
            route_decision_id: None,
            compaction_plan_version: None,
            auth_selection: None,
            run_choices: Some(json!({"config": {
                "tools": [], "tool_choice": broca_protocol::ToolChoice::Auto,
                "generation": broca_protocol::GenerationConfig::default(),
                "response_format": broca_protocol::request::ResponseFormat::default(),
                "context_epoch": 0,
            }})),
            origin: None,
            principal: None,
            scope: None,
            hooks: None,
            keep_warm: Some(json!("auto")),
            on_restart: None,
            seeded: None,
            refusal_schedule: None,
            imported_ids: None,
            message_marks: None,
            named_speaker: None,
        };
        let mut operations = Operations::default();
        operations
            .apply(&Record::RunStart(Box::new(start)))
            .unwrap();
        let mut refrozen = original;
        refrozen["cache"]["ttl"] = json!("1h");
        operations
            .apply(&Record::ConfigRefrozen(ConfigRefrozen {
                run_id: "run".into(),
                ts_ms: None,
                renderer_version: 1,
                render_config: refrozen,
            }))
            .unwrap();
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let mut observer = Observer::new(
            BindIdentity::new("/synthetic", "host", "session"),
            [1; 16],
            1,
            tx,
            Arc::new(AtomicU64::new(0)),
            SharedParts::new(Box::new(parts)),
            Default::default(),
        );
        let run = operations.run.as_ref().unwrap();
        let start = run.start.as_ref().unwrap();
        let event = StepFinished::new(
            StepRef::new("broca", "synthetic", "incarnation", 1),
            "run",
            "1",
            std::num::NonZeroU64::new(1).unwrap(),
            "completed",
            "host",
            "session",
            "anthropic",
            "synthetic",
            100,
            200,
        );
        observer.seed_run(start, None);
        assert_eq!(observer.cache(&event).unwrap().ttl_ms, Some(300_000));
        observer.seed_run(start, run.refrozen_render.as_ref());
        assert_eq!(
            serde_json::to_value(observer.cache(&event)).unwrap(),
            json!({"ttl_ms":3_600_000,"warming":{"on":true}})
        );
    }

    #[test]
    fn stored_model_shares_and_iteration_ttls_are_used_without_guesses() {
        let mut end = StepEnd {
            run_id: "run".into(),
            step_id: 1,
            usage: None,
            finish_reason: Some(broca_protocol::FinishReason::Stop),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
            call_count: 0,
            hooks: None,
            budget_finalize: None,
            total_usage: None,
            stop_met: None,
            limiting_stop_met: None,
            fallback: Some(json!({"usage_iterations":[
                {"model":"served", "cache_creation":{"ephemeral_5m_input_tokens":0, "ephemeral_1h_input_tokens":12}},
                {"model":"served", "cache_creation":{"ephemeral_1h_input_tokens":8}}
            ]})),
            by_model: vec![ModelStepUsage {
                provider: "serving-provider".into(),
                model: "served".into(),
                variant: None,
                usage: serde_json::from_value(
                    json!({"input_tokens":100, "cache_write_tokens":20, "output_tokens":20, "reasoning_tokens":7}),
                )
                .unwrap(),
            }],
        };
        let mut event = Facts {
            position: StepRef::new("broca", "session_ref", "incarnation", 1),
            run_id: "run",
            step_id: 1,
            attempt: 0,
            status: "completed",
            harness: "harness",
            session: "session",
            provider: "requested-provider",
            model: "requested",
            started_at_ms: 100,
            finished_at_ms: 200,
            attempt_id: "0123456789abcdef0123456789abcdef",
        }
        .build()
        .unwrap();
        apply_step_end(&mut event, &end);
        let event = finish(event, &step_envelope(&end), false);
        assert_eq!(event.provider, "serving-provider");
        assert_eq!(event.model, "served");
        assert_eq!(event.input_tokens, Some(100));
        assert_eq!(event.reasoning_included_in_output, Some(true));
        assert_eq!(event.cache_write_5m_tokens, Some(0));
        assert_eq!(event.cache_write_1h_tokens, Some(20));
        assert_eq!(event.provider_request_id, None);
        end.fallback = None;
        let absent = step_envelope(&end);
        assert_eq!(absent.cache_write_5m_tokens, None);
        assert_eq!(absent.cache_write_1h_tokens, None);
    }
}
