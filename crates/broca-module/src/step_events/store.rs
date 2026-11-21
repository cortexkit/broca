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
    /// The catalog provider the run borrowed rows from (`RunConfig::served_provider`), which
    /// its steps are reported as.
    served_provider: Option<String>,
    work_class: Option<String>,
    context_limit: Option<u64>,
    route_decision_id: Option<String>,
    frozen_render_config: serde_json::Value,
    keep_warm: Option<broca_protocol::request::KeepWarm>,
}

impl Run {
    /// The provider the run's steps are reported as: the borrowed catalog provider, else its
    /// model's.
    fn billed_provider(&self) -> &str {
        self.served_provider
            .as_deref()
            .unwrap_or(self.model.provider_module_id.as_str())
    }
}

fn apply_step_end(event: &mut StepFinished, end: &broca_session_store::record::StepEnd) {
    if let [serving] = end.by_model.as_slice() {
        event.provider.clone_from(&serving.provider);
        event.model.clone_from(&serving.model);
    } else if end.by_model.is_empty() {
        if let Some(model) = end
            .fallback
            .as_ref()
            .map(broca_protocol::json::Json::parse)
            .as_ref()
            .and_then(|v| v.get("served_model"))
            .and_then(|v| v.as_str())
        {
            event.model = model.to_owned();
        }
    }
}

/// What a completed step measured, from its stored end.
fn step_measured(end: &broca_session_store::record::StepEnd) -> Measured {
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
    let mut measured = Measured {
        usage,
        finish_reason: end.finish_reason,
        // Usage is canonical: recorded reasoning is a subset of output, not extra output.
        reasoning_included_in_output: usage.and_then(|usage| usage.reasoning_tokens.map(|_| true)),
        ..Default::default()
    };
    let fallback = end.fallback.as_ref().map(broca_protocol::json::Json::parse);
    if let Some(iterations) = fallback
        .as_ref()
        .and_then(|v| v.get("usage_iterations"))
        .and_then(|v| v.as_array())
    {
        for iteration in iterations {
            // Only model-tagged measurements have attribution.
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
            measured.cache_write_5m_tokens = broca_protocol::usage::sum_reported(
                measured.cache_write_5m_tokens,
                split("ephemeral_5m_input_tokens"),
            );
            measured.cache_write_1h_tokens = broca_protocol::usage::sum_reported(
                measured.cache_write_1h_tokens,
                split("ephemeral_1h_input_tokens"),
            );
        }
    }
    measured
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
    message_ids: broca_session_loop::entries::MessageIds,
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
            message_ids: Default::default(),
        }
    }

    pub(crate) fn with_message_ids(mut self, ids: broca_session_loop::entries::MessageIds) -> Self {
        self.message_ids = ids;
        self
    }

    pub(crate) fn run_start(&mut self, start: &RunStart) {
        self.runs.insert(
            start.run_id.clone(),
            Run {
                model: start.model.clone(),
                served_provider: start
                    .run_choices
                    .as_ref()
                    .and_then(|v| v.get("config"))
                    .and_then(|v| v.get("served_provider"))
                    .and_then(|v| v.as_str())
                    .map(str::to_owned),
                context_limit: start.context_limit,
                route_decision_id: start.route_decision_id.clone(),
                frozen_render_config: broca_session_loop::run::run_frozen_facts(start, &self.parts)
                    .map(|facts| facts.frozen_render_config)
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
        let frozen = if event.provider == run.billed_provider() {
            &run.frozen_render_config
        } else {
            &serde_json::Value::Null
        };
        super::cache_report(
            frozen,
            &event.provider,
            stops.as_ref().and(run.keep_warm.as_ref()),
            stops
                .as_ref()
                .and_then(|stops| stops.get(&event.run_id))
                .map(String::as_str),
        )
    }

    pub(crate) fn appended(
        &mut self,
        appended: Appended<'_>,
        position: broca_session_store::session::AppendPosition,
    ) {
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
                    self.synced(position);
                }
            }
            Appended::Synced => self.synced(position),
            Appended::Replaced => {
                self.message_ids.clear();
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
                    // run::record_attempt syncs tool_args_trailing_garbage before message
                    // placement. Preserve its failed hint: it is published before its message
                    // exists, so no message_id can be reported at this publication point.
                    "tool_args_trailing_garbage" => "failed",
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
                    provider: run.billed_provider(),
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
                let message_id = self.message_ids.take(&end.run_id, end.step_id);
                if let Some(events) = self.completed.remove(&(end.run_id.clone(), end.step_id)) {
                    let last = events.len().saturating_sub(1);
                    for (index, mut event) in events.into_iter().enumerate() {
                        // Only the final completed attempt supplied the reply placed by this step.
                        if index == last {
                            event.message_id.clone_from(&message_id);
                            if event.message_id.is_none() {
                                tracing::warn!(target: "broca.step_events",
                                    session_addr = %broca_core::session_addr(&self.bind),
                                    session_ref = %event.session_ref, run_id = %end.run_id,
                                    step_id = end.step_id,
                                    "completed step has no assistant entry id; publishing without message_id");
                            }
                        }
                        apply_step_end(&mut event, end);
                        event.cache = self.cache(&event);
                        self.unsynced.push(finish(event, &step_measured(end)));
                    }
                }
            }
            Record::Warmed(warmed) => {
                let Some(id) = &warmed.attempt_id else {
                    return;
                };
                let Some(facts) = warmed
                    .billing
                    .as_ref()
                    .map(broca_protocol::json::Json::parse)
                else {
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
                    provider: run.billed_provider(),
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
                let measured = Measured {
                    usage: Some(warmed.usage),
                    reasoning_included_in_output: warmed.usage.reasoning_tokens.map(|_| true),
                    ..Default::default()
                };
                self.unsynced.push(finish(event, &measured));
            }
            Record::RunEnd(end) => {
                self.message_ids.forget_run(&end.run_id);
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

    fn synced(&mut self, position: broca_session_store::session::AppendPosition) {
        for event in self.unsynced.drain(..) {
            let Some(id) = event.attempt_id.as_ref() else {
                continue;
            };
            if !self.emitted.insert(id.clone()) {
                continue;
            }
            tracing::debug!(target: "step-events.evidence",
                storage = "store", session_ref = %event.session_ref,
                log_incarnation = %event.log_incarnation, step_seq = event.seq,
                attempt_id = %id,
                log_id = %position.log.0.iter().map(|byte| format!("{byte:02x}")).collect::<String>(),
                record_end = position.written_through, synced_through = position.synced_through,
                "step event durable handoff");
            if event.check().is_err() || self.tx.try_send(event).is_err() {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_session_store::record::{Attempt, ModelStepUsage, StepEnd};
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
        let start = run_start(
            broca_protocol::ModelHandle::new("anthropic", "synthetic"),
            setup,
            json!({}),
            Some(json!("auto")),
        );
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
            ]}).into()),
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
        let event = finish(event, &step_measured(&end));
        assert_eq!(event.provider, "serving-provider");
        assert_eq!(event.model, "served");
        assert_eq!(event.input_tokens, Some(100));
        assert_eq!(event.reasoning_included_in_output, Some(true));
        assert_eq!(event.cache_write_5m_tokens, Some(0));
        assert_eq!(event.cache_write_1h_tokens, Some(20));
        assert_eq!(event.provider_request_id, None);
        end.fallback = None;
        let absent = step_measured(&end);
        assert_eq!(absent.cache_write_5m_tokens, None);
        assert_eq!(absent.cache_write_1h_tokens, None);
    }

    fn vectors() -> serde_json::Value {
        serde_json::from_str(cortexkit_role_llm_runner::vectors::file("step-events.json").unwrap())
            .unwrap()
    }

    /// A run start as the run loop writes it. `config` adds members to the run's frozen choices
    /// (the observer reads `served_provider` and `work_class` there).
    fn run_start(
        model: broca_protocol::ModelHandle,
        setup: Vec<broca_session_store::record::PartRef>,
        config: serde_json::Value,
        keep_warm: Option<serde_json::Value>,
    ) -> RunStart {
        let mut choices = json!({"config": {
            "tools": [], "tool_choice": broca_protocol::ToolChoice::Auto,
            "generation": broca_protocol::GenerationConfig::default(),
            "response_format": broca_protocol::request::ResponseFormat::default(),
            "context_epoch": 0,
        }});
        choices["config"]
            .as_object_mut()
            .unwrap()
            .extend(config.as_object().cloned().unwrap_or_default());
        RunStart {
            run_id: "run".into(),
            model,
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
            run_choices: Some(choices),
            origin: None,
            principal: None,
            scope: None,
            hooks: None,
            keep_warm,
            on_restart: None,
            seeded: None,
            refusal_schedule: None,
            imported_ids: None,
            message_marks: None,
            named_speaker: None,
        }
    }

    /// One HTTP attempt of step 7, as the run loop records it.
    fn attempt(attempt: u32, outcome: &str, sent_at_ms: Option<u64>, ts_ms: u64) -> Attempt {
        Attempt {
            run_id: "run".into(),
            step_id: 7,
            attempt,
            attempt_id: Some("0123456789abcdef0123456789abcdef".into()),
            outcome: outcome.into(),
            http_status: Some(200),
            request_body_len: 4096,
            request_body_sha256: broca_session_store::codec::Digest([0; 32]),
            response_body_len: 100,
            response_body_sha256: broca_session_store::codec::Digest([0; 32]),
            ts_ms,
            sent_at_ms,
            first_content_at_ms: None,
            last_content_at_ms: None,
            transport: None,
            served_account_ref: None,
            billing: None,
            telemetry: None,
            reached_cache: None,
            requested_cache_ttl_ms: None,
        }
    }

    /// The end of step 7, with `usage` and its per-iteration cache lifetimes in `fallback`.
    fn step_end(usage: Option<serde_json::Value>, fallback: Option<serde_json::Value>) -> StepEnd {
        StepEnd {
            run_id: "run".into(),
            step_id: 7,
            usage: usage.map(|usage| serde_json::from_value(usage).unwrap()),
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
            fallback: fallback.map(Into::into),
            by_model: Vec::new(),
        }
    }

    /// A session's append observer with its publisher queue, as a session open wires it.
    struct Rig {
        observer: Observer,
        rx: tokio::sync::mpsc::Receiver<StepFinished>,
        dropped: Arc<AtomicU64>,
    }

    impl Rig {
        fn new(parts: broca_session_loop::parts::MemoryParts, stops: &[(&str, &str)]) -> Self {
            let (tx, rx) = tokio::sync::mpsc::channel(16);
            let dropped = Arc::new(AtomicU64::new(0));
            let warm_stops = Arc::new(std::sync::Mutex::new(
                stops
                    .iter()
                    .map(|(run, stop)| (run.to_string(), stop.to_string()))
                    .collect(),
            ));
            let observer = Observer::new(
                BindIdentity::new("/synthetic", "host_a", "session_a"),
                [0x11; 16],
                1,
                tx,
                dropped.clone(),
                broca_session_loop::parts::SharedParts::new(Box::new(parts)),
                warm_stops,
            );
            Self {
                observer,
                rx,
                dropped,
            }
        }

        /// The events sent for `records`, appended from seq 1 in one durable append.
        fn sent(&mut self, records: &[Record]) -> Vec<StepFinished> {
            self.observer.appended(
                Appended::Records {
                    first: 1,
                    records,
                    durable: true,
                },
                broca_session_store::session::AppendPosition {
                    log: broca_session_store::codec::Id128([0; 16]),
                    written_through: 0,
                    synced_through: 0,
                },
            );
            std::iter::from_fn(|| self.rx.try_recv().ok()).collect()
        }
    }

    fn handle(provider: &str, model: &str) -> broca_protocol::ModelHandle {
        broca_protocol::ModelHandle::new(provider, model)
    }

    #[test]
    fn a_stored_step_matches_the_contract_step_event_vectors() {
        for full in [false, true] {
            let vector = &vectors()[if full { "full" } else { "minimal" }];
            let started = vector["started_at_ms"].as_u64().unwrap();
            let finished = vector["finished_at_ms"].as_u64().unwrap();
            let mut start = run_start(
                handle("provider_a", "model_a"),
                Vec::new(),
                if full {
                    json!({"work_class": "future_work_class"})
                } else {
                    json!({})
                },
                None,
            );
            if full {
                start.context_limit = Some(200_000);
            }
            let end = if full {
                step_end(
                    Some(
                        json!({"input_tokens":100, "cached_input_tokens":50, "cache_write_tokens":30, "output_tokens":20, "reasoning_tokens":7}),
                    ),
                    Some(
                        json!({"usage_iterations":[{"model":"model_a", "cache_creation":{"ephemeral_5m_input_tokens":10, "ephemeral_1h_input_tokens":20}}]}),
                    ),
                )
            } else {
                StepEnd {
                    finish_reason: None,
                    ..step_end(None, None)
                }
            };
            let mut rig = Rig::new(Default::default(), &[]);
            let sent = rig.sent(&[
                Record::RunStart(Box::new(start)),
                Record::Attempt(attempt(
                    if full { 1 } else { 0 },
                    "complete",
                    Some(started),
                    finished,
                )),
                Record::StepEnd(end),
            ]);
            let [event] = sent.as_slice() else {
                panic!("one event per completed step: {sent:?}");
            };
            let mut expected = vector.clone();
            // The vectors use invented host identities; broca names itself as the runner, the
            // session by its agent reference, and the step by its attempt record's position and
            // numeric id, and always joins on the attempt's durable random identity.
            expected["runner"] = json!("broca");
            expected["run_id"] = json!("run");
            expected["session_ref"] = json!(broca_core::agent_session_ref(&BindIdentity::new(
                "/synthetic",
                "host_a",
                "session_a"
            )));
            expected["log_incarnation"] = json!("11".repeat(16));
            expected["seq"] = json!(2);
            expected["step_id"] = json!("7");
            expected["attempt_id"] = json!("0123456789abcdef0123456789abcdef");
            expected["terminal_status"] = json!("completed");
            if full {
                // The stored step has no assistant entry in this test, and new storage records
                // neither the provider's request id nor a physical body length.
                let expected = expected.as_object_mut().unwrap();
                expected.remove("harness_step_ref");
                expected.remove("provider_request_id");
                expected.remove("request_body_bytes");
                expected["finish_reason"] = json!("stop");
            }
            let actual = serde_json::to_value(event).unwrap();
            assert_eq!(actual, expected);
            for member in vectors()["refused_members"].as_array().unwrap() {
                for name in member.as_object().unwrap().keys() {
                    assert!(actual.get(name).is_none(), "content member {name} escaped");
                }
            }
            let decoded = cortexkit_role_llm_runner::step_events::decode_step_finished(
                "ck.account_a.live.broca.step.finished.v1",
                &serde_json::to_vec(event).unwrap(),
            )
            .unwrap();
            assert_eq!(&decoded, event);
        }
    }

    #[test]
    fn synthetic_step_cache_samples() {
        use broca_provider::{
            spec::WireFamilyId,
            wire::{CacheClass, CachePolicy, CacheTtl, MarkerSyntax},
        };
        use broca_session_loop::parts::{MemoryParts, Parts};
        use broca_session_store::record::PartKind;
        let dir = std::env::var_os("BROCA_STEP_CACHE_SAMPLES_DIR").map(std::path::PathBuf::from);
        if let Some(dir) = &dir {
            std::fs::create_dir_all(dir).unwrap();
        }
        for (name, provider, model) in [
            ("anthropic-5m-off", "anthropic", "claude-synthetic"),
            ("anthropic-1h-off", "anthropic", "claude-synthetic"),
            ("chatgpt-implicit-off", "openai-chatgpt", "gpt-synthetic"),
            ("anthropic-5m-auto-on", "anthropic", "claude-synthetic"),
            ("anthropic-5m-auto-stopped", "anthropic", "claude-synthetic"),
        ] {
            let chatgpt = provider == "openai-chatgpt";
            let ttl = if name.contains("1h") {
                CacheTtl::OneHour
            } else {
                CacheTtl::FiveMinutes
            };
            let policy = CachePolicy {
                version: 1,
                class: CacheClass::Markers {
                    syntax: MarkerSyntax::AnthropicCacheControl,
                    ttl,
                    max_breakpoints: 4,
                    lookback_blocks: 20,
                    tail: None,
                    omit_ttl: ttl == CacheTtl::FiveMinutes,
                },
            };
            let family = if chatgpt {
                WireFamilyId::OpenAiResponsesChatgpt
            } else {
                WireFamilyId::AnthropicMessages
            };
            let frozen = json!({"family_id":family, "cache":if chatgpt { serde_json::Value::Null } else { serde_json::to_value(policy).unwrap() }});
            let mut parts = MemoryParts::default();
            let setup = vec![parts.put(PartKind::RenderConfig, &frozen).unwrap()];
            let stopped = name.ends_with("stopped");
            let mut rig = Rig::new(parts, if stopped { &[("run", "budget")] } else { &[] });
            let sent = rig.sent(&[
                Record::RunStart(Box::new(run_start(
                    handle(provider, model),
                    setup,
                    json!({}),
                    Some(json!(if name.contains("auto") { "auto" } else { "off" })),
                ))),
                Record::Attempt(attempt(1, "complete", Some(1_790_000_000_000), 1_790_000_001_000)),
                Record::StepEnd(step_end(
                    Some(json!({"input_tokens":100, "cached_input_tokens":50, "cache_write_tokens":30, "output_tokens":20})),
                    None,
                )),
            ]);
            let [event] = sent.as_slice() else {
                panic!("{name}: one event per completed step: {sent:?}");
            };
            let value = serde_json::to_value(event).unwrap();
            if chatgpt {
                assert_eq!(value["cache"]["implicit"], true, "{name}");
                assert!(value["cache"].get("ttl_ms").is_none(), "{name}");
            } else {
                assert_eq!(
                    value["cache"]["ttl_ms"],
                    if ttl == CacheTtl::OneHour {
                        3_600_000
                    } else {
                        300_000
                    },
                    "{name}"
                );
            }
            assert_eq!(
                value["cache"]["warming"]["on"],
                name.contains("auto") && !stopped,
                "{name}"
            );
            assert_eq!(
                value["cache"]["warming"].get("stop_reason"),
                stopped.then_some(&json!("cost-cap")),
                "{name}"
            );
            if let Some(dir) = &dir {
                let body = serde_json::to_vec_pretty(event).unwrap();
                std::fs::write(dir.join(format!("{name}.json")), body).unwrap();
            }
        }
    }

    #[test]
    fn multi_model_cache_write_split_requires_every_model_and_exact_total() {
        let first = json!({"model":"a", "cache_creation":{"ephemeral_5m_input_tokens":10, "ephemeral_1h_input_tokens":20}});
        for (second_writes, second_split, total, expected) in [
            (7, json!({}), 37, None),
            (
                7,
                json!({"ephemeral_5m_input_tokens":3, "ephemeral_1h_input_tokens":4}),
                37,
                Some((13, 24)),
            ),
            (
                8,
                json!({"ephemeral_5m_input_tokens":3, "ephemeral_1h_input_tokens":4}),
                38,
                None,
            ),
            (7, json!({"ephemeral_5m_input_tokens":3}), 37, None),
        ] {
            let mut end = step_end(
                None,
                Some(
                    json!({"usage_iterations":[first, {"model":"b", "cache_creation":second_split}]}),
                ),
            );
            end.by_model = [("a", 30), ("b", second_writes)]
                .into_iter()
                .map(|(model, writes)| ModelStepUsage {
                    provider: "provider_a".into(),
                    model: model.into(),
                    variant: None,
                    usage: serde_json::from_value(json!({"cache_write_tokens": writes})).unwrap(),
                })
                .collect();
            let mut rig = Rig::new(Default::default(), &[]);
            let sent = rig.sent(&[
                Record::RunStart(Box::new(run_start(
                    handle("provider_a", "a"),
                    Vec::new(),
                    json!({}),
                    None,
                ))),
                Record::Attempt(attempt(0, "complete", Some(100), 200)),
                Record::StepEnd(end),
            ]);
            let [event] = sent.as_slice() else {
                panic!("one event per completed step: {sent:?}");
            };
            assert_eq!(event.cache_write_tokens, Some(total));
            assert_eq!(
                event.cache_write_5m_tokens.zip(event.cache_write_1h_tokens),
                expected
            );
            if expected.is_none() {
                let body = serde_json::to_value(event).unwrap();
                assert!(body.get("cache_write_5m_tokens").is_none());
                assert!(body.get("cache_write_1h_tokens").is_none());
            }
        }
    }

    #[test]
    fn recorded_content_intervals_and_decision_are_optional_and_checked() {
        let sent = |first: Option<u64>, last: Option<u64>, decision: Option<&str>| {
            let mut start = run_start(handle("provider_a", "model_a"), Vec::new(), json!({}), None);
            start.route_decision_id = decision.map(str::to_owned);
            let mut timed = attempt(0, "complete", Some(1_000), 1_100);
            timed.first_content_at_ms = first;
            timed.last_content_at_ms = last;
            let mut rig = Rig::new(Default::default(), &[]);
            let sent = rig.sent(&[
                Record::RunStart(Box::new(start)),
                Record::Attempt(timed),
                Record::StepEnd(step_end(None, None)),
            ]);
            let [event] = sent.as_slice() else {
                panic!("one event per completed step: {sent:?}");
            };
            serde_json::to_value(event).unwrap()
        };
        let value = sent(Some(1_020), Some(1_080), Some("frozen-caller"));
        assert_eq!(value["route_decision_id"], "frozen-caller");
        assert_eq!(value["sent_to_last_content_ms"], 80);
        assert_eq!(value["first_to_last_content_ms"], 60);
        // Unrecorded content times, and times that cannot be true (content before the send),
        // leave the intervals out rather than reporting a negative or invented span.
        for (first, last) in [(None, None), (Some(990), Some(1_080))] {
            let absent = sent(first, last, None);
            for key in [
                "route_decision_id",
                "sent_to_last_content_ms",
                "first_to_last_content_ms",
            ] {
                assert!(absent.get(key).is_none(), "{key} for {first:?}..{last:?}");
            }
        }
    }

    #[test]
    fn interrupted_attempts_have_no_invented_usage_or_new_position() {
        // A completed attempt whose step never ended: the run ended first.
        let mut rig = Rig::new(Default::default(), &[]);
        let sent = rig.sent(&[
            Record::RunStart(Box::new(run_start(
                handle("provider_a", "model_a"),
                Vec::new(),
                json!({}),
                None,
            ))),
            Record::Attempt(attempt(0, "complete", Some(100), 200)),
            Record::RunEnd(broca_session_store::record::RunEnd {
                run_id: "run".into(),
                reason: broca_protocol::caller::RunFinishReason::Interrupted,
                usage: serde_json::from_value(json!({"input_tokens": 100})).ok(),
                ts_ms: Some(300),
                retries_used: 0,
                interrupt_reason: None,
                error: None,
            }),
        ]);
        let [event] = sent.as_slice() else {
            panic!("one event for the attempt: {sent:?}");
        };
        assert_eq!(event.terminal_status, "interrupted");
        assert_eq!(event.seq, 2);
        assert_eq!(
            event.attempt_id.as_deref(),
            Some("0123456789abcdef0123456789abcdef")
        );
        assert_eq!(event.input_tokens, None);
        assert_eq!(event.finished_at_ms, 200);
        // An attempt a crash sealed in flight carries no usage either.
        let mut rig = Rig::new(Default::default(), &[]);
        let sent = rig.sent(&[
            Record::RunStart(Box::new(run_start(
                handle("provider_a", "model_a"),
                Vec::new(),
                json!({}),
                None,
            ))),
            Record::Attempt(attempt(0, "sealed_in_flight", Some(100), 200)),
            Record::StepEnd(step_end(Some(json!({"input_tokens": 100})), None)),
        ]);
        let [event] = sent.as_slice() else {
            panic!("one event for the attempt: {sent:?}");
        };
        assert_eq!(event.terminal_status, "interrupted");
        assert_eq!(event.input_tokens, None);
    }

    #[test]
    fn missing_identity_or_send_time_drops_the_hint_instead_of_inventing_facts() {
        let start = || {
            Record::RunStart(Box::new(run_start(
                handle("provider_a", "model_a"),
                Vec::new(),
                json!({}),
                None,
            )))
        };
        let mut anonymous = attempt(0, "failed", Some(100), 200);
        anonymous.attempt_id = None;
        let mut rig = Rig::new(Default::default(), &[]);
        assert!(rig.sent(&[start(), Record::Attempt(anonymous)]).is_empty());
        assert_eq!(rig.dropped.load(Ordering::Relaxed), 0);
        // The event needs a start time, and an attempt's completion time is not one.
        let mut rig = Rig::new(Default::default(), &[]);
        assert!(rig
            .sent(&[start(), Record::Attempt(attempt(0, "failed", None, 200))])
            .is_empty());
        assert_eq!(rig.dropped.load(Ordering::Relaxed), 1);
    }
}
