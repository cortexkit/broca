//! Warming a session's prompt cache between turns, by the mode its latest run froze
//! (llm-runner `keep_warm`): `auto` buys reads only while their cost since the last real request
//! stays below one cache rewrite; `always` and `hours` stop after a fixed count, and `hours`
//! warms only inside its window.

use broca_core::keep_warm_wait::WarmTiming;
use broca_session_loop::writer::Writer as _;
use broca_session_store::record::Record;

impl<B: broca_session_store::backend::SessionBackend + Send + Sync + 'static> super::StoreActor<B> {
    /// Newest first, no farther back than the last real model attempt.
    pub(super) async fn idle_warm_records(&self) -> Result<Vec<(u64, Record)>, String> {
        self.session
            .ledger_tail_until(|record| matches!(record, Record::Attempt(_)))
            .await
            .map_err(|error| format!("warm_history: {error:?}"))
    }

    pub(super) async fn bill_recorded_idle_warms(&self) {
        let Some(billing) = &self.billing else { return };
        let Some(run) = self
            .session
            .operations()
            .run
            .as_ref()
            .filter(|run| run.finished.is_some())
        else {
            return;
        };
        let run_id = run.run_id.as_str();
        let Ok(records) = self
            .session
            .ledger_tail_until(|record| match record {
                Record::Billed(billed) => billed.run_id == run_id,
                Record::RunStart(start) => start.run_id == run_id,
                _ => false,
            })
            .await
        else {
            return;
        };
        let after_billed =
            matches!(records.last(), Some((_, Record::Billed(billed))) if billed.run_id == run_id);
        // Automatic and manual warms alike: each is exported as its own fact.
        let late_warm = records
            .iter()
            .any(|(_, record)| matches!(record, Record::Warmed(warm) if warm.run_id == run_id));
        if !after_billed || !late_warm {
            return;
        }
        // A warm can arrive after the latest run's Billed record. Recover a crash between that
        // warm's durable write and its spend insert, without touching already-settled older runs.
        if let Err(error) = billing
            .insert(&self.session, &self.key, &self.parts, run_id)
            .await
        {
            tracing::warn!(target: "cache_warm", session = %broca_core::session_addr(&self.key), %error, "could not recover warm spend facts");
        }
    }
    pub(super) async fn idle_warm_due(&self) -> Result<u64, String> {
        if self.run.is_some() || self.stopping || self.close_when_idle {
            return Err("run_active_or_closing".into());
        }
        let operations = self.session.operations();
        // Its owner ended a released session: nothing keeps its cache.
        if operations.released_at_ms.is_some() {
            return Err("released".into());
        }
        let run = operations.run.as_ref().ok_or("no_run")?;
        let start = run.start.as_ref().ok_or("no_run_start")?;
        if run.finished.is_none()
            || self
                .one_shot_runs
                .contains(&broca_protocol::RunId(run.run_id.clone()))
        {
            return Err("unfinished_or_one_shot".into());
        }
        let keep_warm: broca_protocol::request::KeepWarm = start
            .keep_warm
            .clone()
            .and_then(|keep_warm| serde_json::from_value(keep_warm).ok())
            .unwrap_or_default();
        if !keep_warm.warms() {
            return Err("keep_warm_off".into());
        }
        let config =
            super::warm::run_config(run, start, &self.parts).map_err(|e| format!("{e:?}"))?;
        let declaration = self
            .registry
            .as_ref()
            .and_then(|r| r.cache_declaration(&config))
            .ok_or("cache_policy_unavailable")?;
        declaration.warm?;
        let ttl_ms = declaration
            .ttl_ms
            .filter(|ttl| *ttl > 0)
            .ok_or("cache_ttl_unavailable")?;
        let send = operations.last_cache_send.as_ref().ok_or("no_cache_send")?;
        broca_core::cache_warm::check_warm_expiry(
            Some(broca_core::CacheClockReading {
                last_sent_at_ms: send.sent_at_ms,
                ttl_ms: Some(ttl_ms),
                prefix_tokens: None,
            }),
            (self.now_ms)(),
        )
        .map_err(|_| "cache_expired")?;
        // Only `auto` is priced; the other modes warm without a rate.
        let auto = keep_warm == broca_protocol::request::KeepWarm::Auto;
        let costs = if auto {
            let prefix = run
                .last_step_usage
                .and_then(broca_core::cache_warm::prefix_tokens)
                .ok_or("prefix_tokens_unavailable")?;
            Some(self.executor.warm_costs(&config, prefix, ttl_ms)?)
        } else {
            None
        };
        // Sealed history matters too: actor eviction and log rotation must not reset the caps.
        let records = self.idle_warm_records().await?;
        let (mut spent, mut warms) = (0.0, 0_u32);
        for (_, record) in &records {
            match record {
                Record::Attempt(_) => break,
                Record::Warmed(warm) => {
                    if warm.usage.cached_input_tokens == Some(0) {
                        return Err("no_cache_read".into());
                    }
                    warms = warms.saturating_add(1);
                    spent += match costs {
                        Some(_) if super::keep_warm::responses(&config) => {
                            self.executor.responses_warm_cost(&config, warm.usage)?
                        }
                        Some(costs) => costs.warm,
                        None => 0.0,
                    };
                }
                _ => {}
            }
        }
        let due = WarmTiming::for_ttl(ttl_ms, self.executor.warm_after_send_ms(&config))
            .next_due_ms(send.sent_at_ms);
        let costs = costs.ok_or_else(|| "not priced".to_owned());
        broca_core::keep_warm::allows_warm(&keep_warm, costs, spent, warms, due)
            .map_err(|(reason, _)| reason.to_owned())?;
        Ok(due)
    }

    pub(super) async fn arm_idle_warm(&self) {
        let Some(registry) = &self.registry else {
            return;
        };
        match self.idle_warm_due().await {
            Ok(due) => registry.arm_idle_warm(&self.key, due, (self.now_ms)()),
            Err(reason) => registry.stop_idle_warm(&self.key, &reason),
        }
    }
}
