//! `session.warm` on a session in the session store: re-send the session's next model request
//! shaped to generate nothing, keeping the provider's cached prefix alive.
//!
//! Every refusal is decided from what the actor holds before anything is sent, in this order: a
//! lineage named, a running or paused run, no model step yet, an exhausted account, a cache the
//! session's frozen config can't warm, and a cache whose estimated lifetime is spent. The
//! cache's clock is the session's own: the operations fold's last request that reached the
//! cache (`last_cache_send`), a warm included.
//!
//! The request warmed is the one the run loop would send for the latest run's next step, built
//! from the session's state without any provider (`broca_session_loop::run::offline_request`).
//! Its prefix is the prefix the
//! session's last request cached. The actor renders it with the frozen config's wire family to
//! learn its digest; the executor renders it again with the provider it builds and sends it
//! only when the bytes match. A request holding media is refused by name: rendering media from
//! the session's own objects isn't built.

use std::sync::Arc;

use broca_session_loop::writer::Writer as _;
use broca_session_store::record::{Record, Warmed};
use tokio::sync::oneshot;

use super::{unserved, StoreActor};
use crate::actor::WarmOutcome;
use crate::executor::{WarmBase, WarmError, WarmJob};

/// A warm whose request the executor is sending: what records it, and its caller.
pub(super) struct PendingStoreWarm {
    pub(super) idle_auto: bool,
    reply: oneshot::Sender<WarmOutcome>,
    run_id: String,
    step_id: u64,
    request_body_sha256: String,
    ttl_ms: u64,
    /// The cache lifetime the session's requests ask for, which the warm's record keeps.
    requested_ttl_ms: Option<u64>,
    pub(super) task: tokio::task::JoinHandle<Result<broca_core::WarmSent, WarmError>>,
}

/// What a warm's completion needs, decided before it is sent.
struct Prepared {
    run_id: String,
    step_id: u64,
    request_body_sha256: String,
    ttl_ms: u64,
    requested_ttl_ms: Option<u64>,
    job: WarmJob,
}

impl<B: broca_session_store::backend::SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// `session.warm`: refuse it here, or start sending it on its own task, whose end comes back
    /// through [`StoreActor::finish_warm`]. A refusal sends and writes nothing.
    pub(super) async fn warm(
        &mut self,
        lineage_id: Option<String>,
        reply: oneshot::Sender<WarmOutcome>,
        idle_auto: bool,
    ) {
        // At most one warm per session at a time, as today: a second would only repeat the
        // first's effect and cost.
        if self.warm.is_some() {
            let _ = reply.send(WarmOutcome::Refused(broca_wire::WarmRefusal::new(
                broca_wire::WarmRefusalDetail::WarmInFlight,
            )));
            return;
        }
        if idle_auto {
            if let Err(reason) = self.idle_warm_due().await {
                if let Some(registry) = &self.registry {
                    registry.stop_idle_warm(&self.key, &reason);
                }
                let _ = reply.send(WarmOutcome::Error(reason));
                return;
            }
        }
        let prepared = match self.prepare_warm(lineage_id, idle_auto).await {
            Ok(prepared) => prepared,
            Err(outcome) => {
                if idle_auto {
                    if let Some(registry) = &self.registry {
                        registry.stop_idle_warm(&self.key, "warm_refused");
                    }
                }
                let _ = reply.send(*outcome);
                return;
            }
        };
        let executor = Arc::clone(&self.executor);
        let job = prepared.job;
        let task = tokio::spawn(async move {
            // A panicking executor still ends the warm, so the session can be warmed again.
            futures::FutureExt::catch_unwind(std::panic::AssertUnwindSafe(executor.warm(job)))
                .await
                .unwrap_or_else(|_| {
                    Err(WarmError::Unavailable(
                        "the warm task panicked before it had a result".into(),
                    ))
                })
        });
        self.warm = Some(PendingStoreWarm {
            idle_auto,
            reply,
            run_id: prepared.run_id,
            step_id: prepared.step_id,
            request_body_sha256: prepared.request_body_sha256,
            ttl_ms: prepared.ttl_ms,
            requested_ttl_ms: prepared.requested_ttl_ms,
            task,
        });
    }

    /// Decides every refusal, in order, and builds the request to warm. Nothing is sent or
    /// written here.
    async fn prepare_warm(
        &mut self,
        lineage_id: Option<String>,
        idle_auto: bool,
    ) -> Result<Prepared, Box<WarmOutcome>> {
        use broca_wire::WarmRefusalDetail as Refusal;
        let refuse = |detail| {
            Err(Box::new(WarmOutcome::Refused(
                broca_wire::WarmRefusal::new(detail),
            )))
        };
        let now_ms = (self.now_ms)();
        // Lineage ids belonged to the old log format, so a session in the session store has
        // none: a warm naming one names a history this session isn't.
        if let Some(requested) = lineage_id {
            return refuse(Refusal::LineageChanged { requested });
        }
        // A running run's in-flight request could be warmed only with the exact bytes its
        // transport sent, which a run doesn't hand the actor yet.
        if self.run.is_some() {
            return Err(Box::new(WarmOutcome::Error(unserved(
                "a warm while a run is running",
            ))));
        }
        if let Some((run_id, reason)) = self.paused() {
            return refuse(Refusal::RunPaused {
                run_id: run_id.0,
                reason,
            });
        }
        let operations = self.session.operations();
        let Some(run) = operations.run.as_ref() else {
            return refuse(Refusal::NoStepYet);
        };
        if operations.last_send_run.is_none() && run.step_attempt.is_none() {
            return refuse(Refusal::NoStepYet);
        }
        if let Some(resets_at_ms) = account_wall(run, now_ms) {
            return refuse(Refusal::AccountWalled { resets_at_ms });
        }
        let Some(start) = run.start.as_ref() else {
            return refuse(Refusal::NoStepYet);
        };
        let error = |error: broca_session_loop::run::LoopError| {
            Box::new(WarmOutcome::Error(format!(
                "the session's latest run start: {error:?}"
            )))
        };
        let config = run_config(run, start, &self.parts).map_err(error)?;

        // What the session's frozen config allows, before its clock is read.
        let declaration = self
            .registry
            .as_ref()
            .and_then(|registry| registry.cache_declaration(&config));
        let (ttl_ms, requested_ttl_ms) = match &declaration {
            None => {
                return refuse(Refusal::WarmUnsupported {
                    reason: "this runner cannot read the session's cache policy".into(),
                })
            }
            Some(crate::cache::CacheDeclaration {
                warm: Err(reason), ..
            }) => {
                return refuse(Refusal::WarmUnsupported {
                    reason: reason.clone(),
                })
            }
            Some(declaration) => (declaration.ttl_ms, declaration.requested_ttl_ms),
        };
        let reading =
            operations
                .last_cache_send
                .as_ref()
                .map(|send| broca_core::CacheClockReading {
                    last_sent_at_ms: send.sent_at_ms,
                    ttl_ms,
                    prefix_tokens: None,
                });
        // As today: an expired prefix would be written again in full, which a subscription's
        // free writes allow and a metered request doesn't.
        let subscription = config
            .auth_selection
            .as_ref()
            .is_some_and(|auth| auth.credential_id.split(':').next() == Some("chatgpt"));
        if !subscription {
            if let Err(reason) = broca_core::cache_warm::check_warm_expiry(reading, now_ms) {
                return refuse(Refusal::CacheExpired {
                    expires_at_ms: reading.and_then(|reading| reading.expires_at_ms()),
                    reason,
                });
            }
        }

        let step_id = run.completed_steps + 1;
        let unavailable = |reason: String| {
            Box::new(WarmOutcome::Refused(broca_wire::WarmRefusal::new(
                Refusal::WarmPrefixUnavailable {
                    request_body_sha256: None,
                    reason,
                },
            )))
        };
        if idle_auto {
            let request = self.recorded_warm_request().await.map_err(unavailable)?;
            let records = self.idle_warm_records().await.map_err(unavailable)?;
            let headers = records
                .iter()
                .find_map(|(_, record)| match record {
                    Record::Warmed(warm) => warm.ratelimit_headers.clone(),
                    Record::Attempt(attempt) => (attempt
                        .telemetry
                        .as_ref()
                        .map(broca_protocol::json::Json::parse)
                        .and_then(|t| {
                            t.get("account_observation")?
                                .get("ratelimit_headers")
                                .cloned()
                        }))
                    .map(Into::into),
                    _ => None,
                })
                .and_then(|value| serde_json::from_value(value.parse()).ok())
                .unwrap_or_default();
            if let Err(reason) = self.executor.warm_quota(&request.config, &headers).await {
                return refuse(Refusal::WarmUnsupported { reason });
            }
            // A quota query may outlast the prefix it was meant to protect.
            self.idle_warm_due().await.map_err(unavailable)?;
            let mut media = Some(self.run_media().await);
            let renderer = super::recorded_warm::media(&mut media).map_err(unavailable)?;
            return Ok(Prepared {
                run_id: request.run_id.0.clone(),
                step_id: request.step_id.0,
                request_body_sha256: request.request_body_sha256.clone(),
                ttl_ms: ttl_ms.unwrap_or_default(),
                requested_ttl_ms,
                job: WarmJob {
                    session: self.key.clone(),
                    config: request.config.clone(),
                    base: WarmBase::RecordedMedia {
                        request: Box::new(request),
                        renderer,
                    },
                    attempt_hook: None,
                },
            });
        }
        let opts = broca_session_loop::run::offline_request(
            &self.session,
            &self.parts,
            &self.key,
            step_id,
        )
        .await
        .map_err(|error| unavailable(format!("the session's next request: {error:?}")))?;
        if opts
            .prompt
            .iter()
            .any(|message| !broca_session_store::session::message_media(message).is_empty())
        {
            return Err(Box::new(WarmOutcome::Error(unserved(
                "a warm whose request holds media",
            ))));
        }
        let frozen: broca_provider::wire::FrozenRenderConfig =
            serde_json::from_value(config.frozen_render_config.clone())
                .map_err(|error| unavailable(format!("the session's render config: {error}")))?;
        let family = broca_provider::registry::wire_family(frozen.family_id)
            .ok_or_else(|| unavailable("the session's wire family isn't known".into()))?;
        let body = family.render_with_provenance(&frozen.for_step(&opts), &opts, None);
        let request_body_sha256 = broca_core::render::request_digest_hex(&body);
        let job = WarmJob {
            session: self.key.clone(),
            config: config.clone(),
            base: WarmBase::Recorded(Box::new(broca_core::RecordedRequest {
                run_id: broca_protocol::RunId(start.run_id.clone()),
                step_id: broca_protocol::StepId(step_id),
                config,
                opts,
                request_body_sha256: request_body_sha256.clone(),
                route: None,
            })),
            attempt_hook: None,
        };
        Ok(Prepared {
            run_id: start.run_id.clone(),
            step_id,
            request_body_sha256,
            ttl_ms: ttl_ms.unwrap_or_default(),
            requested_ttl_ms,
            job,
        })
    }

    /// The warm's task ended: record a warm the provider answered, durable before its caller
    /// hears of it, and answer the caller. The record moves the session's cache clock: the
    /// operations fold dates the cache from it.
    pub(super) async fn finish_warm(
        &mut self,
        result: Result<Result<broca_core::WarmSent, WarmError>, tokio::task::JoinError>,
    ) {
        let Some(pending) = self.warm.take() else {
            return;
        };
        let result = result.unwrap_or_else(|error| {
            Err(WarmError::Unavailable(format!(
                "the warm task ended without a result: {error}"
            )))
        });
        let sent = match result {
            Ok(sent) => sent,
            Err(error) => {
                if pending.idle_auto {
                    if let Some(registry) = &self.registry {
                        registry.stop_idle_warm(&self.key, "warm_failed");
                    }
                }
                let _ = pending.reply.send(crate::actor::warm_error_outcome(
                    error,
                    pending.request_body_sha256,
                ));
                return;
            }
        };
        let expires_at_ms = sent.sent_at_ms.saturating_add(pending.ttl_ms);
        if pending.idle_auto {
            let record = match super::keep_warm::warmed(
                &broca_protocol::RunId(pending.run_id.clone()),
                pending.step_id,
                &pending.request_body_sha256,
                &sent,
                pending.ttl_ms,
                pending.requested_ttl_ms,
            ) {
                Ok(record) => Record::Warmed(record),
                Err(error) => {
                    let _ = pending.reply.send(WarmOutcome::Error(error));
                    return;
                }
            };
            let (durable, written) = oneshot::channel();
            self.record_kept_warm(super::keep_warm::WarmRecord {
                record,
                finished_at_ms: (self.now_ms)(),
                request_body_bytes: sent.request_body_bytes,
                durable,
            })
            .await;
            if let Err(error) = written
                .await
                .unwrap_or_else(|_| Err("warm writer closed".into()))
            {
                if let Some(registry) = &self.registry {
                    registry.stop_idle_warm(&self.key, "record_failed");
                }
                let _ = pending.reply.send(WarmOutcome::Error(error));
                return;
            }
            if let Some(billing) = &self.billing {
                if let Err(error) = billing
                    .insert(&self.session, &self.key, &self.parts, &pending.run_id)
                    .await
                {
                    if let Some(registry) = &self.registry {
                        registry.stop_idle_warm(&self.key, "billing_failed");
                    }
                    let _ = pending.reply.send(WarmOutcome::Error(error));
                    return;
                }
            }
            self.arm_idle_warm().await;
            let _ = pending
                .reply
                .send(WarmOutcome::Warmed(broca_wire::WarmReply {
                    request_body_sha256: pending.request_body_sha256,
                    warm_body_sha256: sent.warm_body_sha256,
                    cache_read_tokens: sent.usage.cached_input_tokens,
                    cache_write_tokens: sent.usage.cache_write_tokens,
                    input_tokens: sent.usage.input_tokens,
                    ttl_ms: pending.ttl_ms,
                    expires_at_ms,
                    expiry_basis: broca_wire::EXPIRY_BASIS_ESTIMATED.into(),
                }));
            return;
        }
        let digest =
            |hex: &str| sha256_digest(hex).ok_or_else(|| format!("{hex} is not a SHA-256 digest"));
        let headers: std::collections::BTreeMap<String, String> =
            sent.ratelimit_headers.iter().cloned().collect();
        let record = match (
            digest(&pending.request_body_sha256),
            digest(&sent.warm_body_sha256),
        ) {
            (Ok(request_body_sha256), Ok(warm_body_sha256)) => Record::Warmed(Warmed {
                run_id: pending.run_id,
                step_id: pending.step_id,
                trigger: None,
                request_body_sha256,
                warm_body_sha256,
                usage: sent.usage,
                served_account_ref: sent.served_account_ref.clone(),
                sent_at_ms: sent.sent_at_ms,
                ttl_ms: Some(pending.ttl_ms),
                expires_at_ms: Some(expires_at_ms),
                attempt_id: None,
                billing: None,
                ratelimit_headers: ((!headers.is_empty())
                    .then(|| serde_json::to_value(&headers).expect("headers serialize")))
                .map(Into::into),
                // A warm answered without a billing envelope reached the cache
                // (`warm_reached_the_cache`).
                reached_cache: Some(true),
                requested_cache_ttl_ms: pending.requested_ttl_ms,
            }),
            (Err(error), _) | (_, Err(error)) => {
                let _ = pending.reply.send(WarmOutcome::Error(error));
                return;
            }
        };
        if let Err(error) = broca_session_loop::writer::Writer::append(
            &mut self.durable(),
            std::slice::from_ref(&record),
            true,
        )
        .await
        {
            // The provider answered (and billed) the warm; only its record failed.
            tracing::warn!(
                target: "cache",
                session = %self.key.session,
                "a warm on the new storage was sent but not recorded: {error:?}",
            );
        }
        let _ = pending
            .reply
            .send(WarmOutcome::Warmed(broca_wire::WarmReply {
                request_body_sha256: pending.request_body_sha256,
                warm_body_sha256: sent.warm_body_sha256,
                cache_read_tokens: sent.usage.cached_input_tokens,
                cache_write_tokens: sent.usage.cache_write_tokens,
                input_tokens: sent.usage.input_tokens,
                ttl_ms: pending.ttl_ms,
                expires_at_ms,
                expiry_basis: broca_wire::EXPIRY_BASIS_ESTIMATED.into(),
            }));
    }
}

/// The config `run` runs under: the one its start `start` froze, with the refreezes it recorded
/// since then applied.
pub(crate) fn run_config(
    run: &broca_session_store::fold::operations::Run,
    start: &broca_session_store::record::RunStart,
    parts: &dyn broca_session_loop::parts::Parts,
) -> Result<broca_wal::RunConfig, broca_session_loop::run::LoopError> {
    let mut config = broca_session_loop::run::run_config_of_start(start, parts)?;
    // A refreeze before the run's first provider call renders and bills under what it froze.
    if let Some(refrozen) = &run.refrozen_render {
        config.frozen_render_config = refrozen["render_config"].clone();
        if let Some(version) = refrozen["renderer_version"].as_u64() {
            config.renderer_version = version as u32;
        }
    }
    if let Some(selection) = &run.refrozen_auth {
        config.auth_selection = serde_json::from_value(selection.clone()).ok();
    }
    Ok(config)
}

/// When the latest run ended or paused on an exhausted provider account whose reset is still in
/// the future, return that reset time. A later run starts with no error. A warm that reached
/// the cache also clears it, but no warm is sent while it stands, so only a reset that has
/// passed lifts it.
fn account_wall(run: &broca_session_store::fold::operations::Run, now_ms: u64) -> Option<u64> {
    let error: broca_protocol::ProviderError =
        serde_json::from_value(run.end_error.clone()?).ok()?;
    if !error.is_account_limit_exhausted() {
        return None;
    }
    error
        .resets_at_ms
        .filter(|resets_at_ms| *resets_at_ms > now_ms)
}

/// The digest a lowercase hex SHA-256 spells.
pub(super) fn sha256_digest(hex: &str) -> Option<broca_session_store::codec::Digest> {
    if hex.len() != 64 {
        return None;
    }
    let mut bytes = [0; 32];
    for (at, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(hex.get(at * 2..at * 2 + 2)?, 16).ok()?;
    }
    Some(broca_session_store::codec::Digest(bytes))
}
