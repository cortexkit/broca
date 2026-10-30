//! Offline per-attempt cache evidence. Unverifiable replay is a gap, never an inferred rewrite.

use std::{collections::BTreeMap, path::Path};

use broca_core::{Clock, FileWalStore};
use broca_protocol::{BindIdentity, Usage};
use broca_provider::{
    cache_evidence::{compare_rendered_requests, TtlKind, View},
    spec::WireFamilyId,
    wire::{CacheClass, CacheTtl, FrozenRenderConfig},
};
use broca_wal::{FramedRecord, WalRecord};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

pub fn read(root: &Path, bind: &BindIdentity) -> Result<Vec<Value>, String> {
    bind.validate_keyable().map_err(|e| e.to_string())?;
    let store = FileWalStore::new(root.join("wal"));
    let clock = Clock::for_state_root(root).map_err(|_| "cannot read retention clock")?;
    let (lineage, frames) = crate::cache_dump::exportable_frames(&store, bind, clock.now_ms())?;
    let rows = rows(&frames, lineage.map(|id| id.to_string()).as_deref())?;
    if rows.is_empty() {
        return Err(format!("no model attempts for session {:?}", bind.session));
    }
    Ok(rows)
}

struct Previous {
    locator: Option<String>,
    rendered: Option<(FrozenRenderConfig, Vec<u8>)>,
    sent: Option<u64>,
    model: String,
    account: Option<String>,
    view: Option<u64>,
    usage: Option<Usage>,
}

/// The dashboard denominator uses the last cache write when present, otherwise all prior input.
/// Unknown meters stay unknown: a missing cache-read count is not evidence of a full miss.
pub fn retention(previous: Option<Usage>, current: Option<Usage>) -> Value {
    let Some((previous, read)) = previous.zip(current.and_then(|u| u.cached_input_tokens)) else {
        return json!({"ratio":null,"status":"unmetered","expected":null});
    };
    let Some(previous_read) = previous.cached_input_tokens else {
        return json!({"ratio":null,"status":"unmetered","expected":null});
    };
    let extra = if previous.cache_write_tokens.is_some_and(|n| n > 0) {
        previous.cache_write_tokens
    } else {
        previous.input_tokens
    };
    let Some(expected) = extra.and_then(|n| previous_read.checked_add(n)) else {
        return json!({"ratio":null,"status":"unmetered","expected":null});
    };
    if expected == 0 || (previous_read == 0 && previous.cache_write_tokens.unwrap_or(0) == 0) {
        return json!({"ratio":null,"status":"base","expected":expected});
    }
    let ratio = read as f64 / expected as f64;
    let status = if read == 0 {
        "full_bust"
    } else if ratio >= 0.95 {
        "stable"
    } else if ratio >= 0.8 {
        "warning"
    } else {
        "bust"
    };
    json!({"ratio":ratio,"status":status,"expected":expected})
}

fn ttl(frozen: &FrozenRenderConfig) -> (Option<u64>, TtlKind) {
    match frozen.cache.as_ref().map(|c| &c.class) {
        Some(CacheClass::Markers { ttl, .. }) => (
            Some(match ttl {
                CacheTtl::FiveMinutes => 300_000,
                CacheTtl::OneHour => 3_600_000,
            }),
            TtlKind::Explicit,
        ),
        Some(CacheClass::Hints { retention, .. }) => (
            match retention.as_deref() {
                Some("24h") => Some(86_400_000),
                _ => None,
            },
            TtlKind::Implicit,
        ),
        Some(CacheClass::None) | None => (
            None,
            if matches!(
                frozen.family_id,
                WireFamilyId::AnthropicMessages | WireFamilyId::BedrockConverse
            ) {
                TtlKind::Explicit
            } else {
                TtlKind::Implicit
            },
        ),
    }
}

pub fn rows(frames: &[FramedRecord], lineage: Option<&str>) -> Result<Vec<Value>, String> {
    let records: Vec<_> = frames.iter().map(|f| f.record.clone()).collect();
    let mut rows = Vec::new();
    let mut config = None;
    let mut run = None;
    let mut boundary = None;
    let mut cap = None;
    let mut view = None;
    let mut previous: Option<Previous> = None;
    let mut step_usage = BTreeMap::new();
    let mut corrections = BTreeMap::new();
    // Only the final complete attempt of a legacy step can inherit that step's meter.
    let mut completed = BTreeMap::new();
    let mut run_index = 0;
    for (index, frame) in frames.iter().enumerate() {
        match &frame.record {
            WalRecord::RunStarted { .. } => run_index = index,
            WalRecord::ModelAttemptFinished {
                step_id, outcome, ..
            } if *outcome == broca_protocol::provider::AttemptOutcome::Complete => {
                completed.insert((run_index, step_id.0), index);
            }
            WalRecord::ModelStepFinished { step_id, usage, .. } => {
                step_usage.insert((run_index, step_id.0), *usage);
            }
            WalRecord::AttemptCorrected {
                attempt_id,
                reservation,
                envelope,
                ..
            } => {
                corrections.insert(attempt_id, (reservation, envelope));
            }
            _ => {}
        }
    }
    run_index = 0;
    for (index, frame) in frames.iter().enumerate() {
        match &frame.record {
            WalRecord::RunStarted {
                run_id, config: c, ..
            } => {
                config = Some(c);
                run = Some(run_id);
                run_index = index;
                boundary = None;
                cap = None;
            }
            WalRecord::StepStarted { .. } => {
                boundary = Some(index);
                cap = None;
            }
            WalRecord::OutputReservationClamped {
                max_output_tokens, ..
            } => cap = Some(*max_output_tokens),
            WalRecord::CompactionViewApplied { view_version, .. } => view = Some(*view_version),
            WalRecord::ModelAttemptFinished {
                step_id,
                attempt,
                attempt_id,
                reservation,
                envelope,
                request_body_sha256,
                sent_at_ms,
                served_account_ref,
                websocket_request,
                ..
            } => {
                let locator = lineage.map(|id| format!("{id}:{}", frame.seq));
                let corrected = attempt_id.as_ref().and_then(|id| corrections.get(id));
                let reservation = corrected
                    .map(|(r, _)| r.as_ref())
                    .or(reservation.as_deref());
                let envelope = corrected.map(|(_, e)| e.as_ref()).or(envelope.as_deref());
                let sent = envelope.and_then(|e| e.sent_at_ms).or(*sent_at_ms);
                let account = reservation
                    .and_then(|r| r.credential.account_ref.clone())
                    .or_else(|| served_account_ref.clone());
                let model = reservation
                    .map(|r| format!("{}/{}", r.provider, r.model_sent))
                    .or_else(|| {
                        config.map(|c| {
                            format!(
                                "{}/{}",
                                c.model.provider_module_id,
                                c.frozen_render_config
                                    .get("wire_model_id")
                                    .and_then(Value::as_str)
                                    .unwrap_or(&c.model.model_id)
                            )
                        })
                    })
                    .unwrap_or_default();
                let usage = envelope.and_then(|e| e.usage).or_else(|| {
                    (envelope.is_none() && completed.get(&(run_index, step_id.0)) == Some(&index))
                        .then(|| step_usage.get(&(run_index, step_id.0)).copied())
                        .flatten()
                });
                let rebuilt =
                    boundary
                        .zip(config)
                        .zip(run)
                        .and_then(|((boundary, config), run)| {
                            crate::wal_render::render_step_with_output_cap(
                                &records[..=boundary],
                                config,
                                &run.0,
                                step_id.0,
                                cap,
                            )
                            .ok()
                        });
                let reason = match &rebuilt {
                    None => Some("render_unavailable"),
                    Some((_, _, body))
                        if format!("{:x}", Sha256::digest(body)) != *request_body_sha256 =>
                    {
                        Some("digest_mismatch")
                    }
                    _ => None,
                };
                let rendered = rebuilt
                    .filter(|_| reason.is_none())
                    .map(|(frozen, _, body)| (frozen, body));
                let mut evidence = None;
                let mut comparison_reason = reason;
                if let Some(prev) = &previous {
                    if let (Some((pf, pb)), Some((cf, cb))) = (&prev.rendered, &rendered) {
                        if pf.family_id == cf.family_id {
                            let mut e = compare_rendered_requests(cf.family_id, pb, cb)?;
                            e.compared_to = prev.locator.clone();
                            e.idle_gap_ms =
                                prev.sent.zip(sent).map_or(0, |(a, b)| b.saturating_sub(a));
                            (e.cache_ttl_ms, e.ttl_kind) = ttl(pf);
                            e.model_changed = prev.model != model;
                            e.account_changed = prev
                                .account
                                .as_ref()
                                .zip(account.as_ref())
                                .is_some_and(|(a, b)| a != b);
                            e.view = (view.is_some() || prev.view.is_some()).then_some(View {
                                applied: view,
                                previous: prev.view,
                            });
                            evidence = Some(e);
                        } else {
                            comparison_reason = Some("family_changed");
                        }
                    } else if reason.is_none() {
                        comparison_reason = Some("previous_render_unavailable");
                    }
                } else if reason.is_none() {
                    comparison_reason = Some("first_attempt");
                }
                let codex = websocket_request.as_ref().and_then(|w| w.codex.as_deref());
                rows.push(json!({"kind":"cache_attempt", "wal_seq":frame.seq, "lineage_id":lineage,
                    "run_id":run, "step_id":step_id, "attempt":attempt, "attempt_id":attempt_id,
                    "sent_at_ms":sent, "model":model, "account_ref":account,
                    "wire_family":rendered.as_ref().map(|(f,_)| f.family_id),
                    "verified":rendered.is_some(), "reason":comparison_reason,
                    "idle_gap_known":previous.as_ref().is_some_and(|p| p.sent.zip(sent).is_some()),
                    "account_comparison_known":previous.as_ref().is_some_and(|p| p.account.as_ref().zip(account.as_ref()).is_some()),
                    "cache_evidence":evidence, "usage":usage,
                    "retention":retention(previous.as_ref().and_then(|p| p.usage), usage),
                    "continuation":codex.map(|c| c.continuation),
                    "full_input_reason":codex.and_then(|c| c.full_input_reason.as_deref())}));
                previous = Some(Previous {
                    locator,
                    rendered,
                    sent,
                    model,
                    account,
                    view,
                    usage,
                });
            }
            _ => {}
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(value: Value) -> WalRecord {
        serde_json::from_value(value).unwrap()
    }

    fn fixture() -> Vec<FramedRecord> {
        let mut start: Value = serde_json::from_str(include_str!(
            "../../broca-wal/tests/golden/pre_step_transforms/run_started.json"
        ))
        .unwrap();
        let mut frozen = broca_provider::families::anthropic_apikey_frozen_config("m");
        frozen.cache = Some(broca_provider::wire::CachePolicy {
            version: 1,
            class: CacheClass::Markers {
                syntax: broca_provider::wire::MarkerSyntax::AnthropicCacheControl,
                ttl: CacheTtl::OneHour,
                max_breakpoints: 4,
                lookback_blocks: 20,
                tail: None,
                omit_ttl: false,
            },
        });
        start["config"]["frozen_render_config"] = serde_json::to_value(frozen).unwrap();
        let start = record(start);
        let WalRecord::RunStarted { config, run_id, .. } = &start else {
            unreachable!()
        };
        let mut records = vec![
            start.clone(),
            record(json!({"type":"step_started","step_id":1})),
        ];
        let (_, _, body) = crate::wal_render::render_step(&records, config, &run_id.0, 1).unwrap();
        for (attempt, outcome, sent) in [(0, "retryable_error", 100), (1, "complete", 250)] {
            records.push(record(json!({"type":"model_attempt_finished","step_id":1,"attempt":attempt,
                "request_body_len":body.len(),"request_body_sha256":format!("{:x}",Sha256::digest(&body)),
                "response_body_len":0,"response_body_sha256":"", "outcome":outcome,
                "ts_ms":sent+50,"sent_at_ms":sent,"served_account_ref":if attempt == 0 {"one"} else {"two"}})));
        }
        records.push(record(json!({"type":"model_step_finished","step_id":1,
            "usage":{"input_tokens":10,"cached_input_tokens":100,"cache_write_tokens":50},
            "assistant_message":{"message_id":"m1","content":[{"type":"text","text":"answer"}]}})));
        records
            .into_iter()
            .enumerate()
            .map(|(i, record)| FramedRecord {
                seq: i as u64 + 1,
                fence: 1,
                record,
            })
            .collect()
    }

    #[test]
    fn every_attempt_keeps_its_locator_send_time_and_account_without_borrowing_retry_usage() {
        let frames = fixture();
        let rows = rows(&frames, Some("lineage")).unwrap();
        assert_eq!(rows.len(), 2);
        assert!(rows[0]["usage"].is_null());
        assert_eq!(rows[0]["reason"], "first_attempt");
        assert_eq!(rows[1]["usage"]["cached_input_tokens"], 100);
        let e = &rows[1]["cache_evidence"];
        assert_eq!(e["compared_to"], "lineage:3");
        assert_eq!(e["idle_gap_ms"], 150);
        assert_eq!(e["cache_ttl_ms"], 3_600_000);
        assert_eq!(e["ttl_kind"], "explicit");
        assert_eq!(e["account_changed"], true);
        assert_eq!(e["extends_previous"], true);
        assert!(e["first_divergence"].is_null());
        let serialized = serde_json::to_string(&rows).unwrap();
        assert!(!serialized.contains("answer"));
        assert!(!serialized.contains("request_body"));
    }

    #[test]
    fn digest_mismatch_is_a_gap_not_a_rewrite_and_missing_send_time_stays_unknown() {
        let mut frames = fixture();
        if let WalRecord::ModelAttemptFinished {
            request_body_sha256,
            sent_at_ms,
            ..
        } = &mut frames[2].record
        {
            *request_body_sha256 = "mismatch".into();
            *sent_at_ms = None;
        }
        let rows = rows(&frames, Some("lineage")).unwrap();
        assert_eq!(rows[0]["verified"], false);
        assert_eq!(rows[0]["reason"], "digest_mismatch");
        assert_eq!(rows[1]["reason"], "previous_render_unavailable");
        assert!(rows[1]["cache_evidence"].is_null());
        assert_eq!(rows[1]["idle_gap_known"], false);
    }

    #[test]
    fn later_steps_and_runs_compare_with_the_immediately_previous_attempt() {
        let mut frames = fixture();
        let WalRecord::RunStarted { config, run_id, .. } = &frames[0].record else {
            unreachable!()
        };
        let config = config.clone();
        let run_id = run_id.clone();
        let mut records: Vec<_> = frames.iter().map(|f| f.record.clone()).collect();
        records.push(record(json!({"type":"turn_finished","step_id":1})));
        records.push(record(json!({"type":"step_started","step_id":2})));
        let (_, _, body) = crate::wal_render::render_step(&records, &config, &run_id.0, 2).unwrap();
        records.push(record(json!({"type":"model_attempt_finished","step_id":2,"attempt":0,
            "request_body_len":body.len(),"request_body_sha256":format!("{:x}",Sha256::digest(&body)),
            "response_body_len":1,"response_body_sha256":"", "outcome":"complete",
            "ts_ms":800,"sent_at_ms":700,"served_account_ref":"two"})));
        records.push(record(json!({"type":"model_step_finished","step_id":2,
            "usage":{"input_tokens":10,"cached_input_tokens":150,"cache_write_tokens":0},
            "assistant_message":{"message_id":"m2","content":[{"type":"text","text":"second answer"}]}})));
        records.push(record(json!({"type":"turn_finished","step_id":2})));
        records.push(record(
            json!({"type":"run_finished","reason":"completed","usage":{}}),
        ));
        let mut start = serde_json::to_value(&records[0]).unwrap();
        start["run_id"] = json!("second-run");
        start["config"]["frozen_render_config"]["wire_model_id"] = json!("different-wire-model");
        let start = record(start);
        let WalRecord::RunStarted { config, run_id, .. } = &start else {
            unreachable!()
        };
        let (config, run_id) = (config.clone(), run_id.clone());
        records.push(start);
        records.push(record(json!({"type":"step_started","step_id":1})));
        let (_, _, body) = crate::wal_render::render_step(&records, &config, &run_id.0, 1).unwrap();
        records.push(record(json!({"type":"model_attempt_finished","step_id":1,"attempt":0,
            "request_body_len":body.len(),"request_body_sha256":format!("{:x}",Sha256::digest(&body)),
            "response_body_len":0,"response_body_sha256":"", "outcome":"complete",
            "ts_ms":1000,"sent_at_ms":900,"served_account_ref":"two"})));
        frames = records
            .into_iter()
            .enumerate()
            .map(|(i, record)| FramedRecord {
                seq: i as u64 + 1,
                fence: 1,
                record,
            })
            .collect();
        let rows = rows(&frames, Some("lineage")).unwrap();
        assert_eq!(rows.len(), 4);
        assert_eq!(rows[2]["cache_evidence"]["extends_previous"], true);
        assert_eq!(rows[2]["retention"]["expected"], 150);
        assert_eq!(rows[2]["retention"]["status"], "stable");
        assert_eq!(rows[3]["cache_evidence"]["model_changed"], true);
        assert_eq!(
            rows[3]["cache_evidence"]["compared_to"],
            format!("lineage:{}", rows[2]["wal_seq"].as_u64().unwrap())
        );
    }

    #[test]
    fn dashboard_retention_thresholds_and_write_denominator_are_exact() {
        let usage = |read, write, input| Usage {
            cached_input_tokens: Some(read),
            cache_write_tokens: Some(write),
            input_tokens: Some(input),
            ..Default::default()
        };
        let previous = usage(60, 40, 500);
        for (read, status) in [
            (100, "stable"),
            (95, "stable"),
            (94, "warning"),
            (80, "warning"),
            (79, "bust"),
            (0, "full_bust"),
        ] {
            let result = retention(Some(previous), Some(usage(read, 0, 10)));
            assert_eq!(result["status"], status);
            assert_eq!(result["expected"], 100);
        }
        assert_eq!(
            retention(Some(usage(60, 0, 40)), Some(usage(95, 0, 0)))["status"],
            "stable"
        );
        assert_eq!(
            retention(Some(usage(0, 0, 100)), Some(usage(0, 0, 100)))["status"],
            "base"
        );
        assert_eq!(
            retention(Some(previous), Some(Usage::default()))["status"],
            "unmetered"
        );
    }
}
