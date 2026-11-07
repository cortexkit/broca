use std::collections::BTreeMap;

use broca_session_loop::parts::MemoryParts;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::{Placement, Record};
use broca_session_store::session::{Carry, Session};
use broca_wal::{FramedRecord, WalRecord};
use futures::executor::block_on;

use super::{bind_of, compare, log_session, triple, warms_at, Comparison};
use crate::convert;

fn log(lines: &[String]) -> Vec<FramedRecord> {
    lines
        .iter()
        .enumerate()
        .map(|(i, line)| FramedRecord {
            seq: i as u64 + 1,
            fence: 0,
            record: serde_json::from_str::<WalRecord>(line)
                .unwrap_or_else(|e| panic!("fixture line {i}: {e}")),
        })
        .collect()
}

fn start(run: &str, ts: u64) -> String {
    format!(
        r#"{{"type":"run_started","run_id":"{run}","ts_ms":{ts},"session":{{"project_root":"/project","harness":"runner","session":"s"}},"config":{{"model":{{"provider_module_id":"p","model_id":"m"}},"tools":[],"tool_choice":{{"type":"auto"}},"generation":{{}},"response_format":{{"type":"text"}},"context_epoch":0,"renderer_version":1,"auth_selection":{{"credential_id":"apikey:p:work","account_ref":"acct-1"}},"frozen_render_config":{{"usage":"open_ai_usage"}}}},"input":[{{"role":"user","content":[{{"type":"text","text":"hi"}}]}}],"origin":{{"kind":"fresh"}}}}"#
    )
}

fn step(id: u64, at: u64, extra: &str) -> String {
    format!(
        r#"{{"type":"model_step_finished","step_id":{id},"assistant_message":{{"message_id":"m{id}","content":[{{"type":"text","text":"ok"}}]}},"usage":{{"input_tokens":{at},"output_tokens":2}},"finish_reason":"stop","retries_used":1,"written_at_ms":{at}{extra}}}"#
    )
}

fn warm(run: &str, step: u64, at: u64) -> String {
    let a = "a".repeat(64);
    let b = "b".repeat(64);
    format!(
        r#"{{"type":"cache_warmed","run_id":"{run}","step_id":{step},"request_body_sha256":"{a}","warm_body_sha256":"{b}","usage":{{"input_tokens":5,"cache_read_tokens":40}},"sent_at_ms":{at}}}"#
    )
}

fn finished(at: u64) -> String {
    format!(
        r#"{{"type":"run_finished","reason":"completed","usage":{{}},"ts_ms":{at},"retries_used":4}}"#
    )
}

/// Four runs, with what today's builder reads across runs:
/// - `r1`: a refreeze before its first step and an auth refreeze between steps, each splitting
///   its segments, a step routed to the refusal-fallback model, which bills in a segment of its
///   own, and a warm of the routed step after the run's end, billed under the model the step
///   went to;
/// - `r2`: paused, then continued to its end;
/// - a warm of `r1` after `r2` and `r3` started, still billed under `r1`;
/// - `r3`: an ordinary run that ends;
/// - `r4`: paused and never ended, the session's last run.
fn runs() -> Vec<FramedRecord> {
    let route =
        r#","route":{"model":{"provider_module_id":"p","model_id":"fallback"},"reason":"refusal"}"#;
    log(&[
        start("r1", 100),
        r#"{"type":"run_config_refrozen","renderer_version":2,"ts_ms":105,"frozen_render_config":{"usage":"open_ai_usage_reasoning_separate"}}"#.into(),
        r#"{"type":"step_started","step_id":1}"#.into(),
        step(1, 110, route),
        r#"{"type":"step_started","step_id":2}"#.into(),
        step(2, 130, ""),
        r#"{"type":"run_auth_refrozen","ts_ms":140,"auth_selection":{"credential_id":"oauth:p:other"}}"#.into(),
        r#"{"type":"step_started","step_id":3}"#.into(),
        step(3, 150, ""),
        finished(200),
        warm("r1", 1, 250),
        start("r2", 300),
        r#"{"type":"step_started","step_id":1}"#.into(),
        step(1, 310, ""),
        r#"{"type":"run_paused","run_id":"r2","reason":"restart","ts_ms":320}"#.into(),
        r#"{"type":"step_started","step_id":2}"#.into(),
        step(2, 330, ""),
        finished(340),
        start("r3", 400),
        warm("r1", 2, 410),
        r#"{"type":"step_started","step_id":1}"#.into(),
        step(1, 420, ""),
        finished(500),
        start("r4", 600),
        r#"{"type":"step_started","step_id":1}"#.into(),
        step(1, 610, ""),
        r#"{"type":"run_paused","run_id":"r4","reason":"restart","ts_ms":620}"#.into(),
    ])
}

/// Converts `old`, writes it into a session in the new storage, and compares today's facts with
/// the new path's, open and after sealing at the newest cut. Also checks the converted session
/// owes no billing. `bind` replaces the identity the store bills with, to show a wrong one shows.
fn compared(
    old: &[FramedRecord],
    bind: Option<broca_protocol::BindIdentity>,
) -> (Comparison, Comparison) {
    block_on(async {
        let converted = convert(old, None).unwrap();
        assert!(converted.lost.is_empty(), "{:?}", converted.lost);
        let backend = MemoryBackend::default();
        let triple = triple(&log_session(old).unwrap());
        let bind = bind.unwrap_or_else(|| bind_of(&triple));
        let deployment = DeploymentKey::new(1, &[3; 32]);
        let addr = SessionAddr("billing".into());
        let mut session =
            Session::create(&backend, addr.clone(), triple.clone(), &deployment, false)
                .await
                .unwrap();
        let mut warm_seqs = Vec::new();
        let mut cuts = Vec::new();
        for record in &converted.records {
            let seq = session
                .append(std::slice::from_ref(record), false)
                .await
                .unwrap();
            match record {
                Record::Warmed(_) => warm_seqs.push(seq),
                Record::Input(input) if input.placement == Placement::RunInput => cuts.push(seq),
                _ => {}
            }
        }
        assert!(
            session.operations.billing_owed.is_empty(),
            "a converted session owes billing"
        );
        let today = warms_at(old, &warm_seqs).unwrap();
        let parts = MemoryParts {
            parts: converted.parts.clone(),
        };
        let open = compare(old, &today, &session, &bind, &parts).await;
        let cut = *cuts.last().unwrap();
        session
            .seal_carrying(Some(cut), &Carry::default(), 0)
            .await
            .unwrap();
        session.close().await.unwrap();
        let view = Session::view(&backend, addr, &triple, &deployment)
            .await
            .unwrap();
        let sealed = compare(old, &today, &*view, &bind, &parts).await;
        (open, sealed)
    })
}

/// Every run's facts from the new path equal today's, field for field and fact id included,
/// open and after sealing, across refreezes, refusal routing, pauses, a run left paused and warms
/// billed after their run's end. The warms' ordinals are the store's seqs, which differ from the
/// log's.
#[test]
fn every_runs_facts_equal_todays_open_and_sealed() {
    let old = runs();
    let (open, sealed) = compared(&old, None);
    for (name, comparison) in [("open", &open), ("sealed", &sealed)] {
        assert_eq!(comparison.runs, 4, "{name}");
        assert_eq!(comparison.differences, BTreeMap::new(), "{name}");
        assert!(comparison.unbilled.is_empty(), "{name}");
        // r1: four segments (before the refreeze, the routed step, the step after it, after the
        // auth refreeze) and two warms; r2, r3 and r4: one each.
        assert_eq!(comparison.facts_today, 9, "{name}");
        assert_eq!(comparison.facts_new, 9, "{name}");
        assert_eq!(comparison.facts_equal, 9, "{name}");
        assert_eq!(comparison.warm_facts, 2, "{name}");
        assert_eq!(comparison.warm_moved, 2, "{name}");
    }
}

/// The comparison isn't vacuous: billing under another session's identity changes every fact's
/// id and session, and the comparison names both fields.
#[test]
fn a_fact_under_another_identity_differs_by_field() {
    let old = runs();
    let other = broca_protocol::BindIdentity::new("/elsewhere", "runner", "s");
    let (open, _) = compared(&old, Some(other));
    assert_eq!(open.facts_equal, 0);
    assert_eq!(open.differences["fact_id"], 9);
    assert_eq!(open.differences["session"], 9);
    assert_eq!(open.differing_runs, ["r1", "r2", "r3", "r4"]);
}
