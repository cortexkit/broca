use broca_core::billing::{BillingEvent, BillingRecord};
use broca_protocol::BindIdentity;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use futures::executor::block_on;

use super::{billing_records, run_records};
use crate::parts::MemoryParts;

/// A stored history: its records and setup parts, as the converter that moved sessions from the
/// old storage wrote them. In `log`, run `r1` ends and is warmed twice, once after `r2`
/// started; `r2` pauses, then continues to its end; `r3` pauses and never ends. Every run
/// starts on model `p/m` with the input "hi", and each step answers "ok" with 3 input tokens.
/// `cache-write-split` is `log` whose first step reports 30 cache-write tokens, split 10
/// five-minute and 20 one-hour, served on the priority tier.
struct Stored {
    records: Vec<broca_session_store::record::Record>,
    parts: std::collections::BTreeMap<broca_session_store::codec::Digest, Vec<u8>>,
}

fn stored(name: &str) -> Stored {
    let text = match name {
        "log" => include_str!("fixtures/log.json"),
        "cache-write-split" => include_str!("fixtures/cache-write-split.json"),
        other => panic!("no stored history {other}"),
    };
    let fixture: serde_json::Value = serde_json::from_str(text).unwrap();
    let bytes = |value: &serde_json::Value| -> Vec<u8> {
        let text = value.as_str().unwrap();
        (0..text.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&text[at..at + 2], 16).unwrap())
            .collect()
    };
    Stored {
        records: fixture["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|record| {
                let kind = u32::try_from(record["kind"].as_u64().unwrap()).unwrap();
                broca_session_store::record::Record::decode(kind, &bytes(&record["body"])).unwrap()
            })
            .collect(),
        parts: fixture["parts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|part| {
                let digest: [u8; 32] = bytes(&part[0]).try_into().unwrap();
                (broca_session_store::codec::Digest(digest), bytes(&part[1]))
            })
            .collect(),
    }
}

/// The session `log` is stored as, and its setup parts.
async fn session(backend: &MemoryBackend) -> (Session<&MemoryBackend>, MemoryParts) {
    let converted = stored("log");
    let triple = SessionTriple {
        project_root: "/project".into(),
        harness: "runner".into(),
        session: "s".into(),
    };
    let mut session = Session::create(
        backend,
        SessionAddr("feed".into()),
        triple,
        &DeploymentKey::new(1, &[1; 32]),
        false,
    )
    .await
    .unwrap();
    session.append(&converted.records, true).await.unwrap();
    let parts = MemoryParts {
        parts: converted.parts,
    };
    (session, parts)
}

fn bind() -> BindIdentity {
    BindIdentity::new("/project", "runner", "s")
}

#[test]
fn stored_cache_write_lifetimes_reach_the_shared_billing_builder() {
    block_on(async {
        let converted = stored("cache-write-split");
        let backend = MemoryBackend::default();
        let mut session = Session::create(
            &backend,
            SessionAddr("cache-write-split".into()),
            SessionTriple {
                project_root: "/project".into(),
                harness: "runner".into(),
                session: "s".into(),
            },
            &DeploymentKey::new(1, &[1; 32]),
            false,
        )
        .await
        .unwrap();
        session.append(&converted.records, true).await.unwrap();
        let parts = MemoryParts {
            parts: converted.parts,
        };
        let rebuilt = run_records(&session, "r1", &bind(), &parts).await.unwrap();
        let usage = rebuilt
            .iter()
            .find_map(|framed| match &framed.event {
                BillingEvent::StepFinished { usage, .. } => Some(usage),
                _ => None,
            })
            .unwrap();
        assert_eq!(usage.cache_write_tokens, Some(30));
        assert_eq!(usage.cache_write_5m_tokens, Some(10));
        assert_eq!(usage.cache_write_1h_tokens, Some(20));
        let resolver = |_: &str, _: &str| (broca_core::billing::ChargeBasis::Unknown, 1);
        let facts = broca_core::billing::compute_segments_for_run(
            &rebuilt,
            &broca_protocol::RunId("r1".into()),
            "synthetic-source",
            &resolver,
        );
        let mut without_splits = rebuilt.clone();
        for framed in &mut without_splits {
            if let BillingEvent::StepFinished { usage, .. } = &mut framed.event {
                usage.cache_write_5m_tokens = None;
                usage.cache_write_1h_tokens = None;
            }
        }
        let old_facts = broca_core::billing::compute_segments_for_run(
            &without_splits,
            &broca_protocol::RunId("r1".into()),
            "synthetic-source",
            &resolver,
        );
        for (fact, old) in facts.iter().zip(&old_facts) {
            assert_eq!(fact.fact_id, old.fact_id);
        }
        assert_eq!(facts[0].usage.cache_write_tokens, Some(30));
        assert_eq!(facts[0].usage.cache_write_5m_tokens, Some(10));
        assert_eq!(facts[0].usage.cache_write_1h_tokens, Some(20));
        assert_eq!(
            facts[0].usage_by_service_tier.as_ref().unwrap()["priority"],
            facts[0].usage
        );
        let mut comparable = facts.clone();
        for fact in &mut comparable {
            fact.usage.cache_write_5m_tokens = None;
            fact.usage.cache_write_1h_tokens = None;
            if let Some(tiers) = &mut fact.usage_by_service_tier {
                for usage in tiers.values_mut() {
                    usage.cache_write_5m_tokens = None;
                    usage.cache_write_1h_tokens = None;
                }
            }
        }
        assert_eq!(
            serde_json::to_vec(&comparable).unwrap(),
            serde_json::to_vec(&old_facts).unwrap()
        );
    });
}

/// Each fed record's kind, and the run it names when it names one.
fn kinds(records: &[BillingRecord]) -> Vec<String> {
    records
        .iter()
        .map(|record| match &record.event {
            BillingEvent::RunStarted { run_id, .. } => format!("run_started {}", run_id.0),
            BillingEvent::ConfigRefrozen { .. } => "config_refrozen".into(),
            BillingEvent::AuthRefrozen { .. } => "auth_refrozen".into(),
            BillingEvent::StepFinished { .. } => "step_finished".into(),
            BillingEvent::StepRefused { .. } => "step_refused".into(),
            BillingEvent::RunFinished { .. } => "run_finished".into(),
            BillingEvent::RunPaused { .. } => "run_paused".into(),
            BillingEvent::Warmed { run_id, .. } => format!("warmed {}", run_id.0),
        })
        .collect()
}

/// For `run`, billing feeds only what that run reads, never the whole history.
fn fed(run: &str) -> Vec<String> {
    block_on(async {
        let backend = MemoryBackend::default();
        let (session, parts) = session(&backend).await;
        kinds(&run_records(&session, run, &bind(), &parts).await.unwrap())
    })
}

#[test]
fn an_ended_run_is_fed_its_own_records_and_its_later_warms() {
    assert_eq!(
        fed("r1"),
        [
            "run_started r1",
            "step_finished",
            "run_finished",
            "warmed r1",
            "warmed r1",
        ]
    );
}

/// A pause doesn't end a run: what follows it until its end is fed too.
#[test]
fn a_paused_run_is_fed_through_its_end() {
    assert_eq!(
        fed("r2"),
        [
            "run_started r2",
            "step_finished",
            "run_paused",
            "step_finished",
            "run_finished",
        ]
    );
}

/// No record before a run's start and no other run's warm is fed; a run that never ended is fed
/// through the end of the records.
#[test]
fn a_later_run_is_fed_nothing_from_before_it() {
    assert_eq!(fed("r3"), ["run_started r3", "step_finished", "run_paused"]);
}

/// If another run starts before a run ends, that run is fed the later start to close its last
/// segment and no other records from the later run except warms naming the earlier run.
#[test]
fn an_unended_run_is_fed_the_start_that_closes_it() {
    block_on(async {
        let backend = MemoryBackend::default();
        let (session, parts) = session(&backend).await;
        let records: Vec<_> = session
            .records_from(1)
            .await
            .unwrap()
            .into_iter()
            .filter(|(_, record)| !matches!(record, Record::RunEnd(end) if end.run_id == "r1"))
            .collect();
        assert_eq!(
            kinds(&billing_records(&records, "r1", &bind(), &parts).unwrap()),
            [
                "run_started r1",
                "step_finished",
                "warmed r1",
                "run_started r2",
                "warmed r1",
            ]
        );
    });
}
