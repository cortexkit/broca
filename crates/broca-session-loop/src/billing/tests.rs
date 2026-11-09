use broca_protocol::BindIdentity;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use broca_wal::{FramedRecord, WalRecord};
use futures::executor::block_on;

use super::{run_records, today_records};
use crate::parts::MemoryParts;

fn start(run: &str) -> String {
    format!(
        r#"{{"type":"run_started","run_id":"{run}","ts_ms":1,"session":{{"project_root":"/project","harness":"runner","session":"s"}},"config":{{"model":{{"provider_module_id":"p","model_id":"m"}},"tools":[],"tool_choice":{{"type":"auto"}},"generation":{{}},"response_format":{{"type":"text"}},"context_epoch":0,"renderer_version":1}},"input":[{{"role":"user","content":[{{"type":"text","text":"hi"}}]}}],"origin":{{"kind":"fresh"}}}}"#
    )
}

fn step_started(id: u64) -> String {
    format!(r#"{{"type":"step_started","step_id":{id}}}"#)
}

fn step(id: u64) -> String {
    format!(
        r#"{{"type":"model_step_finished","step_id":{id},"assistant_message":{{"message_id":"m{id}","content":[{{"type":"text","text":"ok"}}]}},"usage":{{"input_tokens":3}},"finish_reason":"stop","retries_used":0}}"#
    )
}

const FINISHED: &str =
    r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#;

fn paused(run: &str) -> String {
    format!(r#"{{"type":"run_paused","run_id":"{run}","reason":"restart"}}"#)
}

fn warm(run: &str) -> String {
    let a = "a".repeat(64);
    format!(
        r#"{{"type":"cache_warmed","run_id":"{run}","step_id":1,"request_body_sha256":"{a}","warm_body_sha256":"{a}","usage":{{}},"sent_at_ms":9}}"#
    )
}

/// `r1` ends and is warmed twice, once after `r2` started; `r2` pauses, then continues to its
/// end; `r3` pauses and never ends.
fn log() -> Vec<FramedRecord> {
    [
        start("r1"),
        step_started(1),
        step(1),
        FINISHED.into(),
        warm("r1"),
        start("r2"),
        warm("r1"),
        step_started(1),
        step(1),
        paused("r2"),
        step_started(2),
        step(2),
        FINISHED.into(),
        start("r3"),
        step_started(1),
        step(1),
        paused("r3"),
    ]
    .iter()
    .enumerate()
    .map(|(i, line)| FramedRecord {
        seq: i as u64 + 1,
        fence: 0,
        record: serde_json::from_str::<WalRecord>(line).unwrap(),
    })
    .collect()
}

/// The session `log()` converts to, and its setup parts.
async fn session(backend: &MemoryBackend) -> (Session<&MemoryBackend>, MemoryParts) {
    let converted = broca_session_convert::convert(&log(), None).unwrap();
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

/// Each fed record's type, and the run it names when it names one.
fn kinds(records: &[FramedRecord]) -> Vec<String> {
    records
        .iter()
        .map(|framed| {
            let value = serde_json::to_value(&framed.record).unwrap();
            let run = value
                .get("run_id")
                .and_then(|run| run.as_str())
                .map(|run| format!(" {run}"))
                .unwrap_or_default();
            format!("{}{run}", value["type"].as_str().unwrap())
        })
        .collect()
}

/// What billing feeds today's builder for `run`: only what the builder reads for that run,
/// never the whole history.
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
            "model_step_finished",
            "run_finished",
            "cache_warmed r1",
            "cache_warmed r1",
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
            "model_step_finished",
            "run_paused r2",
            "model_step_finished",
            "run_finished",
        ]
    );
}

/// No record before a run's start and no other run's warm is fed; a run that never ended is fed
/// through the end of the records.
#[test]
fn a_later_run_is_fed_nothing_from_before_it() {
    assert_eq!(
        fed("r3"),
        ["run_started r3", "model_step_finished", "run_paused r3"]
    );
}

/// A run another run's start follows without its end is fed that start, which closes the run's
/// last segment as today's builder closes it, and nothing else of the later run but warms naming
/// the earlier one.
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
            kinds(&today_records(&records, "r1", &bind(), &parts).unwrap()),
            [
                "run_started r1",
                "model_step_finished",
                "cache_warmed r1",
                "run_started r2",
                "cache_warmed r1",
            ]
        );
    });
}
