//! The run events a session in the new storage hands its run-event sink.

use std::sync::{Arc, Mutex};

use broca_protocol::run_events::{RunEvent, RunEventDetail, StructuredOutputStatus};
use broca_protocol::{BindIdentity, RunFinishReason, RunId};
use broca_session_loop::parts::{MemoryParts, SharedParts};
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::session::Session;
use tokio::sync::{mpsc, Notify};

use super::tests::{
    auth_failure, bind, deployment, executor, idle, request, send, send_as, text_step, triple,
    Commands, LoopExecutor, ADDR,
};
use super::StoreActor;
use crate::actor::{SendKey, SendOutcome};
use crate::store_run_events::RunEventSink;
use broca_store::bus_events::RunEventStore as _;

/// One call of the sink: the events of one durable batch.
struct Handed {
    epoch: String,
    through: u64,
    events: Vec<RunEvent>,
}

#[derive(Default)]
struct Recorded(Mutex<Vec<Handed>>);

impl RunEventSink for Recorded {
    fn durable(&self, session: &BindIdentity, epoch: &str, through: u64, events: Vec<RunEvent>) {
        assert_eq!(session, &bind());
        self.0.lock().unwrap().push(Handed {
            epoch: epoch.to_owned(),
            through,
            events,
        });
    }
}

/// A store actor over a new session in memory, handing its run events to `sink`.
async fn actor(
    executor: LoopExecutor,
    sink: Arc<dyn RunEventSink>,
) -> (
    Commands,
    tokio::task::JoinHandle<Session<&'static MemoryBackend>>,
) {
    let backend: &'static MemoryBackend = Box::leak(Box::default());
    let session = Session::create(
        backend,
        SessionAddr(ADDR.into()),
        triple(),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let parts = SharedParts::new(Box::new(MemoryParts::default()));
    let actor = StoreActor::new(bind(), session, Arc::new(executor), Box::new(parts))
        .with_readers(Arc::new(move || backend))
        .with_run_events(sink);
    let (commands, receiver) = mpsc::unbounded_channel();
    let served = tokio::spawn(actor.serve(receiver));
    (Commands::over(commands, backend), served)
}

/// The kind of the record at each of `seqs`, as the session stores it.
async fn kinds(session: &Session<&'static MemoryBackend>, seqs: &[u64]) -> Vec<&'static str> {
    let last = session.next_seq() - 1;
    let log = session.from_log_holding(1, last).await.unwrap();
    seqs.iter()
        .map(|seq| {
            log.records
                .iter()
                .find(|(at, _)| at == seq)
                .map(|(_, record)| record.name())
                .unwrap_or("absent")
        })
        .collect()
}

/// A run with a steer hands, once each record is durable, `run.started` at its start,
/// `run.steer_consumed` at the step that places the steer, and `run.ended` at its end, each
/// with the record's own seq and the session's incarnation as its epoch.
#[tokio::test]
async fn a_run_hands_its_events_once_their_records_are_durable() {
    let gate = Arc::new(Notify::new());
    let sink = Arc::new(Recorded::default());
    let (commands, served) = actor(
        executor(
            vec![text_step("a1", "hello"), text_step("a2", "again")],
            Some(Arc::clone(&gate)),
        ),
        Arc::clone(&sink) as Arc<dyn RunEventSink>,
    )
    .await;
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    assert_eq!(
        send_as(
            &commands,
            "r-steer",
            request("steered"),
            SendKey::SendId("steer-1".into()),
            broca_protocol::Delivery::Steer
        )
        .await,
        SendOutcome::Active {
            run_id: RunId("r1".into()),
            delivered: None
        }
    );
    gate.notify_one();
    gate.notify_one();
    idle(&commands).await;
    drop(commands);
    let session = served.await.unwrap();

    let handed = std::mem::take(&mut *sink.0.lock().unwrap());
    let epoch: String = session
        .head()
        .incarnation
        .0
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let mut events = Vec::new();
    let mut through = 0;
    for batch in &handed {
        assert_eq!(
            batch.epoch, epoch,
            "every batch names the session's incarnation"
        );
        assert!(batch.through >= through, "batches arrive in seq order");
        through = batch.through;
        for event in &batch.events {
            assert!(
                event.seq <= batch.through,
                "an event is handed only once its record is durable"
            );
            events.push(event.clone());
        }
    }
    assert_eq!(
        through,
        session.next_seq() - 1,
        "the last batch reaches the end"
    );
    let details: Vec<RunEventDetail> = events.iter().map(|e| e.detail.clone()).collect();
    assert_eq!(
        details,
        [
            RunEventDetail::Started {},
            RunEventDetail::SteerConsumed {
                step: 2,
                consumed: 1,
                send_ids: vec!["steer-1".into()],
                truncated: false,
            },
            RunEventDetail::Ended {
                terminal: RunFinishReason::Completed,
                error_class: None,
                structured_output: Some(StructuredOutputStatus::Absent),
            },
        ]
    );
    let session_ref = broca_core::wal_writer::agent_session_ref(&bind());
    for event in &events {
        assert_eq!(event.run_id, "r1");
        assert_eq!(event.session_ref, session_ref);
        assert!(event.at_ms > 0);
    }
    let seqs: Vec<u64> = events.iter().map(|event| event.seq).collect();
    assert_eq!(
        kinds(&session, &seqs).await,
        ["RunStart", "StepStart", "RunEnd"],
        "each event carries the seq of the record it reports"
    );
}

/// A run paused for sign-in hands `run.started` and then `run.paused`, naming the pause.
#[tokio::test]
async fn a_run_paused_for_sign_in_hands_run_paused() {
    let sink = Arc::new(Recorded::default());
    let (commands, served) = actor(
        executor(vec![auth_failure()], None),
        Arc::clone(&sink) as Arc<dyn RunEventSink>,
    )
    .await;
    assert!(matches!(
        send(&commands, "r1", "first").await,
        SendOutcome::Active { .. }
    ));
    idle(&commands).await;
    drop(commands);
    let _session = served.await.unwrap();
    let details: Vec<RunEventDetail> = sink
        .0
        .lock()
        .unwrap()
        .iter()
        .flat_map(|batch| batch.events.iter().map(|event| event.detail.clone()))
        .collect();
    assert_eq!(
        details,
        [
            RunEventDetail::Started {},
            RunEventDetail::Paused {
                pause_reason: broca_protocol::PauseReason::AuthRequired
            },
        ]
    );
}

/// Records run events in a run index's outbox, as the module does.
struct Outbox(Arc<broca_store::SqliteStore>);

impl RunEventSink for Outbox {
    fn durable(&self, session: &BindIdentity, epoch: &str, _: u64, mut events: Vec<RunEvent>) {
        assert!(self
            .0
            .record_run_events(session, epoch, &mut events, 0)
            .unwrap());
    }
}

/// Writes the publications the runs above produce to `$BROCA_SAMPLE_DIR`, one JSON file per
/// event, for readers to check before the module publishes them: each run's events go through
/// the module's own outbox and are read back as the publisher sends them.
#[tokio::test]
#[ignore = "writes reader samples; run with BROCA_SAMPLE_DIR set"]
async fn write_run_event_samples() {
    let dir = std::path::PathBuf::from(std::env::var("BROCA_SAMPLE_DIR").unwrap());
    std::fs::create_dir_all(&dir).unwrap();
    let store = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
    store.enable_bus_events("box_sample").unwrap();
    let sink: Arc<dyn RunEventSink> = Arc::new(Outbox(Arc::clone(&store)));
    let gate = Arc::new(Notify::new());
    let (commands, served) = actor(
        executor(
            vec![text_step("a1", "hello"), text_step("a2", "again")],
            Some(Arc::clone(&gate)),
        ),
        Arc::clone(&sink),
    )
    .await;
    send(&commands, "r1", "first").await;
    send_as(
        &commands,
        "r-steer",
        request("steered"),
        SendKey::SendId("steer-1".into()),
        broca_protocol::Delivery::Steer,
    )
    .await;
    gate.notify_one();
    gate.notify_one();
    idle(&commands).await;
    drop(commands);
    served.await.unwrap();
    let (commands, served) = actor(executor(vec![auth_failure()], None), sink).await;
    send(&commands, "r2", "second").await;
    idle(&commands).await;
    drop(commands);
    served.await.unwrap();
    let mut n = 0;
    while let Some(publication) = store.pending_bus_event().unwrap() {
        n += 1;
        let sample = serde_json::json!({
            "subject": publication.subject,
            "nats_msg_id": publication.event_key,
            "headers": publication.headers,
            "body": String::from_utf8(publication.body.clone()).unwrap(),
        });
        let name = format!("{n:02}-{}.json", publication.headers["kind"]);
        std::fs::write(dir.join(name), serde_json::to_vec_pretty(&sample).unwrap()).unwrap();
        store
            .acknowledge_bus_event(
                &publication,
                cortexkit_bus_trait::PublishReceipt::new(n, false),
                1_794_790_000_000,
            )
            .unwrap();
    }
    assert_eq!(
        n, 5,
        "started, steer_consumed and ended, then started and paused"
    );
}
