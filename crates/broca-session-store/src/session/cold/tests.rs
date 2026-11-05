use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::ids::MessageId;
use broca_protocol::request::ModelHandle;
use futures::executor::block_on;

use super::Cold;
use crate::backend::{MemoryBackend, SessionAddr, SessionBackend};
use crate::codec::Message as _;
use crate::head::{self, SessionTriple};
use crate::idindex::IdKind;
use crate::keys::DeploymentKey;
use crate::record::{Assistant, Input, Placement, Record, RunEnd, RunStart};
use crate::session::{Session, SessionError};

const DAY: u64 = 24 * 60 * 60 * 1000;
const IDLE: u64 = 7 * DAY;

fn triple() -> SessionTriple {
    SessionTriple {
        project_root: "/project".into(),
        harness: "broca".into(),
        session: "s".into(),
    }
}

fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[5; 32])
}

fn addr() -> SessionAddr {
    SessionAddr("0123456789abcdef".into())
}

fn model() -> ModelHandle {
    serde_json::from_value(serde_json::json!({"provider_module_id": "p", "model_id": "m"})).unwrap()
}

fn start(run: &str, at: u64) -> Record {
    Record::RunStart(Box::new(RunStart {
        run_id: run.into(),
        model: model(),
        setup: Vec::new(),
        renderer_version: 1,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        ts_ms: None,
        written_at_ms: Some(at),
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: None,
        render_config: None,
        auth_selection: None,
        run_choices: None,
        origin: None,
        principal: None,
        scope: None,
        hooks: None,
        keep_warm: None,
        on_restart: None,
        seeded: None,
        refusal_schedule: None,
    }))
}

fn end(run: &str, at: u64) -> Record {
    Record::RunEnd(RunEnd {
        run_id: run.into(),
        reason: RunFinishReason::Completed,
        usage: None,
        ts_ms: Some(at),
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    })
}

fn input(run: &str, text: &str, at: u64) -> Record {
    Record::Input(Input {
        run_id: run.into(),
        placement: Placement::RunInput,
        message: Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }]),
        written_at_ms: Some(at),
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        pruned: Vec::new(),
    })
}

/// An assistant answer carrying opaque reasoning before its text.
fn answer(run: &str, text: &str, at: u64) -> Record {
    Record::Assistant(Assistant {
        run_id: run.into(),
        step_id: 1,
        provider_message_id: MessageId(format!("m-{text}")),
        message: Message::assistant_from(
            vec![
                ContentBlock::RedactedReasoning {
                    data: format!("opaque-{text}"),
                    provider_format: None,
                },
                ContentBlock::Text { text: text.into() },
            ],
            model(),
        ),
        usage: None,
        written_at_ms: Some(at),
        last_content_at_ms: None,
        budget_finalize: None,
        tags_v1: None,
        pruned: Vec::new(),
    })
}

/// The opaque payloads left in the window's messages.
fn opaque(messages: &[Message]) -> Vec<String> {
    messages
        .iter()
        .flat_map(|m| &m.content)
        .filter_map(|block| match block {
            ContentBlock::RedactedReasoning { data, .. } if !data.is_empty() => Some(data.clone()),
            _ => None,
        })
        .collect()
}

fn texts(messages: &[Message]) -> Vec<String> {
    messages
        .iter()
        .flat_map(|m| &m.content)
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.clone()),
            _ => None,
        })
        .collect()
}

/// Two finished runs, the last record written at `at`.
async fn finished<B: SessionBackend>(backend: &B, at: u64) -> Session<'_, B> {
    let mut session = Session::create(backend, addr(), triple(), &deployment(), false)
        .await
        .unwrap();
    for run in ["a", "b"] {
        session.append(&[start(run, at)], true).await.unwrap();
        session
            .append(
                &[input(run, &format!("ask {run}"), at), answer(run, run, at)],
                true,
            )
            .await
            .unwrap();
        session.append(&[end(run, at)], true).await.unwrap();
    }
    session
}

async fn stored_cold_since(backend: &MemoryBackend) -> Option<u64> {
    let (bytes, _) = backend.read_head(&addr()).await.unwrap().unwrap();
    head::open(&bytes, &deployment()).unwrap().0.cold_since_ms
}

/// A session idle for longer than the threshold, with every run finished, goes cold: its ledger
/// is sealed, the window keeps every message but loses its opaque reasoning, and the stored head
/// records when it went cold. A reload rebuilds exactly that window, and opening the writer wakes
/// the session.
#[test]
fn a_finished_idle_session_goes_cold_with_its_reasoning_pruned() {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = finished(&backend, 1_000).await;
        assert_eq!(opaque(&session.conversation.messages).len(), 2);
        let now = 1_000 + IDLE + 1;
        let cold = session.cold(now, IDLE).await.unwrap();
        assert!(
            matches!(cold, Cold::Applied { pruned: true, .. }),
            "{cold:?}"
        );
        assert!(opaque(&session.conversation.messages).is_empty());
        assert_eq!(
            texts(&session.conversation.messages),
            ["ask a", "a", "ask b", "b"]
        );
        assert_eq!(session.head().cold_since_ms, Some(now));
        assert_eq!(stored_cold_since(&backend).await, Some(now));
        // The ledger is sealed: its ids are found through the id index.
        assert_eq!(session.find_id(IdKind::RunId, "a").await.unwrap().len(), 2);
        let window = session.conversation.encode();
        session.close().await.unwrap();

        let loaded = Session::load(&backend, addr(), &triple(), &deployment())
            .await
            .unwrap();
        assert_eq!(loaded.conversation.encode(), window);
        assert_eq!(loaded.head().cold_since_ms, None);
        assert_eq!(stored_cold_since(&backend).await, None);
    });
}

/// A session used more recently than the threshold is left alone.
#[test]
fn an_active_session_is_left_alone() {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = finished(&backend, 1_000).await;
        let before = (session.conversation.encode(), session.operations.encode());
        let logs = backend.log_names(&addr());
        let cold = session.cold(1_000 + IDLE - 1, IDLE).await.unwrap();
        assert_eq!(
            cold,
            Cold::Active {
                last_active_ms: 1_000
            }
        );
        assert_eq!(
            (session.conversation.encode(), session.operations.encode()),
            before
        );
        assert_eq!(backend.log_names(&addr()), logs);
        assert!(backend.object_names(&addr()).is_empty());
    });
}

/// A session whose open log carries no time can't be confirmed idle, so nothing happens.
#[test]
fn a_session_with_no_timed_record_is_left_alone() {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = Session::create(&backend, addr(), triple(), &deployment(), false)
            .await
            .unwrap();
        assert_eq!(session.cold(100 * DAY, IDLE).await.unwrap(), Cold::Unknown);
        assert!(backend.object_names(&addr()).is_empty());
    });
}

/// A session with an unfinished run keeps every byte: its ledger is sealed, but nothing is
/// pruned, so the run can resume exactly.
#[test]
fn an_unfinished_run_keeps_its_reasoning() {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = finished(&backend, 1_000).await;
        session.append(&[start("c", 1_000)], true).await.unwrap();
        session
            .append(&[input("c", "ask c", 1_000), answer("c", "c", 1_000)], true)
            .await
            .unwrap();
        let cold = session.cold(1_000 + IDLE, IDLE).await.unwrap();
        assert!(
            matches!(cold, Cold::Applied { pruned: false, .. }),
            "{cold:?}"
        );
        assert_eq!(opaque(&session.conversation.messages).len(), 3);
        session.close().await.unwrap();
        let loaded = Session::load(&backend, addr(), &triple(), &deployment())
            .await
            .unwrap();
        assert_eq!(opaque(&loaded.conversation.messages).len(), 3);
    });
}

/// A cold step that fails before its commit undoes itself, like every change: nothing it created
/// is left, and the next cold step runs.
#[test]
fn a_cold_step_that_fails_before_its_commit_undoes_itself() {
    let backend = MemoryBackend::default();
    block_on(async {
        let mut session = finished(&backend, 1_000).await;
        backend.fail_once(2);
        assert!(session.cold(1_000 + IDLE, IDLE).await.is_err());
        assert!(backend.object_names(&addr()).is_empty());
        assert_eq!(stored_cold_since(&backend).await, None);
        let cold = session.cold(1_000 + IDLE, IDLE).await;
        assert!(
            matches!(cold, Ok(Cold::Applied { pruned: true, .. })),
            "{cold:?}"
        );
        assert!(!matches!(cold, Err(SessionError::Poisoned)));
    });
}
