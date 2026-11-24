//! What a run's step reads from its session: the work each model step does must follow what the
//! step adds, never the session's history. Each test counts the reads a writer serves while a run
//! takes its steps, and checks that a run with more steps makes no more of them than one with
//! fewer, where the reads only depend on the history.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use broca_core::provider::StubProvider;
use broca_core::stub_plane::StubToolPlane;
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason, ToolCall};
use broca_protocol::{
    BindIdentity, MessageId, NormalizedEvent, ProviderControl, RunId, SubmissionRequest,
    ToolCallId, Usage,
};
use broca_session_loop::parts::MemoryParts;
use broca_session_loop::run::Seams;
use broca_session_loop::writer::Writer;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::Record;
use broca_session_store::session::{Session, SessionError};
use serde_json::json;

fn bind() -> BindIdentity {
    BindIdentity::new("/project", "step-cost", "s1")
}

async fn session(backend: &MemoryBackend) -> Session<&MemoryBackend> {
    Session::create(
        backend,
        SessionAddr("0123456789abcdef".into()),
        SessionTriple {
            project_root: "/project".into(),
            harness: "step-cost".into(),
            session: "s1".into(),
        },
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap()
}

/// A model step that calls `read` once, with call id `call`.
fn tool_step(call: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(format!("m-{call}")),
                content: vec![
                    ContentBlock::Text {
                        text: "Let me read the file first.".into(),
                    },
                    ContentBlock::ToolCall(ToolCall {
                        thought_signature: None,
                        provider_state: None,
                        tool_call_id: ToolCallId(call.into()),
                        tool_name: "read".into(),
                        input: json!({"path": format!("src/{call}.rs")}).into(),
                    }),
                ],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(1_000),
                output_tokens: Some(20),
                ..Usage::default()
            },
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ]
}

/// A model step that answers in text and ends the run.
fn answer_step(id: &str) -> Vec<NormalizedEvent> {
    vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: MessageId(id.into()),
                content: vec![ContentBlock::Text {
                    text: "Done.".into(),
                }],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage {
                input_tokens: Some(1_000),
                output_tokens: Some(10),
                ..Usage::default()
            },
            finish_reason: FinishReason::Stop,
            retries_used: 0,
        }),
    ]
}

/// The reads a writer served, by kind.
#[derive(Default)]
struct Reads {
    run_start: AtomicUsize,
    send_records: AtomicUsize,
    entry_at: AtomicUsize,
}

/// A session's writer that counts the reads the loop makes through it.
struct Counting<'s, 'b> {
    session: &'s mut Session<&'b MemoryBackend>,
    reads: Arc<Reads>,
    /// Serve no queued send's records, as for a send whose records can't be found.
    hide_sends: bool,
}

impl Writer for Counting<'_, '_> {
    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        Writer::append(&mut *self.session, records, barrier).await
    }

    async fn caught_up(&mut self) -> Result<(), SessionError> {
        Writer::caught_up(&mut *self.session).await
    }

    async fn send_records(&mut self, submission_id: &str) -> Result<Vec<Record>, SessionError> {
        self.reads.send_records.fetch_add(1, Ordering::SeqCst);
        if self.hide_sends {
            return Ok(Vec::new());
        }
        Writer::send_records(&mut *self.session, submission_id).await
    }

    async fn append_with(
        &mut self,
        build: &mut (dyn for<'s> FnMut(&'s Conversation, &'s Operations, u64) -> Vec<Record>
                  + Send),
        barrier: bool,
    ) -> Result<Vec<Record>, SessionError> {
        Writer::append_with(&mut *self.session, build, barrier).await
    }

    fn conversation(&self) -> &Conversation {
        &self.session.conversation
    }

    fn operations(&self) -> &Operations {
        &self.session.operations
    }

    fn session_id(&self) -> String {
        Writer::session_id(&*self.session)
    }

    fn next_seq(&self) -> u64 {
        self.session.next_seq()
    }

    fn retired_tag_floor(&self) -> u64 {
        self.session.retired_tag_floor()
    }

    fn message_id_prefix(&self) -> String {
        self.session.message_id_prefix()
    }

    async fn open_log_records(&self) -> Result<Vec<(u64, Record)>, SessionError> {
        self.session.open_log_records().await
    }

    async fn run_start(
        &self,
        run_id: &str,
    ) -> Result<Option<broca_session_store::record::RunStart>, SessionError> {
        self.reads.run_start.fetch_add(1, Ordering::SeqCst);
        Writer::run_start(&*self.session, run_id).await
    }

    async fn window_message_whole(
        &self,
        ordinal: u64,
    ) -> Result<std::sync::Arc<broca_protocol::Message>, SessionError> {
        Writer::window_message_whole(&*self.session, ordinal).await
    }

    async fn release(&mut self, blocks: &[(u64, Vec<usize>)]) -> Result<(), SessionError> {
        Writer::release(&mut *self.session, blocks).await
    }

    async fn tool_intent(
        &self,
        call_key: &str,
    ) -> Result<Option<broca_session_store::record::ToolIntent>, SessionError> {
        Writer::tool_intent(&*self.session, call_key).await
    }

    async fn enrollment(
        &self,
    ) -> Result<Option<broca_session_store::session::EnrollmentOverlay>, SessionError> {
        Writer::enrollment(&*self.session).await
    }

    async fn put_enrollment(
        &mut self,
        entries: &broca_session_store::record::EnrolledEntries,
    ) -> Result<broca_session_store::record::EnrollmentOverlayRef, SessionError> {
        Writer::put_enrollment(&mut *self.session, entries).await
    }

    async fn seal_at_entry(
        &mut self,
        ordinal: u64,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        Writer::seal_at_entry(&mut *self.session, ordinal, carry, now_ms).await
    }

    async fn seal_ledger(
        &mut self,
        carry: &broca_session_store::session::Carry<'_>,
        now_ms: u64,
    ) -> Result<(), SessionError> {
        Writer::seal_ledger(&mut *self.session, carry, now_ms).await
    }

    async fn entry_by_mid(
        &self,
        mid: &str,
    ) -> Result<Option<broca_session_store::session::StoredEntry>, SessionError> {
        Writer::entry_by_mid(&*self.session, mid).await
    }

    async fn entry_at(
        &self,
        ordinal: u64,
    ) -> Result<Option<broca_session_store::session::StoredEntry>, SessionError> {
        self.reads.entry_at.fetch_add(1, Ordering::SeqCst);
        Writer::entry_at(&*self.session, ordinal).await
    }

    async fn moved_through(&self) -> Result<Option<(String, u64)>, SessionError> {
        Writer::moved_through(&*self.session).await
    }

    async fn entries_by_mid(
        &self,
        mids: &[String],
    ) -> Result<Vec<broca_session_store::session::StoredEntry>, SessionError> {
        Writer::entries_by_mid(&*self.session, mids).await
    }

    async fn sealed_derived(
        &self,
    ) -> Result<Option<Arc<dyn std::any::Any + Send + Sync>>, SessionError> {
        Writer::sealed_derived(&*self.session).await
    }

    async fn keep_sealed_derived(
        &self,
        value: Arc<dyn std::any::Any + Send + Sync>,
    ) -> Result<(), SessionError> {
        Writer::keep_sealed_derived(&*self.session, value).await
    }
}

/// Runs `run_id` on `session` through this crate's run loop, the model answering `steps` in
/// turn, and returns the reads the run made.
async fn run_on(
    session: &mut Session<&MemoryBackend>,
    parts: &mut MemoryParts,
    run_id: &str,
    steps: Vec<Vec<NormalizedEvent>>,
) -> Arc<Reads> {
    run_with(session, parts, run_id, steps, false, None).await
}

/// [`run_on`], its writer serving no queued send's records when `hide_sends` is set. The request
/// declares a `read` tool its policy doesn't allow, so the run offers no tool, unless `offered`
/// names a tool it declares and allows instead.
async fn run_with(
    session: &mut Session<&MemoryBackend>,
    parts: &mut MemoryParts,
    run_id: &str,
    steps: Vec<Vec<NormalizedEvent>>,
    hide_sends: bool,
    offered: Option<&str>,
) -> Arc<Reads> {
    let provider = StubProvider::new(steps);
    let request: SubmissionRequest = serde_json::from_value(json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [{"role": "user", "content": [{"type": "text", "text": format!("task {run_id}")}]}],
        "tools": [{
            "name": offered.unwrap_or("read"),
            "description": "Reads a file.",
            "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}},
        }],
        "policy_allowed": offered.into_iter().collect::<Vec<_>>(),
        "stop_when": [{"type": "step_count", "n": 50}],
    }))
    .unwrap();
    let mut request = broca_core::RunRequest::from_submission(
        request,
        RunId(run_id.into()),
        bind(),
        broca_protocol::SubmissionId(String::new()),
    );
    let control = broca_core::RunControl {
        cancel: Some(Default::default()),
        ..Default::default()
    };
    let now = || 1_000;
    let admission = broca_session_loop::admit::admission(
        &mut request,
        &control,
        &provider,
        Writer::operations(&*session),
        Some(now()),
    )
    .unwrap();
    let tools = StubToolPlane::new().with_canned_text("read", "fn main() {}");
    let seams = Seams {
        person_steers: None,
        provider: &provider,
        tools: &tools,
        bind: bind(),
        out: None,
        cancel: Default::default(),
        stop: Default::default(),
        held: None,
        approval: &broca_core::capability::AllowAll,
        hook_plane: None,
        now_ms: &now,
        session_address: None,
        compaction_plane: None,
        compaction_session: None,
        compaction_changes: false,
        keep_warm: None,
        account_reports: None,
        max_same_account_wait: Default::default(),
        auth_selection: None,
        step_progress: None,
        model_call_ready: None,
        pause_for_auth_at_start: None,
        account_continue: None,
    };
    let reads = Arc::new(Reads::default());
    let mut writer = Counting {
        session,
        reads: Arc::clone(&reads),
        hide_sends,
    };
    broca_session_loop::run::run(&mut writer, parts, admission, &seams)
        .await
        .unwrap();
    reads
}

/// A session holding `runs` answered runs, and a run on it taking `tool_steps` tool steps before
/// its answer: the reads that last run made.
async fn reads_of_a_run(runs: usize, tool_steps: usize) -> Arc<Reads> {
    let backend = MemoryBackend::default();
    let mut session = session(&backend).await;
    let mut parts = MemoryParts::default();
    for index in 0..runs {
        run_on(
            &mut session,
            &mut parts,
            &format!("h{index}"),
            vec![answer_step(&format!("a{index}"))],
        )
        .await;
    }
    let mut steps: Vec<_> = (0..tool_steps)
        .map(|n| tool_step(&format!("c{n}")))
        .collect();
    steps.push(answer_step("last"));
    run_on(&mut session, &mut parts, "measured", steps).await
}

/// A run reads each window run's start once, not at every step: its part-number rules never
/// change once the run started.
#[tokio::test]
async fn window_run_starts_are_read_once_per_run() {
    let short = reads_of_a_run(4, 1).await;
    let long = reads_of_a_run(4, 6).await;
    let short = short.run_start.load(Ordering::SeqCst);
    let long = long.run_start.load(Ordering::SeqCst);
    assert!(short > 0, "the run reads its window runs' starts");
    assert_eq!(
        long, short,
        "five more steps read the window runs' starts again"
    );
}

/// A model step looks up every id it needs in memory: a run's steps read no record back from the
/// open log, though the runs before it, in the same open log, have records an id lookup reads
/// back from the log once the latest run is another.
#[tokio::test]
async fn a_step_reads_no_record_back_from_the_open_log() {
    use broca_session_store::idindex::{open_record_reads, IdKind};
    let backend = MemoryBackend::default();
    let mut session = session(&backend).await;
    let mut parts = MemoryParts::default();
    for index in 0..4 {
        run_on(
            &mut session,
            &mut parts,
            &format!("h{index}"),
            vec![
                tool_step(&format!("h{index}-call")),
                answer_step(&format!("a{index}")),
            ],
        )
        .await;
        // A record naming a tool call's key, as a dispatched call's intent does: once a later run
        // starts, a lookup reads it back from the open log.
        session
            .append(
                &[Record::Event(broca_session_store::record::Event {
                    name: "tool_dispatched".into(),
                    run_id: Some(format!("h{index}")),
                    body: Some(json!({"call_key": format!("h{index}-key")})),
                })],
                true,
            )
            .await
            .unwrap();
    }
    let before = open_record_reads();
    let mut steps: Vec<_> = (0..6).map(|n| tool_step(&format!("c{n}"))).collect();
    steps.push(answer_step("last"));
    run_on(&mut session, &mut parts, "measured", steps).await;
    assert_eq!(
        open_record_reads(),
        before,
        "the run's steps read records back from the open log"
    );

    // The counter sees a lookup that does read one back: a history run's call key.
    let found = session.find_id(IdKind::CallKey, "h0-key").await.unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(open_record_reads(), before + 1);
}

/// The records queuing ordered send `id`, which only an idle session can start: a running run
/// reads it at every boundary and leaves it waiting.
fn waiting_send(id: &str) -> Vec<Record> {
    use broca_session_store::record::{DeliveryOrdered, QueuedSend};
    let order: broca_protocol::DeliveryOrder =
        serde_json::from_value(json!({"modes": ["idle_start"]})).unwrap();
    vec![
        Record::DeliveryOrdered(DeliveryOrdered {
            submission_id: id.into(),
            order: serde_json::to_value(&order).unwrap(),
            sent_by: None,
        }),
        Record::QueuedSend(QueuedSend {
            media: Vec::new(),
            named_speaker: None,
            kept_notice: None,
            message_marks: None,
            submission_id: id.into(),
            send_id: None,
            input: vec![broca_protocol::Message::new(
                broca_protocol::content::Role::User,
                vec![ContentBlock::Text {
                    text: "later".into(),
                }],
            )],
            setup: Vec::new(),
            written_at_ms: Some(1_000),
            ts_ms: None,
            request: Some(
                json!({
                    "model": {"provider_module_id": "stub", "model_id": "m"},
                    "policy_allowed": [],
                    "tool_choice": {"type": "auto"},
                    "generation": {},
                    "stop_when": [],
                })
                .into(),
            ),
            delivery: None,
            receipt: None,
        }),
    ]
}

/// An ordered send waiting through a run's boundaries is read once, not at every boundary and
/// batch close: its records never change while it waits.
#[tokio::test]
async fn a_waiting_ordered_send_is_read_once_per_run() {
    let backend = MemoryBackend::default();
    let mut session = session(&backend).await;
    let mut parts = MemoryParts::default();
    session.append(&waiting_send("q1"), true).await.unwrap();
    let mut steps: Vec<_> = (0..4).map(|n| tool_step(&format!("c{n}"))).collect();
    steps.push(answer_step("last"));
    let reads = run_on(&mut session, &mut parts, "measured", steps).await;
    assert!(
        Writer::operations(&session)
            .queue
            .iter()
            .any(|pending| pending.submission_id == "q1"),
        "the send still waits"
    );
    assert_eq!(
        reads.send_records.load(Ordering::SeqCst),
        1,
        "the waiting send's records are read again"
    );
}

/// An ordered send whose records can't be found is ended at the boundary that finds it, as a
/// send whose run failed for it, and the run goes on: it no longer fails every boundary of every
/// run while the send waits.
#[tokio::test]
async fn an_ordered_send_without_its_records_ends_and_the_run_goes_on() {
    let backend = MemoryBackend::default();
    let mut session = session(&backend).await;
    let mut parts = MemoryParts::default();
    session.append(&waiting_send("q1"), true).await.unwrap();
    let steps = vec![tool_step("c0"), answer_step("last")];
    let reads = run_with(&mut session, &mut parts, "measured", steps, true, None).await;
    assert!(
        Writer::operations(&session).queue.is_empty(),
        "the send left the queue"
    );
    assert_eq!(reads.send_records.load(Ordering::SeqCst), 1);
    let failure = session
        .open_log_records()
        .await
        .unwrap()
        .into_iter()
        .find_map(|(_, record)| match record {
            Record::Retraction(retraction) if retraction.submission_id == "q1" => {
                retraction.failure
            }
            _ => None,
        })
        .expect("the send is retracted with a failure");
    let failure: broca_records::record::QueuedAdmissionFailure =
        serde_json::from_value(failure).unwrap();
    assert_eq!(failure.run_id.0, "measured");
    assert_eq!(failure.error.class, broca_protocol::ErrorClass::Permanent);
    assert_eq!(failure.error.provider_code.as_deref(), Some("runner_error"));
}

/// A session whose earlier run offered a tool the next doesn't, so the session has a signed floor,
/// and a run on it taking `tool_steps` tool steps before its answer: the entry reads by place
/// that last run made.
async fn floor_reads_of_a_run(tool_steps: usize) -> usize {
    let backend = MemoryBackend::default();
    let mut session = session(&backend).await;
    let mut parts = MemoryParts::default();
    run_with(
        &mut session,
        &mut parts,
        "h0",
        vec![answer_step("a0")],
        false,
        Some("list"),
    )
    .await;
    let mut steps: Vec<_> = (0..tool_steps)
        .map(|n| tool_step(&format!("c{n}")))
        .collect();
    steps.push(answer_step("last"));
    let reads = run_on(&mut session, &mut parts, "measured", steps).await;
    let floor = broca_session_loop::run::signed_floor(&session, &parts, &mut Default::default())
        .await
        .unwrap();
    assert!(
        floor.is_some_and(|floor| floor > 0),
        "a signed floor: {floor:?}"
    );
    reads.entry_at.load(Ordering::SeqCst)
}

/// Every step states the entry the session's signed floor goes through, by its id. A placed
/// message keeps its id, so a run reads that entry from the session once, not at every step.
#[tokio::test]
async fn the_signed_floor_entry_is_read_once_per_run() {
    let short = floor_reads_of_a_run(1).await;
    let long = floor_reads_of_a_run(6).await;
    assert!(short > 0, "the run reads the floor's entry");
    assert_eq!(long, short, "five more steps read the floor's entry again");
}
