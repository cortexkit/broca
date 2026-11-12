//! A run moves to another provider account when the provider refuses a step because the run's
//! account has spent its usage window (a quota wall), and routing offers another account.
//!
//! Every test runs the real loop on a real session in memory. The provider is scripted per
//! account, and the executor's side of a move (`AccountContinue`) is scripted too: it answers
//! with the offer, hold or refusal a test names, and hands over the provider of the account it
//! offers. A writer wrapped around the session logs every append with its barrier, and the
//! providers log every request into the same log, so a test can read the order in which records
//! became durable and requests went out.

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use broca_core::provider::{Provider, StepRuntime};
use broca_core::stub_plane::StubToolPlane;
use broca_protocol::content::{AssistantMessage, ContentBlock, FinishReason, ToolCall};
use broca_protocol::error::{AccountContinueAnswer, AccountLimitWindow, ProviderError};
use broca_protocol::provider::{
    AccountLimitDisposition, AttemptAccountLimit, AttemptAccountObservation, AttemptOutcome,
};
use broca_protocol::request::CallOptions;
use broca_protocol::{
    BindIdentity, MessageId, ModelAttempt, NormalizedEvent, ProviderControl, RunFinishReason,
    RunId, StepId, SubmissionRequest, ToolCallId, Usage,
};
use broca_session_loop::account_move::{AccountContinue, Answer, Move, Wall};
use broca_session_loop::parts::MemoryParts;
use broca_session_loop::run::{AccountReports, Seams};
use broca_session_loop::writer::Writer;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::operations::Operations;
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::Record;
use broca_session_store::session::{Session, SessionError};
use serde_json::{json, Value};
use tokio::sync::mpsc;

/// The time every test runs at.
const NOW: u64 = 1_000_000;
/// How long the offers in these tests are reserved for: a minute.
const OFFER_VALID_MS: u64 = 60_000;
const RUN: &str = "moving-run";

fn bind() -> BindIdentity {
    BindIdentity::new("/project", "account-move", "s1")
}

async fn session(backend: &MemoryBackend) -> Session<&MemoryBackend> {
    Session::create(
        backend,
        SessionAddr("0123456789abcdef".into()),
        SessionTriple {
            project_root: "/project".into(),
            harness: "account-move".into(),
            session: "s1".into(),
        },
        &DeploymentKey::new(1, &[7; 32]),
        false,
    )
    .await
    .unwrap()
}

/// The selection routing gave the run for `account`, under the decision `decision`.
fn routed(account: &str, decision: &str) -> broca_wal::AuthSelection {
    serde_json::from_value(json!({
        "credential_id": format!("cred-{account}"),
        "account_ref": account,
        "resolved_credential_id": format!("cred-{account}"),
        "auth_method": "oauth",
        "account_decision": {
            "path": "routed",
            "admission_id": RUN,
            "decision_id": decision,
            "conversation_key": "broca-session:conversation",
        },
    }))
    .unwrap()
}

/// The render config of a run on `account`: a persona that names it, as the Claude
/// subscription route's does.
fn render_config(account: &str) -> Value {
    json!({"claude_code_persona": {"account_uuid": account}})
}

/// What one account's provider answers for a step.
#[derive(Clone)]
enum Reply {
    /// The model calls `read`.
    Tool,
    /// The model answers in text.
    Answer,
    /// A quota wall on `window` that resets at `resets_at_ms`.
    Wall {
        window: &'static str,
        resets_at_ms: u64,
    },
    /// The account has no credit left.
    Credit,
}

/// A request a provider was asked to send.
#[derive(Clone)]
struct Sent {
    account: String,
    step: u64,
    first_attempt: u32,
    opts: CallOptions,
}

/// The order of what became durable and what was sent.
type Log = Arc<Mutex<Vec<String>>>;

/// One account's provider.
struct Account {
    name: String,
    replies: Mutex<VecDeque<Reply>>,
    sent: Arc<Mutex<Vec<Sent>>>,
    log: Log,
    render_config: Value,
    /// When the account last accepted a request, which a timed pause on it weighs.
    accepted_at_ms: Option<u64>,
    /// The output caps the provider allows, which let the loop lower a step's cap.
    output_limits: Option<broca_core::output_reservation::OutputLimits>,
}

impl Account {
    fn new(name: &str, replies: Vec<Reply>, sent: &Arc<Mutex<Vec<Sent>>>, log: &Log) -> Self {
        Self {
            name: name.into(),
            replies: Mutex::new(replies.into()),
            sent: Arc::clone(sent),
            log: Arc::clone(log),
            render_config: render_config(name),
            accepted_at_ms: None,
            output_limits: None,
        }
    }
}

fn attempt(ordinal: u32, account: &str, failed: Option<AttemptAccountLimit>) -> ModelAttempt {
    let refused = failed.is_some();
    ModelAttempt {
        attempt: ordinal,
        request_body_len: 10,
        request_body_sha256: "0".repeat(64),
        http_status: Some(if refused { 429 } else { 200 }),
        content_type: None,
        response_body_len: 0,
        response_body_sha256: "0".repeat(64),
        response_body_cap: None,
        outcome: if refused {
            AttemptOutcome::HttpError
        } else {
            AttemptOutcome::Complete
        },
        ts_ms: NOW,
        content_progress: Default::default(),
        sent_at_ms: Some(NOW),
        transport: None,
        websocket_request: None,
        cache_evidence: None,
        websocket_close_code: None,
        input_transformations: None,
        input_transformations_unmeasured: false,
        credential_refresh: None,
        served_account_ref: Some(account.into()),
        account_observation: Box::new(AttemptAccountObservation {
            failure_received_at_ms: None,
            ratelimit_headers: Default::default(),
            limit: failed,
        }),
    }
}

fn message(step: u64, tool: bool) -> AssistantMessage {
    let mut content = vec![ContentBlock::Text {
        text: format!("step {step}"),
    }];
    if tool {
        content.push(ContentBlock::ToolCall(ToolCall {
            thought_signature: None,
            tool_call_id: ToolCallId(format!("call-{step}")),
            tool_name: "read".into(),
            input: json!({"path": "src/main.rs"}),
        }));
    }
    AssistantMessage {
        message_id: MessageId(format!("m-{step}")),
        content,
    }
}

impl Account {
    fn events(&self, step: u64, first_attempt: u32, reply: Reply) -> Vec<NormalizedEvent> {
        let finished = |tool: bool| {
            vec![
                NormalizedEvent::Control(ProviderControl::AttemptFinished {
                    attempt: attempt(first_attempt, &self.name, None),
                }),
                NormalizedEvent::Control(ProviderControl::AssistantMessage {
                    message: message(step, tool),
                }),
                NormalizedEvent::Control(ProviderControl::Finished {
                    usage: Usage {
                        input_tokens: Some(1_000),
                        output_tokens: Some(10),
                        ..Usage::default()
                    },
                    finish_reason: if tool {
                        FinishReason::ToolCalls
                    } else {
                        FinishReason::Stop
                    },
                    retries_used: 0,
                }),
            ]
        };
        let refused = |window: AccountLimitWindow, resets_at_ms: Option<u64>, error: Value| {
            vec![
                NormalizedEvent::Control(ProviderControl::AttemptFinished {
                    attempt: attempt(
                        first_attempt,
                        &self.name,
                        Some(AttemptAccountLimit {
                            disposition: AccountLimitDisposition::ACCOUNT,
                            observed_at_ms: NOW - 10,
                            resets_at_ms,
                            resets_at_source: None,
                            limit_window: Some(window),
                        }),
                    ),
                }),
                NormalizedEvent::Control(ProviderControl::Error {
                    error: serde_json::from_value::<ProviderError>(error).unwrap(),
                    retries_used: 0,
                }),
            ]
        };
        match reply {
            Reply::Tool => finished(true),
            Reply::Answer => finished(false),
            Reply::Wall {
                window,
                resets_at_ms,
            } => refused(
                window.into(),
                Some(resets_at_ms),
                json!({
                    "class": "transient",
                    "message": "the account's usage window is spent",
                    "status": 429,
                    "provider_code": "account_limit_exhausted",
                    "resets_at_ms": resets_at_ms,
                    "limit_window": window,
                }),
            ),
            Reply::Credit => refused(
                AccountLimitWindow::Credit,
                None,
                json!({
                    "class": "permanent",
                    "message": "the account has no credit left",
                    "status": 402,
                    "provider_code": "account_limit_exhausted",
                    "limit_window": "credit",
                }),
            ),
        }
    }
}

#[async_trait::async_trait]
impl Provider for Account {
    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        unreachable!("the loop calls step_with_runtime")
    }

    async fn step_with_runtime(
        &self,
        step_id: StepId,
        opts: Arc<CallOptions>,
        runtime: StepRuntime,
    ) -> mpsc::Receiver<NormalizedEvent> {
        self.log
            .lock()
            .unwrap()
            .push(format!("send {} step {}", self.name, step_id.0));
        self.sent.lock().unwrap().push(Sent {
            account: self.name.clone(),
            step: step_id.0,
            first_attempt: runtime.first_attempt,
            opts: (*opts).clone(),
        });
        let reply = self
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| panic!("{} has no reply for step {}", self.name, step_id.0));
        let (tx, rx) = mpsc::channel(16);
        for event in self.events(step_id.0, runtime.first_attempt, reply) {
            tx.send(event).await.unwrap();
        }
        rx
    }

    fn reports_attempt_evidence(&self) -> bool {
        true
    }

    fn render_config_blob(&self) -> Value {
        self.render_config.clone()
    }

    fn output_limits(&self) -> Option<broca_core::output_reservation::OutputLimits> {
        self.output_limits
    }

    fn last_accepted_at_ms(&self, _account: &str) -> Option<u64> {
        self.accepted_at_ms
    }
}

/// What the scripted routing answers, in turn.
enum Scripted {
    /// An offer of `account`, reserved until `valid_until_ms`. `persona` says whether the run's
    /// render config names the account, so the move refreezes it.
    Offer {
        account: &'static str,
        valid_until_ms: u64,
        persona: bool,
    },
    /// An offer whose selection names another account than the offer: what the executor's
    /// vault check refuses, checked here again by the loop.
    OfferNaming {
        account: &'static str,
        names: &'static str,
    },
    Hold(&'static str, Option<u64>),
    Refuse(&'static str),
    /// No usable answer: routing unreachable, or not declaring the operation.
    Nothing,
}

/// What a wall asked, as the executor saw it.
#[derive(Debug, Clone, PartialEq)]
struct Asked {
    credential_id: String,
    account: String,
    limit_window: String,
    resets_at_ms: Option<u64>,
    decision_id: Option<String>,
    refused: Vec<String>,
    reask: bool,
    attempt_seq: u64,
}

/// The executor's side of a move, scripted.
struct Routing {
    answers: Mutex<VecDeque<Scripted>>,
    providers: Mutex<HashMap<String, Box<dyn Provider>>>,
    asked: Mutex<Vec<Asked>>,
    adopted: Mutex<Vec<broca_wal::AuthSelection>>,
}

#[async_trait::async_trait]
impl AccountContinue<Box<dyn Provider>> for Routing {
    async fn ask(&self, wall: &Wall<'_>) -> Answer<Box<dyn Provider>> {
        self.asked.lock().unwrap().push(Asked {
            credential_id: wall.credential_id.into(),
            account: wall.account.into(),
            limit_window: wall.limit_window.into(),
            resets_at_ms: wall.resets_at_ms,
            decision_id: wall
                .selection
                .account_decision
                .as_ref()
                .and_then(|decision| decision.decision_id.clone()),
            refused: wall.refused_accounts.to_vec(),
            reask: wall.reask,
            attempt_seq: wall.attempt_seq,
        });
        let answer = self
            .answers
            .lock()
            .unwrap()
            .pop_front()
            .expect("routing was asked more often than the test scripted");
        let offer = |account: &str,
                     selection: broca_wal::AuthSelection,
                     valid_until_ms: u64,
                     persona: bool| {
            let provider = self
                .providers
                .lock()
                .unwrap()
                .remove(account)
                .unwrap_or_else(|| panic!("no provider for {account}"));
            let render =
                persona.then(|| (provider.renderer_version(), provider.render_config_blob()));
            Answer::Move(Box::new(Move {
                selection,
                account: account.into(),
                valid_until_ms,
                render,
                provider,
            }))
        };
        match answer {
            Scripted::Offer {
                account,
                valid_until_ms,
                persona,
            } => offer(
                account,
                routed(account, &format!("decision-{account}")),
                valid_until_ms,
                persona,
            ),
            Scripted::OfferNaming { account, names } => offer(
                account,
                routed(names, &format!("decision-{account}")),
                NOW + OFFER_VALID_MS,
                false,
            ),
            Scripted::Hold(code, until_ms) => Answer::Stay(Some(AccountContinueAnswer {
                routing_code: code.into(),
                until_ms,
            })),
            Scripted::Refuse(code) => Answer::Stay(Some(AccountContinueAnswer {
                routing_code: code.into(),
                until_ms: None,
            })),
            Scripted::Nothing => Answer::Stay(None),
        }
    }

    fn adopted(&self, selection: &broca_wal::AuthSelection) {
        self.adopted.lock().unwrap().push(selection.clone());
    }
}

/// The run's reports to routing, in the order the loop made them.
struct Reports(Log);

impl AccountReports for Reports {
    fn run_started(&self, _event_id: &str, _seq: u64) {
        self.0.lock().unwrap().push("report started".into());
    }

    fn attempt_recorded(&self, _event_id: &str, _seq: u64, attempt: &ModelAttempt) {
        self.0.lock().unwrap().push(format!(
            "report attempt {}",
            attempt.served_account_ref.as_deref().unwrap_or("?")
        ));
    }

    /// The loop hands each attempt's report the selection it was sent under, which must name
    /// the account that served it: after a move, the new one.
    fn attempt_recorded_on(
        &self,
        event_id: &str,
        seq: u64,
        attempt: &ModelAttempt,
        selection: Option<&broca_wal::AuthSelection>,
    ) {
        assert_eq!(
            selection.and_then(|selection| selection.account_ref.as_deref()),
            attempt.served_account_ref.as_deref(),
            "an attempt's report names the selection it was sent under"
        );
        self.attempt_recorded(event_id, seq, attempt);
    }

    fn account_moved(&self, _event_id: &str, _seq: u64, selection: &broca_wal::AuthSelection) {
        self.0.lock().unwrap().push(format!(
            "report moved {}",
            selection.account_ref.as_deref().unwrap_or("?")
        ));
    }
}

/// A session's writer that logs each append: the kinds of its records, and whether it waited
/// for them to be durable. `refuse_moves` makes the append of a move's records fail.
struct Logging<'s, 'b> {
    session: &'s mut Session<&'b MemoryBackend>,
    log: Log,
    refuse_moves: bool,
    /// Asks the runner to stop once this many tool batches are durable, so the run pauses for
    /// a restart at the boundary after the last of them.
    stop_after_batches: Option<(usize, tokio_util::sync::CancellationToken)>,
}

fn kind(record: &Record) -> String {
    let debug = format!("{record:?}");
    debug.split('(').next().unwrap_or_default().to_owned()
}

impl Logging<'_, '_> {
    fn batch_written(&mut self, records: &[Record]) {
        if !records
            .iter()
            .any(|record| matches!(record, Record::BatchEnd(_)))
        {
            return;
        }
        if let Some((left, stop)) = self.stop_after_batches.as_mut() {
            *left = left.saturating_sub(1);
            if *left == 0 {
                stop.cancel();
            }
        }
    }
}

impl Writer for Logging<'_, '_> {
    async fn append(&mut self, records: &[Record], barrier: bool) -> Result<u64, SessionError> {
        if self.refuse_moves
            && records
                .iter()
                .any(|record| matches!(record, Record::AuthRefrozen(_)))
        {
            return Err(SessionError::Poisoned);
        }
        let seq = Writer::append(&mut *self.session, records, barrier).await?;
        let kinds: Vec<String> = records.iter().map(kind).collect();
        self.log.lock().unwrap().push(format!(
            "append {} {}",
            if barrier { "durable" } else { "buffered" },
            kinds.join(",")
        ));
        self.batch_written(records);
        Ok(seq)
    }

    async fn send_records(&mut self, submission_id: &str) -> Result<Vec<Record>, SessionError> {
        Writer::send_records(&mut *self.session, submission_id).await
    }

    async fn append_with(
        &mut self,
        build: &mut (dyn for<'s> FnMut(&'s Conversation, &'s Operations, u64) -> Vec<Record>
                  + Send),
        barrier: bool,
    ) -> Result<Vec<Record>, SessionError> {
        let refuse = self.refuse_moves;
        let mut refused = false;
        let mut checked = |conversation: &Conversation, operations: &Operations, first_seq: u64| {
            let records = build(conversation, operations, first_seq);
            if refuse
                && records
                    .iter()
                    .any(|record| matches!(record, Record::AuthRefrozen(_)))
            {
                refused = true;
                return Vec::new();
            }
            records
        };
        let records = Writer::append_with(&mut *self.session, &mut checked, barrier).await?;
        if refused {
            return Err(SessionError::Poisoned);
        }
        if !records.is_empty() {
            let kinds: Vec<String> = records.iter().map(kind).collect();
            self.log.lock().unwrap().push(format!(
                "append {} {}",
                if barrier { "durable" } else { "buffered" },
                kinds.join(",")
            ));
        }
        self.batch_written(&records);
        Ok(records)
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
        Writer::run_start(&*self.session, run_id).await
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

/// One run's scenario.
struct Case {
    /// Each account's replies, in order. The run starts on the first account.
    accounts: Vec<(&'static str, Vec<Reply>)>,
    /// Routing's answers, in turn.
    answers: Vec<Scripted>,
    /// The selection the run starts on; routing's decision for the first account by default.
    selection: Option<broca_wal::AuthSelection>,
    /// Whether the session's executor can ask routing at all.
    can_ask: bool,
    /// A timed pause on the same account: the run consented, the boot cap and the time the
    /// account last accepted a request.
    account_limit_pause: bool,
    wait: broca_core::SameAccountWait,
    accepted_at_ms: Option<u64>,
    refuse_moves: bool,
    /// Every account's output limits, and the run's context limit, so a step's cap is lowered.
    output_limits: Option<broca_core::output_reservation::OutputLimits>,
    context_limit: Option<u64>,
    /// The session's compaction provider: the run is admitted under a plan that names it.
    compaction: Option<Arc<Compactor>>,
}

impl Case {
    fn new(accounts: Vec<(&'static str, Vec<Reply>)>, answers: Vec<Scripted>) -> Self {
        Self {
            accounts,
            answers,
            selection: None,
            can_ask: true,
            account_limit_pause: false,
            wait: Default::default(),
            accepted_at_ms: None,
            refuse_moves: false,
            output_limits: None,
            context_limit: None,
            compaction: None,
        }
    }
}

/// A wall on the seven-day window, resetting in three days.
fn wall() -> Reply {
    Reply::Wall {
        window: "seven_day",
        resets_at_ms: NOW + 3 * 86_400_000,
    }
}

fn offer(account: &'static str) -> Scripted {
    Scripted::Offer {
        account,
        valid_until_ms: NOW + OFFER_VALID_MS,
        persona: false,
    }
}

/// What a run did.
struct Ran {
    finished: Result<broca_session_loop::Finished, broca_session_loop::LoopError>,
    records: Vec<(u64, Record)>,
    sent: Vec<Sent>,
    log: Vec<String>,
    asked: Vec<Asked>,
    adopted: Vec<broca_wal::AuthSelection>,
    segments: Vec<broca_core::billing::SpendSegment>,
    operations: Operations,
}

fn request() -> broca_core::RunRequest {
    let request: SubmissionRequest = serde_json::from_value(json!({
        "model": {"provider_module_id": "stub", "model_id": "m"},
        "input": [{"role": "user", "content": [{"type": "text", "text": "read the file"}]}],
        "tools": [{
            "name": "read",
            "description": "Reads a file.",
            "input_schema": {"type": "object", "properties": {"path": {"type": "string"}}},
        }],
        "stop_when": [{"type": "step_count", "n": 50}],
    }))
    .unwrap();
    broca_core::RunRequest::from_submission(
        request,
        RunId(RUN.into()),
        bind(),
        broca_protocol::SubmissionId(String::new()),
    )
}

#[allow(clippy::too_many_arguments, clippy::borrowed_box)]
fn seams<'a>(
    provider: &'a Box<dyn Provider>,
    tools: &'a StubToolPlane,
    now: &'a (dyn Fn() -> u64 + Sync),
    routing: Option<&'a Routing>,
    log: &Log,
    selection: broca_wal::AuthSelection,
    wait: broca_core::SameAccountWait,
    stop: tokio_util::sync::CancellationToken,
) -> Seams<'a, Box<dyn Provider>, StubToolPlane> {
    Seams {
        provider,
        tools,
        bind: bind(),
        out: None,
        cancel: Default::default(),
        stop,
        held: None,
        approval: &broca_core::capability::AllowAll,
        hook_plane: None,
        now_ms: now,
        session_address: None,
        compaction_plane: None,
        compaction_session: None,
        compaction_changes: false,
        keep_warm: None,
        account_reports: Some(Arc::new(Reports(Arc::clone(log)))),
        max_same_account_wait: wait,
        auth_selection: Some(selection),
        step_progress: None,
        model_call_ready: None,
        account_continue: routing.map(|routing| routing as &dyn AccountContinue<Box<dyn Provider>>),
    }
}

async fn run_case(case: Case) -> Ran {
    let backend = MemoryBackend::default();
    let mut session = session(&backend).await;
    let mut parts = MemoryParts::default();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let log: Log = Arc::default();
    let mut accounts = case.accounts.into_iter();
    let (first, replies) = accounts.next().expect("a first account");
    let mut first_account = Account::new(first, replies, &sent, &log);
    first_account.accepted_at_ms = case.accepted_at_ms;
    first_account.output_limits = case.output_limits;
    let output_limits = case.output_limits;
    let provider: Box<dyn Provider> = Box::new(first_account);
    let routing = Routing {
        answers: Mutex::new(case.answers.into()),
        providers: Mutex::new(
            accounts
                .map(|(name, replies)| {
                    (
                        name.to_owned(),
                        Box::new(Account {
                            output_limits,
                            ..Account::new(name, replies, &sent, &log)
                        }) as Box<dyn Provider>,
                    )
                })
                .collect(),
        ),
        asked: Default::default(),
        adopted: Default::default(),
    };
    let selection = case
        .selection
        .unwrap_or_else(|| routed(first, &format!("decision-{first}")));
    let mut request = request();
    let compacted = case.compaction.is_some();
    let context_limit = case.context_limit.or(compacted.then_some(COMPACTED_LIMIT));
    request.context_limit = context_limit;
    let control = broca_core::RunControl {
        cancel: Some(Default::default()),
        auth_selection: Some(selection.clone()),
        account_limit_pause: case.account_limit_pause,
        admitted_context_limit: context_limit,
        manifest: compacted.then(|| compacted_manifest(&request.tools)),
        new_log_compaction_plan_version: compacted
            .then_some(cortexkit_compaction_view::PLAN_VERSION),
        ..Default::default()
    };
    let now = || NOW;
    let admission = broca_session_loop::admit::admission(
        &mut request,
        &control,
        &provider,
        Writer::operations(&session),
        Some(now()),
    )
    .unwrap();
    let tools = StubToolPlane::new().with_canned_text("read", "fn main() {}");
    let mut seams = seams(
        &provider,
        &tools,
        &now,
        case.can_ask.then_some(&routing),
        &log,
        selection,
        case.wait,
        Default::default(),
    );
    if let Some(compactor) = &case.compaction {
        seams.session_address = Some(broca_core::wal_writer::session_addr(&bind()));
        seams.compaction_plane = Some(&**compactor as &dyn broca_core::CompactionPlane);
    }
    let mut writer = Logging {
        session: &mut session,
        log: Arc::clone(&log),
        refuse_moves: case.refuse_moves,
        stop_after_batches: None,
    };
    let finished = broca_session_loop::run::run(&mut writer, &mut parts, admission, &seams).await;
    drop(seams);
    let records = session.open_log_records().await.unwrap();
    let rebuilt = broca_session_loop::billing::run_records(&session, RUN, &bind(), &parts)
        .await
        .unwrap();
    let resolver = |_: &str, _: &str| (broca_core::billing::ChargeBasis::Unknown, 1);
    let segments = broca_core::billing::compute_segments_for_run(
        &rebuilt,
        &RunId(RUN.into()),
        "test-source",
        &resolver,
    );
    let sent = sent.lock().unwrap().clone();
    let log = log.lock().unwrap().clone();
    Ran {
        finished,
        records,
        sent,
        log,
        asked: routing.asked.into_inner().unwrap(),
        adopted: routing.adopted.into_inner().unwrap(),
        segments,
        operations: session.operations.clone(),
    }
}

impl Ran {
    fn reason(&self) -> RunFinishReason {
        self.finished.as_ref().expect("the loop runs").reason
    }

    /// The error the run ended with, as its end records it.
    fn end_error(&self) -> Value {
        self.records
            .iter()
            .find_map(|(_, record)| match record {
                Record::RunEnd(end) => end.error.clone(),
                _ => None,
            })
            .expect("the run ended with an error")
    }

    fn auth_refreezes(&self) -> Vec<broca_wal::AuthSelection> {
        self.records
            .iter()
            .filter_map(|(_, record)| match record {
                Record::AuthRefrozen(refrozen) => {
                    Some(serde_json::from_value(refrozen.auth_selection.clone()).unwrap())
                }
                _ => None,
            })
            .collect()
    }

    /// Each attempt record's step, ordinal and account.
    fn attempts(&self) -> Vec<(u64, u32, String)> {
        self.records
            .iter()
            .filter_map(|(_, record)| match record {
                Record::Attempt(attempt) => Some((
                    attempt.step_id,
                    attempt.attempt,
                    attempt.served_account_ref.clone().unwrap_or_default(),
                )),
                _ => None,
            })
            .collect()
    }

    fn sent_on(&self, account: &str) -> Vec<&Sent> {
        self.sent
            .iter()
            .filter(|sent| sent.account == account)
            .collect()
    }
}

/// The request a step's options render to, without the transport's diagnostics (the step
/// context names the credential and account, and is never rendered).
fn rendered(opts: &CallOptions) -> Vec<u8> {
    let mut opts = opts.clone();
    opts.step_context = None;
    broca_core::render::canonical_request_bytes(&opts)
}

fn position(log: &[String], from: usize, entry: &str) -> usize {
    log[from..]
        .iter()
        .position(|line| line == entry)
        .map(|at| at + from)
        .unwrap_or_else(|| panic!("{entry:?} is not in the log after {from}: {log:#?}"))
}

/// A wall on account A at step 2, an offer of B: the step completes on B as the next attempt of
/// the same step, and nothing went to B before the move was durable.
#[tokio::test]
async fn a_wall_moves_the_step_to_the_offered_account() {
    let ran = run_case(Case::new(
        vec![("A", vec![Reply::Tool, wall()]), ("B", vec![Reply::Answer])],
        vec![offer("B")],
    ))
    .await;
    assert_eq!(ran.reason(), RunFinishReason::Completed);

    // The refused attempt is durable, then the move, then the request on B.
    let log = &ran.log;
    let walled = position(log, 0, "send A step 2");
    let refused = position(log, walled, "append durable Attempt");
    let moved = position(log, refused, "append durable AuthRefrozen");
    let resent = position(log, moved, "send B step 2");
    assert!(walled < refused && refused < moved && moved < resent);
    // Routing hears of the refused attempt, then of the move, then of the attempt on B.
    let reported = position(log, walled, "report attempt A");
    let reported_move = position(log, reported, "report moved B");
    position(log, reported_move, "report attempt B");
    assert!(reported_move < resent);

    // The re-sent request renders the refused one's bytes; only the step context, which the
    // transport reads for its diagnostics, names the new account.
    let on_a = ran.sent_on("A");
    let on_b = ran.sent_on("B");
    let (refused, resent) = (on_a[1], on_b[0]);
    assert_eq!((refused.step, resent.step), (2, 2));
    assert_eq!(rendered(&refused.opts), rendered(&resent.opts));
    let context = |sent: &Sent| {
        let context = sent.opts.step_context.clone().unwrap();
        (context.credential_id, context.account_ref)
    };
    assert_eq!(context(refused), (Some("cred-A".into()), Some("A".into())));
    assert_eq!(context(resent), (Some("cred-B".into()), Some("B".into())));

    // The attempts of step 2 are one sequence over both accounts.
    assert_eq!(resent.first_attempt, 1);
    assert_eq!(
        ran.attempts(),
        vec![(1, 0, "A".into()), (2, 0, "A".into()), (2, 1, "B".into())]
    );

    // What routing was asked, from the refused attempt's own record.
    assert_eq!(ran.asked.len(), 1);
    let asked = &ran.asked[0];
    assert_eq!(asked.credential_id, "cred-A");
    assert_eq!(asked.account, "A");
    assert_eq!(asked.limit_window, "seven_day");
    assert_eq!(asked.resets_at_ms, Some(NOW + 3 * 86_400_000));
    assert_eq!(asked.decision_id.as_deref(), Some("decision-A"));
    assert_eq!(asked.refused, vec!["A".to_owned()]);
    assert!(!asked.reask);

    // The move names B under the offer's decision, and the run's state, which its status
    // reads, names B from then on.
    let b = routed("B", "decision-B");
    assert_eq!(ran.auth_refreezes(), vec![b.clone()]);
    assert_eq!(ran.adopted, vec![b.clone()]);
    assert_eq!(
        ran.operations.run.as_ref().unwrap().refrozen_auth,
        Some(serde_json::to_value(&b).unwrap())
    );
    // A move that leaves the render config alone refreezes no config.
    assert!(!ran
        .records
        .iter()
        .any(|(_, record)| matches!(record, Record::ConfigRefrozen(_))));

    // The run's spend: A up to the wall, B after.
    let accounts: Vec<_> = ran
        .segments
        .iter()
        .map(|segment| (segment.account_ref.clone(), segment.usage.input_tokens))
        .collect();
    assert_eq!(
        accounts,
        vec![
            (Some("A".into()), Some(1_000)),
            (Some("B".into()), Some(1_000))
        ]
    );
}

/// A move on a route whose render config names the account refreezes the config with the
/// account, in one durable batch. Billing closes a segment at each of the two records, so the
/// spend has an empty segment between A's and B's; it carries no quantity, and the segments
/// around it don't depend on it.
#[tokio::test]
async fn a_move_that_refreezes_the_persona_writes_both_records_in_one_batch() {
    let ran = run_case(Case::new(
        vec![("A", vec![Reply::Tool, wall()]), ("B", vec![Reply::Answer])],
        vec![Scripted::Offer {
            account: "B",
            valid_until_ms: NOW + OFFER_VALID_MS,
            persona: true,
        }],
    ))
    .await;
    assert_eq!(ran.reason(), RunFinishReason::Completed);
    let moved = position(&ran.log, 0, "append durable ConfigRefrozen,AuthRefrozen");
    position(&ran.log, moved, "send B step 2");
    let run = ran.operations.run.as_ref().unwrap();
    assert_eq!(
        run.refrozen_render.as_ref().unwrap()["render_config"],
        render_config("B")
    );
    let segments: Vec<_> = ran
        .segments
        .iter()
        .map(|segment| {
            (
                segment.account_ref.clone(),
                segment.usage,
                segment.usage_policy_version,
            )
        })
        .collect();
    for segment in &ran.segments {
        eprintln!("segment {}", serde_json::to_string(segment).unwrap());
    }
    assert_eq!(segments.len(), 3, "{segments:#?}");
    assert_eq!(segments[0].0.as_deref(), Some("A"));
    assert_eq!(segments[0].1.input_tokens, Some(1_000));
    assert_eq!(segments[1].0.as_deref(), Some("A"));
    assert_eq!(segments[1].1, Usage::default());
    let empty = serde_json::to_value(ran.segments[1].usage).unwrap();
    assert!(
        empty
            .as_object()
            .unwrap()
            .values()
            .all(|value| !value.is_number()),
        "the empty segment carries no quantity, not a zero: {empty}"
    );
    assert_eq!(segments[2].0.as_deref(), Some("B"));
    assert_eq!(segments[2].1.input_tokens, Some(1_000));
}

/// The error a run ends with at a wall when it doesn't move, with routing's answer in it.
async fn ends_without_moving(answers: Vec<Scripted>) -> (Ran, Value) {
    let ran = run_case(Case::new(
        vec![
            ("A", vec![Reply::Tool, wall()]),
            ("A", vec![Reply::Answer]),
            ("B", vec![Reply::Answer]),
            ("C", vec![Reply::Answer]),
        ],
        answers,
    ))
    .await;
    assert_eq!(ran.reason(), RunFinishReason::Error);
    assert!(ran.auth_refreezes().is_empty());
    assert!(ran.adopted.is_empty());
    assert_eq!(ran.sent.len(), 2, "only A's two requests went out");
    assert!(
        !ran.log.iter().any(|line| line.starts_with("report moved")),
        "no move is reported to routing: {:#?}",
        ran.log
    );
    let error = ran.end_error();
    (ran, error)
}

/// The same run when it can't ask: today's ending, which the other endings are compared with.
async fn todays_ending() -> Value {
    let mut case = Case::new(vec![("A", vec![Reply::Tool, wall()])], vec![]);
    case.can_ask = false;
    let ran = run_case(case).await;
    assert_eq!(ran.reason(), RunFinishReason::Error);
    ran.end_error()
}

/// `error` is today's, plus routing's answer under `account_continue` when it gave one.
fn is_todays_with(error: &Value, answer: Option<Value>, today: &Value) {
    let mut without = error.clone();
    let member = without.as_object_mut().unwrap().remove("account_continue");
    assert_eq!(member, answer);
    assert_eq!(&without, today);
    assert_eq!(error["provider_code"], "account_limit_exhausted");
}

#[tokio::test]
async fn a_hold_ends_the_run_with_routings_answer() {
    let today = todays_ending().await;
    let (_, error) = ends_without_moving(vec![Scripted::Hold(
        "no_compatible_account",
        Some(NOW + 3_600_000),
    )])
    .await;
    eprintln!("before: {}", serde_json::to_string(&today).unwrap());
    eprintln!("after:  {}", serde_json::to_string(&error).unwrap());
    // A hold that names a time also says when to retry: an hour from now, as whole seconds.
    let mut error = error;
    assert_eq!(today.get("retry_after_secs"), None);
    let retry = error.as_object_mut().unwrap().remove("retry_after_secs");
    assert_eq!(retry, Some(json!(3600.0)));
    is_todays_with(
        &error,
        Some(json!({"routing_code": "no_compatible_account", "until_ms": NOW + 3_600_000})),
        &today,
    );
    let (_, error) = ends_without_moving(vec![Scripted::Hold("no_compatible_account", None)]).await;
    is_todays_with(
        &error,
        Some(json!({"routing_code": "no_compatible_account"})),
        &today,
    );
}

#[tokio::test]
async fn a_refusal_ends_the_run_with_routings_code() {
    let today = todays_ending().await;
    let (_, error) = ends_without_moving(vec![Scripted::Refuse("account_pinned")]).await;
    is_todays_with(
        &error,
        Some(json!({"routing_code": "account_pinned"})),
        &today,
    );
}

#[tokio::test]
async fn no_answer_from_routing_ends_the_run_as_today() {
    let today = todays_ending().await;
    let (ran, error) = ends_without_moving(vec![Scripted::Nothing]).await;
    assert_eq!(ran.asked.len(), 1);
    is_todays_with(&error, None, &today);
}

/// An offer that lapsed before the move could be written is dead: routing is asked once more,
/// at the same wall, and a second dead offer ends the run.
#[tokio::test]
async fn an_expired_offer_asks_once_more_and_then_ends_the_run() {
    let today = todays_ending().await;
    let expired = |account| Scripted::Offer {
        account,
        valid_until_ms: NOW,
        persona: false,
    };
    let (ran, error) = ends_without_moving(vec![expired("B"), expired("C")]).await;
    is_todays_with(&error, None, &today);
    assert_eq!(ran.asked.len(), 2);
    assert!(!ran.asked[0].reask && ran.asked[1].reask);
    assert_eq!(ran.asked[0].attempt_seq, ran.asked[1].attempt_seq);
}

#[tokio::test]
async fn an_expired_offer_followed_by_a_valid_one_moves() {
    let ran = run_case(Case::new(
        vec![
            ("A", vec![Reply::Tool, wall()]),
            ("B", vec![]),
            ("C", vec![Reply::Answer]),
        ],
        vec![
            Scripted::Offer {
                account: "B",
                valid_until_ms: NOW - 1,
                persona: false,
            },
            offer("C"),
        ],
    ))
    .await;
    assert_eq!(ran.reason(), RunFinishReason::Completed);
    assert_eq!(ran.auth_refreezes(), vec![routed("C", "decision-C")]);
    assert!(ran.sent_on("B").is_empty());
}

#[tokio::test]
async fn an_offer_of_the_walled_account_ends_the_run() {
    let today = todays_ending().await;
    let (_, error) = ends_without_moving(vec![offer("A")]).await;
    is_todays_with(&error, None, &today);
}

#[tokio::test]
async fn an_offer_whose_credential_names_another_account_ends_the_run() {
    let today = todays_ending().await;
    let (_, error) = ends_without_moving(vec![Scripted::OfferNaming {
        account: "B",
        names: "C",
    }])
    .await;
    is_todays_with(&error, None, &today);
}

/// The move's records can't be written: the run stops with the store's error, and nothing was
/// sent on, or adopted for, the offered account.
#[tokio::test]
async fn a_move_that_cant_be_written_sends_nothing_on_the_new_account() {
    let mut case = Case::new(
        vec![("A", vec![Reply::Tool, wall()]), ("B", vec![Reply::Answer])],
        vec![offer("B")],
    );
    case.refuse_moves = true;
    let ran = run_case(case).await;
    assert!(
        matches!(
            ran.finished,
            Err(broca_session_loop::LoopError::Session(
                SessionError::Poisoned
            ))
        ),
        "{:?} {:#?}",
        ran.finished.as_ref().map(|finished| finished.reason),
        ran.log
    );
    assert!(ran.sent_on("B").is_empty());
    assert!(ran.adopted.is_empty());
    assert!(ran.auth_refreezes().is_empty());
}

#[tokio::test]
async fn a_credit_wall_does_not_ask() {
    let ran = run_case(Case::new(
        vec![("A", vec![Reply::Tool, Reply::Credit])],
        vec![],
    ))
    .await;
    assert_eq!(ran.reason(), RunFinishReason::Error);
    assert!(ran.asked.is_empty());
}

#[tokio::test]
async fn a_fixed_key_does_not_ask() {
    let mut case = Case::new(vec![("A", vec![Reply::Tool, wall()])], vec![]);
    let mut pinned = routed("A", "decision-A");
    pinned.account_decision.as_mut().unwrap().path = "pinned".into();
    case.selection = Some(pinned);
    let ran = run_case(case).await;
    assert_eq!(ran.reason(), RunFinishReason::Error);
    assert!(ran.asked.is_empty());
}

#[tokio::test]
async fn a_session_whose_executor_cant_ask_does_not_ask() {
    let mut case = Case::new(vec![("A", vec![Reply::Tool, wall()])], vec![]);
    case.can_ask = false;
    let ran = run_case(case).await;
    assert_eq!(ran.reason(), RunFinishReason::Error);
    assert!(ran.asked.is_empty());
}

/// A wall that resets soon, on a run that consented to wait, pauses on the same account as
/// today, and routing isn't asked.
#[tokio::test]
async fn a_wall_today_answers_with_a_timed_pause_pauses_and_does_not_ask() {
    let mut case = Case::new(
        vec![(
            "A",
            vec![
                Reply::Tool,
                Reply::Wall {
                    window: "five_hour",
                    resets_at_ms: NOW + 60_000,
                },
            ],
        )],
        vec![],
    );
    case.account_limit_pause = true;
    case.wait = broca_core::SameAccountWait::Minutes(15);
    case.accepted_at_ms = Some(NOW - 1_000);
    let ran = run_case(case).await;
    let finished = ran.finished.as_ref().unwrap();
    assert_eq!(
        finished.paused,
        Some(broca_protocol::caller::PauseReason::AccountLimit)
    );
    assert!(ran.asked.is_empty());
    assert!(ran.auth_refreezes().is_empty());
}

/// Two walls in one step with three accounts: A, then B, then C, and the second question
/// names the account the first offer gave and its decision.
#[tokio::test]
async fn two_walls_in_one_step_move_from_a_to_b_to_c() {
    let ran = run_case(Case::new(
        vec![
            ("A", vec![Reply::Tool, wall()]),
            ("B", vec![wall()]),
            ("C", vec![Reply::Answer]),
        ],
        vec![offer("B"), offer("C")],
    ))
    .await;
    assert_eq!(ran.reason(), RunFinishReason::Completed);
    assert_eq!(
        ran.attempts(),
        vec![
            (1, 0, "A".into()),
            (2, 0, "A".into()),
            (2, 1, "B".into()),
            (2, 2, "C".into())
        ]
    );
    let second = &ran.asked[1];
    assert_eq!(second.account, "B");
    assert_eq!(second.credential_id, "cred-B");
    assert_eq!(second.decision_id.as_deref(), Some("decision-B"));
    assert_eq!(second.refused, vec!["A".to_owned(), "B".to_owned()]);
    assert_eq!(
        ran.auth_refreezes(),
        vec![routed("B", "decision-B"), routed("C", "decision-C")]
    );
    let on_c = ran.sent_on("C");
    assert_eq!(on_c[0].first_attempt, 2);
    assert_eq!(rendered(&ran.sent_on("A")[1].opts), rendered(&on_c[0].opts));
}

/// A second wall in the step is never answered by going back to an account that refused it.
#[tokio::test]
async fn a_second_wall_never_moves_back_to_the_first_account() {
    let ran = run_case(Case::new(
        vec![
            ("A", vec![Reply::Tool, wall()]),
            ("B", vec![wall()]),
            ("A", vec![Reply::Answer]),
        ],
        vec![offer("B"), offer("A")],
    ))
    .await;
    assert_eq!(ran.reason(), RunFinishReason::Error);
    assert_eq!(ran.auth_refreezes(), vec![routed("B", "decision-B")]);
    assert_eq!(ran.sent.len(), 3, "A twice and B once");
}

/// A run that moved pauses for a restart, and its resume continues on the account it moved
/// to, under the render config the move refroze: no drift, and no second refreeze.
#[tokio::test]
async fn a_moved_run_resumes_on_the_account_it_moved_to() {
    let backend = MemoryBackend::default();
    let mut session = session(&backend).await;
    let mut parts = MemoryParts::default();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let log: Log = Arc::default();
    let stop = tokio_util::sync::CancellationToken::new();
    let provider: Box<dyn Provider> =
        Box::new(Account::new("A", vec![Reply::Tool, wall()], &sent, &log));
    let b = Account::new("B", vec![Reply::Tool], &sent, &log);
    let routing = Routing {
        answers: Mutex::new(
            vec![Scripted::Offer {
                account: "B",
                valid_until_ms: NOW + OFFER_VALID_MS,
                persona: true,
            }]
            .into(),
        ),
        providers: Mutex::new(HashMap::from([(
            "B".to_owned(),
            Box::new(b) as Box<dyn Provider>,
        )])),
        asked: Default::default(),
        adopted: Default::default(),
    };
    let a = routed("A", "decision-A");
    let mut request = request();
    request.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    let control = broca_core::RunControl {
        cancel: Some(Default::default()),
        auth_selection: Some(a.clone()),
        ..Default::default()
    };
    let now = || NOW;
    let admission = broca_session_loop::admit::admission(
        &mut request,
        &control,
        &provider,
        Writer::operations(&session),
        Some(now()),
    )
    .unwrap();
    let tools = StubToolPlane::new().with_canned_text("read", "fn main() {}");
    let first = seams(
        &provider,
        &tools,
        &now,
        Some(&routing),
        &log,
        a,
        Default::default(),
        stop.clone(),
    );
    let mut writer = Logging {
        session: &mut session,
        log: Arc::clone(&log),
        refuse_moves: false,
        // The runner is asked to stop once step 2's batch, run after the move, is durable: the
        // run pauses for a restart at the next boundary.
        stop_after_batches: Some((2, stop.clone())),
    };
    let paused = broca_session_loop::run::run(&mut writer, &mut parts, admission, &first)
        .await
        .unwrap();
    assert_eq!(
        paused.paused,
        Some(broca_protocol::caller::PauseReason::Restart),
        "{paused:?} {:#?}",
        log.lock().unwrap()
    );
    drop(first);

    // The runner starts again. The executor admits the resume from the run's recorded config,
    // which names B and B's render config (see `run_config_of_run`).
    let recorded = broca_session_loop::run::run_config_of_run(
        writer.operations().run.as_ref().unwrap(),
        &parts,
    )
    .unwrap();
    let b = routed("B", "decision-B");
    assert_eq!(recorded.auth_selection.as_ref(), Some(&b));
    assert_eq!(recorded.frozen_render_config, render_config("B"));
    let resumed_on: Box<dyn Provider> =
        Box::new(Account::new("B", vec![Reply::Answer], &sent, &log));
    let second = seams(
        &resumed_on,
        &tools,
        &now,
        Some(&routing),
        &log,
        b.clone(),
        Default::default(),
        Default::default(),
    );
    let finished = broca_session_loop::resume(&mut writer, &parts, &second)
        .await
        .unwrap();
    assert_eq!(finished.reason, RunFinishReason::Completed);
    drop(second);
    let records = session.open_log_records().await.unwrap();
    let refreezes = records
        .iter()
        .filter(|(_, record)| matches!(record, Record::AuthRefrozen(_)))
        .count();
    let config_refreezes = records
        .iter()
        .filter(|(_, record)| matches!(record, Record::ConfigRefrozen(_)))
        .count();
    assert_eq!((refreezes, config_refreezes), (1, 1));
    let sent = sent.lock().unwrap();
    let last = sent.last().unwrap();
    assert_eq!((last.account.as_str(), last.step), ("B", 3));
    assert_eq!(
        last.opts
            .step_context
            .as_ref()
            .unwrap()
            .account_ref
            .as_deref(),
        Some("B")
    );
}

/// The ride before a step entered again after a move compares the account of the run's last
/// accepted request (its admission before the move) with the account it moved to, so the
/// compaction provider hears `account_change`: the two send contexts name different accounts,
/// and the rule (`ride_causes`) names the change exactly when both name one and they differ.
#[test]
fn the_ride_after_a_move_compares_the_old_account_with_the_new_one() {
    let context = |account: &str| {
        let mut request = request();
        let control = broca_core::RunControl {
            auth_selection: Some(routed(account, &format!("decision-{account}"))),
            ..Default::default()
        };
        let provider: Box<dyn Provider> = Box::new(Account::new(
            account,
            Vec::new(),
            &Arc::default(),
            &Arc::default(),
        ));
        let admission = broca_session_loop::admit::admission(
            &mut request,
            &control,
            &provider,
            &Operations::default(),
            Some(NOW),
        )
        .unwrap();
        broca_core::compaction_v2::host::context_of(
            &broca_session_loop::compaction::frozen(&admission),
            None,
        )
    };
    let (before, after) = (context("A"), context("B"));
    assert!(before.account.is_some() && after.account.is_some());
    assert_ne!(before.account, after.account);
    assert_eq!(before.model, after.model);
    assert_eq!(before.surface, after.surface);
}

/// The step entered again on the new account keeps the output cap its refused attempt recorded:
/// the cap is lowered once, before the first request of the step, and the re-sent request
/// carries the same cap, with no second record.
#[tokio::test]
async fn the_re_sent_step_keeps_its_recorded_output_cap() {
    let mut case = Case::new(
        vec![("A", vec![Reply::Tool, wall()]), ("B", vec![Reply::Answer])],
        vec![offer("B")],
    );
    // A cap far above what the context leaves beside the measured input, so step 2's cap is
    // lowered before its first request.
    case.output_limits = Some(broca_core::output_reservation::OutputLimits {
        max: 4_000_000_000,
        floor: 1,
    });
    case.context_limit = Some(2_000_000_000);
    let ran = run_case(case).await;
    assert_eq!(ran.reason(), RunFinishReason::Completed, "{:#?}", ran.log);
    let cap = |sent: &Sent| sent.opts.step_context.as_ref().unwrap().max_output_tokens;
    let (refused, resent) = (ran.sent_on("A")[1], ran.sent_on("B")[0]);
    assert!(cap(refused).is_some(), "the step's cap was lowered");
    assert!(cap(refused) < Some(4_000_000_000));
    assert_eq!(cap(resent), cap(refused));
    let lowered: Vec<_> = ran
        .records
        .iter()
        .filter_map(|(_, record)| match record {
            Record::OutputCapLowered(lowered) if lowered.step_id == 2 => {
                Some(lowered.max_output_tokens)
            }
            _ => None,
        })
        .collect();
    assert_eq!(lowered.len(), 1, "{lowered:?}");
    assert_eq!(Some(lowered[0]), cap(resent));
}

/// The context limit a compacted run freezes.
const COMPACTED_LIMIT: u64 = 200_000;

/// A plan that names a compaction provider, for a run with `tools`.
fn compacted_manifest(tools: &[broca_protocol::Tool]) -> Arc<broca_wal::PlanManifest> {
    let tools = Arc::new(tools.to_vec());
    Arc::new(broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(
            json!({
                "tool_items": [{"provider": "provider"}],
                "compaction_item": {"provider": "compactor", "preset": "head", "params": {}},
                "composition": {},
                "render_tags": true,
            })
            .to_string(),
        )
        .unwrap(),
        plan_identity: "plan-compacted".into(),
        composition_digest: "composition-1".into(),
        fetches: vec![broca_wal::ProviderFetch {
            provider: "provider".into(),
            outcome: broca_wal::FetchOutcome::Answered {
                catalog_digest: "catalog-1".into(),
                tools: Arc::clone(&tools),
                text: None,
                result_ops: None,
                reply: None,
                declarations_frozen: None,
                reference_slots: None,
            },
        }],
        tools,
        system_text: "You compact.".into(),
        system_digest: "system-1".into(),
        join_version: 1,
        role_versions: [("tool-provider".to_string(), "v1".to_string())].into(),
        declarations: Vec::new(),
    })
}

/// A compaction provider that records every ride and answers it from a script: `nothing`, or
/// a view that shortens the tool output it was told about.
#[derive(Default)]
struct Compactor {
    /// Whether rides are answered with a view.
    view: bool,
    rides: Mutex<Vec<Value>>,
    /// The tool part the notices named last: its entry id, part id and digest. The run has one.
    tool_part: Mutex<Option<(String, String, String)>>,
}

impl Compactor {
    fn remember(&self, value: &Value) {
        match value {
            Value::Object(members) => {
                if let (Some(mid), Some(parts)) = (
                    members.get("mid").and_then(Value::as_str),
                    members.get("parts").and_then(Value::as_array),
                ) {
                    if let Some(tool) = parts.iter().find(|part| part["kind"] == "tool") {
                        // The newest digest wins: a replacement names the completed output,
                        // not the running call an earlier notice described.
                        *self.tool_part.lock().unwrap() = Some((
                            mid.to_owned(),
                            tool["id"].as_str().unwrap().to_owned(),
                            tool["digest"].as_str().unwrap().to_owned(),
                        ));
                    }
                }
                members.values().for_each(|member| self.remember(member));
            }
            Value::Array(items) => items.iter().for_each(|item| self.remember(item)),
            _ => {}
        }
    }
}

#[async_trait::async_trait]
impl broca_core::CompactionPlane for Compactor {
    async fn setup(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: broca_core::compaction_provider::setup::SetupRequest,
    ) -> Result<
        broca_core::compaction_provider::setup::SetupAnswer,
        broca_core::CompactionUnavailable,
    > {
        panic!("a session on the current compaction contract never calls the first protocol")
    }

    async fn provider_capabilities(&self, _: &str) -> Option<Vec<String>> {
        Some(Vec::new())
    }

    async fn call_v2(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        method: &str,
        params: Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, broca_core::compaction_v2::runtime::CallError>
    {
        self.remember(&params);
        let answer = match method {
            "compaction.setup" => json!({"answer": "ready"}),
            "compaction.notice" => json!({"answer": "received"}),
            "compaction.ride" => {
                self.rides.lock().unwrap().push(params);
                let part = self.tool_part.lock().unwrap().clone();
                match part.filter(|_| self.view) {
                    None => json!({"answer": "nothing"}),
                    Some((mid, part, expect)) => {
                        use cortexkit_role_compaction::view::{Edit, EditOp, View};
                        let view = View::seal(
                            1,
                            vec![Edit::new(
                                1,
                                EditOp::Replace {
                                    mid,
                                    part,
                                    field: "output".into(),
                                    value: json!("compacted tool output"),
                                    expect,
                                },
                            )],
                        );
                        json!({"answer": "view", "view": view})
                    }
                }
            }
            other => panic!("unexpected compaction method {other}"),
        };
        Ok(serde_json::value::to_raw_value(&answer).unwrap())
    }
}

/// On a compacted session, the step entered again on the new account rides first, naming the
/// account change, and with no view to apply sends the refused request's bytes.
#[tokio::test]
async fn a_move_on_a_compacted_session_rides_with_account_change() {
    let compactor = Arc::new(Compactor::default());
    let mut case = Case::new(
        vec![("A", vec![Reply::Tool, wall()]), ("B", vec![Reply::Answer])],
        vec![offer("B")],
    );
    case.compaction = Some(Arc::clone(&compactor));
    let ran = run_case(case).await;
    assert_eq!(ran.reason(), RunFinishReason::Completed, "{:#?}", ran.log);
    let rides = compactor.rides.lock().unwrap().clone();
    assert_eq!(
        rides.len(),
        1,
        "one ride, before the re-sent request: {rides:#?}"
    );
    assert_eq!(
        rides[0]["causes"],
        json!(["account_change"]),
        "{:#}",
        rides[0]
    );
    let (refused, resent) = (ran.sent_on("A")[1], ran.sent_on("B")[0]);
    assert_eq!(rendered(&refused.opts), rendered(&resent.opts));
    assert!(!ran
        .records
        .iter()
        .any(|(_, record)| matches!(record, Record::ViewApplied(_))));
}

/// On a compacted session whose provider answers the move's ride with a newer view, the re-sent
/// request renders under it, and the step records the view it applied.
#[tokio::test]
async fn a_move_applies_the_view_its_ride_brings_to_the_re_sent_request() {
    let compactor = Arc::new(Compactor {
        view: true,
        ..Compactor::default()
    });
    let mut case = Case::new(
        vec![("A", vec![Reply::Tool, wall()]), ("B", vec![Reply::Answer])],
        vec![offer("B")],
    );
    case.compaction = Some(Arc::clone(&compactor));
    let ran = run_case(case).await;
    assert_eq!(ran.reason(), RunFinishReason::Completed, "{:#?}", ran.log);
    let rides = compactor.rides.lock().unwrap().clone();
    assert_eq!(rides.len(), 1, "{rides:#?}");
    assert_eq!(rides[0]["causes"], json!(["account_change"]));
    let applied: Vec<_> = ran
        .records
        .iter()
        .filter_map(|(_, record)| match record {
            Record::ViewApplied(applied) => Some((applied.step_id, applied.version)),
            _ => None,
        })
        .collect();
    assert_eq!(
        applied,
        vec![(2, 1)],
        "tool part {:?}\nride {:#}\nlog {:#?}",
        compactor.tool_part.lock().unwrap(),
        rides[0],
        ran.log
    );
    let (refused, resent) = (ran.sent_on("A")[1], ran.sent_on("B")[0]);
    let text = |sent: &Sent| String::from_utf8(rendered(&sent.opts)).unwrap();
    // The step-1 tool result the view replaces: what the refused request carried, and what the
    // re-sent one no longer does.
    let original = "not in this run's capability manifest";
    assert!(text(refused).contains(original), "{}", text(refused));
    assert!(!text(refused).contains("compacted tool output"));
    assert!(!text(resent).contains(original), "{}", text(resent));
    assert!(text(resent).contains("compacted tool output"));
}

/// The run of the first test, on `session`, as a session's store actor runs it: a wall on A at
/// step 2, routing offering B with a persona to refreeze, step 2 answered on B. Its outcome and
/// the requests it sent.
async fn moving_run(
    session: &mut Session<&MemoryBackend>,
    parts: &mut MemoryParts,
) -> (
    Result<broca_session_loop::Finished, broca_session_loop::LoopError>,
    Vec<Sent>,
) {
    let sent = Arc::new(Mutex::new(Vec::new()));
    let log: Log = Arc::default();
    let provider: Box<dyn Provider> =
        Box::new(Account::new("A", vec![Reply::Tool, wall()], &sent, &log));
    let routing = moving_routing(&sent, &log);
    let a = routed("A", "decision-A");
    let mut request = request();
    request.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    let control = broca_core::RunControl {
        cancel: Some(Default::default()),
        auth_selection: Some(a.clone()),
        ..Default::default()
    };
    let now = || NOW;
    let admission = broca_session_loop::admit::admission(
        &mut request,
        &control,
        &provider,
        Writer::operations(&*session),
        Some(now()),
    )
    .unwrap();
    let tools = StubToolPlane::new().with_canned_text("read", "fn main() {}");
    let seams = seams(
        &provider,
        &tools,
        &now,
        Some(&routing),
        &log,
        a,
        Default::default(),
        Default::default(),
    );
    let finished = broca_session_loop::run::run(session, parts, admission, &seams).await;
    drop(seams);
    let sent = sent.lock().unwrap().clone();
    (finished, sent)
}

/// Routing that offers B, whose provider answers the step it is given.
fn moving_routing(sent: &Arc<Mutex<Vec<Sent>>>, log: &Log) -> Routing {
    Routing {
        answers: Mutex::new(
            vec![Scripted::Offer {
                account: "B",
                valid_until_ms: NOW + OFFER_VALID_MS,
                persona: true,
            }]
            .into(),
        ),
        providers: Mutex::new(HashMap::from([(
            "B".to_owned(),
            Box::new(Account::new("B", vec![Reply::Answer], sent, log)) as Box<dyn Provider>,
        )])),
        asked: Default::default(),
        adopted: Default::default(),
    }
}

/// A crash at every point of the move, then a restart: the store reopened from what it holds,
/// and the run resumed through the real loop.
///
/// This lives here rather than in broca-crash-harness because the move exists only on the
/// session store, whose appends are atomic: a crash falls between appends (the store fails
/// every write after the `n`th), never inside the batch that holds the config and account
/// refreezes. The crash harness drives only the older storage's log.
///
/// At every crash point: no attempt names B unless the record moving the run to B precedes it;
/// the config and account refreezes are both there or both absent; the run's fold names the
/// account of the newest refreeze; and a resume renders the run's request from the log, on the
/// account and with the persona `run_config_of_run` reads, the same bytes the run sends without
/// a crash, with no drift refusal.
#[tokio::test]
async fn a_crash_at_every_point_of_a_move_resumes_on_what_the_log_holds() {
    // The step-2 request the run sends without a crash, on either account.
    let reference = {
        let backend = MemoryBackend::default();
        let mut session = session(&backend).await;
        let mut parts = MemoryParts::default();
        let (finished, sent) = moving_run(&mut session, &mut parts).await;
        assert_eq!(finished.unwrap().reason, RunFinishReason::Completed);
        let step2: Vec<_> = sent.iter().filter(|sent| sent.step == 2).collect();
        assert_eq!(step2.len(), 2);
        assert_eq!(rendered(&step2[0].opts), rendered(&step2[1].opts));
        rendered(&step2[0].opts)
    };
    let mut crashes = 0;
    let mut resumed_sends = 0;
    for mutations in 0.. {
        let backend = MemoryBackend::default();
        let mut session = session(&backend).await;
        let mut parts = MemoryParts::default();
        backend.crash_after(Some(mutations));
        let (outcome, _) = moving_run(&mut session, &mut parts).await;
        backend.crash_after(None);
        if outcome.is_ok() {
            break;
        }
        crashes += 1;
        drop(session);
        let mut loaded = Session::load(
            &backend,
            SessionAddr("0123456789abcdef".into()),
            &SessionTriple {
                project_root: "/project".into(),
                harness: "account-move".into(),
                session: "s1".into(),
            },
            &DeploymentKey::new(1, &[7; 32]),
        )
        .await
        .unwrap();
        let context = format!("crash after {mutations} writes");
        let records = loaded.open_log_records().await.unwrap();
        // 1. No attempt on B before the record that moves the run to B.
        let mut moved_to_b = false;
        for (_, record) in &records {
            match record {
                Record::AuthRefrozen(auth) if auth.auth_selection["account_ref"] == "B" => {
                    moved_to_b = true;
                }
                Record::Attempt(attempt) => {
                    let value = serde_json::to_value(attempt).unwrap();
                    if value["attempt"]["served_account_ref"] == "B" {
                        assert!(moved_to_b, "{context}: an attempt on B before the move");
                    }
                }
                _ => {}
            }
        }
        // 2. The config and account refreezes are written together or not at all.
        let count = |is: fn(&Record) -> bool| records.iter().filter(|(_, r)| is(r)).count();
        let configs = count(|record| matches!(record, Record::ConfigRefrozen(_)));
        let auths = count(|record| matches!(record, Record::AuthRefrozen(_)));
        assert_eq!(configs, auths, "{context}");
        let Some(run) = loaded.operations.run.clone() else {
            continue;
        };
        // 3. The fold names the newest refreeze's account.
        let newest = records.iter().rev().find_map(|(_, record)| match record {
            Record::AuthRefrozen(auth) => Some(auth.auth_selection.clone()),
            _ => None,
        });
        assert_eq!(run.refrozen_auth, newest, "{context}");
        if run.finished.is_some() {
            continue;
        }
        // 4. The restart resumes the run on what the log holds.
        let recorded = broca_session_loop::run::run_config_of_run(&run, &parts).unwrap();
        let selection = recorded.auth_selection.clone().unwrap();
        let account = selection.account_ref.clone().unwrap();
        assert_eq!(account, if moved_to_b { "B" } else { "A" }, "{context}");
        let sent = Arc::new(Mutex::new(Vec::new()));
        let log: Log = Arc::default();
        // A answers the steps the log has no answer for yet: step 1's tool call, then the wall.
        let step_one_ended = records
            .iter()
            .any(|(_, record)| matches!(record, Record::StepEnd(end) if end.step_id == 1));
        let replies = if account == "A" && step_one_ended {
            vec![wall()]
        } else if account == "A" {
            vec![Reply::Tool, wall()]
        } else {
            vec![Reply::Answer]
        };
        let mut resumed_on = Account::new(&account, replies, &sent, &log);
        resumed_on.render_config = recorded.frozen_render_config.clone();
        let resumed_on: Box<dyn Provider> = Box::new(resumed_on);
        let routing = moving_routing(&sent, &log);
        let tools = StubToolPlane::new().with_canned_text("read", "fn main() {}");
        let now = || NOW;
        let seams = seams(
            &resumed_on,
            &tools,
            &now,
            Some(&routing),
            &log,
            selection,
            Default::default(),
            Default::default(),
        );
        let finished = broca_session_loop::resume(&mut loaded, &parts, &seams)
            .await
            .unwrap_or_else(|error| panic!("{context}: {error:?}"));
        drop(seams);
        assert_eq!(finished.reason, RunFinishReason::Completed, "{context}");
        let sent = sent.lock().unwrap().clone();
        for request in sent.iter().filter(|sent| sent.step == 2) {
            resumed_sends += 1;
            assert_eq!(rendered(&request.opts), reference, "{context}");
            assert_eq!(
                request
                    .opts
                    .step_context
                    .as_ref()
                    .unwrap()
                    .account_ref
                    .as_deref(),
                Some(request.account.as_str()),
                "{context}"
            );
        }
        let after = loaded.open_log_records().await.unwrap();
        assert!(
            !after
                .iter()
                .any(|(_, record)| matches!(record, Record::RunEnd(end) if end.error.is_some())),
            "{context}: no drift refusal"
        );
    }
    assert!(crashes > 5, "the run was cut at {crashes} points");
    assert!(resumed_sends > 0, "some resume sent step 2 again");
}
/// A run on A pauses for a restart after its first step, then resumes on `account` (routing
/// gave it that one) with a provider rendering `render`. What the resume did: its end, the log
/// and the requests sent.
async fn resume_paused_run_on(
    account: &str,
    render: Value,
) -> (
    Result<broca_session_loop::Finished, broca_session_loop::LoopError>,
    Vec<(u64, Record)>,
    Vec<Sent>,
) {
    let backend = MemoryBackend::default();
    let mut session = session(&backend).await;
    let mut parts = MemoryParts::default();
    let sent = Arc::new(Mutex::new(Vec::new()));
    let log: Log = Arc::default();
    let stop = tokio_util::sync::CancellationToken::new();
    let provider: Box<dyn Provider> = Box::new(Account::new("A", vec![Reply::Tool], &sent, &log));
    let a = routed("A", "decision-A");
    let mut request = request();
    request.on_restart = Some(broca_protocol::request::OnRestart::Resume);
    let control = broca_core::RunControl {
        cancel: Some(Default::default()),
        auth_selection: Some(a.clone()),
        ..Default::default()
    };
    let now = || NOW;
    let admission = broca_session_loop::admit::admission(
        &mut request,
        &control,
        &provider,
        Writer::operations(&session),
        Some(now()),
    )
    .unwrap();
    let tools = StubToolPlane::new().with_canned_text("read", "fn main() {}");
    let first = seams(
        &provider,
        &tools,
        &now,
        None,
        &log,
        a,
        Default::default(),
        stop.clone(),
    );
    let mut writer = Logging {
        session: &mut session,
        log: Arc::clone(&log),
        refuse_moves: false,
        stop_after_batches: Some((1, stop.clone())),
    };
    let paused = broca_session_loop::run::run(&mut writer, &mut parts, admission, &first)
        .await
        .unwrap();
    assert_eq!(
        paused.paused,
        Some(broca_protocol::caller::PauseReason::Restart)
    );
    drop(first);
    let mut resumed_on = Account::new(account, vec![Reply::Answer], &sent, &log);
    resumed_on.render_config = render;
    let resumed_on: Box<dyn Provider> = Box::new(resumed_on);
    let second = seams(
        &resumed_on,
        &tools,
        &now,
        None,
        &log,
        routed(account, &format!("decision-{account}")),
        Default::default(),
        Default::default(),
    );
    let finished = broca_session_loop::resume(&mut writer, &parts, &second).await;
    drop(second);
    let records = session.open_log_records().await.unwrap();
    let sent = sent.lock().unwrap().clone();
    (finished, records, sent)
}

/// A paused run that resumes on another account than it ran on, after provider calls, takes
/// the provider's persona refrozen for that account: the config and the account are refrozen
/// in one batch, and the next request goes out on the new account.
#[tokio::test]
async fn a_resume_on_another_account_refreezes_the_persona_with_it() {
    let (finished, records, sent) = resume_paused_run_on("C", render_config("C")).await;
    assert_eq!(
        finished.unwrap().reason,
        RunFinishReason::Completed,
        "{records:#?}"
    );
    let refreezes: Vec<_> = records
        .iter()
        .filter_map(|(seq, record)| match record {
            Record::ConfigRefrozen(config) => Some((*seq, config.render_config.clone())),
            Record::AuthRefrozen(auth) => Some((*seq, auth.auth_selection["account_ref"].clone())),
            _ => None,
        })
        .collect();
    assert_eq!(refreezes.len(), 2, "{refreezes:#?}");
    assert_eq!(refreezes[0].1, render_config("C"));
    assert_eq!(refreezes[1].1, json!("C"));
    assert_eq!(
        refreezes[0].0 + 1,
        refreezes[1].0,
        "one batch, config first"
    );
    let last = sent.last().unwrap();
    assert_eq!((last.account.as_str(), last.step), ("C", 2));
}

/// Any other change of the render config after a provider call still ends the run, account
/// change or not, and so does a persona change without one.
#[tokio::test]
async fn a_resume_whose_config_changes_beyond_the_persona_still_ends_the_run() {
    let mut beyond = render_config("C");
    beyond["other"] = json!(1);
    for (account, render) in [("C", beyond), ("A", render_config("C"))] {
        let (finished, records, sent) = resume_paused_run_on(account, render).await;
        assert_eq!(finished.unwrap().reason, RunFinishReason::Interrupted);
        assert!(!records
            .iter()
            .any(|(_, record)| matches!(record, Record::ConfigRefrozen(_))));
        assert_eq!(sent.len(), 1, "only the first step was sent");
    }
}
